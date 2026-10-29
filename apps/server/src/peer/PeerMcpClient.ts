import {
  type EnvironmentId,
  OrchestratorMcpFailure,
  type PeerLinkError,
  type ProviderInteractionMode,
  type RuntimeMode,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpBody from "effect/http/HttpBody";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import type * as HttpClientResponse from "effect/http/HttpClientResponse";

import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import * as PeerLinks from "./PeerLinks.ts";

/** The only MCP protocol version a T3 Code `/mcp` registers. */
const PROTOCOL_VERSION = "2025-06-18";

/** The limits a call carries to the peer: the calling agent's modes here. */
export interface PeerCallLimits {
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: ProviderInteractionMode;
}

/**
 * Calls a linked environment's T3 MCP tools as this environment's outside
 * agent. Each call carries the calling agent's limits in `T3-Mode-Limit`, so
 * the peer enforces the narrower of those and the link's own access. A peer
 * HTTP 401 marks that link generation revoked until a new link replaces it.
 */
export class PeerMcpClient extends Context.Service<
  PeerMcpClient,
  {
    /** Calls `tool` on the peer and decodes its structured result with `success`. */
    readonly call: <S extends Schema.Decoder<unknown>>(input: {
      readonly environmentId: EnvironmentId;
      readonly tool: string;
      readonly arguments: Record<string, unknown>;
      readonly limits: PeerCallLimits;
      readonly success: S;
    }) => Effect.Effect<S["Type"], OrchestratorMcpFailure>;
  }
>()("t3/peer/PeerMcpClient") {}

const ToolCallResponse = Schema.Struct({
  result: Schema.optional(
    Schema.Struct({
      content: Schema.Array(
        Schema.Struct({ type: Schema.String, text: Schema.optional(Schema.String) }),
      ),
      structuredContent: Schema.optional(Schema.Unknown),
      isError: Schema.optional(Schema.Boolean),
    }),
  ),
  error: Schema.optional(Schema.Struct({ code: Schema.Number, message: Schema.String })),
});
const decodeToolCallResponse = Schema.decodeUnknownEffect(Schema.fromJsonString(ToolCallResponse));
const decodeFailure = Schema.decodeUnknownOption(
  Schema.fromJsonString(
    Schema.Struct({ code: OrchestratorMcpFailure.fields.code, message: Schema.String }),
  ),
);
const LINK_REVOKED_MESSAGE =
  "The linked environment no longer accepts this link. It may have been revoked there; link it again.";

class PeerMcpUnauthorized extends Error {
  readonly _tag = "PeerMcpUnauthorized" as const;
}

const isPeerMcpUnauthorized = (error: unknown): error is PeerMcpUnauthorized =>
  error instanceof PeerMcpUnauthorized;

const revokedLinkFailure = () =>
  new OrchestratorMcpFailure({ code: "capability_denied", message: LINK_REVOKED_MESSAGE });

/** A peer link problem, as the agent that asked hears it. */
const fromLinkError = (error: PeerLinkError) =>
  new OrchestratorMcpFailure({
    code:
      error.reason === "unknown_link"
        ? "invalid_request"
        : error.reason === "expired" ||
            error.reason === "revoked" ||
            error.reason === "incompatible"
          ? "capability_denied"
          : "orchestration_error",
    message: error.message,
  });

/** Not final, so a task already running there resumes once the beta is on there again. */
const linkedEnvironmentsOffThere = new OrchestratorMcpFailure({
  code: "orchestration_error",
  message:
    "The linked environment has Linked environments turned off. Turn it on in its Settings → Labs.",
});

/** The body of an MCP response: JSON, or a single server-sent event carrying JSON. */
const responseJson = (contentType: string | undefined, text: string) =>
  contentType?.includes("text/event-stream") === true
    ? text
        .split("\n")
        .filter((line) => line.startsWith("data:"))
        .map((line) => line.slice("data:".length).trim())
        .join("")
    : text;

const make = Effect.gen(function* () {
  const links = yield* PeerLinks.PeerLinks;
  const httpClient = yield* HttpClient.HttpClient;
  const peerHttpClient = HttpClient.transform(httpClient, (response) =>
    Effect.provideService(response, FetchHttpClient.RequestInit, { redirect: "manual" }),
  );
  type Session = {
    environmentId: EnvironmentId;
    url: string;
    token: string;
    sessionId: string;
  };
  type SessionState = {
    readonly generations: ReadonlyMap<EnvironmentId, number>;
    readonly sessions: ReadonlyMap<string, Session>;
  };
  type PublishResult =
    | { readonly _tag: "stale" }
    | { readonly _tag: "duplicate"; readonly retained: Session }
    | { readonly _tag: "published"; readonly retained: Session };
  // One MCP session per peer and address, reused across calls. The peer keeps
  // sessions in memory, so a restart there answers 404 and we start a new one.
  const sessionState = yield* Ref.make<SessionState>({
    generations: new Map(),
    sessions: new Map(),
  });

  const post = (url: string, token: string, body: unknown, headers: Record<string, string>) =>
    peerHttpClient.execute(
      HttpClientRequest.post(`${url}/mcp`).pipe(
        HttpClientRequest.setHeaders({
          accept: "application/json, text/event-stream",
          authorization: `Bearer ${token}`,
          ...headers,
        }),
        HttpClientRequest.setBody(HttpBody.text(JSON.stringify(body), "application/json")),
      ),
    );

  const drainResponse = (response: HttpClientResponse.HttpClientResponse) =>
    response.text.pipe(Effect.timeout(Duration.seconds(3)), Effect.ignore);
  const readResponse = (response: HttpClientResponse.HttpClientResponse) =>
    response.text.pipe(
      Effect.timeout(Duration.seconds(3)),
      Effect.mapError(
        () =>
          new OrchestratorMcpFailure({
            code: "orchestration_error",
            message: "The linked environment stopped answering.",
          }),
      ),
    );

  const closeSession = (session: Session) =>
    Effect.uninterruptibleMask(() =>
      Effect.interruptible(
        peerHttpClient
          .execute(
            HttpClientRequest.make("DELETE")(`${session.url}/mcp`).pipe(
              HttpClientRequest.setHeaders({
                accept: "application/json, text/event-stream",
                authorization: `Bearer ${session.token}`,
                "mcp-session-id": session.sessionId,
                "mcp-protocol-version": PROTOCOL_VERSION,
              }),
            ),
          )
          .pipe(Effect.flatMap((response) => response.text)),
      ).pipe(Effect.timeout(Duration.seconds(3)), Effect.ignore),
    );

  const clearSessions = Effect.fn("PeerMcpClient.clearSessions")(function* (
    environmentId: EnvironmentId,
  ) {
    const current = yield* Ref.modify(sessionState, (state) => [
      Array.from(state.sessions.values()).filter(
        (session) => session.environmentId === environmentId,
      ),
      {
        generations: new Map(state.generations).set(
          environmentId,
          (state.generations.get(environmentId) ?? 0) + 1,
        ),
        sessions: new Map(
          Array.from(state.sessions).filter(
            ([, session]) => session.environmentId !== environmentId,
          ),
        ),
      },
    ]);
    yield* Effect.forEach(current, closeSession, { concurrency: "unbounded", discard: true });
  });

  const recordRevocation = (environmentId: EnvironmentId, secretName: string) =>
    links.recordOutcome(environmentId, secretName, {
      error: LINK_REVOKED_MESSAGE,
      revoked: true,
    });

  yield* links.subscribeRefreshes.pipe(
    Stream.runForEach((refresh) => clearSessions(refresh.environmentId)),
    Effect.forkScoped({ startImmediately: true }),
  );

  const openSession = Effect.fn("PeerMcpClient.openSession")(function* (
    environmentId: EnvironmentId,
    url: string,
    token: string,
    secretName: string,
    limitHeader: Record<string, string>,
  ) {
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const initialized = yield* restore(
          post(
            url,
            token,
            {
              jsonrpc: "2.0",
              id: 0,
              method: "initialize",
              params: {
                protocolVersion: PROTOCOL_VERSION,
                capabilities: {},
                clientInfo: { name: "t3code-peer", version: "1" },
              },
            },
            limitHeader,
          ),
        );
        const sessionId = initialized.headers["mcp-session-id"];
        if (initialized.status !== 200) {
          if (initialized.status === 401) {
            yield* recordRevocation(environmentId, secretName);
            return yield* Effect.fail(new PeerMcpUnauthorized());
          }
          yield* restore(drainResponse(initialized));
          if (initialized.status === 403) return yield* linkedEnvironmentsOffThere;
          return yield* new OrchestratorMcpFailure({
            code: "orchestration_error",
            message: `The linked environment refused to start an MCP session (HTTP ${initialized.status}).`,
          });
        }
        if (sessionId === undefined) {
          yield* restore(drainResponse(initialized));
          return yield* new OrchestratorMcpFailure({
            code: "orchestration_error",
            message: "The linked environment did not return an MCP session.",
          });
        }
        const session = { environmentId, url, token, sessionId };
        return yield* Effect.gen(function* () {
          yield* restore(readResponse(initialized).pipe(Effect.asVoid));
          const notification = yield* restore(
            post(
              url,
              token,
              { jsonrpc: "2.0", method: "notifications/initialized" },
              {
                ...limitHeader,
                "mcp-session-id": sessionId,
                "mcp-protocol-version": PROTOCOL_VERSION,
              },
            ),
          );
          if (notification.status < 200 || notification.status >= 300) {
            if (notification.status === 401) {
              yield* recordRevocation(environmentId, secretName);
              yield* restore(drainResponse(notification));
              return yield* Effect.fail(new PeerMcpUnauthorized());
            }
            yield* restore(drainResponse(notification));
            if (notification.status === 403) return yield* linkedEnvironmentsOffThere;
            return yield* new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: `The linked environment refused to initialize the MCP session (HTTP ${notification.status}).`,
            });
          }
          yield* restore(readResponse(notification).pipe(Effect.asVoid));
          return session;
        }).pipe(
          Effect.onExit((exit) => (Exit.isFailure(exit) ? closeSession(session) : Effect.void)),
        );
      }),
    );
  });

  const openAndPublish = Effect.fn("PeerMcpClient.openAndPublish")(function* (
    environmentId: EnvironmentId,
    url: string,
    token: string,
    secretName: string,
    limitHeader: Record<string, string>,
    sessionKey: string,
    generation: number,
    staleMessage: string,
  ) {
    // Once openSession has acquired a remote session, do not allow cancellation
    // between that acquisition and publishing or closing it.
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const opened = yield* restore(
          openSession(environmentId, url, token, secretName, limitHeader),
        );
        const published = yield* Ref.modify(
          sessionState,
          (current): readonly [PublishResult, SessionState] => {
            if ((current.generations.get(environmentId) ?? 0) !== generation) {
              return [{ _tag: "stale" as const }, current];
            }
            const retained = current.sessions.get(sessionKey);
            if (retained !== undefined) return [{ _tag: "duplicate" as const, retained }, current];
            return [
              { _tag: "published" as const, retained: opened },
              {
                generations: current.generations,
                sessions: new Map(current.sessions).set(sessionKey, opened),
              },
            ];
          },
        );
        if (published._tag === "stale") {
          yield* closeSession(opened);
          return yield* new OrchestratorMcpFailure({
            code: "orchestration_error",
            message: staleMessage,
          });
        }
        if (published._tag === "duplicate") yield* closeSession(opened);
        return published.retained;
      }),
    );
  });

  const call: PeerMcpClient["Service"]["call"] = Effect.fn("PeerMcpClient.call")(function* (input) {
    const resolved = yield* links.resolve(input.environmentId).pipe(Effect.mapError(fromLinkError));
    const limitHeader = {
      [McpInvocationContext.MODE_LIMIT_HEADER]: `${input.limits.runtimeMode}/${input.limits.interactionMode}`,
    };
    const sessionKey = `${input.environmentId} ${resolved.url} ${resolved.secretName}`;
    const callOnce = (sessionId: string) =>
      post(
        resolved.url,
        resolved.token,
        {
          jsonrpc: "2.0",
          id: 1,
          method: "tools/call",
          params: { name: input.tool, arguments: input.arguments },
        },
        { ...limitHeader, "mcp-session-id": sessionId, "mcp-protocol-version": PROTOCOL_VERSION },
      );
    return yield* Effect.gen(function* () {
      const callEffect = Effect.gen(function* () {
        const state = yield* Ref.get(sessionState);
        const generation = state.generations.get(input.environmentId) ?? 0;
        const existing = state.sessions.get(sessionKey);
        let activeSession = existing;
        if (existing === undefined) {
          activeSession = yield* openAndPublish(
            input.environmentId,
            resolved.url,
            resolved.token,
            resolved.secretName,
            limitHeader,
            sessionKey,
            generation,
            "The linked environment changed while its MCP session was opening.",
          );
        }
        let activeSessionId =
          activeSession?.sessionId ??
          (yield* Ref.get(sessionState)).sessions.get(sessionKey)?.sessionId;
        if (activeSessionId === undefined) {
          return yield* new OrchestratorMcpFailure({
            code: "orchestration_error",
            message: "The linked environment has no usable MCP session.",
          });
        }
        let response = yield* callOnce(activeSessionId);
        if (response.status === 404) {
          yield* drainResponse(response);
          const failedSessionId = activeSessionId;
          yield* Ref.update(sessionState, (current) => {
            const retained = current.sessions.get(sessionKey);
            if (retained?.sessionId !== failedSessionId) return current;
            const sessions = new Map(current.sessions);
            sessions.delete(sessionKey);
            return { generations: current.generations, sessions };
          });
          const renewed = yield* openAndPublish(
            input.environmentId,
            resolved.url,
            resolved.token,
            resolved.secretName,
            limitHeader,
            sessionKey,
            generation,
            "The linked environment changed while its MCP session was reopening.",
          );
          activeSessionId = renewed.sessionId;
          response = yield* callOnce(activeSessionId);
        }
        if (response.status === 401) {
          return yield* Effect.uninterruptibleMask((restore) =>
            Effect.gen(function* () {
              yield* recordRevocation(input.environmentId, resolved.secretName);
              const closeActiveSession = Effect.gen(function* () {
                const session = yield* Ref.modify(sessionState, (current) => {
                  const session = current.sessions.get(sessionKey);
                  if (session?.sessionId !== activeSessionId) return [undefined, current];
                  const next = new Map(current.sessions);
                  next.delete(sessionKey);
                  return [session, { generations: current.generations, sessions: next }];
                });
                if (session !== undefined) yield* closeSession(session);
              });
              yield* restore(drainResponse(response)).pipe(Effect.ensuring(closeActiveSession));
              return yield* Effect.fail(new PeerMcpUnauthorized());
            }),
          );
        }
        if (response.status === 403) {
          yield* drainResponse(response);
          return yield* linkedEnvironmentsOffThere;
        }
        if (response.status !== 200) {
          yield* drainResponse(response);
          return yield* new OrchestratorMcpFailure({
            code: "orchestration_error",
            message: `The linked environment answered HTTP ${response.status}.`,
          });
        }
        const body = yield* decodeToolCallResponse(
          responseJson(response.headers["content-type"], yield* response.text),
        ).pipe(
          Effect.mapError(
            () =>
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "The linked environment answered in an unexpected shape.",
              }),
          ),
        );
        if (body.error !== undefined || body.result === undefined) {
          return yield* new OrchestratorMcpFailure({
            code: "invalid_request",
            message: body.error?.message ?? "The linked environment returned no result.",
          });
        }
        const text = body.result.content.map((part) => part.text ?? "").join("");
        if (body.result.isError === true) {
          // A refusal keeps the peer's own code, so the agent sees why it was refused.
          const refused = decodeFailure(text);
          return yield* refused._tag === "Some"
            ? new OrchestratorMcpFailure({
                code: refused.value.code,
                message: refused.value.message,
              })
            : new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: text || "The linked environment refused the call.",
              });
        }
        const success = Schema.fromJsonString(input.success);
        return yield* Schema.decodeEffect(success)(text).pipe(
          Effect.mapError(
            () =>
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message:
                  "The linked environment's answer did not match this version. Update both environments.",
              }),
          ),
        );
      });
      return yield* callEffect.pipe(
        Effect.tapError((error) =>
          links.recordOutcome(input.environmentId, resolved.secretName, {
            error: isPeerMcpUnauthorized(error)
              ? LINK_REVOKED_MESSAGE
              : error instanceof Error
                ? error.message
                : "The linked environment rejected the call.",
            ...(isPeerMcpUnauthorized(error) ? { revoked: true } : {}),
          }),
        ),
        Effect.tap(() =>
          links.recordOutcome(input.environmentId, resolved.secretName, { error: null }),
        ),
        Effect.catchIf(isPeerMcpUnauthorized, () => Effect.fail(revokedLinkFailure())),
        Effect.catchTags({
          HttpClientError: () =>
            Effect.fail(
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "The linked environment stopped answering.",
              }),
            ),
        }),
      );
    });
  });

  return PeerMcpClient.of({ call });
});

export const layer = Layer.effect(PeerMcpClient, make);
