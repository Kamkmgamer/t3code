import {
  type EnvironmentId,
  OrchestratorMcpFailure,
  type PeerLinkError,
  type ProviderInteractionMode,
  type RuntimeMode,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
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
import { withManagedResponse } from "./PeerHttpResponse.ts";

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
    secretName: string;
  };
  type Opening = {
    readonly environmentId: EnvironmentId;
    readonly secretName: string;
    readonly generation: number;
    readonly count: number;
  };
  type SessionState = {
    readonly openings: ReadonlyMap<string, Opening>;
    readonly sessions: ReadonlyMap<string, Session>;
  };
  type PublishResult =
    | { readonly _tag: "stale" }
    | { readonly _tag: "duplicate"; readonly retained: Session }
    | { readonly _tag: "published"; readonly retained: Session };
  // One MCP session per peer and address, reused across calls. The peer keeps
  // sessions in memory, so a restart there answers 404 and we start a new one.
  const sessionState = yield* Ref.make<SessionState>({
    openings: new Map(),
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

  const withResponse = <A, E, R, E2, R2>(
    acquire: Effect.Effect<HttpClientResponse.HttpClientResponse, E, R>,
    use: (
      response: HttpClientResponse.HttpClientResponse,
      restore: <B, E3, R3>(effect: Effect.Effect<B, E3, R3>) => Effect.Effect<B, E3, R3>,
    ) => Effect.Effect<A, E2, R2>,
    acquireTimeout = Duration.seconds(10),
  ) =>
    withManagedResponse(acquire, use, {
      acquireTimeout,
      releaseTimeout: Duration.seconds(3),
      onAcquireTimeout: () =>
        new OrchestratorMcpFailure({
          code: "orchestration_error",
          message: "The linked environment stopped answering.",
        }),
    });
  const readResponse = (response: HttpClientResponse.HttpClientResponse) =>
    response.stream.pipe(
      Stream.decodeText(),
      Stream.mkString,
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
        withResponse(
          peerHttpClient.execute(
            HttpClientRequest.make("DELETE")(`${session.url}/mcp`).pipe(
              HttpClientRequest.setHeaders({
                accept: "application/json, text/event-stream",
                authorization: `Bearer ${session.token}`,
                "mcp-session-id": session.sessionId,
                "mcp-protocol-version": PROTOCOL_VERSION,
              }),
            ),
          ),
          () => Effect.void,
          Duration.seconds(3),
        ),
      ).pipe(Effect.timeout(Duration.seconds(3)), Effect.ignore),
    );

  const reconcileSessions = Effect.fn("PeerMcpClient.reconcileSessions")(function* () {
    const generations = yield* links.currentGenerations;
    const generationByEnvironment = new Map(
      generations.map((generation) => [generation.environmentId, generation]),
    );
    const now = yield* DateTime.now;
    const isCurrent = (environmentId: EnvironmentId, secretName: string) => {
      const generation = generationByEnvironment.get(environmentId);
      return (
        generation !== undefined &&
        generation.secretName === secretName &&
        !generation.revoked &&
        !DateTime.isLessThanOrEqualTo(generation.expiresAt, now)
      );
    };
    const stale = yield* Ref.modify(sessionState, (current) => {
      const staleSessions = Array.from(current.sessions.values()).filter(
        (session) => !isCurrent(session.environmentId, session.secretName),
      );
      const sessions = new Map(current.sessions);
      for (const [key, session] of sessions) {
        if (!isCurrent(session.environmentId, session.secretName)) sessions.delete(key);
      }
      const openings = new Map(current.openings);
      for (const [key, opening] of openings) {
        if (!isCurrent(opening.environmentId, opening.secretName)) {
          openings.set(key, { ...opening, generation: opening.generation + 1 });
        }
      }
      return [staleSessions, { openings, sessions }];
    });
    yield* Effect.forEach(stale, closeSession, { concurrency: "unbounded", discard: true });
  });

  const recordRevocation = (environmentId: EnvironmentId, secretName: string) =>
    links
      .recordOutcome(environmentId, secretName, {
        error: LINK_REVOKED_MESSAGE,
        revoked: true,
      })
      .pipe(Effect.mapError(fromLinkError));

  yield* links.subscribeRefreshes.pipe(
    Stream.runForEach(() =>
      reconcileSessions().pipe(
        Effect.catchTags({
          PeerLinkError: (error) =>
            Effect.logWarning("Could not reconcile linked-environment sessions", error),
        }),
      ),
    ),
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
        let initializedSession: Session | undefined;
        const session = yield* restore(
          withResponse(
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
            (initialized, restoreResponse) =>
              Effect.gen(function* () {
                if (initialized.status !== 200) {
                  if (initialized.status === 401) {
                    yield* recordRevocation(environmentId, secretName);
                    return yield* Effect.fail(new PeerMcpUnauthorized());
                  }
                  if (initialized.status === 403) return yield* linkedEnvironmentsOffThere;
                  return yield* new OrchestratorMcpFailure({
                    code: "orchestration_error",
                    message: `The linked environment refused to start an MCP session (HTTP ${initialized.status}).`,
                  });
                }
                const sessionId = initialized.headers["mcp-session-id"];
                if (sessionId === undefined) {
                  return yield* new OrchestratorMcpFailure({
                    code: "orchestration_error",
                    message: "The linked environment did not return an MCP session.",
                  });
                }
                initializedSession = { environmentId, url, token, sessionId, secretName };
                yield* restoreResponse(readResponse(initialized).pipe(Effect.asVoid));
                return initializedSession;
              }),
          ),
        ).pipe(
          Effect.onExit((exit) =>
            Exit.isFailure(exit) && initializedSession !== undefined
              ? closeSession(initializedSession)
              : Effect.void,
          ),
        );
        const sessionId = session.sessionId;
        return yield* Effect.gen(function* () {
          yield* restore(
            withResponse(
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
              (notification, restoreResponse) =>
                Effect.gen(function* () {
                  if (notification.status < 200 || notification.status >= 300) {
                    if (notification.status === 401) {
                      yield* recordRevocation(environmentId, secretName);
                      return yield* Effect.fail(new PeerMcpUnauthorized());
                    }
                    if (notification.status === 403) return yield* linkedEnvironmentsOffThere;
                    return yield* new OrchestratorMcpFailure({
                      code: "orchestration_error",
                      message: `The linked environment refused to initialize the MCP session (HTTP ${notification.status}).`,
                    });
                  }
                  yield* restoreResponse(readResponse(notification).pipe(Effect.asVoid));
                }),
            ),
          );
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
    staleMessage: string,
  ) {
    // Once openSession has acquired a remote session, do not allow cancellation
    // between that acquisition and publishing or closing it.
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const generation = yield* Ref.modify(sessionState, (current) => {
          const opening = current.openings.get(sessionKey) ?? {
            environmentId,
            secretName,
            generation: 0,
            count: 0,
          };
          const openings = new Map(current.openings);
          openings.set(sessionKey, { ...opening, count: opening.count + 1 });
          return [opening.generation, { ...current, openings }];
        });
        return yield* Effect.ensuring(
          Effect.gen(function* () {
            const opened = yield* restore(
              openSession(environmentId, url, token, secretName, limitHeader),
            );
            let retained = false;
            return yield* Effect.ensuring(
              Effect.gen(function* () {
                if (
                  !(yield* links
                    .isCurrentGeneration(environmentId, secretName)
                    .pipe(Effect.mapError(fromLinkError)))
                ) {
                  return yield* new OrchestratorMcpFailure({
                    code: "orchestration_error",
                    message: staleMessage,
                  });
                }
                const published = yield* Ref.modify(
                  sessionState,
                  (current): readonly [PublishResult, SessionState] => {
                    if (current.openings.get(sessionKey)?.generation !== generation) {
                      return [{ _tag: "stale" as const }, current];
                    }
                    const currentSession = current.sessions.get(sessionKey);
                    if (currentSession !== undefined) {
                      return [{ _tag: "duplicate" as const, retained: currentSession }, current];
                    }
                    return [
                      { _tag: "published" as const, retained: opened },
                      {
                        openings: current.openings,
                        sessions: new Map(current.sessions).set(sessionKey, opened),
                      },
                    ];
                  },
                );
                if (published._tag === "stale") {
                  return yield* new OrchestratorMcpFailure({
                    code: "orchestration_error",
                    message: staleMessage,
                  });
                }
                if (published._tag === "duplicate") {
                  yield* closeSession(opened);
                  retained = true;
                  return published.retained;
                }
                retained = true;
                return published.retained;
              }),
              Effect.suspend(() => (retained ? Effect.void : closeSession(opened))),
            );
          }),
          Ref.update(sessionState, (current) => {
            const opening = current.openings.get(sessionKey);
            if (opening === undefined) return current;
            const openings = new Map(current.openings);
            if (opening.count === 1) openings.delete(sessionKey);
            else openings.set(sessionKey, { ...opening, count: opening.count - 1 });
            return { ...current, openings };
          }),
        );
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
        const send = (sessionId: string, recoverMissingSession: boolean) => {
          let unauthorized = false;
          const closeActiveSession = Effect.gen(function* () {
            const session = yield* Ref.modify(sessionState, (current) => {
              const session = current.sessions.get(sessionKey);
              if (session?.sessionId !== sessionId) return [undefined, current];
              const next = new Map(current.sessions);
              next.delete(sessionKey);
              return [session, { ...current, sessions: next }];
            });
            if (session !== undefined) yield* closeSession(session);
          });
          return withResponse(callOnce(sessionId), (response, restoreResponse) =>
            Effect.gen(function* () {
              if (response.status === 404 && recoverMissingSession) {
                return { _tag: "missing-session" as const };
              }
              if (response.status === 401) {
                unauthorized = true;
                yield* recordRevocation(input.environmentId, resolved.secretName);
                return yield* Effect.fail(new PeerMcpUnauthorized());
              }
              if (response.status === 403) return yield* linkedEnvironmentsOffThere;
              if (response.status !== 200) {
                return yield* new OrchestratorMcpFailure({
                  code: "orchestration_error",
                  message: `The linked environment answered HTTP ${response.status}.`,
                });
              }
              const body = yield* decodeToolCallResponse(
                responseJson(
                  response.headers["content-type"],
                  yield* restoreResponse(readResponse(response)),
                ),
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
              const value = yield* Schema.decodeEffect(success)(text).pipe(
                Effect.mapError(
                  () =>
                    new OrchestratorMcpFailure({
                      code: "orchestration_error",
                      message:
                        "The linked environment's answer did not match this version. Update both environments.",
                    }),
                ),
              );
              return { _tag: "success" as const, value };
            }),
          ).pipe(Effect.onExit(() => (unauthorized ? closeActiveSession : Effect.void)));
        };

        const first = yield* send(activeSessionId, true);
        if (first._tag === "success") return first.value;
        yield* Ref.update(sessionState, (current) => {
          const retained = current.sessions.get(sessionKey);
          if (retained?.sessionId !== activeSessionId) return current;
          const sessions = new Map(current.sessions);
          sessions.delete(sessionKey);
          return { ...current, sessions };
        });
        const renewed = yield* openAndPublish(
          input.environmentId,
          resolved.url,
          resolved.token,
          resolved.secretName,
          limitHeader,
          sessionKey,
          "The linked environment changed while its MCP session was reopening.",
        );
        const retried = yield* send(renewed.sessionId, false);
        return retried._tag === "success"
          ? retried.value
          : yield* new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "The linked environment answered HTTP 404.",
            });
      });
      return yield* callEffect.pipe(
        Effect.tapError((error) =>
          links
            .recordOutcome(input.environmentId, resolved.secretName, {
              error: isPeerMcpUnauthorized(error)
                ? LINK_REVOKED_MESSAGE
                : error instanceof Error
                  ? error.message
                  : "The linked environment rejected the call.",
              ...(isPeerMcpUnauthorized(error) ? { revoked: true } : {}),
            })
            .pipe(Effect.mapError(fromLinkError)),
        ),
        Effect.tap(() =>
          links
            .recordOutcome(input.environmentId, resolved.secretName, { error: null })
            .pipe(Effect.mapError(fromLinkError)),
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
