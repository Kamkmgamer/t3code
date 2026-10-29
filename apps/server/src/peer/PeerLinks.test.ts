import { NodeHttpServer } from "@effect/platform-node";
import {
  type ExecutionEnvironmentDescriptor,
  type PeerLinkRefresh,
  OrchestratorMcpFailure,
} from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as HttpClientResponse from "effect/http/HttpClientResponse";
import * as HttpClientError from "effect/http/HttpClientError";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import { Tool, Toolkit } from "effect/ai";
import {
  FetchHttpClient,
  HttpClient,
  HttpServer,
  HttpServerRequest,
  HttpServerResponse,
} from "effect/http";

import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import * as McpToolAccess from "../mcp/McpToolAccess.ts";
import * as PeerLinks from "./PeerLinks.ts";
import * as ServerSettings from "../serverSettings.ts";
import {
  descriptorOf,
  layerLinkingEnvironment,
  linkTo,
  type ProbeTracker,
  servePeer,
  type ServedPeer,
  layerHttpClientResponseTransform,
} from "./PeerLinks.testkit.ts";
import * as PeerMcpClient from "./PeerMcpClient.ts";

/** What B's probe saw: the calling client and the modes it may start work with. */
const Probe = Schema.Struct({ caller: Schema.String, modes: Schema.String });

const ProbeToolkit = Toolkit.make(
  Tool.make("probe", {
    description: "Reports the calling client and the modes it may start work with.",
    success: Probe,
    failure: OrchestratorMcpFailure,
    failureMode: "return",
    dependencies: [McpInvocationContext.McpInvocationContext],
  }),
  Tool.make("refuse", {
    description: "Always refuses.",
    success: Probe,
    failure: OrchestratorMcpFailure,
    failureMode: "return",
  }),
  Tool.make("looks-revoked", {
    description: "Returns an ordinary tool failure with the link-revoked wording.",
    success: Probe,
    failure: OrchestratorMcpFailure,
    failureMode: "return",
  }),
);

const probeHandlers = McpToolAccess.toLayer(ProbeToolkit, {
  probe: McpToolAccess.reads(() =>
    McpInvocationContext.McpInvocationContext.pipe(
      Effect.map((scope) => {
        const modes = McpInvocationContext.clientModeCeiling(scope.client);
        return {
          caller: scope.client?.label ?? "none",
          modes: `${modes.runtimeMode}/${modes.interactionMode}`,
        };
      }),
    ),
  ),
  refuse: McpToolAccess.reads(() =>
    Effect.fail(
      new OrchestratorMcpFailure({ code: "thread_not_found", message: "No such thread." }),
    ),
  ),
  "looks-revoked": McpToolAccess.reads(() =>
    Effect.fail(
      new OrchestratorMcpFailure({
        code: "capability_denied",
        message:
          "The linked environment no longer accepts this link. It may have been revoked there; link it again.",
      }),
    ),
  ),
});

const serveB = (descriptor: ExecutionEnvironmentDescriptor, probeTracker?: ProbeTracker) =>
  servePeer(
    descriptor,
    McpHttpServer.toolkitRegistration(ProbeToolkit, probeHandlers),
    probeTracker,
  );

/** Environment A, which links to B. */
const makeA = (
  descriptor: ExecutionEnvironmentDescriptor,
  secrets?: typeof ServerSecretStore.layer,
  httpClient?: Layer.Layer<HttpClient.HttpClient>,
) =>
  layerLinkingEnvironment(descriptor, secrets, httpClient).pipe(
    Layer.build,
    Effect.map((context) => ({
      links: Context.get(context, PeerLinks.PeerLinks),
      peer: Context.get(context, PeerMcpClient.PeerMcpClient),
      sql: Context.get(context, SqlClient.SqlClient),
      settings: Context.get(context, ServerSettings.ServerSettingsService),
    })),
  );

const laptop = descriptorOf("environment-laptop", "Laptop");
const box = descriptorOf("environment-box", "Box");
const callPeer = (
  a: Effect.Success<ReturnType<typeof makeA>>,
  environmentId: ExecutionEnvironmentDescriptor["environmentId"],
  limits: PeerMcpClient.PeerCallLimits = { runtimeMode: "full-access", interactionMode: "default" },
  tool = "probe",
) =>
  a.peer.call({
    environmentId,
    tool,
    arguments: {},
    limits,
    success: Probe,
  });
const probe = (
  a: Effect.Success<ReturnType<typeof makeA>>,
  limits: PeerMcpClient.PeerCallLimits = { runtimeMode: "full-access", interactionMode: "default" },
  tool = "probe",
) => callPeer(a, box.environmentId, limits, tool);

const linkedSessions = (b: ServedPeer) => b.linkedSessions;

type TestSignal<A> = {
  readonly promise: Promise<A>;
  readonly wait: Effect.Effect<A>;
  readonly resolve: (value: A) => void;
};

const makeTestSignal = <A>(): TestSignal<A> => {
  let resolve!: (value: A) => void;
  const promise = new Promise<A>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, wait: Effect.promise(() => promise), resolve };
};

const controlledInitializeResponse = (
  active: Ref.Ref<boolean>,
  observed: Deferred.Deferred<{ readonly status: number; readonly sessionId: string | undefined }>,
  body: (
    request: HttpClientRequest.HttpClientRequest,
    response: HttpClientResponse.HttpClientResponse,
  ) => Effect.Effect<string, HttpClientError.HttpClientError>,
) =>
  layerHttpClientResponseTransform((request, response) =>
    Effect.gen(function* () {
      if (
        !(yield* Ref.get(active)) ||
        request.url.endsWith("/mcp") === false ||
        request.method !== "POST" ||
        request.headers["mcp-session-id"] !== undefined ||
        (response.status !== 200 && response.status !== 401)
      ) {
        return response;
      }
      const sessionId = response.headers["mcp-session-id"];
      yield* Deferred.succeed(observed, { status: response.status, sessionId });
      Object.defineProperty(response, "stream", {
        configurable: true,
        value: Stream.fromEffect(
          Effect.suspend(() => body(request, response)).pipe(
            Effect.map((text) => new TextEncoder().encode(text)),
          ),
        ),
      });
      return response;
    }),
  );

const trackedInitializeResponse = (
  active: Ref.Ref<boolean>,
  observed: Deferred.Deferred<{ readonly status: number; readonly sessionId: string | undefined }>,
  bodyStarted: TestSignal<void>,
  bodyCancelled: TestSignal<void>,
  bodyFailed: TestSignal<void>,
  bodyReleased: TestSignal<void>,
  behavior: "stall" | "fail",
) =>
  layerHttpClientResponseTransform((request, response) =>
    Effect.gen(function* () {
      if (
        !(yield* Ref.get(active)) ||
        request.url.endsWith("/mcp") === false ||
        request.method !== "POST" ||
        request.headers["mcp-session-id"] !== undefined ||
        response.status !== 401
      ) {
        return response;
      }
      yield* Deferred.succeed(observed, {
        status: response.status,
        sessionId: response.headers["mcp-session-id"],
      });
      let releasePull = () => {};
      const body = new ReadableStream<Uint8Array>({
        pull(controller) {
          bodyStarted.resolve(undefined);
          if (behavior === "fail") {
            controller.error(new Error("controlled body failure"));
            bodyFailed.resolve(undefined);
            bodyReleased.resolve(undefined);
            return;
          }
          return new Promise<void>((resolve) => {
            releasePull = resolve;
          });
        },
        cancel() {
          releasePull();
          bodyCancelled.resolve(undefined);
          bodyReleased.resolve(undefined);
        },
      });
      return HttpClientResponse.fromWeb(request, new Response(body, { status: response.status }));
    }),
  );

const delayedDeleteResponse = (
  active: Ref.Ref<boolean>,
  bodyStarted: TestSignal<void>,
  bodyGate: TestSignal<void>,
) =>
  layerHttpClientResponseTransform((request, response) =>
    Effect.gen(function* () {
      if (
        !(yield* Ref.get(active)) ||
        request.url.endsWith("/mcp") === false ||
        request.method !== "DELETE"
      ) {
        return response;
      }
      const body = new ReadableStream<Uint8Array>({
        pull(controller) {
          bodyStarted.resolve(undefined);
          return bodyGate.promise.then(() => controller.close());
        },
      });
      return HttpClientResponse.fromWeb(request, new Response(body, { status: 200 }));
    }),
  );

const controlledSessionResponse = (
  active: Ref.Ref<boolean>,
  status: number,
  observed: Deferred.Deferred<{ readonly status: number; readonly sessionId: string | undefined }>,
  bodyStarted: Deferred.Deferred<void>,
  bodyGate: Deferred.Deferred<void>,
) =>
  layerHttpClientResponseTransform((request, response) =>
    Effect.gen(function* () {
      if (
        !(yield* Ref.get(active)) ||
        request.url.endsWith("/mcp") === false ||
        request.method !== "POST" ||
        request.headers["mcp-session-id"] === undefined ||
        response.status !== status
      ) {
        return response;
      }
      yield* Deferred.succeed(observed, {
        status: response.status,
        sessionId: request.headers["mcp-session-id"],
      });
      Object.defineProperty(response, "stream", {
        configurable: true,
        value: Stream.fromEffect(
          Deferred.succeed(bodyStarted, undefined).pipe(
            Effect.andThen(Deferred.await(bodyGate)),
            Effect.as(new TextEncoder().encode("")),
          ),
        ),
      });
      return response;
    }),
  );

const failureBody = (
  request: HttpClientRequest.HttpClientRequest,
  response: HttpClientResponse.HttpClientResponse,
) =>
  Effect.fail(
    new HttpClientError.HttpClientError({
      reason: new HttpClientError.DecodeError({
        request,
        response,
        description: "controlled response body failure",
      }),
    }),
  );

const callPeerTool = (a: Effect.Success<ReturnType<typeof makeA>>) =>
  a.peer.call({
    environmentId: box.environmentId,
    tool: "probe",
    arguments: {},
    limits: { runtimeMode: "full-access", interactionMode: "default" },
    success: Probe,
  });

const verifyInitialize401BodyOwnership = (
  path: "probe" | "tool",
  behavior: "stall" | "fail" | "interrupted",
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const active = yield* Ref.make(false);
      const observed = yield* Deferred.make<{
        readonly status: number;
        readonly sessionId: string | undefined;
      }>();
      const bodyStarted = makeTestSignal<void>();
      const bodyCancelled = makeTestSignal<void>();
      const bodyFailed = makeTestSignal<void>();
      const bodyReleased = makeTestSignal<void>();
      const b = yield* serveB(box);
      const a = yield* makeA(
        laptop,
        undefined,
        trackedInitializeResponse(
          active,
          observed,
          bodyStarted,
          bodyCancelled,
          bodyFailed,
          bodyReleased,
          behavior === "fail" ? "fail" : "stall",
        ),
      );
      yield* linkTo(a.links, b);
      yield* Ref.set(b.initializeResponse, { status: 401 });
      yield* Ref.set(active, true);

      const request =
        path === "probe"
          ? a.links.probe(box.environmentId).pipe(
              Effect.asVoid,
              Effect.mapError(() => "failed" as const),
            )
          : callPeerTool(a).pipe(
              Effect.asVoid,
              Effect.mapError(() => "failed" as const),
            );
      const pending = yield* Effect.exit(request).pipe(
        Effect.forkScoped({ startImmediately: true }),
      );
      expect(yield* Deferred.await(observed)).toEqual({ status: 401, sessionId: undefined });

      const phase = yield* Effect.raceFirst(
        bodyStarted.wait.pipe(Effect.as("body" as const)),
        Fiber.join(pending).pipe(Effect.as("finished" as const)),
      );
      expect(phase).toBe("body");
      expect(Option.map(yield* a.links.get(box.environmentId), (link) => link.revoked)).toEqual(
        Option.some(true),
      );

      if (behavior === "fail") {
        expect(Exit.isFailure(yield* Fiber.join(pending))).toBe(true);
        yield* bodyFailed.wait;
      } else {
        expect(Option.map(yield* a.links.get(box.environmentId), (link) => link.revoked)).toEqual(
          Option.some(true),
        );

        if (behavior === "stall") {
          yield* TestClock.adjust(Duration.seconds(3));
          expect(Exit.isFailure(yield* Fiber.join(pending))).toBe(true);
        } else {
          yield* Fiber.interrupt(pending);
        }
        if (behavior === "interrupted") yield* bodyCancelled.wait;
      }

      yield* bodyReleased.wait;

      expect(Option.map(yield* a.links.get(box.environmentId), (link) => link.revoked)).toEqual(
        Option.some(true),
      );
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest));

/** Another environment on a socket of its own, answering as `descriptor`. */
const serveApart = (descriptor: ExecutionEnvironmentDescriptor, probeTracker?: ProbeTracker) =>
  Layer.build(Layer.fresh(NodeHttpServer.layerTest)).pipe(
    Effect.flatMap((server) =>
      serveB(descriptor, probeTracker).pipe(Effect.provideContext(server)),
    ),
  );

it.effect("links with a pairing code, and the peer holds its agents to the limits they carry", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const a = yield* makeA(laptop);

      const refused = yield* a.links
        .link({ url: b.url, pairingCode: "not-a-code", access: "auto" })
        .pipe(Effect.flip);
      expect(refused.reason).toBe("pairing_rejected");
      expect(yield* linkedSessions(b)).toEqual([]);

      const pairing = yield* b.auth.issuePairingCredential();
      const linked = yield* a.links.link({
        url: b.url,
        pairingCode: pairing.credential,
        access: "auto",
      });
      expect(linked).toMatchObject({
        environmentId: box.environmentId,
        label: "Box",
        urls: [b.url],
        access: "auto",
      });
      // B lists the link under A's name, where it can be revoked.
      expect((yield* linkedSessions(b)).map((session) => session.client.label)).toEqual([
        "T3 Code · Laptop",
      ]);
      expect(
        (yield* a.links.list).map(({ environmentId, status }) => [environmentId, status]),
      ).toEqual([[box.environmentId, "reachable"]]);

      // The caller's limits narrow the link's access; they never widen it.
      expect(
        yield* probe(a, { runtimeMode: "approval-required", interactionMode: "plan" }),
      ).toEqual({ caller: "T3 Code · Laptop", modes: "approval-required/plan" });
      expect(yield* probe(a)).toEqual({ caller: "T3 Code · Laptop", modes: "auto/default" });

      // A refusal keeps the peer's reason.
      const notFound = yield* probe(a, undefined, "refuse").pipe(Effect.flip);
      expect(notFound).toMatchObject({ code: "thread_not_found", message: "No such thread." });

      const looksRevoked = yield* probe(a, undefined, "looks-revoked").pipe(Effect.flip);
      expect(looksRevoked).toMatchObject({
        code: "capability_denied",
        message:
          "The linked environment no longer accepts this link. It may have been revoked there; link it again.",
      });
      expect(Option.map(yield* a.links.get(box.environmentId), (link) => link.revoked)).toEqual(
        Option.some(false),
      );
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect(
  "each machine's Linked environments beta gates links, and a link waits while it is off",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* serveB(box);
        const a = yield* makeA(laptop);
        const pairing = yield* b.auth.issuePairingCredential();
        const link = a.links.link({ url: b.url, pairingCode: pairing.credential, access: "auto" });

        yield* a.settings.updateSettings({ linkedEnvironmentsEnabled: false });
        expect(yield* link.pipe(Effect.flip)).toMatchObject({
          reason: "disabled",
          message: PeerLinks.LINKED_ENVIRONMENTS_OFF,
        });
        expect(yield* Ref.get(b.bearers)).toEqual([]);

        yield* a.settings.updateSettings({ linkedEnvironmentsEnabled: true });
        yield* b.settings.updateSettings({ linkedEnvironmentsEnabled: false });
        const refusedThere = yield* link.pipe(Effect.flip);
        expect(refusedThere.reason).toBe("pairing_rejected");
        expect(refusedThere.message).toContain("Linked environments is off on that machine");
        expect(yield* linkedSessions(b)).toEqual([]);

        yield* b.settings.updateSettings({ linkedEnvironmentsEnabled: true });
        yield* link;
        yield* b.settings.updateSettings({ linkedEnvironmentsEnabled: false });
        // Not final, so a task already running there picks up again once it is back on.
        const waiting = yield* probe(a).pipe(Effect.flip);
        expect(waiting.code).toBe("orchestration_error");
        expect(waiting.message).toContain("has Linked environments turned off");
        yield* b.settings.updateSettings({ linkedEnvironmentsEnabled: true });
        expect(yield* probe(a)).toEqual({ caller: "T3 Code · Laptop", modes: "auto/default" });
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("a link whose row cannot be stored keeps none of its token", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const held = yield* Ref.make<ReadonlySet<string>>(new Set());
      const noted = Layer.effect(
        ServerSecretStore.ServerSecretStore,
        Effect.map(ServerSecretStore.make, (store) =>
          ServerSecretStore.ServerSecretStore.of({
            ...store,
            set: (name, value) =>
              store
                .set(name, value)
                .pipe(Effect.andThen(Ref.update(held, (names) => new Set([...names, name])))),
            remove: (name) =>
              store
                .remove(name)
                .pipe(
                  Effect.andThen(
                    Ref.update(held, (names) => new Set([...names].filter((at) => at !== name))),
                  ),
                ),
          }),
        ),
      );
      const a = yield* makeA(laptop, noted);
      yield* a.sql`
        CREATE TRIGGER refuse_link BEFORE INSERT ON peer_environment_links
        BEGIN SELECT RAISE(ABORT, 'database or disk is full'); END
      `;
      const pairing = yield* b.auth.issuePairingCredential();
      const failed = yield* a.links
        .link({ url: b.url, pairingCode: pairing.credential, access: "auto" })
        .pipe(Effect.flip);
      expect(failed.reason).toBe("storage");
      expect(yield* Ref.get(held)).toEqual(new Set());
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("two links racing to one peer leave the stored access matching the token", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const a = yield* makeA(laptop);
      const [first, second] = yield* Effect.all(
        [b.auth.issuePairingCredential(), b.auth.issuePairingCredential()],
        { concurrency: 2 },
      );
      yield* Effect.all(
        [
          a.links.link({ url: b.url, pairingCode: first.credential, access: "read-only" }),
          a.links.link({ url: b.url, pairingCode: second.credential, access: "full-access" }),
        ],
        { concurrency: 2 },
      );
      const [stored] = yield* a.links.list;
      // The token sent is the one issued with the access the row records.
      const { modes } = yield* probe(a);
      expect(modes).toBe(
        stored!.access === "read-only" ? "approval-required/default" : "full-access/default",
      );
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

/** A bare HTTP server on a socket of its own, answering with `handle`. */
const serveRaw = <E, R>(
  handle: (
    request: HttpServerRequest.HttpServerRequest,
  ) => Effect.Effect<HttpServerResponse.HttpServerResponse, E, R>,
) =>
  Layer.build(Layer.fresh(NodeHttpServer.layerTest)).pipe(
    Effect.flatMap((server) =>
      Effect.gen(function* () {
        yield* HttpServer.serve(Effect.flatMap(HttpServerRequest.HttpServerRequest, handle)).pipe(
          Layer.build,
        );
        const address = (yield* HttpServer.HttpServer).address;
        if (address._tag === "UnixPathAddress") return yield* Effect.die("expected a TCP address");
        return `http://127.0.0.1:${address.port}`;
      }).pipe(Effect.provideContext(server)),
    ),
  );

it.effect("never follows a peer's redirect with the pairing code", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const received = yield* Ref.make<ReadonlyArray<string>>([]);
      const elsewhere = yield* serveRaw((request) =>
        Ref.update(received, (seen) => [...seen, `${request.method} ${request.url}`]).pipe(
          Effect.as(HttpServerResponse.empty({ status: 404 })),
        ),
      );
      // Answers as B, then sends every linking request on to another address.
      const front = yield* serveRaw((request) =>
        request.url.startsWith("/.well-known/t3/environment")
          ? HttpClient.get(`${b.url}${request.url}`).pipe(
              Effect.flatMap((response) => response.text),
              Effect.map((body) =>
                HttpServerResponse.text(body, { contentType: "application/json" }),
              ),
              Effect.provide(FetchHttpClient.layer),
              Effect.orDie,
            )
          : Effect.succeed(
              HttpServerResponse.empty({
                status: 307,
                headers: { location: `${elsewhere}${request.url}` },
              }),
            ),
      );
      const a = yield* makeA(laptop);
      const pairing = yield* b.auth.issuePairingCredential();
      yield* a.links
        .link({ url: front, pairingCode: pairing.credential, access: "auto" })
        .pipe(Effect.flip);
      expect(yield* Ref.get(received)).toEqual([]);
      expect(yield* a.links.list).toEqual([]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("never follows an authenticated MCP redirect with its token", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const received = yield* Ref.make<ReadonlyArray<string>>([]);
      const elsewhere = yield* serveRaw((request) =>
        Ref.update(received, (seen) => [
          ...seen,
          `${request.method} ${request.url} ${request.headers.authorization ?? ""}`,
        ]).pipe(Effect.as(HttpServerResponse.empty({ status: 404 }))),
      );
      const front = yield* serveRaw((request) =>
        request.url.startsWith("/.well-known/t3/environment")
          ? HttpClient.get(`${b.url}${request.url}`).pipe(
              Effect.flatMap((response) => response.text),
              Effect.map((body) =>
                HttpServerResponse.text(body, { contentType: "application/json" }),
              ),
              Effect.provide(FetchHttpClient.layer),
              Effect.orDie,
            )
          : Effect.succeed(
              HttpServerResponse.empty({
                status: 307,
                headers: { location: `${elsewhere}${request.url}` },
              }),
            ),
      );
      const a = yield* makeA(laptop);
      yield* a.links.link({
        url: b.url,
        pairingCode: (yield* b.auth.issuePairingCredential()).credential,
        access: "auto",
      });
      yield* a.sql`UPDATE peer_environment_links SET urls_json = ${JSON.stringify([front])}`;

      const failed = yield* probe(a).pipe(Effect.flip);
      expect(failed).toMatchObject({ code: "orchestration_error" });
      expect(yield* Ref.get(received)).toEqual([]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each([
  [401, "capability_denied"],
  [403, "orchestration_error"],
  [500, "orchestration_error"],
] as const)(
  "closes an MCP session when initialized notification fails with HTTP %s",
  ([status, code]) =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* serveB(box);
        const a = yield* makeA(laptop);
        yield* a.links.link({
          url: b.url,
          pairingCode: (yield* b.auth.issuePairingCredential()).credential,
          access: "auto",
        });
        yield* Ref.set(b.mcpNotificationStatus, status);

        const failed = yield* probe(a).pipe(Effect.flip);
        expect(failed).toMatchObject({ code });
        expect(
          (yield* Ref.get(b.requests)).slice(-3).map((request) => request.split(" ")[0]),
        ).toEqual(["POST", "POST", "DELETE"]);
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("closes an MCP session when initialized notification is interrupted", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const a = yield* makeA(laptop);
      yield* a.links.link({
        url: b.url,
        pairingCode: (yield* b.auth.issuePairingCredential()).credential,
        access: "auto",
      });
      const gate = yield* Deferred.make<void>();
      yield* Ref.set(b.mcpNotificationGate, gate);
      const pending = yield* probe(a).pipe(Effect.forkScoped({ startImmediately: true }));
      yield* Deferred.await(b.mcpNotificationStarted);
      yield* Fiber.interrupt(pending);
      yield* Deferred.succeed(gate, undefined);

      expect((yield* Ref.get(b.requests)).slice(-1)[0]?.split(" ")[0]).toBe("DELETE");
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("closes a probe session when the initialize body fails after its headers arrive", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const active = yield* Ref.make(false);
      const observed = yield* Deferred.make<{
        readonly status: number;
        readonly sessionId: string | undefined;
      }>();
      const bodyStarted = yield* Deferred.make<void>();
      const b = yield* serveB(box);
      const a = yield* makeA(
        laptop,
        undefined,
        controlledInitializeResponse(active, observed, (request, response) =>
          Deferred.succeed(bodyStarted, undefined).pipe(
            Effect.andThen(failureBody(request, response)),
          ),
        ),
      );
      yield* a.links.link({
        url: b.url,
        pairingCode: (yield* b.auth.issuePairingCredential()).credential,
        access: "auto",
      });
      yield* Ref.set(active, true);

      const failed = yield* Effect.exit(a.links.probe(box.environmentId));
      const headers = yield* Deferred.await(observed);
      yield* Deferred.await(bodyStarted);
      expect(Exit.isFailure(failed)).toBe(true);
      expect(headers.status).toBe(200);
      expect(headers.sessionId).toBeDefined();
      expect(
        (yield* Ref.get(b.requests)).filter((request) => request.startsWith("DELETE ")),
      ).toEqual([`DELETE /mcp ${headers.sessionId}`]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("closes a tool session when the initialize body fails after its headers arrive", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const active = yield* Ref.make(false);
      const observed = yield* Deferred.make<{
        readonly status: number;
        readonly sessionId: string | undefined;
      }>();
      const bodyStarted = yield* Deferred.make<void>();
      const b = yield* serveB(box);
      const a = yield* makeA(
        laptop,
        undefined,
        controlledInitializeResponse(active, observed, (request, response) =>
          Deferred.succeed(bodyStarted, undefined).pipe(
            Effect.andThen(failureBody(request, response)),
          ),
        ),
      );
      yield* a.links.link({
        url: b.url,
        pairingCode: (yield* b.auth.issuePairingCredential()).credential,
        access: "auto",
      });
      yield* Ref.set(active, true);

      const failed = yield* Effect.exit(callPeerTool(a));
      const headers = yield* Deferred.await(observed);
      yield* Deferred.await(bodyStarted);
      expect(Exit.isFailure(failed)).toBe(true);
      expect(headers.status).toBe(200);
      expect(headers.sessionId).toBeDefined();
      expect(
        (yield* Ref.get(b.requests)).filter((request) => request.startsWith("DELETE ")),
      ).toEqual([`DELETE /mcp ${headers.sessionId}`]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("closes a tool session when its initialize body times out", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const active = yield* Ref.make(false);
      const observed = yield* Deferred.make<{
        readonly status: number;
        readonly sessionId: string | undefined;
      }>();
      const bodyStarted = yield* Deferred.make<void>();
      const bodyGate = yield* Deferred.make<void>();
      const b = yield* serveB(box);
      const a = yield* makeA(
        laptop,
        undefined,
        controlledInitializeResponse(active, observed, () =>
          Deferred.succeed(bodyStarted, undefined).pipe(
            Effect.andThen(Deferred.await(bodyGate)),
            Effect.as(""),
          ),
        ),
      );
      yield* a.links.link({
        url: b.url,
        pairingCode: (yield* b.auth.issuePairingCredential()).credential,
        access: "auto",
      });
      yield* Ref.set(active, true);

      const pending = yield* Effect.exit(callPeerTool(a)).pipe(
        Effect.forkScoped({ startImmediately: true }),
      );
      const headers = yield* Deferred.await(observed);
      yield* Deferred.await(bodyStarted);
      yield* TestClock.adjust(Duration.seconds(3));
      const failed = yield* Fiber.join(pending);
      expect(Exit.isFailure(failed)).toBe(true);
      expect(headers.status).toBe(200);
      expect(headers.sessionId).toBeDefined();
      expect(
        (yield* Ref.get(b.requests)).filter((request) => request.startsWith("DELETE ")),
      ).toEqual([`DELETE /mcp ${headers.sessionId}`]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("revokes a probe link when HTTP 401 has a failed body", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const active = yield* Ref.make(false);
      const observed = yield* Deferred.make<{
        readonly status: number;
        readonly sessionId: string | undefined;
      }>();
      const bodyStarted = yield* Ref.make(false);
      const b = yield* serveB(box);
      const a = yield* makeA(
        laptop,
        undefined,
        controlledInitializeResponse(active, observed, (request, response) =>
          Ref.set(bodyStarted, true).pipe(Effect.andThen(failureBody(request, response))),
        ),
      );
      yield* a.links.link({
        url: b.url,
        pairingCode: (yield* b.auth.issuePairingCredential()).credential,
        access: "auto",
      });
      yield* Ref.set(b.initializeResponse, { status: 401 });
      yield* Ref.set(active, true);

      const failed = yield* a.links.probe(box.environmentId).pipe(Effect.flip);
      const headers = yield* Deferred.await(observed);
      expect(failed.reason).toBe("revoked");
      expect(headers).toEqual({ status: 401, sessionId: undefined });
      expect(yield* Ref.get(bodyStarted)).toBe(true);
      expect(Option.map(yield* a.links.get(box.environmentId), (link) => link.revoked)).toEqual(
        Option.some(true),
      );
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each(["stall", "fail", "interrupted"] as const)(
  "releases a tool initialize-401 body that %s",
  (behavior) => verifyInitialize401BodyOwnership("tool", behavior),
);

it.effect.each(["stall", "fail", "interrupted"] as const)(
  "releases a probe initialize-401 body that %s",
  (behavior) => verifyInitialize401BodyOwnership("probe", behavior),
);

it.effect("revokes a tool link when HTTP 401 has a pending initialize body", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const active = yield* Ref.make(false);
      const observed = yield* Deferred.make<{
        readonly status: number;
        readonly sessionId: string | undefined;
      }>();
      const bodyStarted = yield* Ref.make(false);
      const bodyGate = yield* Deferred.make<void>();
      const b = yield* serveB(box);
      const a = yield* makeA(
        laptop,
        undefined,
        controlledInitializeResponse(active, observed, () =>
          Ref.set(bodyStarted, true).pipe(Effect.andThen(Deferred.await(bodyGate)), Effect.as("")),
        ),
      );
      yield* a.links.link({
        url: b.url,
        pairingCode: (yield* b.auth.issuePairingCredential()).credential,
        access: "auto",
      });
      yield* Ref.set(b.initializeResponse, { status: 401 });
      yield* Ref.set(active, true);

      const pending = yield* Effect.exit(callPeerTool(a)).pipe(
        Effect.forkScoped({ startImmediately: true }),
      );
      const headers = yield* Deferred.await(observed);
      yield* TestClock.adjust(Duration.seconds(3));
      const failed = yield* Fiber.join(pending);
      expect(Exit.isFailure(failed)).toBe(true);
      expect(headers).toEqual({ status: 401, sessionId: undefined });
      expect(yield* Ref.get(bodyStarted)).toBe(true);
      expect(Option.map(yield* a.links.get(box.environmentId), (link) => link.revoked)).toEqual(
        Option.some(true),
      );
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each(["notification", "tool response", "probe notification"] as const)(
  "persists HTTP 401 from a %s before an interrupted body read",
  (stage) =>
    Effect.scoped(
      Effect.gen(function* () {
        const active = yield* Ref.make(false);
        const observed = yield* Deferred.make<{
          readonly status: number;
          readonly sessionId: string | undefined;
        }>();
        const bodyStarted = yield* Deferred.make<void>();
        const bodyGate = yield* Deferred.make<void>();
        const b = yield* serveB(box);
        const a = yield* makeA(
          laptop,
          undefined,
          controlledSessionResponse(active, 401, observed, bodyStarted, bodyGate),
        );
        yield* a.links.link({
          url: b.url,
          pairingCode: (yield* b.auth.issuePairingCredential()).credential,
          access: "auto",
        });

        if (stage === "tool response") {
          yield* callPeerTool(a);
          yield* Ref.set(b.sessionResponse, { status: 401 });
        } else {
          yield* Ref.set(b.mcpNotificationStatus, 401);
        }
        yield* Ref.set(active, true);

        const pending =
          stage === "probe notification"
            ? yield* a.links
                .probe(box.environmentId)
                .pipe(Effect.ignore, Effect.forkScoped({ startImmediately: true }))
            : yield* callPeerTool(a).pipe(
                Effect.ignore,
                Effect.forkScoped({ startImmediately: true }),
              );
        const interrupted = yield* Ref.make(false);
        const interruptPending = () =>
          Effect.gen(function* () {
            if (yield* Ref.get(interrupted)) return;
            const interrupt = yield* Fiber.interrupt(pending).pipe(
              Effect.forkScoped({ startImmediately: true }),
            );
            yield* TestClock.adjust(Duration.seconds(3));
            yield* Fiber.join(interrupt);
            yield* Ref.set(interrupted, true);
          });
        const releaseBody = Effect.gen(function* () {
          yield* Deferred.succeed(bodyGate, undefined);
          if (!(yield* Ref.get(interrupted))) {
            yield* Fiber.await(pending).pipe(Effect.asVoid);
          }
        });

        yield* Effect.ensuring(
          Effect.gen(function* () {
            const headers = yield* Deferred.await(observed);
            yield* Deferred.await(bodyStarted);
            expect(headers.status).toBe(401);
            expect(headers.sessionId).toBeDefined();
            expect(
              Option.map(yield* a.links.get(box.environmentId), (link) => link.revoked),
            ).toEqual(Option.some(true));

            yield* interruptPending();
            expect(
              Option.map(yield* a.links.get(box.environmentId), (link) => link.revoked),
            ).toEqual(Option.some(true));
            expect(
              (yield* Ref.get(b.requests)).some((request) => request.startsWith("DELETE ")),
            ).toBe(true);
          }),
          releaseBody,
        );
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("keeps a relink usable when a held 401 belongs to the old peer call", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const active = yield* Ref.make(false);
      const observed = yield* Deferred.make<{
        readonly status: number;
        readonly sessionId: string | undefined;
      }>();
      const bodyStarted = yield* Deferred.make<void>();
      const bodyGate = yield* Deferred.make<void>();
      const b = yield* serveB(box);
      const a = yield* makeA(
        laptop,
        undefined,
        controlledSessionResponse(active, 401, observed, bodyStarted, bodyGate),
      );
      yield* a.links.link({
        url: b.url,
        pairingCode: (yield* b.auth.issuePairingCredential()).credential,
        access: "auto",
      });
      yield* Ref.set(b.mcpNotificationStatus, 401);
      yield* Ref.set(active, true);

      const pending = yield* callPeerTool(a).pipe(
        Effect.flip,
        Effect.forkScoped({ startImmediately: true }),
      );
      const verify = Effect.gen(function* () {
        yield* Deferred.await(observed);
        yield* Deferred.await(bodyStarted);
        expect(Option.map(yield* a.links.get(box.environmentId), (link) => link.revoked)).toEqual(
          Option.some(true),
        );

        yield* a.links.link({
          url: b.url,
          pairingCode: (yield* b.auth.issuePairingCredential()).credential,
          access: "read-only",
        });
        expect(Option.map(yield* a.links.get(box.environmentId), (link) => link.revoked)).toEqual(
          Option.some(false),
        );

        yield* Deferred.succeed(bodyGate, undefined);
        expect(yield* Fiber.join(pending)).toMatchObject({ code: "capability_denied" });
        expect(
          Option.map(yield* a.links.get(box.environmentId), (link) => ({
            label: link.label,
            access: link.access,
            revoked: link.revoked,
          })),
        ).toEqual(Option.some({ label: "Box", access: "read-only", revoked: false }));
      });
      yield* Effect.ensuring(
        verify,
        Deferred.succeed(bodyGate, undefined).pipe(
          Effect.andThen(Fiber.await(pending)),
          Effect.asVoid,
        ),
      );
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("does not publish an MCP session opened across an unlink refresh", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const a = yield* makeA(laptop);
      yield* a.links.link({
        url: b.url,
        pairingCode: (yield* b.auth.issuePairingCredential()).credential,
        access: "auto",
      });
      const gate = yield* Deferred.make<void>();
      yield* Ref.set(b.mcpGate, gate);
      const refresh = yield* Stream.runHead(
        a.links.subscribeRefreshes.pipe(
          Stream.filter((event) => event.environmentId === box.environmentId),
        ),
      ).pipe(Effect.forkScoped({ startImmediately: true }));
      const pending = yield* probe(a).pipe(
        Effect.flip,
        Effect.forkScoped({ startImmediately: true }),
      );
      yield* Deferred.await(b.mcpStarted);
      expect(yield* a.links.unlink(box.environmentId)).toBe(true);
      expect(
        Option.map(yield* Fiber.join(refresh), ({ revision, environmentId }) => ({
          revision,
          environmentId,
        })),
      ).toEqual(Option.some({ revision: 1, environmentId: box.environmentId }));
      yield* Deferred.succeed(gate, undefined);

      const failed = yield* Fiber.join(pending);
      expect(failed).toMatchObject({ code: "orchestration_error" });
      expect((yield* Ref.get(b.requests)).slice(-1)[0]?.split(" ")[0]).toBe("DELETE");
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("never sends a link's token to an address that stops answering as that peer", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const a = yield* makeA(laptop);
      const pairing = yield* b.auth.issuePairingCredential();
      yield* a.links.link({ url: b.url, pairingCode: pairing.credential, access: "auto" });
      yield* probe(a);
      expect((yield* Ref.get(b.bearers)).length).toBeGreaterThan(0);

      // The address now answers as another environment, as a reused LAN address would.
      yield* Ref.set(b.descriptor, descriptorOf("environment-impostor", "Impostor"));
      yield* Ref.set(b.bearers, []);
      const failed = yield* probe(a).pipe(Effect.flip);
      expect(failed.message).toContain("Box did not answer");
      expect(yield* Ref.get(b.bearers)).toEqual([]);
      expect((yield* a.links.list).map((link) => link.status)).toEqual(["unreachable"]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("reads one stored link without asking the peer", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const a = yield* makeA(laptop);
      const pairing = yield* b.auth.issuePairingCredential();
      yield* a.links.link({ url: b.url, pairingCode: pairing.credential, access: "auto" });
      yield* Ref.set(b.probes, 0);

      const stored = yield* a.links.get(box.environmentId);
      expect(Option.map(stored, (link) => link.label)).toEqual(Option.some("Box"));
      expect(Option.isNone(yield* a.links.get(laptop.environmentId))).toBe(true);
      expect(yield* Ref.get(b.probes)).toBe(0);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("ignores an outcome from a link generation replaced while it was in flight", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const a = yield* makeA(laptop);
      yield* a.links.link({
        url: b.url,
        pairingCode: (yield* b.auth.issuePairingCredential()).credential,
        access: "auto",
      });
      const old = yield* a.links.resolve(box.environmentId);

      yield* a.links.link({
        url: b.url,
        pairingCode: (yield* b.auth.issuePairingCredential()).credential,
        access: "auto",
      });
      yield* a.links.recordOutcome(box.environmentId, old.secretName, {
        error: "The old request arrived late.",
        revoked: true,
      });

      const current = yield* a.links.get(box.environmentId);
      expect(Option.map(current, (link) => link.lastError)).toEqual(Option.some(null));
      expect(Option.map(current, (link) => link.revoked)).toEqual(Option.some(false));
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("keeps a revocation sticky when outcomes race within one link generation", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const a = yield* makeA(laptop);
      yield* a.links.link({
        url: b.url,
        pairingCode: (yield* b.auth.issuePairingCredential()).credential,
        access: "auto",
      });
      const current = yield* a.links.resolve(box.environmentId);

      yield* Effect.all(
        [
          a.links.recordOutcome(box.environmentId, current.secretName, { error: null }),
          a.links.recordOutcome(box.environmentId, current.secretName, {
            error: "The peer rejected this token.",
            revoked: true,
          }),
        ],
        { concurrency: 2 },
      );

      expect(Option.map(yield* a.links.get(box.environmentId), (link) => link.revoked)).toEqual(
        Option.some(true),
      );
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("publishes link refreshes for unlink operations", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const a = yield* makeA(laptop);
      yield* a.links.link({
        url: b.url,
        pairingCode: (yield* b.auth.issuePairingCredential()).credential,
        access: "auto",
      });
      const refresh = yield* Stream.runHead(
        a.links.subscribeRefreshes.pipe(
          Stream.filter((event) => event.environmentId === box.environmentId),
        ),
      ).pipe(Effect.forkScoped({ startImmediately: true }));
      expect(yield* a.links.unlink(box.environmentId)).toBe(true);
      expect(
        Option.map(yield* Fiber.join(refresh), ({ revision, environmentId }) => ({
          revision,
          environmentId,
        })),
      ).toEqual(Option.some({ revision: 1, environmentId: box.environmentId }));
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("refreshes and evicts an unlinked peer even when secret removal fails", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const failedRemovals = yield* Ref.make(0);
      const failingSecrets = Layer.effect(
        ServerSecretStore.ServerSecretStore,
        Effect.map(ServerSecretStore.make, (store) =>
          ServerSecretStore.ServerSecretStore.of({
            ...store,
            remove: (name) =>
              Ref.update(failedRemovals, (count) => count + 1).pipe(
                Effect.andThen(
                  Effect.fail(
                    new ServerSecretStore.SecretStoreRemoveError({
                      resource: name,
                      cause: "test failure",
                    }),
                  ),
                ),
              ),
          }),
        ),
      );
      const a = yield* makeA(laptop, failingSecrets);
      yield* a.links.link({
        url: b.url,
        pairingCode: (yield* b.auth.issuePairingCredential()).credential,
        access: "auto",
      });
      yield* probe(a);
      const refresh = yield* Stream.runHead(
        a.links.subscribeRefreshes.pipe(
          Stream.filter((event) => event.environmentId === box.environmentId),
        ),
      ).pipe(Effect.forkScoped({ startImmediately: true }));

      const failed = yield* a.links.unlink(box.environmentId).pipe(Effect.flip);
      expect(failed.reason).toBe("storage");
      expect(Option.isNone(yield* a.links.get(box.environmentId))).toBe(true);
      expect(yield* Ref.get(failedRemovals)).toBe(1);
      yield* Deferred.await(b.mcpDeleteStarted);
      expect(
        Option.map(yield* Fiber.join(refresh), ({ revision, environmentId }) => ({
          revision,
          environmentId,
        })),
      ).toEqual(Option.some({ revision: 1, environmentId: box.environmentId }));
      expect((yield* Ref.get(b.requests)).map((request) => request.split(" ")[0])).toContain(
        "DELETE",
      );
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("refreshing one peer keeps another peer session usable", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const cDescriptor = descriptorOf("environment-chair", "Chair");
      const c = yield* serveApart(cDescriptor);
      const a = yield* makeA(laptop);
      yield* linkTo(a.links, b);
      const cPairing = yield* c.auth.issuePairingCredential();
      yield* a.links.link({ url: c.url, pairingCode: cPairing.credential, access: "auto" });
      yield* probe(a);
      yield* callPeer(a, cDescriptor.environmentId);

      const refresh = yield* Stream.runHead(
        a.links.subscribeRefreshes.pipe(
          Stream.filter((event) => event.environmentId === box.environmentId),
        ),
      ).pipe(Effect.forkScoped({ startImmediately: true }));
      expect(yield* a.links.unlink(box.environmentId)).toBe(true);
      yield* a.links.link({
        url: b.url,
        pairingCode: (yield* b.auth.issuePairingCredential()).credential,
        access: "auto",
      });
      expect(Option.map(yield* Fiber.join(refresh), ({ environmentId }) => environmentId)).toEqual(
        Option.some(box.environmentId),
      );

      // C's cached session remains available while B's old one is closed.
      const cRequestsBefore = (yield* Ref.get(c.requests)).length;
      yield* callPeer(a, cDescriptor.environmentId);
      expect((yield* Ref.get(c.requests)).length).toBeGreaterThan(cRequestsBefore);
      expect((yield* Ref.get(c.requests)).some((request) => request.startsWith("DELETE "))).toBe(
        false,
      );
      expect((yield* Ref.get(b.requests)).map((request) => request.split(" ")[0])).toContain(
        "DELETE",
      );
      yield* probe(a);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("closes duplicate sessions opened by concurrent cache misses", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const a = yield* makeA(laptop);
      yield* a.links.link({
        url: b.url,
        pairingCode: (yield* b.auth.issuePairingCredential()).credential,
        access: "auto",
      });
      const gate = yield* Deferred.make<void>();
      yield* Ref.set(b.mcpGate, gate);
      const first = yield* probe(a).pipe(Effect.forkScoped({ startImmediately: true }));
      const second = yield* probe(a).pipe(Effect.forkScoped({ startImmediately: true }));
      yield* Deferred.await(b.mcpConcurrentStarted);
      yield* Deferred.succeed(gate, undefined);
      yield* Fiber.join(first);
      yield* Fiber.join(second);

      expect((yield* linkedSessions(b)).length).toBe(1);
      expect(
        (yield* Ref.get(b.requests)).filter((request) => request.startsWith("DELETE ")),
      ).toHaveLength(1);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("reopens a peer session after restart and reuses the replacement", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const a = yield* makeA(laptop);
      yield* a.links.link({
        url: b.url,
        pairingCode: (yield* b.auth.issuePairingCredential()).credential,
        access: "auto",
      });
      yield* probe(a);
      const firstSessionId = (yield* Ref.get(b.requests))
        .map((request) => request.split(" ")[2])
        .find((sessionId) => sessionId !== "");
      expect(firstSessionId).toBeDefined();
      yield* Ref.set(b.invalidSessionId, firstSessionId!);

      expect(yield* probe(a)).toMatchObject({ modes: "auto/default" });
      expect(yield* probe(a)).toMatchObject({ modes: "auto/default" });
      const sessionIds = (yield* Ref.get(b.requests))
        .map((request) => request.split(" ")[2])
        .filter(Boolean);
      const replacement = sessionIds.find((sessionId) => sessionId !== firstSessionId);
      expect(replacement).toBeDefined();
      expect(sessionIds.slice(-4)).toEqual([firstSessionId, replacement, replacement, replacement]);
      expect(yield* linkedSessions(b)).toHaveLength(1);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("preserves a replacement session when a held 404 request finishes late", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const active = yield* Ref.make(false);
      const responseCount = yield* Ref.make(0);
      const observed = yield* Deferred.make<string>();
      const bodyStarted = yield* Deferred.make<void>();
      const bodyGate = yield* Deferred.make<void>();
      const b = yield* serveB(box);
      const held404 = layerHttpClientResponseTransform((request, response) =>
        Effect.gen(function* () {
          if (
            !(yield* Ref.get(active)) ||
            request.url.endsWith("/mcp") === false ||
            request.method !== "POST" ||
            request.headers["mcp-session-id"] === undefined ||
            response.status !== 404 ||
            (yield* Ref.updateAndGet(responseCount, (count) => count + 1)) !== 1
          ) {
            return response;
          }
          yield* Ref.set(b.sessionResponse, null);
          yield* Deferred.succeed(observed, request.headers["mcp-session-id"]!);
          Object.defineProperty(response, "stream", {
            configurable: true,
            value: Stream.fromEffect(
              Deferred.succeed(bodyStarted, undefined).pipe(
                Effect.andThen(Deferred.await(bodyGate)),
                Effect.as(new TextEncoder().encode("")),
              ),
            ),
          });
          return response;
        }),
      );
      const a = yield* makeA(laptop, undefined, held404);
      yield* a.links.link({
        url: b.url,
        pairingCode: (yield* b.auth.issuePairingCredential()).credential,
        access: "auto",
      });
      yield* callPeerTool(a);
      const sessionIds = () =>
        Ref.get(b.requests).pipe(
          Effect.map((requests) =>
            requests
              .filter((request) => request.startsWith("POST /mcp"))
              .map((request) => request.split(" ")[2])
              .filter((sessionId): sessionId is string => Boolean(sessionId)),
          ),
        );
      const initialSessionId = (yield* sessionIds()).at(-1)!;
      yield* Ref.set(b.invalidSessionId, initialSessionId);
      yield* Ref.set(b.sessionResponse, { status: 404 });
      yield* Ref.set(active, true);

      const failedRequest = yield* callPeerTool(a).pipe(
        Effect.exit,
        Effect.forkScoped({ startImmediately: true }),
      );
      const verify = Effect.gen(function* () {
        expect(yield* Deferred.await(observed)).toBe(initialSessionId);
        yield* Deferred.await(bodyStarted);

        yield* callPeerTool(a);
        const replacementSessionId = (yield* sessionIds()).at(-1)!;
        expect(replacementSessionId).not.toBe(initialSessionId);
        yield* Deferred.succeed(bodyGate, undefined);
        expect(Exit.isSuccess(yield* Fiber.join(failedRequest))).toBe(true);
        expect(yield* linkedSessions(b)).toHaveLength(1);

        yield* callPeerTool(a);
        expect((yield* sessionIds()).at(-1)).toBe(replacementSessionId);
      });
      yield* Effect.ensuring(
        verify,
        Deferred.succeed(bodyGate, undefined).pipe(
          Effect.andThen(Fiber.await(failedRequest)),
          Effect.asVoid,
        ),
      );
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each(["initialize", "notification", "tool call"] as const)(
  "classifies HTTP 401 from %s responses",
  (stage) =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* serveB(box);
        const a = yield* makeA(laptop);
        yield* a.links.link({
          url: b.url,
          pairingCode: (yield* b.auth.issuePairingCredential()).credential,
          access: "auto",
        });
        if (stage === "tool call") yield* probe(a);
        const override = { status: 401 };
        if (stage === "initialize") yield* Ref.set(b.initializeResponse, override);
        else yield* Ref.set(b.sessionResponse, override);

        const failed = yield* probe(a).pipe(Effect.flip);
        expect(failed).toMatchObject({ code: "capability_denied" });
        expect(Option.map(yield* a.links.get(box.environmentId), (link) => link.revoked)).toEqual(
          Option.some(true),
        );
        if (stage === "notification") {
          expect((yield* Ref.get(b.requests)).at(-1)?.split(" ")[0]).toBe("DELETE");
        }
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("marks a probe link revoked from HTTP 401", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const a = yield* makeA(laptop);
      yield* a.links.link({
        url: b.url,
        pairingCode: (yield* b.auth.issuePairingCredential()).credential,
        access: "auto",
      });
      yield* Ref.set(b.initializeResponse, { status: 401 });

      const failed = yield* a.links.probe(box.environmentId).pipe(Effect.flip);
      expect(failed.reason).toBe("revoked");
      expect(Option.map(yield* a.links.get(box.environmentId), (link) => link.revoked)).toEqual(
        Option.some(true),
      );
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("closes the cached MCP session when a tool call gets HTTP 401", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const a = yield* makeA(laptop);
      yield* a.links.link({
        url: b.url,
        pairingCode: (yield* b.auth.issuePairingCredential()).credential,
        access: "auto",
      });
      yield* callPeer(a, box.environmentId);
      const [session] = yield* linkedSessions(b);
      yield* b.auth.revokeSession(session!.sessionId);

      const failed = yield* callPeer(a, box.environmentId).pipe(Effect.flip);
      expect(failed).toMatchObject({ code: "capability_denied" });
      yield* Deferred.await(b.mcpDeleteStarted);
      expect(
        (yield* Ref.get(b.requests)).map((request) => request.split(" ")[0]).slice(-2),
      ).toEqual(["POST", "DELETE"]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("bounds concurrent linked-environment status probes", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const tracker: ProbeTracker = {
        inFlight: yield* Ref.make(0),
        maxInFlight: yield* Ref.make(0),
        gate: yield* Ref.make<Deferred.Deferred<void> | null>(null),
        reached: yield* Deferred.make<void>(),
      };
      const peers = yield* Effect.all(
        [
          serveApart(descriptorOf("environment-peer-0", "Peer 0"), tracker),
          serveApart(descriptorOf("environment-peer-1", "Peer 1"), tracker),
          serveApart(descriptorOf("environment-peer-2", "Peer 2"), tracker),
          serveApart(descriptorOf("environment-peer-3", "Peer 3"), tracker),
          serveApart(descriptorOf("environment-peer-4", "Peer 4"), tracker),
          serveApart(descriptorOf("environment-peer-5", "Peer 5"), tracker),
        ],
        { concurrency: 6 },
      );
      const a = yield* makeA(laptop);
      for (const peer of peers) yield* linkTo(a.links, peer);
      yield* Ref.set(tracker.maxInFlight, 0);
      expect(yield* Ref.get(tracker.inFlight)).toBe(0);

      const gate = yield* Deferred.make<void>();
      yield* Ref.set(tracker.gate, gate);
      const listing = yield* a.links.list.pipe(Effect.forkScoped({ startImmediately: true }));
      yield* Deferred.await(tracker.reached);
      const maxInFlight = yield* Ref.get(tracker.maxInFlight);
      expect(maxInFlight).toBe(4);
      expect(maxInFlight).toBeLessThanOrEqual(4);
      yield* Deferred.succeed(gate, undefined);
      expect((yield* Fiber.join(listing)).length).toBe(6);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("coalesces a slow refresh subscriber to the latest bounded event", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const a = yield* makeA(laptop);
      const boxPeer = yield* serveB(box);
      const chairDescriptor = descriptorOf("environment-chair", "Chair");
      const deskDescriptor = descriptorOf("environment-desk", "Desk");
      const chairPeer = yield* serveApart(chairDescriptor);
      const deskPeer = yield* serveApart(deskDescriptor);
      const received = yield* Ref.make<ReadonlyArray<PeerLinkRefresh>>([]);
      const firstDelivered = yield* Deferred.make<void>();
      const secondDelivered = yield* Deferred.make<void>();
      const latestDelivered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();

      yield* a.links.subscribeRefreshes.pipe(
        Stream.runForEach((event) =>
          Ref.updateAndGet(received, (events) => [...events, event]).pipe(
            Effect.flatMap((events) => {
              if (events.length === 1) return Deferred.succeed(firstDelivered, undefined);
              if (events.length === 2) {
                return Deferred.succeed(secondDelivered, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                );
              }
              return event.environmentId === deskDescriptor.environmentId
                ? Deferred.succeed(latestDelivered, undefined)
                : Effect.void;
            }),
          ),
        ),
        Effect.forkScoped({ startImmediately: true }),
      );
      yield* linkTo(a.links, boxPeer);
      yield* Deferred.await(firstDelivered);
      const boxLink = yield* a.links.resolve(box.environmentId);
      yield* a.links.recordOutcome(box.environmentId, boxLink.secretName, {
        error: "The peer rejected this link.",
        revoked: true,
      });
      yield* Deferred.await(secondDelivered);
      yield* linkTo(a.links, chairPeer);
      yield* linkTo(a.links, deskPeer);
      const expectedLatestRevision = (yield* Ref.get(received)).at(-1)!.revision + 2;
      yield* Deferred.succeed(release, undefined);
      yield* Deferred.await(latestDelivered);

      expect(
        (yield* Ref.get(received)).map(({ environmentId, revision }) => ({
          environmentId,
          revision,
        })),
      ).toEqual([
        { environmentId: box.environmentId, revision: 1 },
        { environmentId: box.environmentId, revision: 2 },
        { environmentId: deskDescriptor.environmentId, revision: expectedLatestRevision },
      ]);
      expect(yield* Ref.get(received)).toHaveLength(3);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("reconciles coalesced link generations without closing unaffected sessions", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const active = yield* Ref.make(false);
      const deleteBodyStarted = makeTestSignal<void>();
      const deleteBodyGate = makeTestSignal<void>();
      const anchorDescriptor = descriptorOf("environment-anchor", "Anchor");
      const chairDescriptor = descriptorOf("environment-chair", "Chair");
      const deskDescriptor = descriptorOf("environment-desk", "Desk");
      const anchor = yield* serveApart(anchorDescriptor);
      const boxPeer = yield* serveB(box);
      const chair = yield* serveApart(chairDescriptor);
      const desk = yield* serveApart(deskDescriptor);
      const a = yield* makeA(
        laptop,
        undefined,
        delayedDeleteResponse(active, deleteBodyStarted, deleteBodyGate),
      );

      yield* linkTo(a.links, anchor);
      yield* linkTo(a.links, boxPeer);
      yield* linkTo(a.links, chair);
      yield* linkTo(a.links, desk);
      yield* callPeer(a, anchorDescriptor.environmentId);
      yield* callPeer(a, box.environmentId);
      yield* callPeer(a, chairDescriptor.environmentId);
      yield* callPeer(a, deskDescriptor.environmentId);
      yield* Ref.set(active, true);

      const anchorLink = yield* a.links.resolve(anchorDescriptor.environmentId);
      yield* a.links.recordOutcome(anchorDescriptor.environmentId, anchorLink.secretName, {
        error: "The anchor link changed.",
        revoked: true,
      });
      yield* deleteBodyStarted.wait;

      const boxLink = yield* a.links.resolve(box.environmentId);
      const deskLink = yield* a.links.resolve(deskDescriptor.environmentId);
      yield* a.links.recordOutcome(box.environmentId, boxLink.secretName, {
        error: "The peer rejected this link.",
        revoked: true,
      });
      yield* a.links.recordOutcome(deskDescriptor.environmentId, deskLink.secretName, {
        error: "The peer rejected this link.",
        revoked: true,
      });
      deleteBodyGate.resolve(undefined);

      yield* Deferred.await(boxPeer.mcpDeleteStarted);
      yield* Deferred.await(desk.mcpDeleteStarted);
      yield* callPeer(a, chairDescriptor.environmentId);

      expect(yield* Ref.get(chair.mcpInitializeCount)).toBe(1);
      expect(
        (yield* Ref.get(chair.requests)).some((request) => request.startsWith("DELETE ")),
      ).toBe(false);
      expect(
        (yield* Ref.get(boxPeer.requests)).some((request) => request.startsWith("DELETE ")),
      ).toBe(true);
      expect((yield* Ref.get(desk.requests)).some((request) => request.startsWith("DELETE "))).toBe(
        true,
      );
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("omits a link unlinked while its status probe is in flight", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const a = yield* makeA(laptop);
      yield* a.links.link({
        url: b.url,
        pairingCode: (yield* b.auth.issuePairingCredential()).credential,
        access: "auto",
      });
      const gate = yield* Deferred.make<void>();
      yield* Ref.set(b.mcpGate, gate);
      const pending = yield* a.links.list.pipe(Effect.forkScoped({ startImmediately: true }));
      yield* Deferred.await(b.mcpStarted);
      expect(yield* a.links.unlink(box.environmentId)).toBe(true);
      yield* Deferred.succeed(gate, undefined);

      expect(yield* Fiber.join(pending)).toEqual([]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect.each(["unlink", "relink"] as const)(
  "does not return probe success after a concurrent %s",
  (change) =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* serveB(box);
        const a = yield* makeA(laptop);
        yield* a.links.link({
          url: b.url,
          pairingCode: (yield* b.auth.issuePairingCredential()).credential,
          access: "auto",
        });
        const gate = yield* Deferred.make<void>();
        yield* Ref.set(b.mcpNotificationGate, gate);
        const pending = yield* a.links
          .probe(box.environmentId)
          .pipe(Effect.exit, Effect.forkScoped({ startImmediately: true }));
        yield* Deferred.await(b.mcpNotificationStarted);

        if (change === "unlink") {
          expect(yield* a.links.unlink(box.environmentId)).toBe(true);
        } else {
          yield* Ref.set(b.descriptor, descriptorOf(box.environmentId, "Replacement box"));
          yield* a.links.link({
            url: b.url,
            pairingCode: (yield* b.auth.issuePairingCredential()).credential,
            access: "read-only",
          });
        }
        yield* Deferred.succeed(gate, undefined);

        expect(Exit.isFailure(yield* Fiber.join(pending))).toBe(true);
        const current = yield* a.links.get(box.environmentId);
        if (change === "unlink") {
          expect(Option.isNone(current)).toBe(true);
        } else {
          expect(
            Option.map(current, (link) => ({
              label: link.label,
              access: link.access,
              revoked: link.revoked,
            })),
          ).toEqual(Option.some({ label: "Replacement box", access: "read-only", revoked: false }));
        }
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("times out a probe whose initialize response never arrives", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const a = yield* makeA(laptop);
      yield* a.links.link({
        url: b.url,
        pairingCode: (yield* b.auth.issuePairingCredential()).credential,
        access: "auto",
      });
      const gate = yield* Deferred.make<void>();
      yield* Ref.set(b.mcpGate, gate);
      const probe = yield* a.links
        .probe(box.environmentId)
        .pipe(Effect.flip, Effect.forkScoped({ startImmediately: true }));
      yield* Deferred.await(b.mcpStarted);
      yield* TestClock.adjust(Duration.seconds(3));
      const failed = yield* Fiber.join(probe);
      expect(failed.reason).toBe("unreachable");
      yield* Deferred.succeed(gate, undefined);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("does not report replacement reachability from a stale list probe", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const a = yield* makeA(laptop);
      yield* linkTo(a.links, b);
      const gate = yield* Deferred.make<void>();
      yield* Ref.set(b.mcpGate, gate);
      const listing = yield* a.links.list.pipe(Effect.forkScoped({ startImmediately: true }));
      yield* Deferred.await(b.mcpStarted);

      const replacement = descriptorOf(box.environmentId, "Replacement box");
      yield* Ref.set(b.descriptor, replacement);
      yield* a.links.link({
        url: b.url,
        pairingCode: (yield* b.auth.issuePairingCredential()).credential,
        access: "read-only",
      });
      yield* Ref.set(b.descriptor, descriptorOf("environment-impostor", "Impostor"));
      yield* Deferred.succeed(gate, undefined);

      expect(
        (yield* Fiber.join(listing)).map(({ label, access, status }) => ({
          label,
          access,
          status,
        })),
      ).toEqual([{ label: "Replacement box", access: "read-only", status: "unreachable" }]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("tells the agent to link again once the peer revokes the link or it expires", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const b = yield* serveB(box);
      const a = yield* makeA(laptop);
      const pairing = yield* b.auth.issuePairingCredential();
      yield* a.links.link({ url: b.url, pairingCode: pairing.credential, access: "auto" });
      expect((yield* a.links.list).map((link) => link.status)).toEqual(["reachable"]);
      const beforeProbeRequests = yield* Ref.get(b.requests);
      expect(beforeProbeRequests.slice(-3).map((request) => request.split(" ")[0])).toEqual([
        "POST",
        "POST",
        "DELETE",
      ]);

      const [session] = yield* linkedSessions(b);
      const refresh = yield* Stream.runHead(
        a.links.subscribeRefreshes.pipe(
          Stream.filter((event) => event.environmentId === box.environmentId),
        ),
      ).pipe(Effect.forkScoped({ startImmediately: true }));
      yield* b.auth.revokeSession(session!.sessionId);
      expect((yield* a.links.list).map((link) => link.status)).toEqual(["revoked"]);
      expect(
        Option.map(yield* Fiber.join(refresh), ({ revision, environmentId }) => ({
          revision,
          environmentId,
        })),
      ).toEqual(Option.some({ revision: 1, environmentId: box.environmentId }));
      const revoked = yield* probe(a).pipe(Effect.flip);
      expect(revoked.code).toBe("capability_denied");
      expect(revoked.message).toContain("revoked there");
      expect((yield* a.links.list).map((link) => link.status)).toEqual(["revoked"]);

      yield* a.links.link({
        url: b.url,
        pairingCode: (yield* b.auth.issuePairingCredential()).credential,
        access: "auto",
      });
      // A link made with a peer that later answers without the fence, as
      // one made before the fence or a peer moved back to such a version.
      yield* Ref.set(b.descriptor, descriptorOf("environment-box", "Box", { linkFence: false }));
      yield* Ref.set(b.bearers, []);
      const unfenced = yield* probe(a).pipe(Effect.flip);
      expect(unfenced.code).toBe("capability_denied");
      expect(unfenced.message).toContain("cannot keep a linked agent's limits");
      expect(yield* Ref.get(b.bearers)).toEqual([]);
      yield* Ref.set(b.descriptor, box);

      // A link stored before links were marked: its session there is not a
      // link's, so the peer would not fence its work even though it can.
      yield* a.sql`UPDATE peer_environment_links SET fenced = 0`;
      // Linking again is its only way back, as for an expired link.
      expect((yield* a.links.list).map((link) => link.status)).toEqual(["expired"]);
      const legacy = yield* probe(a).pipe(Effect.flip);
      expect(legacy.code).toBe("capability_denied");
      expect(legacy.message).toContain("Link it again");
      expect(yield* Ref.get(b.bearers)).toEqual([]);
      yield* a.links.link({
        url: b.url,
        pairingCode: (yield* b.auth.issuePairingCredential()).credential,
        access: "auto",
      });
      yield* probe(a);

      yield* TestClock.adjust(Duration.days(31));
      expect((yield* a.links.list).map((link) => link.status)).toEqual(["expired"]);
      yield* Ref.set(b.bearers, []);
      const expired = yield* probe(a).pipe(Effect.flip);
      expect(expired.code).toBe("capability_denied");
      expect(expired.message).toContain("expired");
      expect(yield* Ref.get(b.bearers)).toEqual([]);
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect(
  "refuses to link itself, plain http off this machine and the tailnet, or a peer that cannot hold a linked agent to its limits",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const b = yield* serveB(
          descriptorOf("environment-box", "Box", { mcpModeLimitHeader: false }),
        );
        const a = yield* makeA(laptop);
        const pairing = yield* b.auth.issuePairingCredential();
        const older = yield* a.links
          .link({ url: b.url, pairingCode: pairing.credential, access: "auto" })
          .pipe(Effect.flip);
        expect(older.reason).toBe("incompatible");
        // Nor one whose link's work could reach the user's own threads there.
        yield* Ref.set(b.descriptor, descriptorOf("environment-box", "Box", { linkFence: false }));
        const unfenced = yield* a.links
          .link({ url: b.url, pairingCode: pairing.credential, access: "auto" })
          .pipe(Effect.flip);
        expect(unfenced.reason).toBe("incompatible");

        // Plain http to a LAN address would carry the code and token in clear.
        const lan = yield* a.links
          .link({
            url: "http://192.168.1.20:3773",
            pairingCode: pairing.credential,
            access: "auto",
          })
          .pipe(Effect.flip);
        expect(lan.message).toContain("plain http");
        const lanAlternate = yield* a.links
          .link({
            url: b.url,
            alternateUrls: ["http://192.168.1.20:3773"],
            pairingCode: pairing.credential,
            access: "auto",
          })
          .pipe(Effect.flip);
        expect(lanAlternate.message).toContain("plain http");

        yield* Ref.set(b.descriptor, laptop);
        const self = yield* a.links
          .link({ url: b.url, pairingCode: pairing.credential, access: "auto" })
          .pipe(Effect.flip);
        expect(self.reason).toBe("self");
        // Neither attempt spent the pairing code on a session.
        expect(yield* linkedSessions(b)).toEqual([]);
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect(
  "linking a picked machine skips an address that answers as another, and never signs in there",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const impostor = yield* serveApart(descriptorOf("environment-impostor", "Impostor"));
        const b = yield* serveB(box);
        const a = yield* makeA(laptop);
        const pairing = yield* b.auth.issuePairingCredential();
        const linked = yield* a.links.link({
          url: impostor.url,
          alternateUrls: [b.url],
          pairingCode: pairing.credential,
          access: "auto",
          expectedEnvironmentId: box.environmentId,
        });
        expect(linked).toMatchObject({ environmentId: box.environmentId, urls: [b.url] });
        yield* probe(a);
        expect(yield* linkedSessions(impostor)).toEqual([]);
        expect(yield* Ref.get(impostor.bearers)).toEqual([]);
        expect((yield* linkedSessions(b)).length).toBe(1);
      }),
    ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);

it.effect("linking a picked machine that answers nowhere names it and links nothing", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const impostor = yield* serveApart(descriptorOf("environment-impostor", "Impostor"));
      const a = yield* makeA(laptop);
      const failed = yield* a.links
        .link({
          url: impostor.url,
          alternateUrls: ["http://192.168.1.20:3773"],
          pairingCode: "unused",
          access: "auto",
          expectedEnvironmentId: box.environmentId,
          expectedLabel: "Box",
        })
        .pipe(Effect.flip);
      expect(failed.reason).toBe("unreachable");
      expect(failed.message).toBe(
        `This environment could not reach Box at ${impostor.url}, http://192.168.1.20:3773.`,
      );
      expect(yield* a.links.list).toEqual([]);
      expect(yield* linkedSessions(impostor)).toEqual([]);

      // Every address refused: the message says why.
      const refused = yield* a.links
        .link({
          url: "http://192.168.1.20:3773",
          pairingCode: "unused",
          access: "auto",
          expectedEnvironmentId: box.environmentId,
          expectedLabel: "Box",
        })
        .pipe(Effect.flip);
      expect(refused.message).toContain("could not reach Box");
      expect(refused.message).toContain("plain http");
    }),
  ).pipe(Effect.provide(NodeHttpServer.layerTest)),
);
