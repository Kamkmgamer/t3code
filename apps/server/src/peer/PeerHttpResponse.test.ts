import { OrchestratorMcpFailure } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpClient from "effect/http/HttpClient";
import * as HttpClientRequest from "effect/http/HttpClientRequest";
import * as Queue from "effect/Queue";
import * as TestClock from "effect/testing/TestClock";

import { makePeerHttpClient, withManagedResponse } from "./PeerHttpResponse.ts";

it.effect("bounds response release when stream cancellation never settles", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const bodyEvents = yield* Queue.unbounded<"pull" | "cancel">();
      const cancelCompleted = yield* Deferred.make<void>();
      const callerCompleted = yield* Deferred.make<void>();
      let releaseCancel!: () => void;
      const cancelResult = new Promise<void>((resolve) => {
        releaseCancel = () => {
          resolve();
          Deferred.doneUnsafe(cancelCompleted, Effect.void);
        };
      });
      const body = new ReadableStream<Uint8Array>({
        pull() {
          Queue.offerUnsafe(bodyEvents, "pull");
          return new Promise<void>(() => {});
        },
        cancel() {
          Queue.offerUnsafe(bodyEvents, "cancel");
          return cancelResult;
        },
      });
      const request = HttpClientRequest.get("http://peer.test/mcp");
      const caller = yield* Effect.gen(function* () {
        const httpClient = yield* HttpClient.HttpClient;
        const peerHttpClient = makePeerHttpClient(httpClient, async () => new Response(body));
        return yield* withManagedResponse(
          peerHttpClient.execute(request),
          () => Effect.succeed("done"),
          {
            acquireTimeout: Duration.seconds(1),
            releaseTimeout: Duration.seconds(2),
            onAcquireTimeout: () =>
              new OrchestratorMcpFailure({
                code: "orchestration_error",
                message: "acquire timed out",
              }),
          },
        );
      }).pipe(
        Effect.ensuring(Deferred.succeed(callerCompleted, undefined)),
        Effect.provide(FetchHttpClient.layer),
        Effect.forkScoped({ startImmediately: true }),
      );

      expect(yield* Queue.take(bodyEvents)).toBe("pull");
      yield* TestClock.adjust(Duration.seconds(2));
      expect(yield* Queue.take(bodyEvents)).toBe("cancel");
      const completedAtDeadline = yield* Deferred.isDone(callerCompleted);
      const upstreamCancelPending = !(yield* Deferred.isDone(cancelCompleted));

      releaseCancel();
      yield* Deferred.await(cancelCompleted);
      yield* Fiber.await(caller);

      expect(completedAtDeadline).toBe(true);
      expect(upstreamCancelPending).toBe(true);
    }),
  ),
);
