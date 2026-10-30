import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import type * as HttpClientResponse from "effect/http/HttpClientResponse";

type Restore = <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;

const releaseResponse = Effect.fn("PeerHttpResponse.releaseResponse")(function* (
  response: HttpClientResponse.HttpClientResponse,
  timeout: Duration.Input,
) {
  const release = yield* Effect.forkDetach(
    Effect.interruptible(
      response.stream.pipe(Stream.runDrain, Effect.timeout(timeout), Effect.ignore),
    ),
    { startImmediately: true, uninterruptible: false },
  );
  yield* Fiber.join(release);
});

/** Owns a response before running work that may be interrupted or fail. */
export const withManagedResponse = <E, R, A, E2, R2, ETimeout>(
  acquire: Effect.Effect<HttpClientResponse.HttpClientResponse, E, R>,
  use: (
    response: HttpClientResponse.HttpClientResponse,
    restore: Restore,
  ) => Effect.Effect<A, E2, R2>,
  options: {
    readonly acquireTimeout: Duration.Input;
    readonly releaseTimeout: Duration.Input;
    readonly onAcquireTimeout: () => ETimeout;
  },
): Effect.Effect<A, E | E2 | ETimeout, R | R2> =>
  Effect.uninterruptibleMask((restore): Effect.Effect<A, E | E2 | ETimeout, R | R2> =>
    Effect.gen(function* () {
      const acquired = yield* restore(acquire.pipe(Effect.timeoutOption(options.acquireTimeout)));
      if (Option.isNone(acquired)) return yield* Effect.fail(options.onAcquireTimeout());
      const response = acquired.value;
      let readerStarted = false;
      const ownedResponse = new Proxy(response, {
        get: (target, property) =>
          property === "stream"
            ? target.stream.pipe(Stream.onStart(Effect.sync(() => (readerStarted = true))))
            : Reflect.get(target, property, target),
      });
      return yield* Effect.ensuring(
        use(ownedResponse, restore),
        Effect.suspend(() =>
          readerStarted ? Effect.void : releaseResponse(response, options.releaseTimeout),
        ),
      );
    }),
  );
