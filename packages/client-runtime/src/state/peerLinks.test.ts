import { EnvironmentId } from "@t3tools/contracts";
import { AtomRegistry } from "effect/reactivity";
import { expect, it } from "vite-plus/test";

import { invalidatePeerLinkCache, peerLinkCacheInvalidation } from "./peerLinks.ts";

it("bumps the peer-link cache signal for direct agent unlinks", () => {
  const registry = AtomRegistry.make();
  const environmentId = EnvironmentId.make("laptop");
  const signal = peerLinkCacheInvalidation(environmentId);
  invalidatePeerLinkCache(registry, environmentId);
  expect(registry.get(signal)).toBe(1);
  registry.dispose();
});
