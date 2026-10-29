import { EnvironmentId, type PeerLinkSummary } from "@t3tools/contracts";
import { AtomRegistry } from "effect/reactivity";
import * as DateTime from "effect/DateTime";
import { expect, it } from "vite-plus/test";

import { invalidatePeerLinkCache, peerLinkCacheInvalidation, peerLinkHealth } from "./peerLinks.ts";

it("bumps the peer-link cache signal for direct agent unlinks", () => {
  const registry = AtomRegistry.make();
  const environmentId = EnvironmentId.make("laptop");
  const signal = peerLinkCacheInvalidation(environmentId);
  invalidatePeerLinkCache(registry, environmentId);
  expect(registry.get(signal)).toBe(1);
  registry.dispose();
});

it("keeps revoked links revoked even when their expiry has passed", () => {
  const now = DateTime.makeUnsafe("2026-10-29T00:00:00.000Z");
  const link = {
    environmentId: EnvironmentId.make("peer"),
    label: "Peer",
    urls: ["https://peer.example.test"],
    access: "auto",
    linkedAt: DateTime.makeUnsafe("2026-09-01T00:00:00.000Z"),
    expiresAt: DateTime.makeUnsafe("2026-10-01T00:00:00.000Z"),
    lastReachedAt: null,
    lastError: "The peer rejected this link.",
    revoked: true,
    status: "revoked",
  } satisfies PeerLinkSummary;
  expect(peerLinkHealth(link, now)).toEqual({ kind: "revoked" });
});
