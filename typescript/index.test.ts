import { describe, expect, test } from "bun:test";
import { CHAINS, HEALTH_MAX_LAG_SECS } from "./config";
import { createApp, INITIAL_SYNC_GRACE_MS } from "./index";
import type { SyncState } from "./syncer";

function makeState(now: number, overrides: Partial<SyncState> = {}): SyncState {
  return {
    startedAt: now,
    lastSuccessAt: null,
    lastSyncedBlock: null,
    latestChainBlock: null,
    observedBlockTimeMs: 1_000,
    lastError: null,
    lastErrorAt: null,
    syncsPerformed: 0,
    blocksIngested: 0,
    ...overrides,
  };
}

function createTestApp(options: {
  now?: number;
  getTimestamps?: (chain: (typeof CHAINS)[number], blocks: number[]) => (number | null)[];
  blockCount?: (chain: (typeof CHAINS)[number]) => number;
  isHealthy?: (chain: (typeof CHAINS)[number]) => boolean;
  getSyncState?: (chain: (typeof CHAINS)[number]) => SyncState;
} = {}) {
  const now = options.now ?? 1_000_000;

  return createApp({
    getTimestamps: options.getTimestamps ?? ((_chain, blocks) => blocks.map(() => null)),
    blockCount: options.blockCount ?? (() => 0),
    isHealthy: options.isHealthy ?? (() => true),
    getSyncState: options.getSyncState ?? (() => makeState(now)),
    log: (_level, _msg, _extra) => {},
    now: () => now,
  });
}

describe("POST /v1/timestamps validation", () => {
  const app = createTestApp();

  const cases: Array<[string, unknown, string]> = [
    ["rejects string chain_id", { chain_id: "1", blocks: [1] }, "unknown_chain"],
    ["rejects zero chain_id", { chain_id: 0, blocks: [1] }, "unknown_chain"],
    ["rejects negative chain_id", { chain_id: -1, blocks: [1] }, "unknown_chain"],
    ["rejects empty chain", { chain: "", blocks: [1] }, "unknown_chain"],
    ["rejects non-array blocks", { chain: "ethereum", blocks: "1" }, "invalid_blocks"],
    ["rejects empty blocks", { chain: "ethereum", blocks: [] }, "invalid_blocks"],
    ["rejects float block", { chain: "ethereum", blocks: [1.5] }, "invalid_block_number"],
    ["rejects negative block", { chain: "ethereum", blocks: [-1] }, "invalid_block_number"],
    [
      "rejects unsafe integer block",
      { chain: "ethereum", blocks: [Number.MAX_SAFE_INTEGER + 1] },
      "invalid_block_number",
    ],
  ];

  for (const [name, body, code] of cases) {
    test(name, async () => {
      const res = await app.request("http://localhost/v1/timestamps", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const payload = await res.json() as { error: string };

      expect(res.status).toBe(400);
      expect(payload.error).toBe(code);
    });
  }

  test("rejects more than 10k blocks", async () => {
    const res = await app.request("http://localhost/v1/timestamps", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chain: "ethereum",
        blocks: Array.from({ length: 10_001 }, (_, i) => i),
      }),
    });
    const body = await res.json() as { error: string };

    expect(res.status).toBe(400);
    expect(body.error).toBe("invalid_blocks");
  });
});

describe("GET /v1/timestamps/:chain/:block validation", () => {
  const app = createTestApp();

  test("rejects decimal-like block params", async () => {
    const res = await app.request("http://localhost/v1/timestamps/ethereum/1.5");
    const body = await res.json() as { error: string };

    expect(res.status).toBe(400);
    expect(body.error).toBe("invalid_block_number");
  });

  test("rejects mixed block params", async () => {
    const res = await app.request("http://localhost/v1/timestamps/ethereum/123abc");
    const body = await res.json() as { error: string };

    expect(res.status).toBe(400);
    expect(body.error).toBe("invalid_block_number");
  });
});

describe("/health", () => {
  test("returns 200 while all chains are initializing", async () => {
    const app = createTestApp();
    const res = await app.request("http://localhost/health");

    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ ok: true });
  });

  test("returns 503 when a chain fails before first success", async () => {
    const now = 1_000_000;
    const failingId = CHAINS[0].id;
    const app = createTestApp({
      now,
      getSyncState: (chain) =>
        chain.id === failingId
          ? makeState(now, {
              lastError: "RPC 401: unauthorized",
              lastErrorAt: now - 1_000,
            })
          : makeState(now),
    });

    const res = await app.request("http://localhost/health");
    const body = await res.json() as { degraded: string[] };

    expect(res.status).toBe(503);
    expect(body.degraded).toContain(`${CHAINS[0].name}: RPC 401: unauthorized`);
  });

  test("returns 503 when initialization exceeds the grace period", async () => {
    const now = 1_000_000;
    const slowId = CHAINS[1].id;
    const app = createTestApp({
      now,
      getSyncState: (chain) =>
        chain.id === slowId
          ? makeState(now, { startedAt: now - INITIAL_SYNC_GRACE_MS - 1 })
          : makeState(now),
    });

    const res = await app.request("http://localhost/health");
    expect(res.status).toBe(503);
  });

  test("returns 503 when a synced chain exceeds lag budget", async () => {
    const now = 1_000_000;
    const laggingId = CHAINS[2].id;
    const app = createTestApp({
      now,
      getSyncState: (chain) =>
        chain.id === laggingId
          ? makeState(now, {
              lastSuccessAt: now - 1_000,
              lastSyncedBlock: 100,
              latestChainBlock: 100 + HEALTH_MAX_LAG_SECS + 1,
            })
          : makeState(now, {
              lastSuccessAt: now - 1_000,
              lastSyncedBlock: 100,
              latestChainBlock: 100,
            }),
    });

    const res = await app.request("http://localhost/health");
    expect(res.status).toBe(503);
  });

  test("returns 503 when a DB becomes unreadable", async () => {
    const badId = CHAINS[3].id;
    const app = createTestApp({
      isHealthy: (chain) => chain.id !== badId,
    });

    const res = await app.request("http://localhost/health");
    const body = await res.json() as { degraded: string[] };

    expect(res.status).toBe(503);
    expect(body.degraded).toContain(`${CHAINS[3].name}: db unreadable`);
  });
});
