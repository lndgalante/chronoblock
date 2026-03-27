import { afterEach, describe, expect, test } from "bun:test";
import { fetchBlockTimestamps, getLatestBlockNumber } from "./rpc";
import type { Chain } from "./config";

const chain: Chain = {
  id: 1,
  name: "ethereum",
  rpc: "http://example.test",
  rpcBatchSize: 2,
  rpcConcurrency: 3,
  finalityBlocks: 64,
};

const originalFetch = globalThis.fetch;
type MockFetch = (
  input: Parameters<typeof fetch>[0],
  init?: Parameters<typeof fetch>[1],
) => ReturnType<typeof fetch>;

afterEach(() => {
  globalThis.fetch = originalFetch;
});

function jsonResponse(body: unknown, init: ResponseInit = {}) {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "Content-Type": "application/json" },
    ...init,
  });
}

function mockFetch(fn: MockFetch) {
  globalThis.fetch = Object.assign(fn, {
    preconnect: originalFetch.preconnect.bind(originalFetch),
  }) as typeof fetch;
}

function batchResponse(from: number, to: number, missing = new Set<number>()) {
  const rows = [];

  for (let block = from; block <= to; block++) {
    rows.push({
      jsonrpc: "2.0",
      id: block - from,
      result: missing.has(block)
        ? null
        : {
            number: `0x${block.toString(16)}`,
            timestamp: `0x${(block + 1000).toString(16)}`,
          },
    });
  }

  return rows;
}

describe("fetchBlockTimestamps", () => {
  test("returns only the contiguous prefix when a middle batch fails", async () => {
    mockFetch(async (_url, init) => {
      const payload = JSON.parse(String(init?.body)) as Array<{ params: [string, boolean] }>;
      const from = parseInt(payload[0].params[0], 16);

      if (from === 2) return new Response("bad request", { status: 400 });
      return jsonResponse(batchResponse(from, from + payload.length - 1));
    });

    const blocks = await fetchBlockTimestamps(chain, 0, 5);
    expect(blocks).toEqual([
      { number: 0, timestamp: 1000 },
      { number: 1, timestamp: 1001 },
    ]);
  });

  test("stops at a missing block inside a successful batch", async () => {
    mockFetch(async (_url, init) => {
      const payload = JSON.parse(String(init?.body)) as Array<{ params: [string, boolean] }>;
      const from = parseInt(payload[0].params[0], 16);
      const to = from + payload.length - 1;
      return jsonResponse(batchResponse(from, to, new Set([2])));
    });

    const blocks = await fetchBlockTimestamps({ ...chain, rpcBatchSize: 4, rpcConcurrency: 1 }, 0, 3);
    expect(blocks).toEqual([
      { number: 0, timestamp: 1000 },
      { number: 1, timestamp: 1001 },
    ]);
  });

  test("returns an empty prefix when all batches fail", async () => {
    mockFetch(async () => new Response("bad request", { status: 400 }));

    const blocks = await fetchBlockTimestamps({ ...chain, rpcConcurrency: 1 }, 0, 1);
    expect(blocks).toEqual([]);
  });

  test("aborts promptly while a fetch is in flight", async () => {
    mockFetch((_url, init) =>
      new Promise((_resolve, reject) => {
        init?.signal?.addEventListener(
          "abort",
          () => {
            const err = new Error("aborted");
            err.name = "AbortError";
            reject(err);
          },
          { once: true },
        );
      }));

    const controller = new AbortController();
    const promise = getLatestBlockNumber(chain, controller.signal);
    queueMicrotask(() => controller.abort());

    await expect(promise).rejects.toThrow("aborted");
  });

  test("does not retry or sleep once the caller aborts", async () => {
    let attempts = 0;
    const controller = new AbortController();

    mockFetch(async () => {
      attempts++;
      controller.abort();
      const err = new Error("aborted");
      err.name = "AbortError";
      throw err;
    });

    const startedAt = Date.now();
    await expect(getLatestBlockNumber(chain, controller.signal)).rejects.toThrow("aborted");

    expect(attempts).toBe(1);
    expect(Date.now() - startedAt).toBeLessThan(500);
  });
});
