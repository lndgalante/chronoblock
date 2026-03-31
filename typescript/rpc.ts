/**
 * @module rpc
 *
 * JSON-RPC client for EVM chains. Handles batching, bounded
 * concurrency, exponential backoff, and timeout. The key invariant
 * is gap-safety: {@link fetchBlockTimestamps} returns only a
 * contiguous prefix so the syncer never skips blocks.
 */

import type { Chain, Block } from "./config";
import { log } from "./log";

/** Max retry attempts for transient RPC failures (429, 5xx, timeouts). */
const MAX_RETRIES = 5;

/** Per-request timeout — prevents a stalled RPC from blocking a sync worker. */
const TIMEOUT_MS = 30_000;

// ── Public API ───────────────────────────────────────────────────────

/** Fetches the chain tip block number via `eth_blockNumber`. */
export async function getLatestBlockNumber(chain: Chain, signal?: AbortSignal): Promise<number> {
  const res = await rpc(chain, "eth_blockNumber", [], signal);
  const n = parseInt(res, 16);
  if (!Number.isFinite(n))
    throw new Error(`${chain.name}: invalid eth_blockNumber response`);
  return n;
}

/**
 * Fetches block timestamps for the inclusive range `[from, to]`.
 *
 * Splits the range into batches of `chain.rpcBatchSize` and fires up
 * to `chain.rpcConcurrency` batches in parallel. Uses
 * `Promise.allSettled` so a single failed batch doesn't discard data
 * from successful siblings.
 *
 * **Gap-safety guarantee:** returns only the contiguous prefix starting
 * at `from`. If block `from + 3` is missing (RPC returned null or its
 * batch failed), the result is `[from, from+1, from+2]`. The syncer
 * inserts those three and retries from `from + 3` next cycle.
 *
 * @returns Contiguous array of blocks starting at `from`. May be
 *          shorter than `to - from + 1` if the RPC returned gaps.
 */
export async function fetchBlockTimestamps(
  chain: Chain,
  from: number,
  to: number,
  signal?: AbortSignal,
): Promise<Block[]> {
  // Split range into batch boundaries.
  const batches: [number, number][] = [];
  for (let s = from; s <= to; s += chain.rpcBatchSize) {
    batches.push([s, Math.min(s + chain.rpcBatchSize - 1, to)]);
  }

  // Fire batches with bounded concurrency. allSettled keeps
  // successful results even when siblings fail.
  const all = new Map<number, number>();

  for (let i = 0; i < batches.length; i += chain.rpcConcurrency) {
    const slice = batches.slice(i, i + chain.rpcConcurrency);
    const settled = await Promise.allSettled(
      slice.map(([bFrom, bTo]) => fetchBatch(chain, bFrom, bTo, signal)),
    );

    let hadFailure = false;

    for (const result of settled) {
      if (result.status === "fulfilled") {
        for (const b of result.value) all.set(b.number, b.timestamp);
      } else {
        hadFailure = true;
      }
    }

    // If any batch in this wave failed, stop — we can't guarantee
    // contiguity beyond this point.
    if (hadFailure) break;
  }

  // Extract the contiguous prefix starting at `from`.
  const out: Block[] = [];
  for (let i = from; i <= to; i++) {
    const ts = all.get(i);
    if (ts === undefined) break;
    out.push({ number: i, timestamp: ts });
  }

  return out;
}

// ── Internal ─────────────────────────────────────────────────────────

/** Fetches a single batch of blocks via JSON-RPC batch request. */
async function fetchBatch(chain: Chain, from: number, to: number, signal?: AbortSignal): Promise<Block[]> {
  const payload = [];
  for (let i = from; i <= to; i++) {
    payload.push({
      jsonrpc: "2.0",
      method: "eth_getBlockByNumber",
      params: [`0x${i.toString(16)}`, false],
      id: i - from,
    });
  }

  const data = await rpcBatch(chain, payload, signal);

  const blocks: Block[] = [];
  for (const r of data) {
    if (r?.error) {
      log("warn", `batch item RPC error: ${r.error.message ?? r.error.code}`, { chain: chain.name, id: r.id });
      continue;
    }
    if (!r?.result) continue;
    const num = parseInt(r.result.number, 16);
    const ts = parseInt(r.result.timestamp, 16);
    if (Number.isFinite(num) && Number.isFinite(ts)) {
      blocks.push({ number: num, timestamp: ts });
    }
  }

  return blocks;
}

/** Whether an error is worth retrying (timeouts, rate limits, server errors). */
function isRetryable(err: unknown): boolean {
  if (err instanceof Error) {
    const msg = err.message;
    // Timeout, network, and explicit retryable status codes.
    if (err.name === "AbortError") return true;
    if (err.name === "TimeoutError") return true;
    if (/fetch failed|ECONNR|ETIMEDOUT|socket/i.test(msg)) return true;
    if (/RPC (429|5\d\d)/.test(msg)) return true;
  }
  return false;
}

/** Sends a single JSON-RPC call with retry + backoff. */
async function rpc(chain: Chain, method: string, params: unknown[], signal?: AbortSignal): Promise<any> {
  return send(chain, { jsonrpc: "2.0", method, params, id: 1 }, false, signal);
}

/** Sends a JSON-RPC batch call with retry + backoff. */
async function rpcBatch(chain: Chain, payload: unknown[], signal?: AbortSignal): Promise<any[]> {
  const res = await send(chain, payload, true, signal);
  if (!Array.isArray(res))
    throw new Error(`${chain.name}: expected array from batch RPC`);
  if (res.length !== payload.length)
    log("warn", `batch response length mismatch: expected ${payload.length}, got ${res.length}`, { chain: chain.name });
  return res;
}

/**
 * Core fetch loop with retry and exponential backoff.
 * Only retries errors classified as transient by {@link isRetryable}.
 */
async function send(chain: Chain, payload: unknown, isBatch: boolean, signal?: AbortSignal): Promise<any> {
  let lastErr: Error | undefined;

  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    if (signal?.aborted) throw new Error("aborted");

    try {
      const timeoutSignal = AbortSignal.timeout(TIMEOUT_MS);
      const res = await fetch(chain.rpc, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
        signal: signal
          ? AbortSignal.any([timeoutSignal, signal])
          : timeoutSignal,
      });

      if (res.status === 429 || res.status >= 500) {
        throw new Error(`${chain.name}: RPC ${res.status}`);
      }
      if (!res.ok) {
        // 4xx (non-429) are not retryable — bad request, auth, etc.
        throw new Error(`${chain.name}: RPC ${res.status}: ${await res.text()}`);
      }

      const json: any = await res.json();

      if (isBatch) return json;
      if (json.error)
        throw new Error(`${chain.name}: RPC error: ${json.error.message}`);
      return json.result;
    } catch (err: any) {
      lastErr = err;
      if (signal?.aborted) throw new Error("aborted");
      if (attempt === MAX_RETRIES || !isRetryable(err)) throw err;
      await sleep(Math.min(1000 * 2 ** attempt, 30_000), signal);
    }
  }

  throw lastErr ?? new Error("unreachable");
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (!signal) return Bun.sleep(ms);
  if (signal.aborted) return Promise.resolve();

  return Promise.race([
    Bun.sleep(ms),
    new Promise<void>((resolve) =>
      signal.addEventListener("abort", () => resolve(), { once: true }),
    ),
  ]);
}
