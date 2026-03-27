/**
 * @module syncer
 *
 * Background sync workers — one per chain, running in parallel.
 *
 * Each worker loops: check the gap between the DB tip and the chain
 * tip, fetch the next chunk, insert, repeat. When caught up, it
 * sleeps for one observed block time before polling again.
 *
 * The observed block time is derived from actual stored timestamps
 * (see {@link observedBlockTimeMs}), so the service self-calibrates
 * if a chain changes its block time.
 *
 * Key design decisions:
 * - Catch-up skips sleep and caches the chain tip to avoid one RPC
 *   call per chunk (~44k calls saved on a full Ethereum backfill).
 * - `observedBlockTimeMs` is only recomputed after a successful sync
 *   that leaves us caught up, never during catch-up (where it would
 *   measure historical blocks, not current ones).
 * - Errors trigger a 3× backoff capped at 30s. After a successful
 *   sync, the error backoff resets.
 */

import { CHAINS, SYNC_CHUNK_SIZE, type Chain } from "./config";
import { lastBlock, insertBlocks, checkpointAll, observedBlockTimeMs } from "./db";
import { getLatestBlockNumber, fetchBlockTimestamps } from "./rpc";
import { log } from "./log";

/** How often to truncate WAL files during sustained writes. */
const CHECKPOINT_INTERVAL_MS = 60_000;

/** Max sleep on errors, prevents runaway backoff. */
const MAX_ERROR_SLEEP_MS = 30_000;

// ── Sync state (exposed for health check + /status) ──────────────────

/** Per-chain sync state, read by the API layer for health and status. */
export interface SyncState {
  startedAt: number;
  lastSuccessAt: number | null;
  lastSyncedBlock: number | null;
  latestChainBlock: number | null;
  observedBlockTimeMs: number;
  lastError: string | null;
  lastErrorAt: number | null;
  syncsPerformed: number;
  blocksIngested: number;
}

const syncStates = new Map<number, SyncState>();

/** Returns the current sync state for a chain (never null). */
export function getSyncState(chain: Chain): SyncState {
  return syncStates.get(chain.id) ?? {
    startedAt: Date.now(),
    lastSuccessAt: null,
    lastSyncedBlock: null,
    latestChainBlock: null,
    observedBlockTimeMs: 1_000,
    lastError: null,
    lastErrorAt: null,
    syncsPerformed: 0,
    blocksIngested: 0,
  };
}

// ── Lifecycle ────────────────────────────────────────────────────────

const controllers = new Map<string, AbortController>();
const loopPromises: Promise<void>[] = [];
let checkpointTimer: ReturnType<typeof setInterval> | null = null;

/** Starts a sync worker for every configured chain. */
export function startAll() {
  for (const chain of CHAINS) {
    syncStates.set(chain.id, {
      startedAt: Date.now(),
      lastSuccessAt: null,
      lastSyncedBlock: lastBlock(chain),
      latestChainBlock: null,
      observedBlockTimeMs: observedBlockTimeMs(chain),
      lastError: null,
      lastErrorAt: null,
      syncsPerformed: 0,
      blocksIngested: 0,
    });

    const ac = new AbortController();
    controllers.set(chain.name, ac);
    loopPromises.push(syncLoop(chain, ac.signal));
  }

  checkpointTimer = setInterval(checkpointAll, CHECKPOINT_INTERVAL_MS);
  log("info", `started ${CHAINS.length} sync workers`, { chain: "all" });
}

/** Stops all sync workers, awaits loop exit, and runs a final WAL checkpoint. */
export async function stopAll() {
  if (checkpointTimer) {
    clearInterval(checkpointTimer);
    checkpointTimer = null;
  }
  for (const ac of controllers.values()) ac.abort();
  controllers.clear();
  await Promise.allSettled(loopPromises);
  loopPromises.length = 0;
  checkpointAll();
}

// ── Sync loop ────────────────────────────────────────────────────────

async function syncLoop(chain: Chain, signal: AbortSignal) {
  // Stagger start so chains don't all hit their RPCs simultaneously.
  await sleep(Math.random() * 2_000, signal);

  let cachedLatest: number | null = null;

  while (!signal.aborted) {
    const state = syncStates.get(chain.id)!;

    try {
      const { catching, latest } = await syncOnce(chain, cachedLatest, signal);
      state.lastError = null;
      state.lastErrorAt = null;
      state.lastSuccessAt = Date.now();

      if (catching) {
        // During catch-up: reuse chain tip, skip sleep and block time refresh.
        cachedLatest = latest;
      } else {
        // Caught up: refresh observed block time from fresh data, then poll.
        cachedLatest = null;
        state.observedBlockTimeMs = observedBlockTimeMs(chain);
        await sleep(state.observedBlockTimeMs, signal);
      }
    } catch (err: any) {
      if (signal.aborted) return;

      cachedLatest = null;
      state.lastError = err.message;
      state.lastErrorAt = Date.now();

      log("error", err.message, { chain: chain.name });
      await sleep(Math.min(state.observedBlockTimeMs * 3, MAX_ERROR_SLEEP_MS), signal);
    }
  }
}

/**
 * Fetches and inserts the next chunk of blocks.
 *
 * @param cachedLatest - Reuse a previously fetched chain tip to avoid
 *   an RPC round trip per chunk during catch-up.
 * @returns `catching: true` if there are more blocks to sync.
 */
async function syncOnce(
  chain: Chain,
  cachedLatest: number | null,
  signal: AbortSignal,
): Promise<{ catching: boolean; latest: number }> {
  const latest = cachedLatest ?? (await getLatestBlockNumber(chain, signal));
  const stored = lastBlock(chain);
  const from = stored !== null ? stored + 1 : 0;

  const state = syncStates.get(chain.id)!;
  state.latestChainBlock = latest;
  state.lastSyncedBlock = stored;

  if (from > latest) return { catching: false, latest };

  const gap = latest - from + 1;
  const to = Math.min(from + SYNC_CHUNK_SIZE - 1, latest);

  log("info", `syncing ${from}→${to} (${gap.toLocaleString()} behind)`, { chain: chain.name });

  const blocks = await fetchBlockTimestamps(chain, from, to, signal);

  if (blocks.length > 0) {
    insertBlocks(chain, blocks);
    state.lastSyncedBlock = blocks[blocks.length - 1].number;
    state.syncsPerformed++;
    state.blocksIngested += blocks.length;
  } else {
    throw new Error(`got 0 blocks for range ${from}→${to}`);
  }

  return { catching: to < latest, latest };
}

// ── Helpers ──────────────────────────────────────────────────────────

/** Sleep that resolves immediately when the abort signal fires. */
function sleep(ms: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return Promise.race([
    Bun.sleep(ms),
    new Promise<void>((resolve) =>
      signal.addEventListener("abort", () => resolve(), { once: true }),
    ),
  ]);
}
