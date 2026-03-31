/**
 * @module index
 *
 * Entry point. Builds the Hono API and, when run directly, starts the
 * background sync workers plus the HTTP server.
 */

import { Hono } from "hono";
import { bodyLimit } from "hono/body-limit";
import { secureHeaders } from "hono/secure-headers";
import { requestId } from "hono/request-id";
import { trimTrailingSlash } from "hono/trailing-slash";
import {
  CHAINS,
  CHAIN_BY_NAME,
  CHAIN_BY_ID,
  PORT,
  HEALTH_MAX_LAG_SECS,
  type Chain,
} from "./config";
import {
  getTimestamps,
  blockCount,
  isHealthy,
  closeAll,
} from "./db";
import { startAll, stopAll, getSyncState, type SyncState } from "./syncer";
import { log } from "./log";

export const INITIAL_SYNC_GRACE_MS = 5 * 60_000;

interface AppDeps {
  getTimestamps: typeof getTimestamps;
  blockCount: typeof blockCount;
  isHealthy: typeof isHealthy;
  getSyncState: typeof getSyncState;
  log: typeof log;
  now: () => number;
}

const defaultAppDeps: AppDeps = {
  getTimestamps,
  blockCount,
  isHealthy,
  getSyncState,
  log,
  now: () => Date.now(),
};

const BLOCK_PARAM_RE = /^\d+$/;

/** Returns a structured error JSON response. */
function errorResponse(c: any, status: number, code: string, message: string) {
  return c.json({ error: code, message, request_id: c.get("requestId") }, status);
}

/** Resolves a chain by name (string) or chain_id (number). */
export function resolveChain(name?: string, id?: number): Chain | undefined {
  if (typeof name === "string" && name.length > 0)
    return CHAIN_BY_NAME.get(name.toLowerCase());
  if (typeof id === "number" && Number.isInteger(id))
    return CHAIN_BY_ID.get(id);
  return undefined;
}

function shouldDegradeChain(sync: SyncState, now: number): string | null {
  if (sync.lastSuccessAt === null) {
    if (sync.lastError) return sync.lastError;
    if (now - sync.startedAt > INITIAL_SYNC_GRACE_MS)
      return `initial sync exceeded ${Math.round(INITIAL_SYNC_GRACE_MS / 1000)}s grace period`;
    return null;
  }

  if (sync.lastSyncedBlock === null || sync.latestChainBlock === null)
    return "sync state incomplete";

  const lagBlocks = sync.latestChainBlock - sync.lastSyncedBlock;
  const lagSecs = (lagBlocks * sync.observedBlockTimeMs) / 1000;
  if (lagSecs > HEALTH_MAX_LAG_SECS)
    return `${lagBlocks} blocks behind (~${Math.round(lagSecs)}s)`;

  return null;
}

export function createApp(deps: AppDeps = defaultAppDeps) {
  const app = new Hono();

  app.use(trimTrailingSlash());
  app.use(secureHeaders());
  app.use(requestId());

  app.use(async (c, next) => {
    const start = deps.now();
    await next();
    if (c.req.path === "/health") return;
    deps.log("info", `${c.req.method} ${c.req.path} ${c.res.status}`, {
      method: c.req.method,
      path: c.req.path,
      status: c.res.status,
      duration: deps.now() - start,
      requestId: c.get("requestId"),
    });
  });

  app.onError((err, c) => {
    deps.log("error", "unhandled route error", {
      error: err.message,
      path: c.req.path,
      requestId: c.get("requestId"),
    });
    return errorResponse(c, 500, "internal_error", "internal server error");
  });

  app.get("/health", (c) => {
    c.header("Cache-Control", "no-store");
    const degraded: string[] = [];
    const now = deps.now();

    for (const ch of CHAINS) {
      if (!deps.isHealthy(ch)) {
        degraded.push(`${ch.name}: db unreadable`);
        continue;
      }

      const reason = shouldDegradeChain(deps.getSyncState(ch), now);
      if (reason) degraded.push(`${ch.name}: ${reason}`);
    }

    if (degraded.length > 0) return c.json({ ok: false, degraded }, 503);
    return c.json({ ok: true });
  });

  const v1 = new Hono();

  v1.post("/timestamps", bodyLimit({ maxSize: 1_048_576 }), async (c) => {
    const contentType = c.req.header("content-type") ?? "";
    if (!contentType.includes("application/json")) {
      return errorResponse(
        c,
        415,
        "unsupported_media_type",
        "Content-Type must be application/json",
      );
    }

    const body = await c.req.json().catch(() => null);
    if (!body || typeof body !== "object" || Array.isArray(body))
      return errorResponse(c, 400, "invalid_json", "invalid JSON");

    const payload = body as {
      chain?: string;
      chain_id?: number;
      blocks?: unknown;
    };

    const chain = resolveChain(payload.chain, payload.chain_id);
    if (!chain) return errorResponse(c, 400, "unknown_chain", "unknown chain");

    const blocks = payload.blocks;
    if (!Array.isArray(blocks) || blocks.length === 0)
      return errorResponse(c, 400, "invalid_blocks", "blocks must be a non-empty array");
    if (blocks.length > 10_000)
      return errorResponse(c, 400, "invalid_blocks", "max 10,000 blocks per request");

    for (let i = 0; i < blocks.length; i++) {
      if (!Number.isSafeInteger(blocks[i]) || blocks[i] < 0) {
        return errorResponse(
          c,
          400,
          "invalid_block_number",
          `invalid block number at index ${i}: ${blocks[i]}`,
        );
      }
    }

    const validated = blocks as number[];
    const ts = deps.getTimestamps(chain, validated);

    const results: Record<string, number | null> = {};
    for (let i = 0; i < validated.length; i++) results[validated[i]] = ts[i];

    return c.json({ chain_id: chain.id, results });
  });

  v1.get("/timestamps/:chain/:block", (c) => {
    const chain = CHAIN_BY_NAME.get(c.req.param("chain").toLowerCase());
    if (!chain) return errorResponse(c, 400, "unknown_chain", "unknown chain");

    const blockParam = c.req.param("block");
    if (!BLOCK_PARAM_RE.test(blockParam))
      return errorResponse(c, 400, "invalid_block_number", "invalid block number");

    const bn = Number(blockParam);
    if (!Number.isSafeInteger(bn))
      return errorResponse(c, 400, "invalid_block_number", "invalid block number");

    const [ts] = deps.getTimestamps(chain, [bn]);
    if (ts === null) return errorResponse(c, 404, "not_found", "block not found");

    const sync = deps.getSyncState(chain);
    const finalized = sync.latestChainBlock !== null
      && bn <= sync.latestChainBlock - chain.finalityBlocks;
    c.header(
      "Cache-Control",
      finalized ? "public, max-age=31536000, immutable" : "public, max-age=60",
    );

    return c.json({ chain_id: chain.id, block_number: bn, timestamp: ts });
  });

  v1.get("/status", (c) => {
    c.header("Cache-Control", "no-store");
    return c.json({
      chains: CHAINS.map((ch) => {
        const sync = deps.getSyncState(ch);
        const lagBlocks =
          sync.latestChainBlock !== null && sync.lastSyncedBlock !== null
            ? sync.latestChainBlock - sync.lastSyncedBlock
            : null;

        return {
          name: ch.name,
          chain_id: ch.id,
          last_synced_block: sync.lastSyncedBlock,
          latest_chain_block: sync.latestChainBlock,
          lag_blocks: lagBlocks,
          observed_block_time_ms: sync.observedBlockTimeMs,
          total_stored: deps.blockCount(ch),
          syncs_performed: sync.syncsPerformed,
          blocks_ingested: sync.blocksIngested,
          last_error: sync.lastError,
          last_error_at: sync.lastErrorAt
            ? new Date(sync.lastErrorAt).toISOString()
            : null,
        };
      }),
    });
  });

  app.route("/v1", v1);
  return app;
}

export const app = createApp();

export function startServer() {
  startAll();

  let stopping = false;

  async function shutdown(code: number) {
    if (stopping) return;
    stopping = true;

    log("info", "shutting down", { code });
    await stopAll();
    closeAll();
    process.exit(code);
  }

  process.on("unhandledRejection", (err) => {
    log("error", "unhandled rejection, exiting", { error: String(err) });
    void shutdown(1);
  });

  process.on("SIGINT", () => {
    void shutdown(0);
  });
  process.on("SIGTERM", () => {
    void shutdown(0);
  });

  Bun.serve({ port: PORT, fetch: app.fetch });
  log("info", `listening on :${PORT}`, { chains: CHAINS.map((c) => c.name) });
}

if (import.meta.main) startServer();
