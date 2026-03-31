/**
 * @module config
 *
 * Chain definitions and environment-driven settings. Validated at
 * import time — invalid config crashes the process before the server
 * starts, so deploy pipelines fail fast.
 */

// ── Types ────────────────────────────────────────────────────────────

/** An EVM-compatible chain the service tracks. */
export interface Chain {
  /** On-chain chain ID (e.g. 1 for Ethereum). */
  id: number;
  /** Lowercase slug used in API routes and DB filenames. */
  name: string;
  /** JSON-RPC endpoint URL. */
  rpc: string;
  /** Max blocks per JSON-RPC batch request. Most RPCs cap at 100. */
  rpcBatchSize: number;
  /** Max concurrent batch requests in flight per sync cycle. */
  rpcConcurrency: number;
  /** Blocks behind chain tip before a block is considered finalized. */
  finalityBlocks: number;
}

/** A block's number and unix timestamp. */
export interface Block {
  number: number;
  timestamp: number;
}

// ── Chains ───────────────────────────────────────────────────────────

/** All supported chains. Only those with an RPC URL in the environment are enabled. */
const CHAIN_CANDIDATES = [
  { id: 1, name: "ethereum", envVar: "ETH_RPC_URL", rpcBatchSize: 100, rpcConcurrency: 5, finalityBlocks: 64 },
  { id: 534352, name: "scroll", envVar: "SCROLL_RPC_URL", rpcBatchSize: 100, rpcConcurrency: 5, finalityBlocks: 300 },
  { id: 57073, name: "ink", envVar: "INK_RPC_URL", rpcBatchSize: 100, rpcConcurrency: 5, finalityBlocks: 300 },
  { id: 998, name: "hyperevm", envVar: "HYPEREVM_RPC_URL", rpcBatchSize: 100, rpcConcurrency: 5, finalityBlocks: 300 },
] as const;

export const CHAINS: Chain[] = CHAIN_CANDIDATES
  .filter((c) => process.env[c.envVar])
  .map((c) => ({
    id: c.id,
    name: c.name,
    rpc: process.env[c.envVar]!,
    rpcBatchSize: c.rpcBatchSize,
    rpcConcurrency: c.rpcConcurrency,
    finalityBlocks: c.finalityBlocks,
  }));

// ── Lookup maps ──────────────────────────────────────────────────────

export const CHAIN_BY_NAME = new Map(CHAINS.map((c) => [c.name, c]));
export const CHAIN_BY_ID = new Map(CHAINS.map((c) => [c.id, c]));

// ── Environment ──────────────────────────────────────────────────────

export const DATA_DIR = process.env.DATA_DIR ?? "./data";
export const PORT = parseInt(process.env.PORT ?? "3000", 10);

/** Max seconds a chain can lag before `/health` returns 503. */
export const HEALTH_MAX_LAG_SECS = parseInt(
  process.env.HEALTH_MAX_LAG_SECS ?? "120",
  10,
);

/** Blocks fetched per sync cycle. Higher = faster catch-up, larger DB transactions. */
export const SYNC_CHUNK_SIZE = parseInt(
  process.env.SYNC_CHUNK_SIZE ?? "2000",
  10,
);

// ── Validation (fail fast on startup) ────────────────────────────────

const errors: string[] = [];

if (!Number.isInteger(PORT) || PORT < 1 || PORT > 65535)
  errors.push(`PORT must be 1–65535, got: ${process.env.PORT}`);
if (!Number.isInteger(HEALTH_MAX_LAG_SECS) || HEALTH_MAX_LAG_SECS < 1)
  errors.push(`HEALTH_MAX_LAG_SECS must be ≥1, got: ${process.env.HEALTH_MAX_LAG_SECS}`);
if (!Number.isInteger(SYNC_CHUNK_SIZE) || SYNC_CHUNK_SIZE < 1)
  errors.push(`SYNC_CHUNK_SIZE must be ≥1, got: ${process.env.SYNC_CHUNK_SIZE}`);

const seenIds = new Set<number>();
const seenNames = new Set<string>();

for (const c of CHAINS) {
  if (seenIds.has(c.id)) errors.push(`duplicate chain id: ${c.id}`);
  if (seenNames.has(c.name)) errors.push(`duplicate chain name: ${c.name}`);
  if (!c.rpc.startsWith("http")) errors.push(`${c.name}: rpc must start with http, got: ${c.rpc}`);
  if (c.rpcBatchSize < 1 || c.rpcBatchSize > 1000)
    errors.push(`${c.name}: rpcBatchSize must be 1–1000, got: ${c.rpcBatchSize}`);
  if (c.rpcConcurrency < 1 || c.rpcConcurrency > 50)
    errors.push(`${c.name}: rpcConcurrency must be 1–50, got: ${c.rpcConcurrency}`);
  seenIds.add(c.id);
  seenNames.add(c.name);
}

if (CHAINS.length === 0)
  errors.push("no chains enabled — set at least one RPC URL (e.g. ETH_RPC_URL)");

if (errors.length > 0) {
  console.error("fatal: config validation failed");
  for (const e of errors) console.error(`  - ${e}`);
  process.exit(1);
}

const skipped = CHAIN_CANDIDATES.filter((c) => !process.env[c.envVar]);
if (skipped.length > 0)
  console.warn(`disabled chains (no RPC): ${skipped.map((c) => c.name).join(", ")}`);
