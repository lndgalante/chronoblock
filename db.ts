/**
 * @module db
 *
 * Per-chain SQLite storage. Each chain gets its own `.db` file in
 * DATA_DIR, enabling independent backup, deletion, and zero
 * contention between chains.
 *
 * Performance notes:
 * - WITHOUT ROWID stores (block_number, timestamp) directly in the
 *   B-tree leaves — no separate rowid table or index indirection.
 * - WAL mode allows concurrent readers while the syncer writes.
 * - 64 MB page cache + 1 GB mmap = most reads hit memory after
 *   a brief warm-up period.
 * - Prepared statements are cached per-chain, never re-compiled.
 */

import { Database, type Statement } from "bun:sqlite";
import { mkdirSync, accessSync, constants, existsSync } from "node:fs";
import { join } from "node:path";
import { DATA_DIR, type Chain, type Block } from "./config";
import { log } from "./log";

try {
  mkdirSync(DATA_DIR, { recursive: true });
  accessSync(DATA_DIR, constants.W_OK);
} catch (err: any) {
  console.error(`fatal: data directory "${DATA_DIR}" is not writable: ${err.message}`);
  process.exit(1);
}

/** Fallback when <2 blocks are stored. 1s is a safe floor. */
const DEFAULT_BLOCK_TIME_MS = 1_000;

/** How many recent blocks to sample for block time estimation. */
const BLOCK_TIME_SAMPLE_SIZE = 50;

// ── Internal ─────────────────────────────────────────────────────────

interface ChainDB {
  db: Database;
  filePath: string;
  stmtGet: Statement;
  stmtGetMany: Map<number, Statement>;
  stmtInsert: Statement;
  stmtMax: Statement;
  stmtCount: Statement;
  stmtBlockTime: Statement;
  cachedCount: number;
  cachedCountAt: number;
}

/** Interval (ms) before `blockCount` re-queries instead of using cache. */
const COUNT_CACHE_TTL_MS = 5_000;

const stores = new Map<number, ChainDB>();

/**
 * Opens (or returns a cached handle for) a chain's database.
 * Creates the schema on first open.
 */
function open(chain: Chain): ChainDB {
  const existing = stores.get(chain.id);
  if (existing) return existing;

  const filePath = join(DATA_DIR, `${chain.name}.db`);
  const db = new Database(filePath, { create: true });

  // Performance pragmas — safe for a single-writer workload.
  db.run("PRAGMA journal_mode = WAL");
  db.run("PRAGMA synchronous = NORMAL");
  db.run("PRAGMA busy_timeout = 5000");    // tolerate brief lock contention
  db.run("PRAGMA cache_size = -64000");     // 64 MB page cache
  db.run("PRAGMA mmap_size = 1073741824");  // 1 GB memory-mapped I/O
  db.run("PRAGMA temp_store = MEMORY");

  db.run(`
    CREATE TABLE IF NOT EXISTS blocks (
      block_number INTEGER PRIMARY KEY,
      timestamp    INTEGER NOT NULL
    ) WITHOUT ROWID, STRICT
  `);

  const store: ChainDB = {
    db,
    filePath,
    stmtGet: db.prepare("SELECT timestamp FROM blocks WHERE block_number = ?"),
    stmtGetMany: new Map(),
    stmtInsert: db.prepare("INSERT OR IGNORE INTO blocks (block_number, timestamp) VALUES (?, ?)"),
    stmtMax: db.prepare("SELECT MAX(block_number) AS m FROM blocks"),
    stmtCount: db.prepare("SELECT COUNT(*) AS c FROM blocks"),
    stmtBlockTime: db.prepare("SELECT timestamp FROM blocks ORDER BY block_number DESC LIMIT ?"),
    cachedCount: 0,
    cachedCountAt: 0,
  };

  stores.set(chain.id, store);
  return store;
}

// ── Read ─────────────────────────────────────────────────────────────

/**
 * Resolves an ordered list of block numbers to their unix timestamps.
 * Returns `null` for any block not yet stored.
 *
 * For small lists, uses a cached prepared statement per block. For
 * larger lists, builds a single `IN (...)` query to reduce JS↔C
 * boundary crossings (~3× faster at 10k blocks).
 */
export function getTimestamps(chain: Chain, blockNumbers: number[]): (number | null)[] {
  const store = open(chain);

  if (blockNumbers.length <= 20) {
    return blockNumbers.map(
      (bn) => (store.stmtGet.get(bn) as { timestamp: number } | null)?.timestamp ?? null,
    );
  }

  const map = new Map<number, number>();
  const CHUNK = 500;

  for (let i = 0; i < blockNumbers.length; i += CHUNK) {
    const slice = blockNumbers.slice(i, i + CHUNK);
    const rows = getManyStatement(store, slice.length).all(...slice) as {
      block_number: number;
      timestamp: number;
    }[];
    for (const r of rows) map.set(r.block_number, r.timestamp);
  }

  return blockNumbers.map((bn) => map.get(bn) ?? null);
}

/** Returns the highest block number stored, or `null` for an empty DB. */
export function lastBlock(chain: Chain): number | null {
  const { stmtMax } = open(chain);
  return (stmtMax.get() as { m: number | null } | null)?.m ?? null;
}

/**
 * Returns the total number of stored blocks.
 * Cached for 5 seconds — `COUNT(*)` is O(n) on SQLite and this
 * endpoint is hit by monitoring dashboards, not hot-path queries.
 */
export function blockCount(chain: Chain): number {
  const store = open(chain);
  const now = Date.now();

  if (now - store.cachedCountAt < COUNT_CACHE_TTL_MS) return store.cachedCount;

  const count = (store.stmtCount.get() as { c: number })?.c ?? 0;
  store.cachedCount = count;
  store.cachedCountAt = now;
  return count;
}

/**
 * Computes the average block time from the most recent N stored blocks.
 *
 * Used by the syncer to calibrate its poll interval and by the health
 * check to convert block lag into seconds. Self-adjusting: if a chain
 * changes its block time (e.g. Scroll 3s → 1s), the service adapts
 * within a few sync cycles with no config change or restart.
 *
 * Returns {@link DEFAULT_BLOCK_TIME_MS} when fewer than 2 blocks
 * are stored (initial sync). Result is clamped to [200ms, 30s].
 */
export function observedBlockTimeMs(chain: Chain): number {
  const { stmtBlockTime } = open(chain);
  const rows = stmtBlockTime.all(BLOCK_TIME_SAMPLE_SIZE) as { timestamp: number }[];

  if (rows.length < 2) return DEFAULT_BLOCK_TIME_MS;

  // Rows are DESC by block_number, so [0] is newest.
  const spanSecs = rows[0].timestamp - rows[rows.length - 1].timestamp;
  const intervals = rows.length - 1;

  if (spanSecs <= 0) {
    log("warn", "block time sample has non-positive span, using default", { chain: chain.name, spanSecs });
    return DEFAULT_BLOCK_TIME_MS;
  }

  return Math.max(200, Math.min((spanSecs / intervals) * 1000, 30_000));
}

// ── Write ────────────────────────────────────────────────────────────

/**
 * Inserts blocks in a single transaction. Uses `INSERT OR IGNORE`
 * so re-syncing a range is idempotent.
 */
export function insertBlocks(chain: Chain, blocks: Block[]) {
  const { db, stmtInsert } = open(chain);
  db.transaction((items: Block[]) => {
    for (const b of items) stmtInsert.run(b.number, b.timestamp);
  })(blocks);
}

// ── Maintenance ──────────────────────────────────────────────────────

/**
 * Checkpoints WAL on every open database. Called periodically to
 * prevent the WAL from growing during sustained catch-up writes.
 *
 * Uses PASSIVE first (moves pages without blocking, always succeeds),
 * then attempts TRUNCATE (requires exclusive WAL access) to reclaim
 * disk space. If TRUNCATE fails due to active readers, the PASSIVE
 * pass already made progress.
 */
export function checkpointAll() {
  for (const { db } of stores.values()) {
    try {
      db.run("PRAGMA optimize");
      db.run("PRAGMA wal_checkpoint(PASSIVE)");
      db.run("PRAGMA wal_checkpoint(TRUNCATE)");
    } catch {
      // TRUNCATE can transiently fail under readers — PASSIVE already
      // moved pages, so the WAL won't grow unbounded.
    }
  }
}

/** Returns `true` if the chain's DB is readable. Used by `/health`. */
export function isHealthy(chain: Chain): boolean {
  try {
    const store = open(chain);
    if (!existsSync(store.filePath)) return false;
    store.stmtCount.get();
    return true;
  } catch {
    return false;
  }
}

// ── Lifecycle ────────────────────────────────────────────────────────

/** Closes all database handles. Safe to call multiple times. */
export function closeAll() {
  for (const { db } of stores.values()) {
    try { db.run("PRAGMA optimize"); } catch { /* best-effort */ }
    try { db.close(); } catch { /* already closed */ }
  }
  stores.clear();
}

function getManyStatement(store: ChainDB, size: number): Statement {
  let stmt = store.stmtGetMany.get(size);
  if (stmt) return stmt;

  const placeholders = new Array(size).fill("?").join(",");
  stmt = store.db.prepare(
    `SELECT block_number, timestamp FROM blocks WHERE block_number IN (${placeholders})`,
  );
  store.stmtGetMany.set(size, stmt);
  return stmt;
}
