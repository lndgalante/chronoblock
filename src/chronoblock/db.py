"""
Per-chain SQLite storage. Each chain gets its own .db file in DATA_DIR,
enabling independent backup, deletion, and zero contention between chains.

Performance notes:
- WITHOUT ROWID stores (block_number, timestamp) directly in B-tree leaves.
- WAL mode allows concurrent readers while the syncer writes.
- 64 MB page cache + 1 GB mmap = most reads hit memory after warm-up.
- Python's sqlite3 module caches prepared statements internally.
"""

from __future__ import annotations

import os
import sqlite3
import sys
import time

from chronoblock.config import Chain, Block, settings
from chronoblock.log import log

try:
    os.makedirs(settings.data_dir, exist_ok=True)
    # Verify writable
    test_path = os.path.join(settings.data_dir, ".write_test")
    with open(test_path, "w") as f:
        f.write("")
    os.remove(test_path)
except OSError as err:
    print(f'fatal: data directory "{settings.data_dir}" is not writable: {err}', file=sys.stderr)
    sys.exit(1)


DEFAULT_BLOCK_TIME_MS = 1_000
BLOCK_TIME_SAMPLE_SIZE = 50
COUNT_CACHE_TTL = 5.0  # seconds


class _ChainDB:
    __slots__ = ("connection", "file_path", "cached_count", "cached_count_at", "_get_many_sql")

    def __init__(self, connection: sqlite3.Connection, file_path: str) -> None:
        self.connection = connection
        self.file_path = file_path
        self.cached_count = 0
        self.cached_count_at = 0.0
        self._get_many_sql: dict[int, str] = {}

    def get_many_sql(self, size: int) -> str:
        sql = self._get_many_sql.get(size)
        if sql is not None:
            return sql
        placeholders = ",".join("?" * size)
        sql = f"SELECT block_number, timestamp FROM blocks WHERE block_number IN ({placeholders})"
        self._get_many_sql[size] = sql
        return sql


_stores: dict[int, _ChainDB] = {}


def _open(chain: Chain) -> _ChainDB:
    existing = _stores.get(chain.id)
    if existing is not None:
        return existing

    file_path = os.path.join(settings.data_dir, f"{chain.name}.db")
    conn = sqlite3.connect(file_path, check_same_thread=False)

    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA cache_size = -64000")
    conn.execute("PRAGMA mmap_size = 1073741824")
    conn.execute("PRAGMA temp_store = MEMORY")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS blocks (
            block_number INTEGER PRIMARY KEY,
            timestamp    INTEGER NOT NULL
        ) WITHOUT ROWID, STRICT
    """)

    store = _ChainDB(conn, file_path)
    _stores[chain.id] = store
    return store


# ── Read ─────────────────────────────────────────────────────────────


def get_timestamps(chain: Chain, block_numbers: list[int]) -> list[int | None]:
    store = _open(chain)

    if len(block_numbers) <= 20:
        results: list[int | None] = []
        for bn in block_numbers:
            row = store.connection.execute(
                "SELECT timestamp FROM blocks WHERE block_number = ?", (bn,)
            ).fetchone()
            results.append(row[0] if row else None)
        return results

    lookup: dict[int, int] = {}
    chunk_size = 500

    for i in range(0, len(block_numbers), chunk_size):
        chunk = block_numbers[i : i + chunk_size]
        sql = store.get_many_sql(len(chunk))
        rows = store.connection.execute(sql, chunk).fetchall()
        for block_number, timestamp in rows:
            lookup[block_number] = timestamp

    return [lookup.get(bn) for bn in block_numbers]


def last_block(chain: Chain) -> int | None:
    store = _open(chain)
    row = store.connection.execute("SELECT MAX(block_number) FROM blocks").fetchone()
    return row[0] if row and row[0] is not None else None


def block_count(chain: Chain) -> int:
    store = _open(chain)
    now = time.monotonic()

    if now - store.cached_count_at < COUNT_CACHE_TTL:
        return store.cached_count

    row = store.connection.execute("SELECT COUNT(*) FROM blocks").fetchone()
    count = row[0] if row else 0
    store.cached_count = count
    store.cached_count_at = now
    return count


def observed_block_time_ms(chain: Chain) -> float:
    store = _open(chain)
    rows = store.connection.execute(
        "SELECT timestamp FROM blocks ORDER BY block_number DESC LIMIT ?",
        (BLOCK_TIME_SAMPLE_SIZE,),
    ).fetchall()

    if len(rows) < 2:
        return DEFAULT_BLOCK_TIME_MS

    # rows[0] is newest, rows[-1] is oldest
    span_secs = rows[0][0] - rows[-1][0]
    intervals = len(rows) - 1

    if span_secs <= 0:
        log("warn", "block time sample has non-positive span, using default", chain=chain.name, span_secs=span_secs)
        return DEFAULT_BLOCK_TIME_MS

    return max(200.0, min((span_secs / intervals) * 1000, 30_000.0))


# ── Write ────────────────────────────────────────────────────────────


def insert_blocks(chain: Chain, blocks: list[Block]) -> None:
    store = _open(chain)
    store.connection.executemany(
        "INSERT OR IGNORE INTO blocks (block_number, timestamp) VALUES (?, ?)",
        blocks,
    )
    store.connection.commit()


# ── Maintenance ──────────────────────────────────────────────────────


def checkpoint_all() -> None:
    for chain_id, store in _stores.items():
        try:
            store.connection.execute("PRAGMA optimize")
            store.connection.execute("PRAGMA wal_checkpoint(PASSIVE)")
            store.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        except sqlite3.Error as err:
            log("warn", "checkpoint failed", chain_id=chain_id, error=str(err))


def is_healthy(chain: Chain) -> bool:
    try:
        store = _open(chain)
        if not os.path.exists(store.file_path):
            return False
        store.connection.execute("SELECT COUNT(*) FROM blocks").fetchone()
        return True
    except Exception:
        return False


# ── Lifecycle ────────────────────────────────────────────────────────


def close_all() -> None:
    for store in _stores.values():
        try:
            store.connection.execute("PRAGMA optimize")
        except Exception:
            pass
        try:
            store.connection.close()
        except Exception:
            pass
    _stores.clear()
