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

import contextlib
import os
import sqlite3
import time

from chronoblock.config import settings
from chronoblock.errors import DataDirError
from chronoblock.log import log
from chronoblock.models import Block, Chain

__all__ = [
    "get_timestamps",
    "last_block",
    "block_count",
    "observed_block_time_ms",
    "insert_blocks",
    "checkpoint_all",
    "is_healthy",
    "close_all",
]

DEFAULT_BLOCK_TIME_MS = 1_000
BLOCK_TIME_SAMPLE_SIZE = 50
COUNT_CACHE_TTL = 5.0  # seconds

_data_dir_verified = False


def _ensure_data_dir() -> None:
    global _data_dir_verified
    if _data_dir_verified:
        return
    try:
        os.makedirs(settings.data_dir, exist_ok=True)
        test_path = os.path.join(settings.data_dir, ".write_test")
        with open(test_path, "w") as f:
            f.write("")
        os.remove(test_path)
    except OSError as err:
        raise DataDirError(f'data directory "{settings.data_dir}" is not writable: {err}') from err
    _data_dir_verified = True


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
    _ensure_data_dir()
    existing = _stores.get(chain.id)
    if existing is not None:
        return existing

    file_path = os.path.join(settings.data_dir, f"{chain.name}.db")
    conn = sqlite3.connect(file_path, check_same_thread=False)

    try:
        result = conn.execute("PRAGMA journal_mode = WAL").fetchone()
        if result is None or result[0].lower() != "wal":
            raise DataDirError(f"WAL mode not supported for {file_path}")

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
    except Exception:
        conn.close()
        raise

    store = _ChainDB(conn, file_path)
    _stores[chain.id] = store
    return store


# ── Read ─────────────────────────────────────────────────────────────


def get_timestamps(chain: Chain, block_numbers: list[int]) -> list[int | None]:
    store = _open(chain)

    if len(block_numbers) <= 20:
        results: list[int | None] = []
        for bn in block_numbers:
            row = store.connection.execute("SELECT timestamp FROM blocks WHERE block_number = ?", (bn,)).fetchone()
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

    block_time: float = (span_secs / intervals) * 1000
    return max(200.0, min(block_time, 30_000.0))


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
        for pragma in ("PRAGMA optimize", "PRAGMA wal_checkpoint(PASSIVE)", "PRAGMA wal_checkpoint(TRUNCATE)"):
            try:
                store.connection.execute(pragma)
            except sqlite3.Error as err:
                log("warn", "checkpoint pragma failed", chain_id=chain_id, pragma=pragma, error=str(err))


def is_healthy(chain: Chain) -> bool:
    try:
        store = _open(chain)
        if not os.path.exists(store.file_path):
            return False
        store.connection.execute("SELECT COUNT(*) FROM blocks").fetchone()
        return True
    except (sqlite3.Error, OSError) as err:
        log("warn", "health check failed", chain=chain.name, error=str(err))
        return False


# ── Lifecycle ────────────────────────────────────────────────────────


def close_all() -> None:
    global _data_dir_verified
    for store in _stores.values():
        with contextlib.suppress(Exception):
            store.connection.execute("PRAGMA optimize")
        with contextlib.suppress(Exception):
            store.connection.close()
    _stores.clear()
    _data_dir_verified = False
