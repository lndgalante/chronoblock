"""Database layer tests."""

from __future__ import annotations

import sqlite3
from types import SimpleNamespace

import pytest

from chronoblock import db
from chronoblock.models import Block, Chain

CHAIN = Chain(
    id=99999,
    name="testchain",
    rpc="http://test.test",
    rpc_batch_size=10,
    rpc_concurrency=1,
    finality_blocks=10,
)


@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    """Each test gets its own data directory and clean connection pool."""
    monkeypatch.setattr(db.config, "settings", SimpleNamespace(data_dir=str(tmp_path)))
    yield
    db.close_all()


class TestInsertAndRead:
    def test_round_trip_single_block(self):
        db.insert_blocks(CHAIN, [Block(100, 1_700_000_000)])
        assert db.get_timestamps(CHAIN, [100]) == [1_700_000_000]

    def test_round_trip_multiple_blocks(self):
        blocks = [Block(i, 1_700_000_000 + i) for i in range(5)]
        db.insert_blocks(CHAIN, blocks)
        assert db.get_timestamps(CHAIN, list(range(5))) == [1_700_000_000 + i for i in range(5)]

    def test_missing_block_returns_none(self):
        db.insert_blocks(CHAIN, [Block(1, 1000)])
        assert db.get_timestamps(CHAIN, [1, 2, 3]) == [1000, None, None]

    def test_chunked_path_for_large_queries(self):
        """Queries >20 blocks use the chunked IN(...) path."""
        blocks = [Block(i, 2000 + i) for i in range(25)]
        db.insert_blocks(CHAIN, blocks)
        result = db.get_timestamps(CHAIN, list(range(25)))
        assert result == [2000 + i for i in range(25)]

    def test_duplicate_insert_keeps_first(self):
        db.insert_blocks(CHAIN, [Block(1, 1000)])
        db.insert_blocks(CHAIN, [Block(1, 9999)])
        assert db.get_timestamps(CHAIN, [1]) == [1000]


class TestLastBlock:
    def test_empty_db(self):
        assert db.last_block(CHAIN) is None

    def test_returns_max_block_number(self):
        db.insert_blocks(CHAIN, [Block(10, 1000), Block(5, 900), Block(20, 1100)])
        assert db.last_block(CHAIN) == 20


class TestBlockCount:
    def test_empty_db(self):
        assert db.block_count(CHAIN) == 0

    def test_correct_count(self):
        db.insert_blocks(CHAIN, [Block(i, 1000 + i) for i in range(10)])
        assert db.block_count(CHAIN) == 10

    def test_cache_returns_stale_value_within_ttl(self):
        db.block_count(CHAIN)  # primes cache with 0
        db.insert_blocks(CHAIN, [Block(1, 1000)])
        assert db.block_count(CHAIN) == 0  # still cached


class TestObservedBlockTimeMs:
    def test_empty_db_returns_default(self):
        assert db.observed_block_time_ms(CHAIN) == 1000.0

    def test_single_row_returns_default(self):
        db.insert_blocks(CHAIN, [Block(1, 1000)])
        assert db.observed_block_time_ms(CHAIN) == 1000.0

    def test_calculates_from_timestamp_span(self):
        # 10 blocks, each 12 seconds apart (like Ethereum)
        blocks = [Block(i, 1000 + i * 12) for i in range(10)]
        db.insert_blocks(CHAIN, blocks)
        assert db.observed_block_time_ms(CHAIN) == 12_000.0

    def test_zero_span_returns_default(self):
        blocks = [Block(i, 1000) for i in range(10)]
        db.insert_blocks(CHAIN, blocks)
        assert db.observed_block_time_ms(CHAIN) == 1000.0

    def test_clamps_to_minimum_200ms(self):
        # 50 blocks, span of 1 second: (1/49)*1000 = 20.4ms → clamped to 200
        blocks = [Block(i, 1000) for i in range(49)] + [Block(49, 1001)]
        db.insert_blocks(CHAIN, blocks)
        assert db.observed_block_time_ms(CHAIN) == 200.0

    def test_clamps_to_maximum_30s(self):
        # 50 blocks, 60 seconds apart: (2940/49)*1000 = 60000ms → clamped to 30000
        blocks = [Block(i, 1000 + i * 60) for i in range(50)]
        db.insert_blocks(CHAIN, blocks)
        assert db.observed_block_time_ms(CHAIN) == 30_000.0


class TestObservedBlockTimeMsNonContiguous:
    def test_non_contiguous_returns_default(self):
        # Insert blocks with a gap so newest - oldest != count - 1
        blocks = [Block(0, 1000), Block(1, 1012), Block(3, 1036)]
        db.insert_blocks(CHAIN, blocks)
        assert db.observed_block_time_ms(CHAIN) == 1000.0


class TestEnsureDataDir:
    def test_raises_on_unwritable_dir(self, monkeypatch, tmp_path):
        db.close_all()
        readonly = tmp_path / "readonly"
        readonly.mkdir()
        readonly.chmod(0o444)
        target = str(readonly / "subdir")
        monkeypatch.setattr(db.config, "settings", SimpleNamespace(data_dir=target))
        from chronoblock.errors import DataDirError

        try:
            with pytest.raises(DataDirError, match="not writable"):
                db._ensure_data_dir()
        finally:
            readonly.chmod(0o755)


class TestIsHealthy:
    def test_healthy_after_insert(self):
        db.insert_blocks(CHAIN, [Block(1, 1000)])
        assert db.is_healthy(CHAIN) is True

    def test_healthy_empty_db(self):
        db.get_timestamps(CHAIN, [1])  # triggers _open and table creation
        assert db.is_healthy(CHAIN) is True

    def test_unhealthy_when_connection_closed(self):
        db.insert_blocks(CHAIN, [Block(1, 1000)])
        db._stores[CHAIN.id].connection.close()
        assert db.is_healthy(CHAIN) is False


class TestCloseAll:
    def test_resets_verified_flag_and_clears_stores(self):
        db.insert_blocks(CHAIN, [Block(1, 1000)])
        assert CHAIN.id in db._stores
        assert db._data_dir_verified is True

        db.close_all()

        assert len(db._stores) == 0
        assert db._data_dir_verified is False

    def test_can_reopen_after_close(self):
        db.insert_blocks(CHAIN, [Block(1, 1000)])
        db.close_all()

        db.insert_blocks(CHAIN, [Block(2, 2000)])
        assert db.get_timestamps(CHAIN, [2]) == [2000]


class TestCheckpointAll:
    def test_handles_closed_connection(self):
        db.insert_blocks(CHAIN, [Block(1, 1000)])
        db._stores[CHAIN.id].connection.close()
        db.checkpoint_all()


class TestWarmCaches:
    def test_scans_chain_table(self):
        db.insert_blocks(CHAIN, [Block(1, 1000), Block(2, 2000)])
        db.warm_caches([CHAIN])


class TestOpenErrorPaths:
    def test_wal_mode_not_supported(self, monkeypatch):
        from unittest.mock import MagicMock

        from chronoblock.errors import DataDirError

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = ("journal",)
        mock_conn = MagicMock()
        mock_conn.execute.return_value = mock_cursor

        monkeypatch.setattr("sqlite3.connect", lambda *a, **kw: mock_conn)

        chain = Chain(id=88888, name="waltest", rpc="http://test.test", rpc_batch_size=10, rpc_concurrency=1, finality_blocks=10)
        with pytest.raises(DataDirError, match="WAL mode not supported"):
            db._open(chain)

        mock_conn.close.assert_called_once()

    def test_exception_during_setup_closes_connection(self, monkeypatch):
        from unittest.mock import MagicMock

        call_count = 0

        def flaky_execute(sql, *args):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                cursor = MagicMock()
                cursor.fetchone.return_value = ("wal",)
                return cursor
            raise sqlite3.OperationalError("disk I/O error")

        mock_conn = MagicMock()
        mock_conn.execute = flaky_execute

        monkeypatch.setattr("sqlite3.connect", lambda *a, **kw: mock_conn)

        chain = Chain(id=77777, name="errortest", rpc="http://test.test", rpc_batch_size=10, rpc_concurrency=1, finality_blocks=10)
        with pytest.raises(sqlite3.OperationalError, match="disk I/O"):
            db._open(chain)

        mock_conn.close.assert_called_once()


class TestIsHealthyFileMissing:
    def test_returns_false_when_db_file_deleted(self):
        db.insert_blocks(CHAIN, [Block(1, 1000)])
        store = db._stores[CHAIN.id]
        store.file_path.unlink()
        assert db.is_healthy(CHAIN) is False


class TestCloseAllErrorPaths:
    def test_handles_close_failure(self):
        db.insert_blocks(CHAIN, [Block(1, 1000)])
        store = db._stores[CHAIN.id]
        store.connection.close()

        # connection is already closed, calling close_all will fail on
        # PRAGMA optimize (already tested) AND on connection.close()
        # Force a scenario where optimize succeeds but close fails
        from unittest.mock import MagicMock

        mock_conn = MagicMock()
        mock_conn.execute.return_value = None  # PRAGMA optimize succeeds
        mock_conn.close.side_effect = sqlite3.ProgrammingError("already closed")
        store.connection = mock_conn

        db.close_all()
        assert len(db._stores) == 0


class TestGetManySql:
    def test_caches_sql_by_size(self):
        db.insert_blocks(CHAIN, [Block(1, 1000)])
        store = db._stores[CHAIN.id]
        sql_a = store.get_many_sql(5)
        sql_b = store.get_many_sql(5)
        assert sql_a is sql_b

    def test_generates_correct_placeholders(self):
        db.insert_blocks(CHAIN, [Block(1, 1000)])
        store = db._stores[CHAIN.id]
        assert store.get_many_sql(3).count("?") == 3
        assert store.get_many_sql(10).count("?") == 10
