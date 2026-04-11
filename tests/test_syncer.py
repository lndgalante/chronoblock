"""Sync worker tests."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from chronoblock.errors import RpcRateLimitError, RpcResponseError
from chronoblock.models import Block, Chain
from chronoblock.syncer import (
    MAX_ERROR_SLEEP,
    MIN_ERROR_SLEEP,
    SyncState,
    _checkpoint_timer,
    _sync_loop,
    _sync_once,
    _sync_states,
    get_sync_state,
)

CHAIN = Chain(
    id=99999,
    name="testchain",
    rpc="http://test.test",
    rpc_batch_size=50,
    rpc_concurrency=2,
    finality_blocks=10,
)


@pytest.fixture(autouse=True)
def clean_sync_states():
    _sync_states.clear()
    yield
    _sync_states.clear()


# ── get_sync_state ──────────────────────────────────────────────────


class TestGetSyncState:
    def test_returns_registered_state(self):
        state = SyncState(last_synced_block=100)
        _sync_states[CHAIN.id] = state
        assert get_sync_state(CHAIN) is state

    def test_returns_default_for_unknown_chain(self):
        state = get_sync_state(CHAIN)
        assert state.last_synced_block is None
        assert state.syncs_performed == 0


# ── start_all / stop_all ───────────────────────────────────────────


class TestLifecycle:
    @patch("chronoblock.syncer.config")
    @patch("chronoblock.syncer.observed_block_time_ms", return_value=12_000.0)
    @patch("chronoblock.syncer.last_block", return_value=42)
    async def test_start_all_creates_tasks_and_states(self, mock_last, mock_bt, mock_config):
        from chronoblock.syncer import _tasks, start_all, stop_all

        mock_config.CHAINS = [CHAIN]
        mock_config.settings = SimpleNamespace(sync_chunk_size=100)

        await start_all()

        assert CHAIN.id in _sync_states
        state = _sync_states[CHAIN.id]
        assert state.last_synced_block == 42
        assert state.observed_block_time_ms == 12_000.0
        # 1 sync task + 1 checkpoint task
        assert len(_tasks) == 2

        await stop_all()
        assert len(_tasks) == 0

    @patch("chronoblock.syncer.config")
    @patch("chronoblock.syncer.observed_block_time_ms", return_value=1000.0)
    @patch("chronoblock.syncer.last_block", return_value=None)
    async def test_stop_all_cancels_tasks(self, mock_last, mock_bt, mock_config):
        from chronoblock.syncer import _tasks, start_all, stop_all

        mock_config.CHAINS = [CHAIN]
        mock_config.settings = SimpleNamespace(sync_chunk_size=100)

        await start_all()
        assert len(_tasks) > 0

        await stop_all()
        assert len(_tasks) == 0


# ── _sync_once ──────────────────────────────────────────────────────


class TestSyncOnce:
    @pytest.fixture(autouse=True)
    def setup_state(self):
        _sync_states[CHAIN.id] = SyncState()

    @patch("chronoblock.syncer.config", SimpleNamespace(settings=SimpleNamespace(sync_chunk_size=100)))
    @patch("chronoblock.syncer.insert_blocks")
    @patch("chronoblock.syncer.fetch_block_timestamps", new_callable=AsyncMock)
    @patch("chronoblock.syncer.get_latest_block_number", new_callable=AsyncMock)
    async def test_fetches_latest_when_no_cache(self, mock_latest, mock_fetch, mock_insert):
        mock_latest.return_value = 5
        mock_fetch.return_value = [Block(i, 1000 + i) for i in range(6)]

        await _sync_once(CHAIN, cached_latest=None)

        mock_latest.assert_called_once_with(CHAIN)

    @patch("chronoblock.syncer.config", SimpleNamespace(settings=SimpleNamespace(sync_chunk_size=100)))
    @patch("chronoblock.syncer.insert_blocks")
    @patch("chronoblock.syncer.fetch_block_timestamps", new_callable=AsyncMock)
    @patch("chronoblock.syncer.get_latest_block_number", new_callable=AsyncMock)
    async def test_skips_rpc_when_cached(self, mock_latest, mock_fetch, mock_insert):
        mock_fetch.return_value = [Block(i, 1000 + i) for i in range(6)]

        await _sync_once(CHAIN, cached_latest=5)

        mock_latest.assert_not_called()

    @patch("chronoblock.syncer.config", SimpleNamespace(settings=SimpleNamespace(sync_chunk_size=100)))
    @patch("chronoblock.syncer.insert_blocks")
    @patch("chronoblock.syncer.fetch_block_timestamps", new_callable=AsyncMock)
    @patch("chronoblock.syncer.get_latest_block_number", new_callable=AsyncMock)
    async def test_caught_up(self, mock_latest, mock_fetch, mock_insert):
        mock_latest.return_value = 5
        mock_fetch.return_value = [Block(i, 1000 + i) for i in range(6)]

        catching, latest = await _sync_once(CHAIN, cached_latest=None)

        assert catching is False
        assert latest == 5

    @patch("chronoblock.syncer.config", SimpleNamespace(settings=SimpleNamespace(sync_chunk_size=3)))
    @patch("chronoblock.syncer.insert_blocks")
    @patch("chronoblock.syncer.fetch_block_timestamps", new_callable=AsyncMock)
    @patch("chronoblock.syncer.get_latest_block_number", new_callable=AsyncMock)
    async def test_behind_returns_catching(self, mock_latest, mock_fetch, mock_insert):
        mock_latest.return_value = 50
        mock_fetch.return_value = [Block(i, 1000 + i) for i in range(3)]

        catching, latest = await _sync_once(CHAIN, cached_latest=None)

        assert catching is True
        assert latest == 50

    @patch("chronoblock.syncer.config", SimpleNamespace(settings=SimpleNamespace(sync_chunk_size=100)))
    @patch("chronoblock.syncer.insert_blocks")
    @patch("chronoblock.syncer.fetch_block_timestamps", new_callable=AsyncMock)
    @patch("chronoblock.syncer.get_latest_block_number", new_callable=AsyncMock)
    async def test_inserts_and_updates_state(self, mock_latest, mock_fetch, mock_insert):
        mock_latest.return_value = 4
        blocks = [Block(i, 1000 + i) for i in range(5)]
        mock_fetch.return_value = blocks

        await _sync_once(CHAIN, cached_latest=None)

        mock_insert.assert_called_once_with(CHAIN, blocks)
        state = _sync_states[CHAIN.id]
        assert state.last_synced_block == 4
        assert state.syncs_performed == 1
        assert state.blocks_ingested == 5

    @patch("chronoblock.syncer.config", SimpleNamespace(settings=SimpleNamespace(sync_chunk_size=100)))
    @patch("chronoblock.syncer.insert_blocks")
    @patch("chronoblock.syncer.fetch_block_timestamps", new_callable=AsyncMock)
    @patch("chronoblock.syncer.get_latest_block_number", new_callable=AsyncMock)
    async def test_raises_on_empty_response(self, mock_latest, mock_fetch, mock_insert):
        mock_latest.return_value = 10
        mock_fetch.return_value = []

        with pytest.raises(RpcResponseError, match="got 0 blocks"):
            await _sync_once(CHAIN, cached_latest=None)

    @patch("chronoblock.syncer.config", SimpleNamespace(settings=SimpleNamespace(sync_chunk_size=100)))
    @patch("chronoblock.syncer.insert_blocks")
    @patch("chronoblock.syncer.fetch_block_timestamps", new_callable=AsyncMock)
    @patch("chronoblock.syncer.get_latest_block_number", new_callable=AsyncMock)
    async def test_noop_when_already_ahead(self, mock_latest, mock_fetch, mock_insert):
        _sync_states[CHAIN.id] = SyncState(last_synced_block=100)
        mock_latest.return_value = 50

        catching, latest = await _sync_once(CHAIN, cached_latest=None)

        assert catching is False
        mock_fetch.assert_not_called()

    @patch("chronoblock.syncer.config", SimpleNamespace(settings=SimpleNamespace(sync_chunk_size=100)))
    @patch("chronoblock.syncer.insert_blocks")
    @patch("chronoblock.syncer.fetch_block_timestamps", new_callable=AsyncMock)
    @patch("chronoblock.syncer.get_latest_block_number", new_callable=AsyncMock)
    async def test_resumes_from_last_synced(self, mock_latest, mock_fetch, mock_insert):
        _sync_states[CHAIN.id] = SyncState(last_synced_block=50)
        mock_latest.return_value = 55
        mock_fetch.return_value = [Block(i, 1000 + i) for i in range(51, 56)]

        await _sync_once(CHAIN, cached_latest=None)

        mock_fetch.assert_called_once_with(CHAIN, 51, 55)


# ── _sync_loop ──────────────────────────────────────────────────────


class TestSyncLoop:
    @pytest.fixture(autouse=True)
    def setup_state(self):
        _sync_states[CHAIN.id] = SyncState()

    async def test_exits_on_cancellation(self):
        with (
            patch("chronoblock.syncer._sync_once", new_callable=AsyncMock, side_effect=asyncio.CancelledError),
            patch("asyncio.sleep", new_callable=AsyncMock),
        ):
            await _sync_loop(CHAIN)

    async def test_caches_latest_during_catchup(self):
        calls: list[int | None] = []

        async def track(chain, cached):
            calls.append(cached)
            if len(calls) >= 3:
                raise asyncio.CancelledError()
            return True, 500

        with (
            patch("chronoblock.syncer._sync_once", side_effect=track),
            patch("asyncio.sleep", new_callable=AsyncMock),
        ):
            await _sync_loop(CHAIN)

        assert calls[0] is None
        assert calls[1] == 500
        assert calls[2] == 500

    async def test_clears_cache_when_caught_up(self):
        calls: list[int | None] = []

        async def track(chain, cached):
            calls.append(cached)
            if len(calls) >= 3:
                raise asyncio.CancelledError()
            return False, 100

        with (
            patch("chronoblock.syncer._sync_once", side_effect=track),
            patch("chronoblock.syncer.observed_block_time_ms", return_value=1000.0),
            patch("asyncio.sleep", new_callable=AsyncMock),
        ):
            await _sync_loop(CHAIN)

        assert calls[0] is None
        assert calls[1] is None

    async def test_recalculates_block_time_when_caught_up(self):
        call_count = 0

        async def once(chain, cached):
            nonlocal call_count
            call_count += 1
            if call_count > 1:
                raise asyncio.CancelledError()
            return False, 100

        with (
            patch("chronoblock.syncer._sync_once", side_effect=once),
            patch("chronoblock.syncer.observed_block_time_ms", return_value=12_000.0) as mock_bt,
            patch("asyncio.sleep", new_callable=AsyncMock),
        ):
            await _sync_loop(CHAIN)

        state = _sync_states[CHAIN.id]
        assert state.observed_block_time_ms == 12_000.0
        mock_bt.assert_called_once_with(CHAIN)

    async def test_error_backoff_escalates(self):
        sleeps: list[float] = []
        call_count = 0

        async def failing(chain, cached):
            nonlocal call_count
            call_count += 1
            if call_count > 3:
                raise asyncio.CancelledError()
            raise RuntimeError("RPC down")

        async def track_sleep(duration):
            sleeps.append(duration)

        with (
            patch("chronoblock.syncer._sync_once", side_effect=failing),
            patch("asyncio.sleep", side_effect=track_sleep),
        ):
            await _sync_loop(CHAIN)

        error_sleeps = [s for s in sleeps if s >= MIN_ERROR_SLEEP]
        assert error_sleeps == [5.0, 10.0, 20.0]

    async def test_error_backoff_caps_at_max(self):
        sleeps: list[float] = []
        call_count = 0

        async def failing(chain, cached):
            nonlocal call_count
            call_count += 1
            if call_count > 5:
                raise asyncio.CancelledError()
            raise RuntimeError("RPC down")

        async def track_sleep(duration):
            sleeps.append(duration)

        with (
            patch("chronoblock.syncer._sync_once", side_effect=failing),
            patch("asyncio.sleep", side_effect=track_sleep),
        ):
            await _sync_loop(CHAIN)

        error_sleeps = [s for s in sleeps if s >= MIN_ERROR_SLEEP]
        assert error_sleeps[-1] == MAX_ERROR_SLEEP
        assert error_sleeps[-2] == MAX_ERROR_SLEEP

    async def test_rate_limit_uses_retry_after(self):
        sleeps: list[float] = []
        call_count = 0

        async def rate_limited(chain, cached):
            nonlocal call_count
            call_count += 1
            if call_count > 1:
                raise asyncio.CancelledError()
            raise RpcRateLimitError("testchain", retry_after=60)

        async def track_sleep(duration):
            sleeps.append(duration)

        with (
            patch("chronoblock.syncer._sync_once", side_effect=rate_limited),
            patch("asyncio.sleep", side_effect=track_sleep),
        ):
            await _sync_loop(CHAIN)

        error_sleeps = [s for s in sleeps if s >= MIN_ERROR_SLEEP]
        assert error_sleeps[0] == 60

    async def test_rate_limit_without_retry_after(self):
        sleeps: list[float] = []
        call_count = 0

        async def rate_limited(chain, cached):
            nonlocal call_count
            call_count += 1
            if call_count > 1:
                raise asyncio.CancelledError()
            raise RpcRateLimitError("testchain", retry_after=None)

        async def track_sleep(duration):
            sleeps.append(duration)

        with (
            patch("chronoblock.syncer._sync_once", side_effect=rate_limited),
            patch("asyncio.sleep", side_effect=track_sleep),
        ):
            await _sync_loop(CHAIN)

        error_sleeps = [s for s in sleeps if s >= MIN_ERROR_SLEEP]
        assert error_sleeps[0] == MIN_ERROR_SLEEP

    async def test_success_resets_consecutive_errors(self):
        sleeps: list[float] = []
        call_count = 0

        async def mixed(chain, cached):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("fail")
            if call_count == 2:
                raise RuntimeError("fail again")
            if call_count == 3:
                return False, 100
            if call_count == 4:
                raise RuntimeError("fail after reset")
            raise asyncio.CancelledError()

        async def track_sleep(duration):
            sleeps.append(duration)

        with (
            patch("chronoblock.syncer._sync_once", side_effect=mixed),
            patch("chronoblock.syncer.observed_block_time_ms", return_value=1000.0),
            patch("asyncio.sleep", side_effect=track_sleep),
        ):
            await _sync_loop(CHAIN)

        error_sleeps = [s for s in sleeps if s >= MIN_ERROR_SLEEP]
        assert error_sleeps == [5.0, 10.0, 5.0]

    async def test_clears_error_state_on_success(self):
        call_count = 0

        async def fail_then_succeed(chain, cached):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("temporary")
            if call_count == 2:
                return False, 100
            raise asyncio.CancelledError()

        with (
            patch("chronoblock.syncer._sync_once", side_effect=fail_then_succeed),
            patch("chronoblock.syncer.observed_block_time_ms", return_value=1000.0),
            patch("asyncio.sleep", new_callable=AsyncMock),
        ):
            await _sync_loop(CHAIN)

        state = _sync_states[CHAIN.id]
        assert state.last_error is None
        assert state.last_error_at is None
        assert state.last_success_at is not None

    async def test_rate_limit_clears_cache(self):
        calls: list[int | None] = []
        call_count = 0

        async def track(chain, cached):
            nonlocal call_count
            calls.append(cached)
            call_count += 1
            if call_count == 1:
                return True, 500
            if call_count == 2:
                raise RpcRateLimitError("testchain", retry_after=1)
            if call_count == 3:
                return False, 100
            raise asyncio.CancelledError()

        with (
            patch("chronoblock.syncer._sync_once", side_effect=track),
            patch("chronoblock.syncer.observed_block_time_ms", return_value=1000.0),
            patch("asyncio.sleep", new_callable=AsyncMock),
        ):
            await _sync_loop(CHAIN)

        assert calls[0] is None
        assert calls[1] == 500
        assert calls[2] is None


# ── _checkpoint_timer ──────────────────────────────────────────────


class TestCheckpointTimer:
    async def test_calls_checkpoint_at_interval(self):
        sleep_count = 0

        async def limited_sleep(duration):
            nonlocal sleep_count
            sleep_count += 1
            if sleep_count > 2:
                raise asyncio.CancelledError()

        with (
            patch("asyncio.sleep", side_effect=limited_sleep),
            patch("chronoblock.syncer.checkpoint_all") as mock_cp,
            pytest.raises(asyncio.CancelledError),
        ):
            await _checkpoint_timer()

        assert mock_cp.call_count == 2
