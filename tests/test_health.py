"""Health degradation logic tests."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from chronoblock.health import INITIAL_SYNC_GRACE_SECS, should_degrade_chain
from chronoblock.syncer import SyncState


def _state(now: float = 1_000_000.0, **overrides: object) -> SyncState:
    defaults: dict[str, object] = {
        "started_at": now,
        "last_success_at": None,
        "last_synced_block": None,
        "latest_chain_block": None,
        "observed_block_time_ms": 1_000.0,
        "last_error": None,
        "last_error_at": None,
        "syncs_performed": 0,
        "blocks_ingested": 0,
    }
    defaults.update(overrides)
    return SyncState(**defaults)  # type: ignore[arg-type]


class TestShouldDegradeChain:
    def test_healthy_within_grace_period(self):
        now = 1_000_000.0
        assert should_degrade_chain(_state(now), now) is None

    def test_degrades_on_error_before_first_success(self):
        now = 1_000_000.0
        result = should_degrade_chain(_state(now, last_error="RPC timeout"), now)
        assert result == "RPC timeout"

    def test_degrades_when_grace_period_exceeded(self):
        now = 1_000_000.0
        started = now - INITIAL_SYNC_GRACE_SECS - 1
        result = should_degrade_chain(_state(now, started_at=started), now)
        assert "grace period" in result

    def test_healthy_when_caught_up(self):
        now = 1_000_000.0
        sync = _state(now, last_success_at=now - 1.0, last_synced_block=100, latest_chain_block=100)
        assert should_degrade_chain(sync, now) is None

    @patch("chronoblock.health.config")
    def test_degrades_when_lag_exceeds_budget(self, mock_config):
        mock_config.settings = SimpleNamespace(health_max_lag_secs=120)
        now = 1_000_000.0
        sync = _state(
            now,
            last_success_at=now - 1.0,
            last_synced_block=100,
            latest_chain_block=100 + 121,
            observed_block_time_ms=1_000.0,
        )
        result = should_degrade_chain(sync, now)
        assert "121 blocks behind" in result

    @patch("chronoblock.health.config")
    def test_healthy_when_lag_within_budget(self, mock_config):
        mock_config.settings = SimpleNamespace(health_max_lag_secs=120)
        now = 1_000_000.0
        sync = _state(
            now,
            last_success_at=now - 1.0,
            last_synced_block=100,
            latest_chain_block=100 + 60,
            observed_block_time_ms=1_000.0,
        )
        assert should_degrade_chain(sync, now) is None

    def test_degrades_on_incomplete_sync_state(self):
        now = 1_000_000.0
        sync = _state(now, last_success_at=now - 1.0, last_synced_block=None, latest_chain_block=100)
        assert should_degrade_chain(sync, now) == "sync state incomplete"

    @pytest.mark.parametrize(
        "last_synced,latest",
        [(None, 100), (100, None), (None, None)],
    )
    def test_degrades_on_any_missing_block_field(self, last_synced, latest):
        now = 1_000_000.0
        sync = _state(now, last_success_at=now - 1.0, last_synced_block=last_synced, latest_chain_block=latest)
        assert should_degrade_chain(sync, now) == "sync state incomplete"
