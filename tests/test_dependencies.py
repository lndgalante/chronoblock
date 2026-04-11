"""Dependency provider tests."""

from __future__ import annotations

from chronoblock.db import block_count, get_timestamps, is_healthy
from chronoblock.dependencies import (
    dep_block_count,
    dep_get_sync_state,
    dep_get_timestamps,
    dep_is_healthy,
    dep_now,
)
from chronoblock.syncer import get_sync_state


class TestDependencyProviders:
    def test_dep_get_timestamps_returns_db_function(self):
        assert dep_get_timestamps() is get_timestamps

    def test_dep_block_count_returns_db_function(self):
        assert dep_block_count() is block_count

    def test_dep_is_healthy_returns_db_function(self):
        assert dep_is_healthy() is is_healthy

    def test_dep_get_sync_state_returns_syncer_function(self):
        assert dep_get_sync_state() is get_sync_state

    def test_dep_now_returns_callable(self):
        fn = dep_now()
        result = fn()
        assert isinstance(result, float)
        assert result > 0
