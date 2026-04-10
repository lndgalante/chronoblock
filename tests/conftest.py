"""Shared test fixtures."""

from __future__ import annotations

import os

# Set dummy RPC URLs before any config import
os.environ.setdefault("ETH_RPC_URL", "http://eth.test")
os.environ.setdefault("BASE_RPC_URL", "http://base.test")
os.environ.setdefault("INK_RPC_URL", "http://ink.test")
os.environ.setdefault("PLASMA_RPC_URL", "http://plasma.test")
os.environ.setdefault("DATA_DIR", "/tmp/chronoblock-test")

import pytest
from fastapi.testclient import TestClient

from chronoblock.api import create_app
from chronoblock.dependencies import (
    BlockCountFn,
    GetSyncStateFn,
    GetTimestampsFn,
    IsHealthyFn,
    dep_block_count,
    dep_get_sync_state,
    dep_get_timestamps,
    dep_is_healthy,
    dep_now,
)
from chronoblock.syncer import SyncState


def make_state(now: float = 1_000_000.0, **overrides: object) -> SyncState:
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


@pytest.fixture
def create_test_app():
    """Factory fixture that creates an app with mocked dependencies."""

    def _create(
        now: float = 1_000_000.0,
        get_timestamps_fn: GetTimestampsFn | None = None,
        block_count_fn: BlockCountFn | None = None,
        is_healthy_fn: IsHealthyFn | None = None,
        get_sync_state_fn: GetSyncStateFn | None = None,
    ) -> TestClient:
        app = create_app()

        ts_fn: GetTimestampsFn = get_timestamps_fn or (lambda _chain, blocks: [None] * len(blocks))
        count_fn: BlockCountFn = block_count_fn or (lambda _chain: 0)
        healthy_fn: IsHealthyFn = is_healthy_fn or (lambda _chain: True)
        sync_fn: GetSyncStateFn = get_sync_state_fn or (lambda _chain: make_state(now))

        app.dependency_overrides[dep_get_timestamps] = lambda: ts_fn
        app.dependency_overrides[dep_block_count] = lambda: count_fn
        app.dependency_overrides[dep_is_healthy] = lambda: healthy_fn
        app.dependency_overrides[dep_get_sync_state] = lambda: sync_fn
        app.dependency_overrides[dep_now] = lambda: lambda: now

        return TestClient(app, raise_server_exceptions=False)

    return _create
