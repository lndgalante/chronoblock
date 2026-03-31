"""Shared test fixtures."""

from __future__ import annotations

import os

# Set dummy RPC URLs before any config import
os.environ.setdefault("ETH_RPC_URL", "http://eth.test")
os.environ.setdefault("SCROLL_RPC_URL", "http://scroll.test")
os.environ.setdefault("INK_RPC_URL", "http://ink.test")
os.environ.setdefault("HYPEREVM_RPC_URL", "http://hyperevm.test")
os.environ.setdefault("DATA_DIR", "/tmp/chronoblock-test")

import time

import pytest
from fastapi.testclient import TestClient

from chronoblock.config import CHAINS, Chain
from chronoblock.syncer import SyncState
from chronoblock.api import (
    create_app,
    dep_get_timestamps,
    dep_block_count,
    dep_is_healthy,
    dep_get_sync_state,
    dep_now,
)


def make_state(now: float = 1_000_000.0, **overrides) -> SyncState:
    defaults = {
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
    return SyncState(**defaults)


@pytest.fixture
def create_test_app():
    """Factory fixture that creates an app with mocked dependencies."""

    def _create(
        now: float = 1_000_000.0,
        get_timestamps_fn=None,
        block_count_fn=None,
        is_healthy_fn=None,
        get_sync_state_fn=None,
    ):
        app = create_app()

        if get_timestamps_fn is None:
            get_timestamps_fn = lambda _chain, blocks: [None] * len(blocks)
        if block_count_fn is None:
            block_count_fn = lambda _chain: 0
        if is_healthy_fn is None:
            is_healthy_fn = lambda _chain: True
        if get_sync_state_fn is None:
            get_sync_state_fn = lambda _chain: make_state(now)

        app.dependency_overrides[dep_get_timestamps] = lambda: get_timestamps_fn
        app.dependency_overrides[dep_block_count] = lambda: block_count_fn
        app.dependency_overrides[dep_is_healthy] = lambda: is_healthy_fn
        app.dependency_overrides[dep_get_sync_state] = lambda: get_sync_state_fn
        app.dependency_overrides[dep_now] = lambda: lambda: now

        return TestClient(app, raise_server_exceptions=False)

    return _create
