"""FastAPI dependency providers for test-friendly DI."""

from __future__ import annotations

import time
from collections.abc import Callable

from chronoblock.db import block_count, get_timestamps, is_healthy
from chronoblock.models import Chain
from chronoblock.syncer import SyncState, get_sync_state

__all__ = [
    "GetTimestampsFn",
    "BlockCountFn",
    "IsHealthyFn",
    "GetSyncStateFn",
    "NowFn",
    "dep_get_timestamps",
    "dep_block_count",
    "dep_is_healthy",
    "dep_get_sync_state",
    "dep_now",
]

type GetTimestampsFn = Callable[[Chain, list[int]], list[int | None]]
type BlockCountFn = Callable[[Chain], int]
type IsHealthyFn = Callable[[Chain], bool]
type GetSyncStateFn = Callable[[Chain], SyncState]
type NowFn = Callable[[], float]


def dep_get_timestamps() -> GetTimestampsFn:
    return get_timestamps


def dep_block_count() -> BlockCountFn:
    return block_count


def dep_is_healthy() -> IsHealthyFn:
    return is_healthy


def dep_get_sync_state() -> GetSyncStateFn:
    return get_sync_state


def dep_now() -> NowFn:
    return time.time
