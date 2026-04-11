"""Health-check degradation logic."""

from __future__ import annotations

from chronoblock import config
from chronoblock.syncer import SyncState

__all__ = ["INITIAL_SYNC_GRACE_SECS", "should_degrade_chain"]

INITIAL_SYNC_GRACE_SECS = 5 * 60


def should_degrade_chain(sync: SyncState, now: float) -> str | None:
    if sync.last_success_at is None:
        if sync.last_error:
            return sync.last_error
        if now - sync.started_at > INITIAL_SYNC_GRACE_SECS:
            return f"initial sync exceeded {INITIAL_SYNC_GRACE_SECS}s grace period"
        return None

    if sync.last_synced_block is None or sync.latest_chain_block is None:
        return "sync state incomplete"

    lag_blocks = sync.latest_chain_block - sync.last_synced_block
    lag_secs = (lag_blocks * sync.observed_block_time_ms) / 1000
    if lag_secs > config.settings.health_max_lag_secs:
        return f"{lag_blocks} blocks behind (~{round(lag_secs)}s)"

    return None
