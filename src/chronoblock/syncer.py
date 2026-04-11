"""
Background sync workers — one per chain, running in parallel.

Each worker loops: check the gap between the DB tip and the chain tip,
fetch the next chunk, insert, repeat. When caught up, it sleeps for
one observed block time before polling again.

Key design:
- Catch-up skips sleep and caches the chain tip to avoid one RPC call
  per chunk (~44k calls saved on a full Ethereum backfill).
- observed_block_time_ms is only recomputed after a successful sync
  that leaves us caught up, not during catch-up.
- Errors trigger a 3x backoff capped at 30s.
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field

from chronoblock import config
from chronoblock.db import (
    checkpoint_all,
    insert_blocks,
    last_block,
    observed_block_time_ms,
)
from chronoblock.errors import RpcRateLimitError, RpcResponseError
from chronoblock.log import log
from chronoblock.models import Chain
from chronoblock.rpc import fetch_block_timestamps, get_latest_block_number

__all__ = ["SyncState", "get_sync_state", "start_all", "stop_all"]

CHECKPOINT_INTERVAL = 60.0
MIN_ERROR_SLEEP = 5.0
MAX_ERROR_SLEEP = 30.0


@dataclass
class SyncState:
    started_at: float = field(default_factory=time.time)
    last_success_at: float | None = None
    last_synced_block: int | None = None
    latest_chain_block: int | None = None
    observed_block_time_ms: float = 1_000.0
    last_error: str | None = None
    last_error_at: float | None = None
    syncs_performed: int = 0
    blocks_ingested: int = 0


_sync_states: dict[int, SyncState] = {}
_tasks: list[asyncio.Task[None]] = []


def get_sync_state(chain: Chain) -> SyncState:
    state = _sync_states.get(chain.id)
    if state is not None:
        return state
    return SyncState()


# ── Lifecycle ────────────────────────────────────────────────────────


async def start_all() -> None:
    last_blocks, block_times = await asyncio.gather(
        asyncio.gather(*[asyncio.to_thread(last_block, ch) for ch in config.CHAINS]),
        asyncio.gather(*[asyncio.to_thread(observed_block_time_ms, ch) for ch in config.CHAINS]),
    )

    for chain, stored, bt in zip(config.CHAINS, last_blocks, block_times, strict=True):
        _sync_states[chain.id] = SyncState(last_synced_block=stored, observed_block_time_ms=bt)
        _tasks.append(asyncio.create_task(_sync_loop(chain), name=f"sync-{chain.name}"))

    _tasks.append(asyncio.create_task(_checkpoint_timer(), name="checkpoint-timer"))
    log("info", f"started {len(config.CHAINS)} sync workers", chain="all")


SHUTDOWN_TIMEOUT = 10.0


async def stop_all() -> None:
    for task in _tasks:
        task.cancel()
    try:
        await asyncio.wait_for(
            asyncio.gather(*_tasks, return_exceptions=True),
            timeout=SHUTDOWN_TIMEOUT,
        )
    except TimeoutError:
        log("warn", "shutdown timed out, some tasks did not exit cleanly", timeout=SHUTDOWN_TIMEOUT)
    _tasks.clear()
    await asyncio.to_thread(checkpoint_all)


# ── Sync loop ────────────────────────────────────────────────────────


async def _sync_loop(chain: Chain) -> None:
    # Stagger start so chains don't all hit their RPCs simultaneously.
    await asyncio.sleep(random.random() * 2.0)

    cached_latest: int | None = None
    consecutive_errors = 0

    while True:
        state = _sync_states[chain.id]
        try:
            catching, latest = await _sync_once(chain, cached_latest)
            state.last_error = None
            state.last_error_at = None
            state.last_success_at = time.time()
            consecutive_errors = 0

            if catching:
                cached_latest = latest
            else:
                cached_latest = None
                state.observed_block_time_ms = await asyncio.to_thread(observed_block_time_ms, chain)
                await asyncio.sleep(state.observed_block_time_ms / 1000)

        except asyncio.CancelledError:
            return
        except RpcRateLimitError as err:
            cached_latest = None
            consecutive_errors += 1
            state.last_error = str(err)
            state.last_error_at = time.time()
            escalated = min(MIN_ERROR_SLEEP * 2 ** (consecutive_errors - 1), MAX_ERROR_SLEEP)
            sleep = max(err.retry_after, escalated) if err.retry_after is not None else escalated
            log("warn", str(err), chain=chain.name, retry_in=sleep)
            await asyncio.sleep(sleep)
        except Exception as err:
            cached_latest = None
            consecutive_errors += 1
            state.last_error = str(err)
            state.last_error_at = time.time()
            sleep = min(MIN_ERROR_SLEEP * 2 ** (consecutive_errors - 1), MAX_ERROR_SLEEP)
            log("error", str(err), chain=chain.name, retry_in=sleep)
            await asyncio.sleep(sleep)


async def _sync_once(chain: Chain, cached_latest: int | None) -> tuple[bool, int]:
    latest = cached_latest if cached_latest is not None else await get_latest_block_number(chain)

    state = _sync_states[chain.id]
    state.latest_chain_block = latest

    stored = state.last_synced_block
    from_block = stored + 1 if stored is not None else 0

    if from_block > latest:
        return False, latest

    gap = latest - from_block + 1
    to_block = min(from_block + config.settings.sync_chunk_size - 1, latest)

    log("info", f"syncing {from_block}→{to_block} ({gap:,} behind)", chain=chain.name)

    blocks = await fetch_block_timestamps(chain, from_block, to_block)

    if blocks:
        await asyncio.to_thread(insert_blocks, chain, blocks)
        state.last_synced_block = blocks[-1].number
        state.syncs_performed += 1
        state.blocks_ingested += len(blocks)
    else:
        raise RpcResponseError(chain.name, f"got 0 blocks for range {from_block}→{to_block}")

    return to_block < latest, latest


async def _checkpoint_timer() -> None:
    while True:
        await asyncio.sleep(CHECKPOINT_INTERVAL)
        try:
            await asyncio.to_thread(checkpoint_all)
        except Exception as err:
            log("warn", "checkpoint failed", error=str(err))
