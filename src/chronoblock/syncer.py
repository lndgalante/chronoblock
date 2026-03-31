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

from chronoblock.config import CHAINS, Chain, settings
from chronoblock.db import (
    last_block,
    insert_blocks,
    checkpoint_all,
    observed_block_time_ms,
)
from chronoblock.rpc import get_latest_block_number, fetch_block_timestamps
from chronoblock.log import log

CHECKPOINT_INTERVAL = 60.0
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
_tasks: list[asyncio.Task] = []


def get_sync_state(chain: Chain) -> SyncState:
    state = _sync_states.get(chain.id)
    if state is not None:
        return state
    return SyncState()


# ── Lifecycle ────────────────────────────────────────────────────────


async def start_all() -> None:
    for chain in CHAINS:
        _sync_states[chain.id] = SyncState(
            last_synced_block=last_block(chain),
            observed_block_time_ms=observed_block_time_ms(chain),
        )
        _tasks.append(asyncio.create_task(_sync_loop(chain)))

    _tasks.append(asyncio.create_task(_checkpoint_timer()))
    log("info", f"started {len(CHAINS)} sync workers", chain="all")


async def stop_all() -> None:
    for task in _tasks:
        task.cancel()
    await asyncio.gather(*_tasks, return_exceptions=True)
    _tasks.clear()
    checkpoint_all()


# ── Sync loop ────────────────────────────────────────────────────────


async def _sync_loop(chain: Chain) -> None:
    # Stagger start so chains don't all hit their RPCs simultaneously.
    await asyncio.sleep(random.random() * 2.0)

    cached_latest: int | None = None

    while True:
        state = _sync_states[chain.id]
        try:
            catching, latest = await _sync_once(chain, cached_latest)
            state.last_error = None
            state.last_error_at = None
            state.last_success_at = time.time()

            if catching:
                cached_latest = latest
            else:
                cached_latest = None
                state.observed_block_time_ms = observed_block_time_ms(chain)
                await asyncio.sleep(state.observed_block_time_ms / 1000)

        except asyncio.CancelledError:
            return
        except Exception as err:
            cached_latest = None
            state.last_error = str(err)
            state.last_error_at = time.time()
            log("error", str(err), chain=chain.name)
            await asyncio.sleep(min(state.observed_block_time_ms / 1000 * 3, MAX_ERROR_SLEEP))


async def _sync_once(chain: Chain, cached_latest: int | None) -> tuple[bool, int]:
    latest = cached_latest if cached_latest is not None else await get_latest_block_number(chain)
    stored = last_block(chain)
    from_block = stored + 1 if stored is not None else 0

    state = _sync_states[chain.id]
    state.latest_chain_block = latest
    state.last_synced_block = stored

    if from_block > latest:
        return False, latest

    gap = latest - from_block + 1
    to_block = min(from_block + settings.sync_chunk_size - 1, latest)

    log("info", f"syncing {from_block}→{to_block} ({gap:,} behind)", chain=chain.name)

    blocks = await fetch_block_timestamps(chain, from_block, to_block)

    if blocks:
        await asyncio.to_thread(insert_blocks, chain, blocks)
        state.last_synced_block = blocks[-1].number
        state.syncs_performed += 1
        state.blocks_ingested += len(blocks)
    else:
        raise RuntimeError(f"got 0 blocks for range {from_block}→{to_block}")

    return to_block < latest, latest


async def _checkpoint_timer() -> None:
    while True:
        await asyncio.sleep(CHECKPOINT_INTERVAL)
        await asyncio.to_thread(checkpoint_all)
