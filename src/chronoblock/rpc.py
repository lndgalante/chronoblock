"""
JSON-RPC client for EVM chains. Handles batching, bounded concurrency,
exponential backoff, and timeout. The key invariant is gap-safety:
fetch_block_timestamps returns only a contiguous prefix so the syncer
never skips blocks.
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx

from chronoblock.config import Block, Chain
from chronoblock.log import log

MAX_RETRIES = 5
TIMEOUT = httpx.Timeout(30.0)

_client: httpx.AsyncClient | None = None


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        _client = httpx.AsyncClient(timeout=TIMEOUT)
    return _client


async def close_client() -> None:
    global _client
    if _client is not None and not _client.is_closed:
        await _client.aclose()
        _client = None


# ── Public API ───────────────────────────────────────────────────────


async def get_latest_block_number(chain: Chain) -> int:
    result = await _rpc(chain, "eth_blockNumber", [])
    n = int(result, 16)
    return n


async def fetch_block_timestamps(chain: Chain, from_block: int, to_block: int) -> list[Block]:
    """
    Fetches block timestamps for the inclusive range [from_block, to_block].

    Gap-safety guarantee: returns only the contiguous prefix starting at
    from_block. If block from_block+3 is missing, the result is
    [from_block, from_block+1, from_block+2].
    """
    batches: list[tuple[int, int]] = []
    s = from_block
    while s <= to_block:
        batches.append((s, min(s + chain.rpc_batch_size - 1, to_block)))
        s += chain.rpc_batch_size

    all_blocks: dict[int, int] = {}

    for i in range(0, len(batches), chain.rpc_concurrency):
        wave = batches[i : i + chain.rpc_concurrency]
        results = await asyncio.gather(
            *(_fetch_batch(chain, b_from, b_to) for b_from, b_to in wave),
            return_exceptions=True,
        )

        had_failure = False
        for result in results:
            if isinstance(result, BaseException):
                had_failure = True
            else:
                for block in result:
                    all_blocks[block.number] = block.timestamp

        if had_failure:
            break

    # Extract contiguous prefix starting at from_block.
    out: list[Block] = []
    for i in range(from_block, to_block + 1):
        ts = all_blocks.get(i)
        if ts is None:
            break
        out.append(Block(number=i, timestamp=ts))

    return out


# ── Internal ─────────────────────────────────────────────────────────


async def _fetch_batch(chain: Chain, from_block: int, to_block: int) -> list[Block]:
    payload = [
        {
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": [f"0x{i:x}", False],
            "id": i - from_block,
        }
        for i in range(from_block, to_block + 1)
    ]

    data = await _rpc_batch(chain, payload)

    blocks: list[Block] = []
    for r in data:
        if r.get("error"):
            err = r["error"]
            log(
                "warn", f"batch item RPC error: {err.get('message', err.get('code'))}", chain=chain.name, id=r.get("id")
            )
            continue
        result = r.get("result")
        if not result:
            continue
        num = int(result["number"], 16)
        ts = int(result["timestamp"], 16)
        blocks.append(Block(number=num, timestamp=ts))

    return blocks


def _is_retryable(err: Exception) -> bool:
    msg = str(err).lower()
    if isinstance(err, httpx.TimeoutException):
        return True
    if isinstance(err, (httpx.ConnectError, httpx.ReadError, httpx.WriteError)):
        return True
    if "rpc 429" in msg or "rpc 5" in msg:
        return True
    return any(s in msg for s in ("econnr", "etimedout", "socket"))


async def _rpc(chain: Chain, method: str, params: list[Any]) -> Any:
    return await _send(chain, {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}, is_batch=False)


async def _rpc_batch(chain: Chain, payload: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = await _send(chain, payload, is_batch=True)
    if not isinstance(result, list):
        raise RuntimeError(f"{chain.name}: expected array from batch RPC")
    if len(result) != len(payload):
        log("warn", f"batch response length mismatch: expected {len(payload)}, got {len(result)}", chain=chain.name)
    return result


async def _send(chain: Chain, payload: dict[str, Any] | list[dict[str, Any]], *, is_batch: bool) -> Any:
    """Core fetch loop with retry and exponential backoff."""
    client = _get_client()
    last_err: Exception | None = None

    for attempt in range(MAX_RETRIES + 1):
        try:
            response = await client.post(
                chain.rpc,
                json=payload,
                headers={"Content-Type": "application/json"},
            )

            if response.status_code == 429 or response.status_code >= 500:
                raise RuntimeError(f"{chain.name}: RPC {response.status_code}")
            if not response.is_success:
                raise RuntimeError(f"{chain.name}: RPC {response.status_code}: {response.text}")

            data = response.json()

            if is_batch:
                return data
            if data.get("error"):
                raise RuntimeError(f"{chain.name}: RPC error: {data['error'].get('message', data['error'])}")
            return data["result"]

        except asyncio.CancelledError:
            raise
        except Exception as err:
            last_err = err
            if attempt == MAX_RETRIES or not _is_retryable(err):
                raise
            await asyncio.sleep(min(1.0 * 2**attempt, 30.0))

    raise last_err or RuntimeError("unreachable")
