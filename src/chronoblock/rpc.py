"""
JSON-RPC client for EVM chains. Handles batching, bounded concurrency,
exponential backoff, and timeout. The key invariant is gap-safety:
fetch_block_timestamps returns only a contiguous prefix so the syncer
never skips blocks.
"""

from __future__ import annotations

import asyncio
from typing import Any, Literal, overload

import httpx

from chronoblock.errors import (
    RpcError,
    RpcRateLimitError,
    RpcResponseError,
    RpcServerError,
    RpcTransportError,
)
from chronoblock.log import log
from chronoblock.models import Block, Chain

__all__ = ["get_latest_block_number", "fetch_block_timestamps", "close_client"]

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
    if not isinstance(result, str):
        raise RpcResponseError(chain.name, f"expected hex string from eth_blockNumber, got {type(result).__name__}")
    try:
        return int(result, 16)
    except ValueError as err:
        raise RpcResponseError(chain.name, f"invalid hex in eth_blockNumber: {result!r}") from err


# ── Internal ─────────────────────────────────────────────────────────


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
        if result is None:
            continue
        try:
            num = int(result["number"], 16)
            ts = int(result["timestamp"], 16)
        except (KeyError, TypeError, ValueError) as err:
            log("warn", "malformed block in RPC response", chain=chain.name, error=str(err), block_data=str(result)[:200])
            continue
        blocks.append(Block(number=num, timestamp=ts))

    return blocks


def _is_retryable(err: Exception) -> bool:
    return isinstance(
        err,
        (
            RpcTransportError,
            RpcRateLimitError,
            RpcServerError,
            httpx.TimeoutException,
            httpx.ConnectError,
            httpx.ReadError,
            httpx.WriteError,
        ),
    )


async def _rpc(chain: Chain, method: str, params: list[Any]) -> Any:
    return await _send(chain, {"jsonrpc": "2.0", "method": method, "params": params, "id": 1}, is_batch=False)


async def _rpc_batch(chain: Chain, payload: list[dict[str, Any]]) -> list[dict[str, Any]]:
    result = await _send(chain, payload, is_batch=True)
    if not isinstance(result, list):
        raise RpcResponseError(chain.name, "expected array from batch RPC")
    if len(result) != len(payload):
        log("warn", f"batch response length mismatch: expected {len(payload)}, got {len(result)}", chain=chain.name)
    return result


@overload
async def _send(chain: Chain, payload: dict[str, Any], *, is_batch: Literal[False]) -> Any: ...
@overload
async def _send(chain: Chain, payload: list[dict[str, Any]], *, is_batch: Literal[True]) -> list[dict[str, Any]]: ...

async def _send(chain: Chain, payload: dict[str, Any] | list[dict[str, Any]], *, is_batch: bool) -> Any:
    """Core fetch loop with retry and exponential backoff."""
    client = _get_client()
    last_err: Exception | None = None

    for attempt in range(MAX_RETRIES + 1):
        try:
            try:
                response = await client.post(
                    chain.rpc,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                )
            except httpx.TimeoutException as err:
                raise RpcTransportError(chain.name, f"timeout: {err}") from err
            except (httpx.ConnectError, httpx.ReadError, httpx.WriteError) as err:
                raise RpcTransportError(chain.name, str(err)) from err

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                raise RpcRateLimitError(chain.name, int(retry_after) if retry_after and retry_after.isdigit() else None)
            if response.status_code >= 500:
                raise RpcServerError(chain.name, response.status_code)
            if not response.is_success:
                raise RpcResponseError(chain.name, f"HTTP {response.status_code}: {response.text[:200]}")

            data = response.json()

            if is_batch:
                return data
            if data.get("error"):
                err_obj = data["error"]
                raise RpcResponseError(chain.name, f"RPC error: {err_obj.get('message', err_obj)}")
            return data["result"]

        except asyncio.CancelledError:
            raise
        except Exception as err:
            last_err = err
            if attempt == MAX_RETRIES or not _is_retryable(err):
                raise
            await asyncio.sleep(min(1.0 * 2**attempt, 30.0))

    raise last_err or RpcError(chain.name, "unreachable")
