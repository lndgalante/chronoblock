"""
JSON-RPC client for EVM chains. Handles batching, bounded concurrency,
exponential backoff, and timeout. The key invariant is gap-safety:
fetch_block_timestamps returns only a contiguous prefix so the syncer
never skips blocks.
"""

from __future__ import annotations

import asyncio
import random
import time
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

__all__ = ["get_latest_block_number", "fetch_block_timestamps", "close_client", "is_retryable"]

MAX_RETRIES = 5
TIMEOUT = httpx.Timeout(30.0)
RPC_RATE_LIMIT = 400  # calls/sec — margin under QuickNode's 500/sec cap

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


# ── Rate limiter ─────────────────────────────────────────────────────


class _RateLimiter:
    """Leaky-bucket rate limiter shared across all chains (account-level limit)."""

    __slots__ = ("_rate", "_next", "_lock")

    def __init__(self, rate: float) -> None:
        self._rate = rate
        self._next = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int) -> None:
        async with self._lock:
            now = time.monotonic()
            self._next = max(self._next, now)
            delay = self._next - now
            self._next += tokens / self._rate

        if delay > 0:
            await asyncio.sleep(delay)


_rate_limiter = _RateLimiter(RPC_RATE_LIMIT)


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
    semaphore = asyncio.Semaphore(chain.rpc_concurrency)

    async def _guarded_fetch(b_from: int, b_to: int) -> list[Block] | Exception:
        async with semaphore:
            try:
                return await _fetch_batch(chain, b_from, b_to)
            except Exception as exc:
                return exc

    results = await asyncio.gather(
        *(_guarded_fetch(b_from, b_to) for b_from, b_to in batches),
    )

    for (b_from, b_to), result in zip(batches, results, strict=True):
        if isinstance(result, RpcRateLimitError):
            raise result
        if isinstance(result, Exception):
            log("warn", "batch failed", chain=chain.name, error=str(result), blocks=f"{b_from}\u2192{b_to}")
        else:
            for block in result:
                all_blocks[block.number] = block.timestamp

    # Extract contiguous prefix starting at from_block.
    out: list[Block] = []
    for i in range(from_block, to_block + 1):
        ts = all_blocks.get(i)
        if ts is None:
            break
        out.append(Block(number=i, timestamp=ts))

    return out


_RATE_LIMIT_KEYWORDS = ("exceeded", "rate limit", "capacity", "too many requests")


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
    error_counts: dict[str, int] = {}

    for r in data:
        if r.get("error"):
            err = r["error"]
            msg = str(err.get("message", err.get("code", "unknown")))
            error_counts[msg] = error_counts.get(msg, 0) + 1
            continue
        result = r.get("result")
        if result is None:
            continue
        try:
            num = int(result["number"], 16)
            ts = int(result["timestamp"], 16)
        except (KeyError, TypeError, ValueError) as err:
            log(
                "warn",
                "malformed block in RPC response",
                chain=chain.name,
                error=str(err),
                block_data=str(result)[:200],
            )
            continue
        blocks.append(Block(number=num, timestamp=ts))

    if error_counts:
        for msg, count in error_counts.items():
            log("warn", f"batch RPC errors: {count}/{len(data)} failed", chain=chain.name, error=msg)

        rate_limited = sum(
            count for msg, count in error_counts.items() if any(kw in msg.lower() for kw in _RATE_LIMIT_KEYWORDS)
        )
        if rate_limited > len(data) // 2:
            raise RpcRateLimitError(chain.name)

    return blocks


def is_retryable(err: Exception) -> bool:
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
        raise RpcResponseError(chain.name, f"batch response length mismatch: expected {len(payload)}, got {len(result)}")
    return result


@overload
async def _send(chain: Chain, payload: dict[str, Any], *, is_batch: Literal[False]) -> Any: ...
@overload
async def _send(chain: Chain, payload: list[dict[str, Any]], *, is_batch: Literal[True]) -> list[dict[str, Any]]: ...


async def _send(chain: Chain, payload: dict[str, Any] | list[dict[str, Any]], *, is_batch: bool) -> Any:
    """Core fetch loop with retry and exponential backoff."""
    client = _get_client()
    last_err: Exception | None = None
    token_count = len(payload) if is_batch else 1

    for attempt in range(MAX_RETRIES + 1):
        await _rate_limiter.acquire(token_count)
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

            try:
                data = response.json()
            except ValueError as err:
                raise RpcResponseError(chain.name, f"invalid JSON in response: {err}") from err

            if is_batch:
                return data
            if data.get("error"):
                err_obj = data["error"]
                raise RpcResponseError(chain.name, f"RPC error: {err_obj.get('message', err_obj)}")
            if "result" not in data:
                raise RpcResponseError(chain.name, "malformed response: missing 'result' key")
            return data["result"]

        except asyncio.CancelledError:
            raise
        except Exception as err:
            last_err = err
            if attempt == MAX_RETRIES or not is_retryable(err):
                raise
            log("warn", f"retry {attempt + 1}/{MAX_RETRIES}", chain=chain.name, error=str(err))
            backoff = min(1.0 * 2**attempt, 30.0) * (0.5 + random.random())
            if isinstance(err, RpcRateLimitError) and err.retry_after is not None:
                backoff = max(backoff, float(err.retry_after))
            await asyncio.sleep(backoff)

    raise last_err or RpcError(chain.name, "unreachable")
