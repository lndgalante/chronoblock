"""RPC client tests — port of rpc.test.ts."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, patch

import httpx
import pytest
import respx

from chronoblock.errors import RpcRateLimitError, RpcResponseError, RpcServerError, RpcTransportError
from chronoblock.models import Block, Chain
from chronoblock.rpc import close_client, fetch_block_timestamps, get_latest_block_number, is_retryable

CHAIN = Chain(
    id=1,
    name="ethereum",
    rpc="http://example.test",
    rpc_batch_size=2,
    rpc_concurrency=3,
    finality_blocks=64,
)


def batch_response(from_block: int, to_block: int, missing: set[int] | None = None) -> list[dict[str, object]]:
    missing = missing or set()
    return [
        {
            "jsonrpc": "2.0",
            "id": block - from_block,
            "result": None
            if block in missing
            else {
                "number": hex(block),
                "timestamp": hex(block + 1000),
            },
        }
        for block in range(from_block, to_block + 1)
    ]


@pytest.fixture(autouse=True)
async def cleanup_client():
    yield
    await close_client()


class TestFetchBlockTimestamps:
    @respx.mock
    @pytest.mark.asyncio
    async def test_contiguous_prefix_on_middle_batch_failure(self):
        def handler(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content)
            from_block = int(payload[0]["params"][0], 16)

            if from_block == 2:
                return httpx.Response(400, text="bad request")
            return httpx.Response(200, json=batch_response(from_block, from_block + len(payload) - 1))

        respx.post("http://example.test").mock(side_effect=handler)

        blocks = await fetch_block_timestamps(CHAIN, 0, 5)
        assert blocks == [Block(0, 1000), Block(1, 1001)]

    @respx.mock
    @pytest.mark.asyncio
    async def test_stops_at_missing_block_in_successful_batch(self):
        def handler(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content)
            from_block = int(payload[0]["params"][0], 16)
            to_block = from_block + len(payload) - 1
            return httpx.Response(200, json=batch_response(from_block, to_block, missing={2}))

        respx.post("http://example.test").mock(side_effect=handler)

        chain = Chain(
            id=1, name="ethereum", rpc="http://example.test", rpc_batch_size=4, rpc_concurrency=1, finality_blocks=64
        )
        blocks = await fetch_block_timestamps(chain, 0, 3)
        assert blocks == [Block(0, 1000), Block(1, 1001)]

    @respx.mock
    @pytest.mark.asyncio
    async def test_empty_prefix_when_all_batches_fail(self):
        respx.post("http://example.test").mock(return_value=httpx.Response(400, text="bad request"))

        chain = Chain(
            id=1, name="ethereum", rpc="http://example.test", rpc_batch_size=2, rpc_concurrency=1, finality_blocks=64
        )
        blocks = await fetch_block_timestamps(chain, 0, 1)
        assert blocks == []

    @respx.mock
    @pytest.mark.asyncio
    async def test_cancellation_during_fetch(self):
        async def slow_handler(request: httpx.Request) -> httpx.Response:
            await asyncio.sleep(10)
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": "0x1"})

        respx.post("http://example.test").mock(side_effect=slow_handler)

        async def run() -> int:
            task = asyncio.create_task(get_latest_block_number(CHAIN))
            await asyncio.sleep(0.05)
            task.cancel()
            return await task

        with pytest.raises(asyncio.CancelledError):
            await run()


class TestGetLatestBlockNumber:
    @respx.mock
    @pytest.mark.asyncio
    async def test_returns_block_number(self):
        respx.post("http://example.test").mock(
            return_value=httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": "0xa"})
        )
        assert await get_latest_block_number(CHAIN) == 10

    @respx.mock
    @pytest.mark.asyncio
    async def test_raises_on_non_string_result(self):
        respx.post("http://example.test").mock(
            return_value=httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": 123})
        )
        with pytest.raises(RpcResponseError, match="expected hex string"):
            await get_latest_block_number(CHAIN)

    @respx.mock
    @pytest.mark.asyncio
    async def test_raises_on_invalid_hex(self):
        respx.post("http://example.test").mock(
            return_value=httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": "not_hex"})
        )
        with pytest.raises(RpcResponseError, match="invalid hex"):
            await get_latest_block_number(CHAIN)


class TestSendErrorHandling:
    @respx.mock
    @pytest.mark.asyncio
    async def test_rate_limit_429(self):
        respx.post("http://example.test").mock(
            return_value=httpx.Response(429, headers={"Retry-After": "30"})
        )
        with patch("chronoblock.rpc.asyncio.sleep", new_callable=AsyncMock), pytest.raises(RpcRateLimitError):
            await get_latest_block_number(CHAIN)

    @respx.mock
    @pytest.mark.asyncio
    async def test_server_error_500(self):
        respx.post("http://example.test").mock(return_value=httpx.Response(502))
        with patch("chronoblock.rpc.asyncio.sleep", new_callable=AsyncMock), pytest.raises(RpcServerError):
            await get_latest_block_number(CHAIN)

    @respx.mock
    @pytest.mark.asyncio
    async def test_non_success_status(self):
        respx.post("http://example.test").mock(return_value=httpx.Response(403, text="forbidden"))
        with pytest.raises(RpcResponseError, match="HTTP 403"):
            await get_latest_block_number(CHAIN)

    @respx.mock
    @pytest.mark.asyncio
    async def test_rpc_error_in_response(self):
        respx.post("http://example.test").mock(
            return_value=httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "error": {"message": "method not found"}})
        )
        with pytest.raises(RpcResponseError, match="RPC error"):
            await get_latest_block_number(CHAIN)

    @respx.mock
    @pytest.mark.asyncio
    async def test_transport_timeout(self):
        respx.post("http://example.test").mock(side_effect=httpx.TimeoutException("timed out"))
        with (
            patch("chronoblock.rpc.asyncio.sleep", new_callable=AsyncMock),
            pytest.raises(RpcTransportError, match="timeout"),
        ):
            await get_latest_block_number(CHAIN)

    @respx.mock
    @pytest.mark.asyncio
    async def test_transport_connect_error(self):
        respx.post("http://example.test").mock(side_effect=httpx.ConnectError("refused"))
        with (
            patch("chronoblock.rpc.asyncio.sleep", new_callable=AsyncMock),
            pytest.raises(RpcTransportError, match="refused"),
        ):
            await get_latest_block_number(CHAIN)

    @respx.mock
    @pytest.mark.asyncio
    async def test_retries_on_transient_then_succeeds(self):
        call_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return httpx.Response(502)
            return httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": "0x5"})

        respx.post("http://example.test").mock(side_effect=handler)
        with patch("chronoblock.rpc.asyncio.sleep", new_callable=AsyncMock):
            result = await get_latest_block_number(CHAIN)
        assert result == 5
        assert call_count == 2


class TestFetchBatchErrorPaths:
    @respx.mock
    @pytest.mark.asyncio
    async def test_skips_rpc_errors_in_batch(self):
        response = [
            {"jsonrpc": "2.0", "id": 0, "result": {"number": "0x0", "timestamp": "0x3e8"}},
            {"jsonrpc": "2.0", "id": 1, "error": {"message": "block not found"}},
        ]
        respx.post("http://example.test").mock(return_value=httpx.Response(200, json=response))

        chain = Chain(id=1, name="ethereum", rpc="http://example.test", rpc_batch_size=10, rpc_concurrency=1, finality_blocks=64)
        blocks = await fetch_block_timestamps(chain, 0, 1)
        assert blocks == [Block(0, 1000)]

    @respx.mock
    @pytest.mark.asyncio
    async def test_skips_malformed_blocks(self):
        response = [
            {"jsonrpc": "2.0", "id": 0, "result": {"number": "0x0", "timestamp": "0x3e8"}},
            {"jsonrpc": "2.0", "id": 1, "result": {"number": "invalid", "timestamp": "0x3e9"}},
        ]
        respx.post("http://example.test").mock(return_value=httpx.Response(200, json=response))

        chain = Chain(id=1, name="ethereum", rpc="http://example.test", rpc_batch_size=10, rpc_concurrency=1, finality_blocks=64)
        blocks = await fetch_block_timestamps(chain, 0, 1)
        assert blocks == [Block(0, 1000)]

    @respx.mock
    @pytest.mark.asyncio
    async def test_raises_rate_limit_when_majority_errors_are_rate_limited(self):
        response = [
            {"jsonrpc": "2.0", "id": 0, "error": {"message": "rate limit exceeded"}},
            {"jsonrpc": "2.0", "id": 1, "error": {"message": "rate limit exceeded"}},
            {"jsonrpc": "2.0", "id": 2, "result": {"number": "0x2", "timestamp": "0x3ea"}},
        ]
        respx.post("http://example.test").mock(return_value=httpx.Response(200, json=response))

        chain = Chain(id=1, name="ethereum", rpc="http://example.test", rpc_batch_size=10, rpc_concurrency=1, finality_blocks=64)
        with pytest.raises(RpcRateLimitError):
            await fetch_block_timestamps(chain, 0, 2)

    @respx.mock
    @pytest.mark.asyncio
    async def test_batch_response_not_list_returns_empty(self):
        """Non-list batch response causes a failure that yields empty contiguous prefix."""
        respx.post("http://example.test").mock(
            return_value=httpx.Response(200, json={"jsonrpc": "2.0", "id": 1, "result": "0x0"})
        )
        chain = Chain(id=1, name="ethereum", rpc="http://example.test", rpc_batch_size=10, rpc_concurrency=1, finality_blocks=64)
        blocks = await fetch_block_timestamps(chain, 0, 0)
        assert blocks == []

    @respx.mock
    @pytest.mark.asyncio
    async def test_rate_limit_error_propagated_from_batch(self):
        """When a batch raises RpcRateLimitError, fetch_block_timestamps re-raises it."""
        call_count = 0

        def handler(request: httpx.Request) -> httpx.Response:
            nonlocal call_count
            call_count += 1
            payload = json.loads(request.content)
            from_block = int(payload[0]["params"][0], 16)
            if from_block == 0:
                return httpx.Response(200, json=batch_response(0, 1))
            return httpx.Response(429)

        respx.post("http://example.test").mock(side_effect=handler)

        chain = Chain(id=1, name="ethereum", rpc="http://example.test", rpc_batch_size=2, rpc_concurrency=2, finality_blocks=64)
        with patch("chronoblock.rpc.asyncio.sleep", new_callable=AsyncMock), pytest.raises(RpcRateLimitError):
            await fetch_block_timestamps(chain, 0, 3)


class TestBatchLengthMismatch:
    @respx.mock
    @pytest.mark.asyncio
    async def test_raises_on_length_mismatch(self):
        """When batch response has fewer items than payload, the batch fails with RpcResponseError."""
        response = [
            {"jsonrpc": "2.0", "id": 0, "result": {"number": "0x0", "timestamp": "0x3e8"}},
        ]
        respx.post("http://example.test").mock(return_value=httpx.Response(200, json=response))

        chain = Chain(id=1, name="ethereum", rpc="http://example.test", rpc_batch_size=10, rpc_concurrency=1, finality_blocks=64)
        # Mismatch is now a hard error — the batch fails and no blocks are returned.
        blocks = await fetch_block_timestamps(chain, 0, 1)
        assert blocks == []


class TestIsRetryable:
    @pytest.mark.parametrize(
        "exc",
        [
            httpx.TimeoutException("timeout"),
            httpx.ConnectError("connection refused"),
            httpx.ReadError("read error"),
            httpx.WriteError("write error"),
            RpcTransportError("eth", "timeout"),
            RpcRateLimitError("eth", retry_after=30),
            RpcServerError("eth", 502),
        ],
    )
    def test_retryable_errors(self, exc):
        assert is_retryable(exc) is True

    @pytest.mark.parametrize(
        "exc",
        [
            ValueError("bad value"),
            TypeError("wrong type"),
            RpcResponseError("eth", "bad request"),
            RuntimeError("unexpected"),
        ],
    )
    def test_non_retryable_errors(self, exc):
        assert is_retryable(exc) is False
