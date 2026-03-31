"""RPC client tests — port of rpc.test.ts."""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest
import respx

from chronoblock.config import Block, Chain
from chronoblock.rpc import _is_retryable, close_client, fetch_block_timestamps, get_latest_block_number

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


class TestIsRetryable:
    @pytest.mark.parametrize(
        "exc",
        [
            httpx.TimeoutException("timeout"),
            httpx.ConnectError("connection refused"),
            httpx.ReadError("read error"),
            httpx.WriteError("write error"),
        ],
    )
    def test_transport_errors_are_retryable(self, exc):
        assert _is_retryable(exc) is True

    @pytest.mark.parametrize(
        "message",
        [
            "RPC 429: rate limited",
            "RPC 500: internal server error",
            "RPC 502: bad gateway",
            "RPC 503: service unavailable",
        ],
    )
    def test_retryable_status_codes_in_message(self, message):
        assert _is_retryable(RuntimeError(message)) is True

    @pytest.mark.parametrize(
        "message",
        [
            "ECONNRESET",
            "ETIMEDOUT",
            "socket hang up",
        ],
    )
    def test_network_error_keywords(self, message):
        assert _is_retryable(RuntimeError(message)) is True

    @pytest.mark.parametrize(
        "exc",
        [
            ValueError("bad value"),
            RuntimeError("RPC 400: bad request"),
            RuntimeError("RPC 401: unauthorized"),
            TypeError("wrong type"),
        ],
    )
    def test_non_retryable_errors(self, exc):
        assert _is_retryable(exc) is False
