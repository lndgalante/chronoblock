"""Middleware edge-case tests."""

from __future__ import annotations

from collections.abc import MutableMapping
from typing import Any

from starlette.types import Receive, Scope, Send

from chronoblock.middleware import RequestIdMiddleware, RequestLoggingMiddleware, SecureHeadersMiddleware


async def _noop_receive() -> MutableMapping[str, Any]:
    return {}


async def _noop_send(message: MutableMapping[str, Any]) -> None:
    pass


class TestNonHttpPassthrough:
    """All three middlewares should pass non-HTTP scopes straight through."""

    async def test_request_id_passes_websocket(self):
        called = False

        async def inner(scope: Scope, receive: Receive, send: Send) -> None:
            nonlocal called
            called = True

        mw = RequestIdMiddleware(inner)
        await mw({"type": "websocket"}, _noop_receive, _noop_send)
        assert called

    async def test_secure_headers_passes_websocket(self):
        called = False

        async def inner(scope: Scope, receive: Receive, send: Send) -> None:
            nonlocal called
            called = True

        mw = SecureHeadersMiddleware(inner)
        await mw({"type": "websocket"}, _noop_receive, _noop_send)
        assert called

    async def test_request_logging_passes_websocket(self):
        called = False

        async def inner(scope: Scope, receive: Receive, send: Send) -> None:
            nonlocal called
            called = True

        mw = RequestLoggingMiddleware(inner)
        await mw({"type": "websocket"}, _noop_receive, _noop_send)
        assert called
