"""Middleware edge-case tests."""

from __future__ import annotations

from chronoblock.middleware import RequestIdMiddleware, RequestLoggingMiddleware, SecureHeadersMiddleware


class TestNonHttpPassthrough:
    """All three middlewares should pass non-HTTP scopes straight through."""

    async def test_request_id_passes_websocket(self):
        called = False

        async def inner(scope, receive, send):
            nonlocal called
            called = True

        mw = RequestIdMiddleware(inner)
        await mw({"type": "websocket"}, None, None)
        assert called

    async def test_secure_headers_passes_websocket(self):
        called = False

        async def inner(scope, receive, send):
            nonlocal called
            called = True

        mw = SecureHeadersMiddleware(inner)
        await mw({"type": "websocket"}, None, None)
        assert called

    async def test_request_logging_passes_websocket(self):
        called = False

        async def inner(scope, receive, send):
            nonlocal called
            called = True

        mw = RequestLoggingMiddleware(inner)
        await mw({"type": "websocket"}, None, None)
        assert called
