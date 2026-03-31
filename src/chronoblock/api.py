"""
FastAPI application — routes, middleware, and health checks.
"""

from __future__ import annotations

import re
import time
import uuid
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from fastapi import Depends, FastAPI, Request, Response
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

from chronoblock.config import (
    CHAIN_BY_ID,
    CHAIN_BY_NAME,
    CHAINS,
    Chain,
    settings,
)
from chronoblock.db import block_count, close_all, get_timestamps, is_healthy
from chronoblock.log import log
from chronoblock.rpc import close_client
from chronoblock.syncer import SyncState, get_sync_state, start_all, stop_all

# Type aliases for dependency-injected callables.
GetTimestampsFn = Callable[[Chain, list[int]], list[int | None]]
BlockCountFn = Callable[[Chain], int]
IsHealthyFn = Callable[[Chain], bool]
GetSyncStateFn = Callable[[Chain], SyncState]
NowFn = Callable[[], float]

INITIAL_SYNC_GRACE_SECS = 5 * 60
BLOCK_PARAM_RE = re.compile(r"^\d+$")


# ── Dependencies (for DI / test overrides) ───────────────────────────


def dep_get_timestamps() -> GetTimestampsFn:
    return get_timestamps


def dep_block_count() -> BlockCountFn:
    return block_count


def dep_is_healthy() -> IsHealthyFn:
    return is_healthy


def dep_get_sync_state() -> GetSyncStateFn:
    return get_sync_state


def dep_now() -> NowFn:
    return time.time


# ── Helpers ──────────────────────────────────────────────────────────


def _error_response(status: int, code: str, message: str, request_id: str | None = None) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={"error": code, "message": message, "request_id": request_id},
    )


def _resolve_chain(name: str | None = None, chain_id: int | None = None) -> Chain | None:
    if isinstance(name, str) and name:
        return CHAIN_BY_NAME.get(name.lower())
    if isinstance(chain_id, int):
        return CHAIN_BY_ID.get(chain_id)
    return None


def _should_degrade_chain(sync: SyncState, now: float) -> str | None:
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
    if lag_secs > settings.health_max_lag_secs:
        return f"{lag_blocks} blocks behind (~{round(lag_secs)}s)"

    return None


# ── Middleware ────────────────────────────────────────────────────────


class RequestIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
        request.state.request_id = request_id
        response = await call_next(request)
        response.headers["X-Request-Id"] = request_id
        return response


class SecureHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        response = await call_next(request)
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "SAMEORIGIN"
        response.headers["X-XSS-Protection"] = "0"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["Content-Security-Policy"] = "default-src 'none'"
        return response


class RequestLoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        start = time.monotonic()
        response = await call_next(request)
        if request.url.path != "/health":
            log(
                "info",
                f"{request.method} {request.url.path} {response.status_code}",
                method=request.method,
                path=request.url.path,
                status=response.status_code,
                duration_ms=round((time.monotonic() - start) * 1000),
                request_id=getattr(request.state, "request_id", None),
            )
        return response


# ── Lifespan ─────────────────────────────────────────────────────────


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    await start_all()
    log("info", f"listening on :{settings.port}", chains=[c.name for c in CHAINS])
    yield
    await stop_all()
    close_all()
    await close_client()


# ── App factory ──────────────────────────────────────────────────────


def create_app() -> FastAPI:
    app = FastAPI(
        title="chronoblock",
        description="Fast block-number → timestamp API for EVM chains",
        lifespan=lifespan,
        redirect_slashes=True,
    )

    app.add_middleware(RequestLoggingMiddleware)
    app.add_middleware(SecureHeadersMiddleware)
    app.add_middleware(RequestIdMiddleware)

    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        request_id = getattr(request.state, "request_id", None)
        log("error", "unhandled route error", error=str(exc), path=request.url.path, request_id=request_id)
        return _error_response(500, "internal_error", "internal server error", request_id)

    # ── Health ───────────────────────────────────────────────────────

    @app.get("/health")
    def health(
        fn_is_healthy: IsHealthyFn = Depends(dep_is_healthy),
        fn_get_sync_state: GetSyncStateFn = Depends(dep_get_sync_state),
        fn_now: NowFn = Depends(dep_now),
    ) -> JSONResponse:
        degraded: list[str] = []
        now = fn_now()

        for ch in CHAINS:
            if not fn_is_healthy(ch):
                degraded.append(f"{ch.name}: db unreadable")
                continue
            reason = _should_degrade_chain(fn_get_sync_state(ch), now)
            if reason:
                degraded.append(f"{ch.name}: {reason}")

        headers = {"Cache-Control": "no-store"}
        if degraded:
            return JSONResponse({"ok": False, "degraded": degraded}, status_code=503, headers=headers)
        return JSONResponse({"ok": True}, headers=headers)

    # ── Timestamps (batch) ───────────────────────────────────────────

    @app.post("/v1/timestamps")
    async def post_timestamps(
        request: Request,
        fn_get_timestamps: GetTimestampsFn = Depends(dep_get_timestamps),
    ) -> JSONResponse:
        request_id = getattr(request.state, "request_id", None)

        content_type = request.headers.get("content-type", "")
        if "application/json" not in content_type:
            return _error_response(415, "unsupported_media_type", "Content-Type must be application/json", request_id)

        try:
            body = await request.json()
        except Exception:
            return _error_response(400, "invalid_json", "invalid JSON", request_id)

        if not isinstance(body, dict):
            return _error_response(400, "invalid_json", "invalid JSON", request_id)

        chain = _resolve_chain(body.get("chain"), body.get("chain_id"))
        if not chain:
            return _error_response(400, "unknown_chain", "unknown chain", request_id)

        blocks = body.get("blocks")
        if not isinstance(blocks, list) or len(blocks) == 0:
            return _error_response(400, "invalid_blocks", "blocks must be a non-empty array", request_id)
        if len(blocks) > 10_000:
            return _error_response(400, "invalid_blocks", "max 10,000 blocks per request", request_id)

        for i, b in enumerate(blocks):
            if not isinstance(b, int) or isinstance(b, bool) or b < 0 or b > 2**53 - 1:
                return _error_response(
                    400, "invalid_block_number", f"invalid block number at index {i}: {b}", request_id
                )

        timestamps = fn_get_timestamps(chain, blocks)
        results = {str(bn): ts for bn, ts in zip(blocks, timestamps, strict=True)}

        return JSONResponse({"chain_id": chain.id, "results": results})

    # ── Timestamps (single) ──────────────────────────────────────────

    @app.get("/v1/timestamps/{chain_name}/{block}")
    def get_single_timestamp(
        chain_name: str,
        block: str,
        fn_get_timestamps: GetTimestampsFn = Depends(dep_get_timestamps),
        fn_get_sync_state: GetSyncStateFn = Depends(dep_get_sync_state),
    ) -> JSONResponse:
        chain = CHAIN_BY_NAME.get(chain_name.lower())
        if not chain:
            return _error_response(400, "unknown_chain", "unknown chain")

        if not BLOCK_PARAM_RE.match(block):
            return _error_response(400, "invalid_block_number", "invalid block number")

        bn = int(block)
        if bn > 2**53 - 1:
            return _error_response(400, "invalid_block_number", "invalid block number")

        timestamps = fn_get_timestamps(chain, [bn])
        ts = timestamps[0]
        if ts is None:
            return _error_response(404, "not_found", "block not found")

        sync = fn_get_sync_state(chain)
        finalized = sync.latest_chain_block is not None and bn <= sync.latest_chain_block - chain.finality_blocks
        cache = "public, max-age=31536000, immutable" if finalized else "public, max-age=60"

        return JSONResponse(
            {"chain_id": chain.id, "block_number": bn, "timestamp": ts},
            headers={"Cache-Control": cache},
        )

    # ── Status ───────────────────────────────────────────────────────

    @app.get("/v1/status")
    def get_status(
        fn_get_sync_state: GetSyncStateFn = Depends(dep_get_sync_state),
        fn_block_count: BlockCountFn = Depends(dep_block_count),
    ) -> JSONResponse:
        chains = []
        for ch in CHAINS:
            sync = fn_get_sync_state(ch)
            lag_blocks = (
                sync.latest_chain_block - sync.last_synced_block
                if sync.latest_chain_block is not None and sync.last_synced_block is not None
                else None
            )
            chains.append(
                {
                    "name": ch.name,
                    "chain_id": ch.id,
                    "last_synced_block": sync.last_synced_block,
                    "latest_chain_block": sync.latest_chain_block,
                    "lag_blocks": lag_blocks,
                    "observed_block_time_ms": sync.observed_block_time_ms,
                    "total_stored": fn_block_count(ch),
                    "syncs_performed": sync.syncs_performed,
                    "blocks_ingested": sync.blocks_ingested,
                    "last_error": sync.last_error,
                    "last_error_at": (
                        datetime.fromtimestamp(sync.last_error_at, tz=UTC).isoformat() if sync.last_error_at else None
                    ),
                }
            )

        return JSONResponse({"chains": chains}, headers={"Cache-Control": "no-store"})

    return app
