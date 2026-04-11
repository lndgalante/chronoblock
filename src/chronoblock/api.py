"""FastAPI application — routes and health checks."""

from __future__ import annotations

import asyncio
import traceback
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from chronoblock import config
from chronoblock.db import close_all, warm_caches
from chronoblock.dependencies import (
    BlockCountFn,
    GetSyncStateFn,
    GetTimestampsFn,
    IsHealthyFn,
    NowFn,
    dep_block_count,
    dep_get_sync_state,
    dep_get_timestamps,
    dep_is_healthy,
    dep_now,
)
from chronoblock.health import should_degrade_chain
from chronoblock.log import log
from chronoblock.middleware import (
    RequestIdMiddleware,
    RequestLoggingMiddleware,
    SecureHeadersMiddleware,
)
from chronoblock.rpc import close_client
from chronoblock.schemas import (
    BLOCK_PARAM_RE,
    TimestampBatchRequest,
    error_response,
    map_validation_error,
    resolve_chain,
)
from chronoblock.seed import download_seed_data
from chronoblock.syncer import start_all, stop_all

__all__ = ["create_app"]


# ── Lifespan ─────────────────────────────────────────────────────────


async def _shutdown() -> None:
    try:
        await stop_all()
    except Exception as err:
        log("error", "stop_all failed during shutdown", error=str(err))
    try:
        await asyncio.to_thread(close_all)
    except Exception as err:
        log("error", "close_all failed during shutdown", error=str(err))
    try:
        await close_client()
    except Exception as err:
        log("error", "close_client failed during shutdown", error=str(err))


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    try:
        await download_seed_data()
    except Exception as err:
        log("error", "seed failed, continuing without seed data", error=str(err), traceback=traceback.format_exc())
    try:
        await start_all()
        await asyncio.to_thread(warm_caches, config.CHAINS)
    except Exception:
        await _shutdown()
        raise
    log("info", f"listening on :{config.settings.port}", chains=[c.name for c in config.CHAINS])
    try:
        yield
    finally:
        await _shutdown()


# ── App factory ──────────────────────────────────────────────────────


def create_app() -> FastAPI:
    app = FastAPI(
        title="chronoblock",
        description="Fast block-number \u2192 timestamp API for EVM chains",
        lifespan=lifespan,
        redirect_slashes=True,
        docs_url=None,
        redoc_url=None,
    )

    app.add_middleware(RequestLoggingMiddleware)
    app.add_middleware(SecureHeadersMiddleware)
    app.add_middleware(RequestIdMiddleware)

    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        request_id = getattr(request.state, "request_id", None)
        log(
            "error",
            "unhandled route error",
            error=str(exc),
            traceback=traceback.format_exc(),
            path=request.url.path,
            request_id=request_id,
        )
        return error_response(500, "internal_error", "internal server error", request_id)

    # ── Health ───────────────────────────────────────────────────────

    @app.get("/health")
    async def health(
        fn_is_healthy: IsHealthyFn = Depends(dep_is_healthy),
        fn_get_sync_state: GetSyncStateFn = Depends(dep_get_sync_state),
        fn_now: NowFn = Depends(dep_now),
    ) -> JSONResponse:
        degraded: list[str] = []
        now = fn_now()

        healthy_results = await asyncio.gather(*[asyncio.to_thread(fn_is_healthy, ch) for ch in config.CHAINS])

        for ch, healthy in zip(config.CHAINS, healthy_results, strict=True):
            if not healthy:
                degraded.append(f"{ch.name}: db unreadable")
                continue
            reason = should_degrade_chain(fn_get_sync_state(ch), now)
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
        if content_type.split(";")[0].strip().lower() != "application/json":
            return error_response(415, "unsupported_media_type", "Content-Type must be application/json", request_id)

        try:
            body = await request.json()
        except (ValueError, TypeError):
            return error_response(400, "invalid_json", "invalid JSON", request_id)

        if not isinstance(body, dict):
            return error_response(400, "invalid_json", "invalid JSON", request_id)

        try:
            req = TimestampBatchRequest.model_validate(body)
        except ValidationError as exc:
            return map_validation_error(exc, request_id)

        chain = resolve_chain(req.chain, req.chain_id)
        if not chain:
            return error_response(400, "unknown_chain", "unknown chain", request_id)

        timestamps = await asyncio.to_thread(fn_get_timestamps, chain, req.blocks)
        results = {str(bn): ts for bn, ts in zip(req.blocks, timestamps, strict=True)}

        return JSONResponse({"chain_id": chain.id, "results": results})

    # ── Timestamps (single) ──────────────────────────────────────────

    @app.get("/v1/timestamps/{chain_name}/{block}")
    async def get_single_timestamp(
        chain_name: str,
        block: str,
        fn_get_timestamps: GetTimestampsFn = Depends(dep_get_timestamps),
        fn_get_sync_state: GetSyncStateFn = Depends(dep_get_sync_state),
    ) -> JSONResponse:
        chain = config.CHAIN_BY_NAME.get(chain_name.lower())
        if not chain:
            return error_response(400, "unknown_chain", "unknown chain")

        if not BLOCK_PARAM_RE.match(block):
            return error_response(400, "invalid_block_number", "invalid block number")

        bn = int(block)
        if bn > 2**53 - 1:
            return error_response(400, "invalid_block_number", "invalid block number")

        timestamps = await asyncio.to_thread(fn_get_timestamps, chain, [bn])
        ts = timestamps[0]
        if ts is None:
            return error_response(404, "not_found", "block not found")

        sync = fn_get_sync_state(chain)
        finalized = sync.latest_chain_block is not None and bn <= sync.latest_chain_block - chain.finality_blocks
        cache = "public, max-age=31536000, immutable" if finalized else "public, max-age=60"

        return JSONResponse(
            {"chain_id": chain.id, "block_number": bn, "timestamp": ts},
            headers={"Cache-Control": cache},
        )

    # ── Status ───────────────────────────────────────────────────────

    @app.get("/v1/status")
    async def get_status(
        fn_get_sync_state: GetSyncStateFn = Depends(dep_get_sync_state),
        fn_block_count: BlockCountFn = Depends(dep_block_count),
    ) -> JSONResponse:
        counts = await asyncio.gather(*[asyncio.to_thread(fn_block_count, ch) for ch in config.CHAINS])

        chains = []
        for ch, count in zip(config.CHAINS, counts, strict=True):
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
                    "total_stored": count,
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
