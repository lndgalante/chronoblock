"""Request/response schemas and validation helpers."""

from __future__ import annotations

import re

from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

from chronoblock import config
from chronoblock.models import Chain

__all__ = [
    "BLOCK_PARAM_RE",
    "TimestampBatchRequest",
    "error_response",
    "map_validation_error",
    "resolve_chain",
]

BLOCK_PARAM_RE = re.compile(r"^\d+$")


class TimestampBatchRequest(BaseModel):
    model_config = ConfigDict(strict=True)

    chain: str | None = None
    chain_id: int | None = None
    blocks: list[int]

    @field_validator("blocks")
    @classmethod
    def validate_blocks(cls, v: list[int]) -> list[int]:
        if not v:
            raise ValueError("blocks must be a non-empty array")
        if len(v) > 10_000:
            raise ValueError("max 10,000 blocks per request")
        for i, b in enumerate(v):
            if b < 0 or b > 2**53 - 1:
                raise ValueError(f"invalid block number at index {i}: {b}")
        return v


def error_response(status: int, code: str, message: str, request_id: str | None = None) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content={"error": code, "message": message, "request_id": request_id},
    )


def map_validation_error(exc: ValidationError, request_id: str | None) -> JSONResponse:
    first = exc.errors()[0]
    loc = first.get("loc", ())
    msg: str = first.get("msg", "validation error")

    if msg.startswith("Value error, "):
        msg = msg[len("Value error, ") :]

    if loc and loc[0] in ("chain", "chain_id"):
        return error_response(400, "unknown_chain", "unknown chain", request_id)

    if loc and loc[0] == "blocks":
        if len(loc) >= 2 and isinstance(loc[1], int):
            val = first.get("input", "?")
            return error_response(
                400, "invalid_block_number", f"invalid block number at index {loc[1]}: {val}", request_id
            )
        if "block number at index" in msg:
            return error_response(400, "invalid_block_number", msg, request_id)
        return error_response(400, "invalid_blocks", msg, request_id)

    return error_response(400, "invalid_blocks", "blocks must be a non-empty array", request_id)


def resolve_chain(name: str | None = None, chain_id: int | None = None) -> Chain | None:
    if name:
        return config.CHAIN_BY_NAME.get(name.lower())
    if chain_id is not None:
        return config.CHAIN_BY_ID.get(chain_id)
    return None
