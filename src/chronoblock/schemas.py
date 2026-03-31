"""Pydantic request/response models for the API."""

from __future__ import annotations

from pydantic import BaseModel, Field


class TimestampRequest(BaseModel):
    chain: str | None = None
    chain_id: int | None = None
    blocks: list[int] = Field(min_length=1, max_length=10_000)


class TimestampResponse(BaseModel):
    chain_id: int
    results: dict[str, int | None]


class SingleTimestampResponse(BaseModel):
    chain_id: int
    block_number: int
    timestamp: int


class HealthResponse(BaseModel):
    ok: bool
    degraded: list[str] | None = None


class StatusChainEntry(BaseModel):
    name: str
    chain_id: int
    last_synced_block: int | None
    latest_chain_block: int | None
    lag_blocks: int | None
    observed_block_time_ms: float
    total_stored: int
    syncs_performed: int
    blocks_ingested: int
    last_error: str | None
    last_error_at: str | None


class StatusResponse(BaseModel):
    chains: list[StatusChainEntry]


class ErrorResponse(BaseModel):
    error: str
    message: str
    request_id: str | None = None
