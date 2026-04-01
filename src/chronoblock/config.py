"""
Chain definitions and environment-driven settings. Validated at import
time — invalid config crashes the process before the server starts,
so deploy pipelines fail fast.
"""

from __future__ import annotations

import os
import sys
from typing import TypedDict

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from chronoblock.errors import ConfigError
from chronoblock.models import Chain

__all__ = ["Settings", "settings", "CHAINS", "CHAIN_BY_NAME", "CHAIN_BY_ID"]

# ── Settings ─────────────────────────────────────────────────────────


class Settings(BaseSettings):
    port: int = Field(default=3000)
    data_dir: str = Field(default="./data")
    health_max_lag_secs: int = Field(default=120)
    sync_chunk_size: int = Field(default=2000)

    seed_url: str | None = Field(default=None)

    eth_rpc_url: str | None = Field(default=None)
    scroll_rpc_url: str | None = Field(default=None)
    ink_rpc_url: str | None = Field(default=None)
    hyperevm_rpc_url: str | None = Field(default=None)

    model_config = SettingsConfigDict(env_file=".env", case_sensitive=False)


# ── Chain candidates ─────────────────────────────────────────────────


class _ChainCandidate(TypedDict):
    id: int
    name: str
    field: str
    rpc_batch_size: int
    rpc_concurrency: int
    finality_blocks: int


_is_deployed = "RAILWAY_ENVIRONMENT_NAME" in os.environ

_CHAIN_CANDIDATES: list[_ChainCandidate] = [
    {
        "id": 1,
        "name": "ethereum",
        "field": "eth_rpc_url",
        "rpc_batch_size": 50 if _is_deployed else 100,
        "rpc_concurrency": 2 if _is_deployed else 5,
        "finality_blocks": 64,
    },
    {
        "id": 534352,
        "name": "scroll",
        "field": "scroll_rpc_url",
        "rpc_batch_size": 50 if _is_deployed else 100,
        "rpc_concurrency": 2 if _is_deployed else 5,
        "finality_blocks": 300,
    },
    {
        "id": 57073,
        "name": "ink",
        "field": "ink_rpc_url",
        "rpc_batch_size": 50 if _is_deployed else 100,
        "rpc_concurrency": 2 if _is_deployed else 5,
        "finality_blocks": 300,
    },
    {
        "id": 998,
        "name": "hyperevm",
        "field": "hyperevm_rpc_url",
        "rpc_batch_size": 50 if _is_deployed else 100,
        "rpc_concurrency": 2 if _is_deployed else 5,
        "finality_blocks": 300,
    },
]


def _build_chains(settings: Settings) -> list[Chain]:
    chains: list[Chain] = []
    for candidate in _CHAIN_CANDIDATES:
        rpc_url: str | None = getattr(settings, candidate["field"])
        if rpc_url:
            chains.append(
                Chain(
                    id=candidate["id"],
                    name=candidate["name"],
                    rpc=rpc_url,
                    rpc_batch_size=candidate["rpc_batch_size"],
                    rpc_concurrency=candidate["rpc_concurrency"],
                    finality_blocks=candidate["finality_blocks"],
                )
            )
    return chains


# ── Validation ───────────────────────────────────────────────────────


def _validate(settings: Settings, chains: list[Chain]) -> None:
    errors: list[str] = []

    if not (1 <= settings.port <= 65535):
        errors.append(f"PORT must be 1–65535, got: {settings.port}")
    if settings.health_max_lag_secs < 1:
        errors.append(f"HEALTH_MAX_LAG_SECS must be ≥1, got: {settings.health_max_lag_secs}")
    if settings.sync_chunk_size < 1:
        errors.append(f"SYNC_CHUNK_SIZE must be ≥1, got: {settings.sync_chunk_size}")

    seen_ids: set[int] = set()
    seen_names: set[str] = set()

    for c in chains:
        if c.id in seen_ids:
            errors.append(f"duplicate chain id: {c.id}")
        if c.name in seen_names:
            errors.append(f"duplicate chain name: {c.name}")
        if not c.rpc.startswith("http"):
            errors.append(f"{c.name}: rpc must start with http, got: {c.rpc}")
        if not (1 <= c.rpc_batch_size <= 1000):
            errors.append(f"{c.name}: rpc_batch_size must be 1–1000, got: {c.rpc_batch_size}")
        if not (1 <= c.rpc_concurrency <= 50):
            errors.append(f"{c.name}: rpc_concurrency must be 1–50, got: {c.rpc_concurrency}")
        seen_ids.add(c.id)
        seen_names.add(c.name)

    if not chains:
        errors.append("no chains enabled — set at least one RPC URL (e.g. ETH_RPC_URL)")

    if errors:
        raise ConfigError(errors)

    skipped = [candidate["name"] for candidate in _CHAIN_CANDIDATES if not getattr(settings, candidate["field"])]
    if skipped:
        print(f"disabled chains (no RPC): {', '.join(skipped)}", file=sys.stderr)


# ── Module-level singletons ──────────────────────────────────────────

settings = Settings()
CHAINS = _build_chains(settings)
try:
    _validate(settings, CHAINS)
except ConfigError as err:
    print("fatal: config validation failed", file=sys.stderr)
    for e in err.errors:
        print(f"  - {e}", file=sys.stderr)
    sys.exit(1)

CHAIN_BY_NAME: dict[str, Chain] = {c.name: c for c in CHAINS}
CHAIN_BY_ID: dict[int, Chain] = {c.id: c for c in CHAINS}
