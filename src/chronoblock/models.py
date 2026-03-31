"""Domain types shared across the codebase."""

from __future__ import annotations

from dataclasses import dataclass
from typing import NamedTuple

__all__ = ["Block", "Chain"]


@dataclass(frozen=True, slots=True)
class Chain:
    id: int
    name: str
    rpc: str
    rpc_batch_size: int
    rpc_concurrency: int
    finality_blocks: int


class Block(NamedTuple):
    number: int
    timestamp: int
