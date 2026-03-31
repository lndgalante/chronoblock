"""Domain exceptions for chronoblock."""

from __future__ import annotations

__all__ = [
    "ConfigError",
    "DataDirError",
    "RpcError",
    "RpcTransportError",
    "RpcRateLimitError",
    "RpcServerError",
    "RpcResponseError",
]


# ── Startup ─────────────────────────────────────────────────────────


class ConfigError(Exception):
    """One or more configuration values are invalid."""

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        detail = "; ".join(errors)
        super().__init__(f"config validation failed: {detail}")


class DataDirError(Exception):
    """The data directory is missing or not writable."""


# ── RPC ─────────────────────────────────────────────────────────────


class RpcError(Exception):
    """Base exception for RPC failures."""

    def __init__(self, chain: str, message: str) -> None:
        self.chain = chain
        super().__init__(f"{chain}: {message}")


class RpcTransportError(RpcError):
    """Network-level failure (timeout, connection reset, etc.)."""


class RpcRateLimitError(RpcError):
    """HTTP 429 — rate limit exceeded."""

    def __init__(self, chain: str, retry_after: int | None = None) -> None:
        self.retry_after = retry_after
        msg = "rate limited"
        if retry_after is not None:
            msg += f" (retry after {retry_after}s)"
        super().__init__(chain, msg)


class RpcServerError(RpcError):
    """HTTP 5xx from the RPC endpoint."""

    def __init__(self, chain: str, status_code: int) -> None:
        self.status_code = status_code
        super().__init__(chain, f"server error {status_code}")


class RpcResponseError(RpcError):
    """The RPC response was malformed or contained an application-level error."""
