"""
Structured JSON logger. Every line is a parseable JSON object for
Datadog, CloudWatch, Grafana Loki, etc. Uses stderr for errors so
they're distinguishable in container runtimes.
"""

from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from typing import Any, Literal

__all__ = ["log"]

LogLevel = Literal["info", "warn", "error"]


def log(level: LogLevel, msg: str, **extra: Any) -> None:
    entry = json.dumps(
        {
            "ts": datetime.now(UTC).isoformat(),
            "level": level,
            "msg": msg,
            **extra,
        }
    )
    stream = sys.stderr if level in ("error", "warn") else sys.stdout
    print(entry, file=stream, flush=True)
