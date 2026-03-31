"""
Structured JSON logger. Every line is a parseable JSON object for
Datadog, CloudWatch, Grafana Loki, etc. Uses stderr for errors so
they're distinguishable in container runtimes.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from typing import Any


def log(level: str, msg: str, **extra: Any) -> None:
    entry = json.dumps({
        "ts": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "msg": msg,
        **extra,
    })
    stream = sys.stderr if level in ("error", "warn") else sys.stdout
    print(entry, file=stream, flush=True)
