"""Entry point. Run with: uvicorn chronoblock.main:app"""

import sys

from chronoblock.errors import ConfigError

try:
    from chronoblock import config
    from chronoblock.api import create_app
except ConfigError as err:
    print("fatal: config validation failed", file=sys.stderr)
    for e in err.errors:
        print(f"  - {e}", file=sys.stderr)
    sys.exit(1)

__all__ = ["app"]

app = create_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=config.settings.port)
