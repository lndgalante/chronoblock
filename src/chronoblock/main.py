"""Entry point. Run with: uvicorn chronoblock.main:app"""

from chronoblock.api import create_app
from chronoblock.config import settings

app = create_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=settings.port)
