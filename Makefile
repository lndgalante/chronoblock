.PHONY: start test cov lint

start:
	uv run python -m chronoblock.main

test:
	uv run pytest tests/ -v

cov:
	uv run pytest tests/ --cov=chronoblock --cov-report=term-missing

lint:
	uv run ruff check src/ tests/
