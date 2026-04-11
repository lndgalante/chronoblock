.PHONY: start test cov lint deploy logs stress

start:
	uv run python -m chronoblock.main

test:
	uv run pytest tests/ -v

cov:
	uv run pytest tests/ --cov=chronoblock --cov-report=term-missing

lint:
	uv run ruff check src/ tests/

deploy:
	railway up --detach

logs:
	railway logs

stress:
	uv run python tests/stress.py
