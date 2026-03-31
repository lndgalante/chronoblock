# chronoblock

Fast API service that maps EVM block numbers to unix timestamps. Built with Python, FastAPI, and SQLite.

One process — starts the HTTP API and syncs all chains in the background. No cron jobs, no external dependencies, no manual work.

## Quick start

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
cp .env.template .env       # configure RPC URLs (at least one required)
uv sync --all-extras         # install dependencies
uv run python -m chronoblock.main   # API + background sync
```

Or with uvicorn directly:

```bash
uv run uvicorn chronoblock.main:app --host 0.0.0.0 --port 3000
```

### Development

```bash
uv run uvicorn chronoblock.main:app --reload   # hot reload
uv run pytest -v                                # test suite
uv run ruff check src/ tests/                   # lint
```

The service will begin syncing all configured chains from block 0 (or resume from the last stored block) and serve the API immediately.

## API

### `POST /v1/timestamps` — batch lookup

```bash
curl -s -X POST http://localhost:3000/v1/timestamps \
  -H "Content-Type: application/json" \
  -d '{"chain": "ethereum", "blocks": [18000000, 18000001, 18000002]}'
```

```json
{
  "chain_id": 1,
  "results": {
    "18000000": 1693526400,
    "18000001": 1693526412,
    "18000002": 1693526424
  }
}
```

- Accepts `chain` (name) or `chain_id` (number).
- Up to 10,000 blocks per request.
- Returns `null` for blocks not yet synced.

### `GET /v1/timestamps/:chain/:block` — single lookup

```bash
curl -s http://localhost:3000/v1/timestamps/ethereum/18000000
```

Returns `Cache-Control: immutable` — block timestamps never change once finalized.

### `GET /v1/status` — per-chain sync stats

Returns lag, observed block time, error state, and ingestion counters for each chain. Useful for dashboards and alerting.

### `GET /health` — load-balancer probe

Returns `200 OK` if all chains are within `HEALTH_MAX_LAG_SECS`. Returns `503` with degradation details otherwise. Chains still performing their initial sync get a grace period.

## Configuration

| Variable | Default | Description |
|---|---|---|
| `PORT` | `3000` | HTTP port |
| `DATA_DIR` | `./data` | SQLite directory (one `.db` per chain) |
| `HEALTH_MAX_LAG_SECS` | `120` | Max lag before `/health` returns 503 |
| `SYNC_CHUNK_SIZE` | `2000` | Blocks per sync cycle |
| `ETH_RPC_URL` | — | Ethereum RPC (set to enable) |
| `SCROLL_RPC_URL` | — | Scroll RPC (set to enable) |
| `INK_RPC_URL` | — | Ink RPC (set to enable) |
| `HYPEREVM_RPC_URL` | — | HyperEVM RPC (set to enable) |

## Add a chain

Add an entry to `_CHAIN_CANDIDATES` in `src/chronoblock/config.py`, add the corresponding RPC URL field to the `Settings` class, and restart. The service validates the config at startup and syncs automatically.

## Architecture

```
src/chronoblock/
  config.py  — Chain definitions, env vars, startup validation
  log.py     — Structured JSON logger (stdout/stderr)
  db.py      — Per-chain SQLite (WAL, mmap, cached statements)
  rpc.py     — JSON-RPC client (batching, concurrency, retry)
  syncer.py  — Background sync workers (one per chain)
  api.py     — FastAPI app, routes, middleware
  main.py    — Entry point (uvicorn)
  schemas.py — Pydantic request/response models
```

**Sync loop:** each chain gets an independent asyncio task that checks `MAX(block_number)` in its SQLite file, fetches the gap from the RPC in concurrent batches, inserts in a single transaction, and either loops immediately (catch-up) or sleeps for one observed block time (steady-state).

**Block time is self-calibrating.** The service computes the average block interval from the last 50 stored blocks. If a chain changes its block time (e.g. Scroll 3s → 1s), the poll interval adjusts automatically — no config change or restart needed.

**Gap-safety.** `fetch_block_timestamps` returns only the contiguous prefix starting at `from_block`. If block N is missing, everything up to N-1 is inserted and the syncer retries from N next cycle. The DB never has holes.

## Production notes

- **Storage:** ~12 bytes/block. Ethereum mainnet (~22M blocks) = ~260 MB. HyperEVM (~30M blocks) = ~360 MB.
- **Read latency:** single lookups are in-process SQLite B-tree lookups on mmap'd pages — sub-microsecond on warm cache. 10k-block batch in <10ms.
- **SQLite tuning:** WAL mode, 64 MB page cache, 1 GB mmap, `WITHOUT ROWID` table, `busy_timeout` for deploy overlap safety, periodic WAL checkpointing.
- **Graceful shutdown:** SIGINT/SIGTERM triggers the FastAPI lifespan shutdown — cancels all sync tasks, checkpoints WAL, closes DB handles.
- **Error handling:** RPC retries with exponential backoff (only for transient errors). Global exception handler on API routes.
- **Health behavior:** `/health` stays green during clean startup, but flips to `503` if a chain fails before its first successful sync, exceeds its initial-sync grace period, becomes unreadable, or falls behind the lag budget.
- **Rate limiting:** not built-in. Use a reverse proxy (nginx, Cloudflare, etc.) in front of the service.
