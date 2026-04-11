"""
Stress tests for the Chronoblock API.

Discovers live chains via /v1/status, then hammers single-block and
batch endpoints with random block numbers bounded by the chain tip.

Usage:
    uv run python tests/stress.py [--base-url URL] [--rounds N] [--concurrency N] [--batch-size N]
"""

from __future__ import annotations

import argparse
import asyncio
import random
import statistics
import sys
import time
from dataclasses import dataclass, field

import httpx

DEFAULT_BASE_URL = "https://chronoblock-production.up.railway.app"
DEFAULT_ROUNDS = 20
DEFAULT_CONCURRENCY = 2
DEFAULT_BATCH_SIZE = 50
DEFAULT_ERROR_THRESHOLD = 0.1  # fail only if >10% of requests error


# ── helpers ──────────────────────────────────────────────────────────


@dataclass
class ChainInfo:
    name: str
    latest_block: int


@dataclass
class Stats:
    label: str
    latencies_ms: list[float] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def record(self, elapsed_ms: float) -> None:
        self.latencies_ms.append(elapsed_ms)

    def record_error(self, message: str) -> None:
        self.errors.append(message)

    @property
    def error_rate(self) -> float:
        total = len(self.latencies_ms) + len(self.errors)
        return len(self.errors) / total if total else 0.0

    def report(self) -> None:
        total = len(self.latencies_ms) + len(self.errors)
        ok = len(self.latencies_ms)
        rate_pct = self.error_rate * 100
        print(f"\n  {self.label}  ({ok}/{total} ok, {len(self.errors)} errors, {rate_pct:.0f}% error rate)")
        if self.latencies_ms:
            p50 = statistics.median(self.latencies_ms)
            p95 = sorted(self.latencies_ms)[int(len(self.latencies_ms) * 0.95)]
            p99 = sorted(self.latencies_ms)[int(len(self.latencies_ms) * 0.99)]
            worst = max(self.latencies_ms)
            print(f"    p50={p50:.0f}ms  p95={p95:.0f}ms  p99={p99:.0f}ms  max={worst:.0f}ms")
        if self.errors:
            unique = set(self.errors)
            for err in list(unique)[:5]:
                print(f"    ERR: {err}")


def random_block(chain: ChainInfo) -> int:
    return random.randint(1, chain.latest_block)


# ── discover chains ──────────────────────────────────────────────────


async def discover_chains(client: httpx.AsyncClient, base_url: str) -> list[ChainInfo]:
    for attempt in range(3):
        resp = await client.get(f"{base_url}/v1/status")
        if resp.status_code == 200:
            break
        print(f"  /v1/status returned {resp.status_code}, retrying ({attempt + 1}/3)...")
        await asyncio.sleep(2 ** attempt)
    resp.raise_for_status()
    data = resp.json()
    entries = data["chains"] if isinstance(data, dict) and "chains" in data else data
    chains: list[ChainInfo] = []
    for entry in entries:
        name = entry["name"]
        latest = entry.get("latest_chain_block") or entry.get("last_synced_block")
        if latest and latest > 0:
            chains.append(ChainInfo(name=name, latest_block=latest))
    return chains


# ── single-block stress ─────────────────────────────────────────────


async def stress_single(
    client: httpx.AsyncClient,
    base_url: str,
    chain: ChainInfo,
    semaphore: asyncio.Semaphore,
    stats: Stats,
) -> None:
    block = random_block(chain)
    url = f"{base_url}/v1/timestamps/{chain.name}/{block}"
    async with semaphore:
        t0 = time.perf_counter()
        try:
            resp = await client.get(url)
            elapsed = (time.perf_counter() - t0) * 1000
            if resp.status_code == 200:
                stats.record(elapsed)
            else:
                stats.record_error(f"HTTP {resp.status_code} for {chain.name}/{block}")
        except httpx.HTTPError as exc:
            elapsed = (time.perf_counter() - t0) * 1000
            stats.record_error(f"{type(exc).__name__}: {exc}")


# ── batch stress ─────────────────────────────────────────────────────


async def stress_batch(
    client: httpx.AsyncClient,
    base_url: str,
    chain: ChainInfo,
    batch_size: int,
    semaphore: asyncio.Semaphore,
    stats: Stats,
) -> None:
    blocks = [random_block(chain) for _ in range(batch_size)]
    payload = {"chain": chain.name, "blocks": blocks}
    async with semaphore:
        t0 = time.perf_counter()
        try:
            resp = await client.post(f"{base_url}/v1/timestamps", json=payload)
            elapsed = (time.perf_counter() - t0) * 1000
            if resp.status_code == 200:
                stats.record(elapsed)
            else:
                stats.record_error(f"HTTP {resp.status_code} batch {chain.name} ({batch_size} blocks)")
        except httpx.HTTPError as exc:
            stats.record_error(f"{type(exc).__name__}: {exc}")


# ── multi-chain batch stress ─────────────────────────────────────────


async def stress_multi_chain_batch(
    client: httpx.AsyncClient,
    base_url: str,
    chains: list[ChainInfo],
    batch_size: int,
    semaphore: asyncio.Semaphore,
    stats: Stats,
) -> None:
    """POST batch requests cycling across all chains."""
    chain = random.choice(chains)
    await stress_batch(client, base_url, chain, batch_size, semaphore, stats)


# ── runner ───────────────────────────────────────────────────────────


async def run(
    base_url: str,
    rounds: int,
    concurrency: int,
    batch_size: int,
    error_threshold: float,
) -> bool:
    timeout = httpx.Timeout(30.0, connect=10.0)
    async with httpx.AsyncClient(timeout=timeout) as client:
        print(f"Discovering chains at {base_url}/v1/status ...")
        chains = await discover_chains(client, base_url)
        if not chains:
            print("ERROR: no chains found in /v1/status")
            return False

        for c in chains:
            print(f"  {c.name}: latest block {c.latest_block:,}")

        semaphore = asyncio.Semaphore(concurrency)
        all_stats: list[Stats] = []

        # 1) single-block per chain
        for chain in chains:
            stats = Stats(label=f"GET /v1/timestamps/{chain.name}/{{block}}")
            all_stats.append(stats)
            tasks = [
                stress_single(client, base_url, chain, semaphore, stats)
                for _ in range(rounds)
            ]
            await asyncio.gather(*tasks)

        # 2) batch per chain
        for chain in chains:
            stats = Stats(label=f"POST /v1/timestamps  chain={chain.name}  batch_size={batch_size}")
            all_stats.append(stats)
            tasks = [
                stress_batch(client, base_url, chain, batch_size, semaphore, stats)
                for _ in range(rounds)
            ]
            await asyncio.gather(*tasks)

        # 3) multi-chain random batch
        if len(chains) > 1:
            stats = Stats(label=f"POST /v1/timestamps  random-chain  batch_size={batch_size}")
            all_stats.append(stats)
            tasks = [
                stress_multi_chain_batch(client, base_url, chains, batch_size, semaphore, stats)
                for _ in range(rounds)
            ]
            await asyncio.gather(*tasks)

        # report
        print("\n" + "=" * 60)
        print(f"RESULTS  (error threshold: {error_threshold * 100:.0f}%)")
        print("=" * 60)
        failed = False
        for s in all_stats:
            s.report()
            if s.error_rate > error_threshold:
                print(f"    FAIL: error rate {s.error_rate * 100:.0f}% exceeds {error_threshold * 100:.0f}% threshold")
                failed = True
        print()
        return not failed


# ── CLI ──────────────────────────────────────────────────────────────


def main() -> None:
    parser = argparse.ArgumentParser(description="Chronoblock API stress test")
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="API base URL")
    parser.add_argument("--rounds", type=int, default=DEFAULT_ROUNDS, help="Requests per test scenario")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Max parallel requests")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Blocks per batch request")
    parser.add_argument(
        "--error-threshold", type=float, default=DEFAULT_ERROR_THRESHOLD,
        help="Max acceptable error rate per scenario (0.0-1.0)",
    )
    args = parser.parse_args()

    print(f"Stress test: {args.rounds} rounds, concurrency={args.concurrency}, batch_size={args.batch_size}")
    ok = asyncio.run(run(args.base_url, args.rounds, args.concurrency, args.batch_size, args.error_threshold))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
