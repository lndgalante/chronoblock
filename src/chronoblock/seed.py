"""Seed data download — fetches and extracts a .tar.gz archive of pre-built databases."""

from __future__ import annotations

import asyncio
import tarfile
from pathlib import Path

import httpx

from chronoblock import config
from chronoblock.log import log

__all__ = ["download_seed_data"]


async def download_seed_data() -> None:
    if not config.settings.seed_url:
        return

    data_dir = Path(config.settings.data_dir)
    if any(data_dir.glob("*.db")):
        log("info", "seed skipped: databases already exist")
        return

    log("info", "downloading seed data", url=config.settings.seed_url)
    data_dir.mkdir(parents=True, exist_ok=True)

    tmp_path = data_dir / "_seed.tar.gz"
    try:
        async with (
            httpx.AsyncClient(timeout=httpx.Timeout(600.0)) as client,
            client.stream("GET", config.settings.seed_url, follow_redirects=True) as resp,
        ):
            resp.raise_for_status()
            chunks: list[bytes] = []
            async for chunk in resp.aiter_bytes(chunk_size=65536):
                chunks.append(chunk)

        await asyncio.to_thread(tmp_path.write_bytes, b"".join(chunks))

        def _extract() -> int:
            with tarfile.open(tmp_path, mode="r:gz") as tar:
                tar.extractall(path=data_dir, filter="data")
            return len(list(data_dir.glob("*.db")))

        count = await asyncio.to_thread(_extract)
        log("info", f"seeded {count} databases from archive")
    finally:
        tmp_path.unlink(missing_ok=True)
