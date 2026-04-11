"""Seed data download — fetches and extracts a .tar.gz archive of pre-built databases."""

from __future__ import annotations

import asyncio
import tarfile
from pathlib import Path

import httpx

from chronoblock import config
from chronoblock.log import log

__all__ = ["download_seed_data"]

MAX_SEED_RETRIES = 2
MAX_SEED_BYTES = 2 * 1024 * 1024 * 1024  # 2 GB


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

    def _extract() -> int:
        with tarfile.open(tmp_path, mode="r:gz") as tar:
            tar.extractall(path=data_dir, filter="data")
        return len(list(data_dir.glob("*.db")))

    last_err: Exception | None = None
    try:
        for attempt in range(MAX_SEED_RETRIES + 1):
            try:
                async with (
                    httpx.AsyncClient(timeout=httpx.Timeout(600.0)) as client,
                    client.stream("GET", config.settings.seed_url, follow_redirects=True) as resp,
                ):
                    resp.raise_for_status()
                    downloaded = 0
                    with tmp_path.open("wb") as f:
                        async for chunk in resp.aiter_bytes(chunk_size=65536):
                            downloaded += len(chunk)
                            if downloaded > MAX_SEED_BYTES:
                                raise RuntimeError(f"seed download exceeds {MAX_SEED_BYTES} byte limit")
                            f.write(chunk)

                count = await asyncio.to_thread(_extract)
                log("info", f"seeded {count} databases from archive")
                return
            except Exception as err:
                last_err = err
                if attempt < MAX_SEED_RETRIES:
                    wait = 2.0 * 2**attempt
                    log("warn", f"seed download failed, retrying in {wait:.0f}s", attempt=attempt + 1, error=str(err))
                    await asyncio.sleep(wait)

        raise last_err  # type: ignore[misc]
    finally:
        tmp_path.unlink(missing_ok=True)
