"""Seed data download tests."""

from __future__ import annotations

import tarfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest
import respx

from chronoblock.seed import download_seed_data


def _make_tar(tmp_path: Path) -> Path:
    """Create a .tar.gz containing a dummy .db file."""
    db_path = tmp_path / "test.db"
    db_path.write_text("fake-db")

    tar_path = tmp_path / "seed.tar.gz"
    with tarfile.open(tar_path, "w:gz") as tar:
        tar.add(db_path, arcname="test.db")

    db_path.unlink()
    return tar_path


class TestDownloadSeedData:
    @patch("chronoblock.seed.config")
    async def test_skips_when_no_seed_url(self, mock_config, tmp_path):
        mock_config.settings = SimpleNamespace(seed_url=None, data_dir=str(tmp_path))
        await download_seed_data()
        assert not list(tmp_path.glob("*.db"))

    @patch("chronoblock.seed.config")
    async def test_skips_when_databases_exist(self, mock_config, tmp_path):
        mock_config.settings = SimpleNamespace(seed_url="http://example.test/seed.tar.gz", data_dir=str(tmp_path))
        (tmp_path / "existing.db").write_text("data")

        await download_seed_data()
        # Should not download — existing db present
        assert list(tmp_path.glob("*.db")) == [tmp_path / "existing.db"]

    @respx.mock
    @patch("chronoblock.seed.config")
    async def test_downloads_and_extracts_seed(self, mock_config, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        mock_config.settings = SimpleNamespace(seed_url="http://example.test/seed.tar.gz", data_dir=str(data_dir))

        tar_path = _make_tar(tmp_path)
        tar_bytes = tar_path.read_bytes()

        respx.get("http://example.test/seed.tar.gz").mock(
            return_value=httpx.Response(200, content=tar_bytes),
        )

        await download_seed_data()

        assert (data_dir / "test.db").exists()
        assert (data_dir / "test.db").read_text() == "fake-db"
        assert not (data_dir / "_seed.tar.gz").exists()

    @respx.mock
    @patch("chronoblock.seed.MAX_SEED_BYTES", 10)
    @patch("chronoblock.seed.MAX_SEED_RETRIES", 0)
    @patch("chronoblock.seed.config")
    async def test_rejects_oversized_download(self, mock_config, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        mock_config.settings = SimpleNamespace(seed_url="http://example.test/seed.tar.gz", data_dir=str(data_dir))

        respx.get("http://example.test/seed.tar.gz").mock(
            return_value=httpx.Response(200, content=b"x" * 100),
        )

        with pytest.raises(RuntimeError, match="byte limit"):
            await download_seed_data()

    @respx.mock
    @patch("chronoblock.seed.config")
    async def test_cleans_up_temp_file_on_failure(self, mock_config, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        mock_config.settings = SimpleNamespace(seed_url="http://example.test/seed.tar.gz", data_dir=str(data_dir))

        respx.get("http://example.test/seed.tar.gz").mock(
            return_value=httpx.Response(500, text="server error"),
        )

        with pytest.raises(httpx.HTTPStatusError):
            await download_seed_data()

        assert not (data_dir / "_seed.tar.gz").exists()
