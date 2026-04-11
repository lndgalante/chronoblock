"""Tests for the main entry point module."""

from __future__ import annotations

import importlib
import sys
from unittest.mock import MagicMock

import pytest


class TestHappyPath:
    def test_import_creates_app(self):
        mod = importlib.import_module("chronoblock.main")
        assert mod.app is not None
        assert hasattr(mod.app, "routes")


class TestConfigErrorPath:
    def test_prints_errors_and_exits(self, monkeypatch, capsys):
        import builtins

        from chronoblock.errors import ConfigError

        original_import = builtins.__import__

        def raising_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "chronoblock" and fromlist and "config" in fromlist:
                raise ConfigError(["no chains enabled — set at least one RPC URL (e.g. ETH_RPC_URL)"])
            return original_import(name, globals, locals, fromlist, level)

        monkeypatch.setattr(builtins, "__import__", raising_import)
        monkeypatch.delitem(sys.modules, "chronoblock.main", raising=False)

        with pytest.raises(SystemExit) as exc_info:
            importlib.import_module("chronoblock.main")

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "config validation failed" in captured.err
        assert "no chains enabled" in captured.err


class TestMainBlock:
    def test_runs_uvicorn(self, monkeypatch):
        mock_uvicorn = MagicMock()
        monkeypatch.setitem(sys.modules, "uvicorn", mock_uvicorn)
        monkeypatch.delitem(sys.modules, "chronoblock.main", raising=False)

        import runpy

        runpy.run_module("chronoblock.main", run_name="__main__")
        mock_uvicorn.run.assert_called_once()
