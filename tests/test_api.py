"""API route tests."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from chronoblock.config import CHAINS, settings
from chronoblock.health import INITIAL_SYNC_GRACE_SECS
from chronoblock.syncer import SyncState
from tests.conftest import make_state

# ── Lifespan tests ───────────────────────────────────────────────────


class TestLifespan:
    async def test_startup_and_shutdown(self):
        from chronoblock.api import create_app, lifespan

        app = create_app()

        with (
            patch("chronoblock.api.download_seed_data", new_callable=AsyncMock) as mock_seed,
            patch("chronoblock.api.start_all", new_callable=AsyncMock) as mock_start,
            patch("chronoblock.api.warm_caches") as mock_warm,
            patch("chronoblock.api.stop_all", new_callable=AsyncMock) as mock_stop,
            patch("chronoblock.api.close_all") as mock_close_db,
            patch("chronoblock.api.close_client", new_callable=AsyncMock) as mock_close_rpc,
        ):
            async with lifespan(app):
                mock_seed.assert_called_once()
                mock_start.assert_called_once()
                mock_warm.assert_called_once()

            mock_stop.assert_called_once()
            mock_close_db.assert_called_once()
            mock_close_rpc.assert_called_once()

    async def test_seed_failure_continues(self):
        from chronoblock.api import create_app, lifespan

        app = create_app()

        with (
            patch("chronoblock.api.download_seed_data", new_callable=AsyncMock, side_effect=RuntimeError("seed fail")),
            patch("chronoblock.api.start_all", new_callable=AsyncMock) as mock_start,
            patch("chronoblock.api.warm_caches"),
            patch("chronoblock.api.stop_all", new_callable=AsyncMock),
            patch("chronoblock.api.close_all"),
            patch("chronoblock.api.close_client", new_callable=AsyncMock),
        ):
            async with lifespan(app):
                mock_start.assert_called_once()

    async def test_startup_failure_triggers_shutdown(self):
        from chronoblock.api import create_app, lifespan

        app = create_app()

        with (
            patch("chronoblock.api.download_seed_data", new_callable=AsyncMock),
            patch("chronoblock.api.start_all", new_callable=AsyncMock, side_effect=RuntimeError("start fail")),
            patch("chronoblock.api.stop_all", new_callable=AsyncMock) as mock_stop,
            patch("chronoblock.api.close_all") as mock_close_db,
            patch("chronoblock.api.close_client", new_callable=AsyncMock) as mock_close_rpc,
        ):
            with pytest.raises(RuntimeError, match="start fail"):
                async with lifespan(app):
                    pass

            mock_stop.assert_called_once()
            mock_close_db.assert_called_once()
            mock_close_rpc.assert_called_once()

    async def test_shutdown_tolerates_stop_all_failure(self):
        from chronoblock.api import create_app, lifespan

        app = create_app()

        with (
            patch("chronoblock.api.download_seed_data", new_callable=AsyncMock),
            patch("chronoblock.api.start_all", new_callable=AsyncMock),
            patch("chronoblock.api.warm_caches"),
            patch("chronoblock.api.stop_all", new_callable=AsyncMock, side_effect=RuntimeError("stop boom")),
            patch("chronoblock.api.close_all") as mock_close_db,
            patch("chronoblock.api.close_client", new_callable=AsyncMock) as mock_close_rpc,
        ):
            async with lifespan(app):
                pass

            mock_close_db.assert_called_once()
            mock_close_rpc.assert_called_once()

    async def test_shutdown_tolerates_close_all_failure(self):
        from chronoblock.api import create_app, lifespan

        app = create_app()

        with (
            patch("chronoblock.api.download_seed_data", new_callable=AsyncMock),
            patch("chronoblock.api.start_all", new_callable=AsyncMock),
            patch("chronoblock.api.warm_caches"),
            patch("chronoblock.api.stop_all", new_callable=AsyncMock),
            patch("chronoblock.api.close_all", side_effect=RuntimeError("close_all boom")),
            patch("chronoblock.api.close_client", new_callable=AsyncMock) as mock_close_rpc,
        ):
            async with lifespan(app):
                pass

            mock_close_rpc.assert_called_once()

    async def test_shutdown_tolerates_close_client_failure(self):
        from chronoblock.api import create_app, lifespan

        app = create_app()

        with (
            patch("chronoblock.api.download_seed_data", new_callable=AsyncMock),
            patch("chronoblock.api.start_all", new_callable=AsyncMock),
            patch("chronoblock.api.warm_caches"),
            patch("chronoblock.api.stop_all", new_callable=AsyncMock),
            patch("chronoblock.api.close_all"),
            patch("chronoblock.api.close_client", new_callable=AsyncMock, side_effect=RuntimeError("close_client boom")),
        ):
            async with lifespan(app):
                pass


# ── Happy-path tests ─────────────────────────────────────────────────


class TestPostTimestamps:
    def test_returns_timestamps_by_chain_name(self, create_test_app):
        client = create_test_app(
            get_timestamps_fn=lambda chain, blocks: [1_700_000_000 + b for b in blocks],
        )
        res = client.post("/v1/timestamps", json={"chain": "ethereum", "blocks": [0, 1, 2]})
        assert res.status_code == 200
        body = res.json()
        assert body["chain_id"] == 1
        assert body["results"] == {"0": 1_700_000_000, "1": 1_700_000_001, "2": 1_700_000_002}

    def test_resolves_chain_by_id(self, create_test_app):
        client = create_test_app(
            get_timestamps_fn=lambda chain, blocks: [1000] * len(blocks),
        )
        res = client.post("/v1/timestamps", json={"chain_id": 1, "blocks": [0]})
        assert res.status_code == 200
        assert res.json()["chain_id"] == 1

    def test_rejects_non_json_content_type(self, create_test_app):
        client = create_test_app()
        res = client.post("/v1/timestamps", content="data", headers={"content-type": "text/plain"})
        assert res.status_code == 415
        assert res.json()["error"] == "unsupported_media_type"

    def test_rejects_malformed_json(self, create_test_app):
        client = create_test_app()
        res = client.post("/v1/timestamps", content="{invalid", headers={"content-type": "application/json"})
        assert res.status_code == 400
        assert res.json()["error"] == "invalid_json"

    def test_rejects_non_dict_json(self, create_test_app):
        client = create_test_app()
        res = client.post("/v1/timestamps", json=[1, 2, 3])
        assert res.status_code == 400
        assert res.json()["error"] == "invalid_json"


class TestGetSingleTimestamp:
    def _synced_state(self, now: float, latest: int = 200) -> SyncState:
        return make_state(now, last_synced_block=latest, latest_chain_block=latest)

    def test_returns_timestamp(self, create_test_app):
        now = 1_000_000.0
        client = create_test_app(
            now=now,
            get_timestamps_fn=lambda chain, blocks: [1_700_000_000],
            get_sync_state_fn=lambda chain: self._synced_state(now),
        )
        res = client.get("/v1/timestamps/ethereum/100")
        assert res.status_code == 200
        body = res.json()
        assert body["chain_id"] == 1
        assert body["block_number"] == 100
        assert body["timestamp"] == 1_700_000_000

    def test_finalized_block_gets_immutable_cache(self, create_test_app):
        now = 1_000_000.0
        client = create_test_app(
            now=now,
            get_timestamps_fn=lambda chain, blocks: [1_700_000_000],
            get_sync_state_fn=lambda chain: self._synced_state(now),
        )
        # Block 100, latest 200, finality 64: 100 <= 200 - 64 = 136 → finalized
        res = client.get("/v1/timestamps/ethereum/100")
        assert "immutable" in res.headers["cache-control"]

    def test_recent_block_gets_short_cache(self, create_test_app):
        now = 1_000_000.0
        client = create_test_app(
            now=now,
            get_timestamps_fn=lambda chain, blocks: [1_700_000_000],
            get_sync_state_fn=lambda chain: self._synced_state(now),
        )
        # Block 180, latest 200, finality 64: 180 > 136 → not finalized
        res = client.get("/v1/timestamps/ethereum/180")
        assert res.headers["cache-control"] == "public, max-age=60"

    def test_returns_404_when_block_not_found(self, create_test_app):
        client = create_test_app(get_timestamps_fn=lambda chain, blocks: [None])
        res = client.get("/v1/timestamps/ethereum/999999")
        assert res.status_code == 404
        assert res.json()["error"] == "not_found"

    def test_rejects_unknown_chain(self, create_test_app):
        client = create_test_app()
        res = client.get("/v1/timestamps/solana/1")
        assert res.status_code == 400
        assert res.json()["error"] == "unknown_chain"


class TestStatus:
    def test_returns_all_chains(self, create_test_app):
        now = 1_000_000.0
        client = create_test_app(
            now=now,
            get_sync_state_fn=lambda chain: make_state(
                now,
                last_synced_block=100,
                latest_chain_block=200,
                syncs_performed=5,
                blocks_ingested=100,
            ),
            block_count_fn=lambda chain: 100,
        )
        res = client.get("/v1/status")
        assert res.status_code == 200
        body = res.json()
        assert len(body["chains"]) == len(CHAINS)
        first = body["chains"][0]
        assert first["name"] == CHAINS[0].name
        assert first["chain_id"] == CHAINS[0].id
        assert first["last_synced_block"] == 100
        assert first["latest_chain_block"] == 200
        assert first["lag_blocks"] == 100
        assert first["total_stored"] == 100

    def test_no_store_cache_header(self, create_test_app):
        client = create_test_app()
        res = client.get("/v1/status")
        assert res.headers["cache-control"] == "no-store"

    def test_formats_error_timestamp_as_iso(self, create_test_app):
        now = 1_000_000.0
        client = create_test_app(
            now=now,
            get_sync_state_fn=lambda chain: make_state(
                now,
                last_error="RPC timeout",
                last_error_at=1_700_000_000.0,
            ),
        )
        res = client.get("/v1/status")
        first = res.json()["chains"][0]
        assert first["last_error"] == "RPC timeout"
        assert "2023-11-14" in first["last_error_at"]


class TestGlobalExceptionHandler:
    def test_returns_500_on_unhandled_error(self, create_test_app):
        def exploding_timestamps(_chain, _blocks):
            raise RuntimeError("boom")

        client = create_test_app(get_timestamps_fn=exploding_timestamps)
        res = client.post("/v1/timestamps", json={"chain": "ethereum", "blocks": [1]})
        assert res.status_code == 500
        body = res.json()
        assert body["error"] == "internal_error"
        assert body["message"] == "internal server error"


class TestMiddleware:
    def test_generates_request_id(self, create_test_app):
        client = create_test_app()
        res = client.get("/health")
        assert "x-request-id" in res.headers
        assert len(res.headers["x-request-id"]) > 0

    def test_echoes_provided_request_id(self, create_test_app):
        client = create_test_app()
        res = client.get("/health", headers={"x-request-id": "test-abc-123"})
        assert res.headers["x-request-id"] == "test-abc-123"

    def test_security_headers_present(self, create_test_app):
        client = create_test_app()
        res = client.get("/health")
        assert res.headers["x-content-type-options"] == "nosniff"
        assert res.headers["x-frame-options"] == "SAMEORIGIN"
        assert res.headers["x-xss-protection"] == "0"
        assert res.headers["referrer-policy"] == "no-referrer"
        assert res.headers["content-security-policy"] == "default-src 'none'"


# ── Validation tests ─────────────────────────────────────────────────


class TestPostTimestampsValidation:
    def test_missing_blocks_field(self, create_test_app):
        client = create_test_app()
        res = client.post("/v1/timestamps", json={"chain": "ethereum"})
        assert res.status_code == 400
        assert res.json()["error"] == "invalid_blocks"

    @pytest.mark.parametrize(
        "name,body,expected_code",
        [
            ("rejects string chain_id", {"chain_id": "1", "blocks": [1]}, "unknown_chain"),
            ("rejects zero chain_id", {"chain_id": 0, "blocks": [1]}, "unknown_chain"),
            ("rejects negative chain_id", {"chain_id": -1, "blocks": [1]}, "unknown_chain"),
            ("rejects empty chain", {"chain": "", "blocks": [1]}, "unknown_chain"),
            ("rejects non-array blocks", {"chain": "ethereum", "blocks": "1"}, "invalid_blocks"),
            ("rejects empty blocks", {"chain": "ethereum", "blocks": []}, "invalid_blocks"),
            ("rejects float block", {"chain": "ethereum", "blocks": [1.5]}, "invalid_block_number"),
            ("rejects negative block", {"chain": "ethereum", "blocks": [-1]}, "invalid_block_number"),
            (
                "rejects unsafe integer block",
                {"chain": "ethereum", "blocks": [2**53]},
                "invalid_block_number",
            ),
        ],
    )
    def test_validation(self, create_test_app, name, body, expected_code):
        client = create_test_app()
        res = client.post("/v1/timestamps", json=body)
        assert res.status_code == 400
        assert res.json()["error"] == expected_code

    def test_rejects_more_than_10k_blocks(self, create_test_app):
        client = create_test_app()
        res = client.post(
            "/v1/timestamps",
            json={"chain": "ethereum", "blocks": list(range(10_001))},
        )
        assert res.status_code == 400
        assert res.json()["error"] == "invalid_blocks"


class TestGetTimestampsValidation:
    def test_rejects_decimal_block(self, create_test_app):
        client = create_test_app()
        res = client.get("/v1/timestamps/ethereum/1.5")
        assert res.status_code == 400
        assert res.json()["error"] == "invalid_block_number"

    def test_rejects_mixed_block(self, create_test_app):
        client = create_test_app()
        res = client.get("/v1/timestamps/ethereum/123abc")
        assert res.status_code == 400
        assert res.json()["error"] == "invalid_block_number"

    def test_rejects_block_exceeding_max_safe_integer(self, create_test_app):
        client = create_test_app()
        res = client.get(f"/v1/timestamps/ethereum/{2**53}")
        assert res.status_code == 400
        assert res.json()["error"] == "invalid_block_number"


class TestHealth:
    def test_returns_200_while_initializing(self, create_test_app):
        client = create_test_app()
        res = client.get("/health")
        assert res.status_code == 200
        assert res.json() == {"ok": True}

    def test_returns_503_when_chain_fails_before_first_success(self, create_test_app):
        now = 1_000_000.0
        failing_id = CHAINS[0].id

        def sync_state(chain):
            if chain.id == failing_id:
                return make_state(now, last_error="RPC 401: unauthorized", last_error_at=now - 1.0)
            return make_state(now)

        client = create_test_app(now=now, get_sync_state_fn=sync_state)
        res = client.get("/health")
        assert res.status_code == 503
        assert f"{CHAINS[0].name}: RPC 401: unauthorized" in res.json()["degraded"]

    def test_returns_503_when_init_exceeds_grace_period(self, create_test_app):
        now = 1_000_000.0
        slow_id = CHAINS[1].id

        def sync_state(chain):
            if chain.id == slow_id:
                return make_state(now, started_at=now - INITIAL_SYNC_GRACE_SECS - 1)
            return make_state(now)

        client = create_test_app(now=now, get_sync_state_fn=sync_state)
        res = client.get("/health")
        assert res.status_code == 503

    def test_returns_503_when_chain_exceeds_lag_budget(self, create_test_app):
        now = 1_000_000.0
        lagging_id = CHAINS[2].id

        def sync_state(chain):
            if chain.id == lagging_id:
                return make_state(
                    now,
                    last_success_at=now - 1.0,
                    last_synced_block=100,
                    latest_chain_block=100 + settings.health_max_lag_secs + 1,
                )
            return make_state(
                now,
                last_success_at=now - 1.0,
                last_synced_block=100,
                latest_chain_block=100,
            )

        client = create_test_app(now=now, get_sync_state_fn=sync_state)
        res = client.get("/health")
        assert res.status_code == 503

    def test_returns_503_when_db_unreadable(self, create_test_app):
        bad_id = CHAINS[3].id

        client = create_test_app(is_healthy_fn=lambda chain: chain.id != bad_id)
        res = client.get("/health")
        body = res.json()
        assert res.status_code == 503
        assert f"{CHAINS[3].name}: db unreadable" in body["degraded"]
