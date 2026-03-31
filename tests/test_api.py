"""API route tests — port of index.test.ts."""

from __future__ import annotations

import pytest

from chronoblock.config import CHAINS, settings
from chronoblock.api import INITIAL_SYNC_GRACE_SECS
from tests.conftest import make_state


class TestPostTimestampsValidation:
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
