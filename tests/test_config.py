"""Config validation tests."""

from __future__ import annotations

import pytest

from chronoblock.config import Settings, validate_config
from chronoblock.errors import ConfigError
from chronoblock.models import Chain


def _chain(**overrides) -> Chain:
    defaults = {
        "id": 1,
        "name": "testchain",
        "rpc": "http://test.test",
        "rpc_batch_size": 50,
        "rpc_concurrency": 2,
        "finality_blocks": 64,
    }
    defaults.update(overrides)
    return Chain(**defaults)


class TestValidate:
    def test_valid_config(self):
        validate_config(Settings(), [_chain()])

    @pytest.mark.parametrize("port", [0, -1, 65536])
    def test_rejects_invalid_port(self, port):
        with pytest.raises(ConfigError, match="PORT"):
            validate_config(Settings(port=port), [_chain()])

    def test_rejects_zero_health_lag(self):
        with pytest.raises(ConfigError, match="HEALTH_MAX_LAG_SECS"):
            validate_config(Settings(health_max_lag_secs=0), [_chain()])

    def test_rejects_negative_health_lag(self):
        with pytest.raises(ConfigError, match="HEALTH_MAX_LAG_SECS"):
            validate_config(Settings(health_max_lag_secs=-5), [_chain()])

    def test_rejects_zero_chunk_size(self):
        with pytest.raises(ConfigError, match="SYNC_CHUNK_SIZE"):
            validate_config(Settings(sync_chunk_size=0), [_chain()])

    def test_rejects_non_http_rpc(self):
        with pytest.raises(ConfigError, match="rpc must start with http"):
            validate_config(Settings(), [_chain(rpc="ws://test")])

    def test_rejects_duplicate_chain_id(self):
        chains = [_chain(id=1, name="alpha"), _chain(id=1, name="beta")]
        with pytest.raises(ConfigError, match="duplicate chain id"):
            validate_config(Settings(), chains)

    def test_rejects_duplicate_chain_name(self):
        chains = [_chain(id=1, name="same"), _chain(id=2, name="same")]
        with pytest.raises(ConfigError, match="duplicate chain name"):
            validate_config(Settings(), chains)

    def test_rejects_no_chains(self):
        with pytest.raises(ConfigError, match="no chains enabled"):
            validate_config(Settings(), [])

    @pytest.mark.parametrize("batch_size", [0, 1001])
    def test_rejects_invalid_batch_size(self, batch_size):
        with pytest.raises(ConfigError, match="rpc_batch_size"):
            validate_config(Settings(), [_chain(rpc_batch_size=batch_size)])

    @pytest.mark.parametrize("concurrency", [0, 51])
    def test_rejects_invalid_concurrency(self, concurrency):
        with pytest.raises(ConfigError, match="rpc_concurrency"):
            validate_config(Settings(), [_chain(rpc_concurrency=concurrency)])

    def test_collects_multiple_errors(self):
        with pytest.raises(ConfigError) as exc_info:
            validate_config(Settings(port=0, sync_chunk_size=0), [])
        assert len(exc_info.value.errors) >= 3
