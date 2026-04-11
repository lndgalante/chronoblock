"""Schema helper tests."""

from __future__ import annotations

from unittest.mock import MagicMock

from chronoblock.schemas import map_validation_error


class TestMapValidationError:
    def test_fallback_when_loc_not_blocks(self):
        """When the first error's loc doesn't start with 'blocks', hit the fallback."""
        mock_exc = MagicMock()
        mock_exc.errors.return_value = [{"loc": ("unknown_field",), "msg": "some error"}]

        response = map_validation_error(mock_exc, "req-123")
        assert response.status_code == 400
        assert response.body is not None
        import json

        body = json.loads(bytes(response.body))
        assert body["error"] == "invalid_blocks"
        assert body["request_id"] == "req-123"
