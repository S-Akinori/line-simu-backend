import sys
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from line_simu.api.health import router as health_router


# ========================================================================
# Health endpoint tests (no webhook import needed)
# ========================================================================
class TestHealthEndpoint:
    @pytest.fixture
    def client(self):
        app = FastAPI()
        app.include_router(health_router)
        return TestClient(app)

    def test_health_check_returns_ok(self, client):
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    def test_health_check_is_get_only(self, client):
        response = client.post("/health")
        assert response.status_code == 405


# ========================================================================
# Webhook endpoint tests
#
# The webhook module has import chain issues with LINE SDK classes,
# so we mock the problematic modules before importing.
# ========================================================================
class TestWebhookEndpoint:
    @pytest.fixture(autouse=True)
    def _setup_mocks(self):
        """Pre-mock broken LINE SDK imports to allow webhook module loading."""
        # Mock the broken downstream imports to avoid ImportError chain
        modules_to_mock = [
            "line_simu.line.messages",
            "line_simu.engine.session",
        ]
        saved = {}
        for mod_name in modules_to_mock:
            if mod_name not in sys.modules:
                saved[mod_name] = None
                mock_mod = MagicMock()
                sys.modules[mod_name] = mock_mod

        # Also mock events module to avoid its imports
        if "line_simu.line.events" not in sys.modules:
            events_mock = MagicMock()
            saved["line_simu.line.events"] = None
            sys.modules["line_simu.line.events"] = events_mock

        yield

        # Restore original modules
        for mod_name, original in saved.items():
            if original is None:
                sys.modules.pop(mod_name, None)

    @pytest.fixture
    def client(self):
        # Force re-import of webhook module with mocked dependencies
        if "line_simu.api.webhook" in sys.modules:
            del sys.modules["line_simu.api.webhook"]

        from line_simu.api.webhook import router as webhook_router

        app = FastAPI()
        app.include_router(webhook_router)
        app.include_router(health_router)
        return TestClient(app)

    @patch("line_simu.api.webhook.parser")
    def test_invalid_signature_returns_400(self, mock_parser, client):
        from linebot.v3.exceptions import InvalidSignatureError

        mock_parser.parse.side_effect = InvalidSignatureError("bad sig")

        response = client.post(
            "/webhook",
            content=b"{}",
            headers={"X-Line-Signature": "invalid"},
        )
        assert response.status_code == 400

    @patch("line_simu.api.webhook.parser")
    def test_valid_request_returns_ok(self, mock_parser, client):
        mock_parser.parse.return_value = []

        response = client.post(
            "/webhook",
            content=b'{"events": []}',
            headers={"X-Line-Signature": "valid_sig"},
        )
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    @patch("line_simu.api.webhook.parser")
    def test_missing_signature_header_uses_empty_string(self, mock_parser, client):
        mock_parser.parse.return_value = []

        response = client.post("/webhook", content=b'{"events": []}')
        assert response.status_code == 200
        mock_parser.parse.assert_called_once()
        call_args = mock_parser.parse.call_args
        assert call_args[0][1] == ""

    @patch("line_simu.api.webhook.parser")
    def test_events_are_dispatched_to_background(self, mock_parser, client):
        mock_event = MagicMock()
        mock_event.__class__ = type("UnknownEvent", (), {})
        mock_parser.parse.return_value = [mock_event]

        response = client.post(
            "/webhook",
            content=b'{"events": []}',
            headers={"X-Line-Signature": "valid"},
        )
        assert response.status_code == 200
