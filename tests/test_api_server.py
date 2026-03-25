"""Tests for API server."""

import json
import threading
import urllib.request

from src.api_server import CloneAPIHandler, _server_state


class TestAPIRouting:
    def test_health_endpoint(self):
        """Test health check returns correct response."""
        # We test the handler logic directly
        from http.server import HTTPServer

        _server_state["config"] = {}
        _server_state["client"] = None
        _server_state["api_key"] = None
        _server_state["queue"] = None

        # Create a minimal test using the server on a random port
        server = HTTPServer(("127.0.0.1", 0), CloneAPIHandler)
        port = server.server_address[1]

        thread = threading.Thread(target=server.handle_request, daemon=True)
        thread.start()

        try:
            url = f"http://127.0.0.1:{port}/api/health"
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read().decode())
                assert data["status"] == "healthy"
        finally:
            server.server_close()

    def test_auth_required(self):
        """Test API key authentication."""
        from http.server import HTTPServer

        _server_state["config"] = {}
        _server_state["client"] = None
        _server_state["api_key"] = "test-key-123"
        _server_state["queue"] = None

        server = HTTPServer(("127.0.0.1", 0), CloneAPIHandler)
        port = server.server_address[1]

        thread = threading.Thread(target=server.handle_request, daemon=True)
        thread.start()

        try:
            url = f"http://127.0.0.1:{port}/api/health"
            req = urllib.request.Request(url)
            # No API key - should get 401
            try:
                urllib.request.urlopen(req, timeout=5)
                assert False, "Should have raised HTTPError"
            except urllib.error.HTTPError as e:
                assert e.code == 401
        finally:
            server.server_close()
            _server_state["api_key"] = None
