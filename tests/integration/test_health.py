"""Integration tests for improved health endpoint."""
from __future__ import annotations


class TestHealthEndpoint:
    async def test_readiness_check(self, client):
        response = await client.get("/health/ready")
        assert response.status_code == 200
        assert response.json()["status"] == "ready"

    async def test_health_returns_json(self, client):
        """Health endpoint uses DI, now mocked via conftest."""
        response = await client.get("/health")
        assert response.status_code == 200
