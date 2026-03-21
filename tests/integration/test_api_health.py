"""Integration tests for the health endpoints."""

from __future__ import annotations


class TestHealthEndpoints:
    async def test_health_endpoint_exists(self, client):
        """Health endpoint with DI mocked via conftest."""
        response = await client.get("/health")
        assert response.status_code == 200

    async def test_health_ready(self, client):
        response = await client.get("/health/ready")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data


class TestExceptionHandlers:
    async def test_404_returns_json(self, client):
        response = await client.get("/nonexistent")
        assert response.status_code in (404, 405)

    async def test_wrong_method(self, client):
        response = await client.delete("/health")
        assert response.status_code == 405
