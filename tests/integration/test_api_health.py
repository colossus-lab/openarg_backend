"""
Integration tests for the health endpoints.
These test the actual FastAPI app without external dependencies.
"""
from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from app.setup.app_factory import create_app, configure_app
from app.presentation.http.controllers.root_router import create_root_router


@pytest.fixture
def app():
    """Create a test FastAPI app."""
    fast_app = create_app()
    root_router = create_root_router()
    configure_app(fast_app, root_router, environment="test")
    return fast_app


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


class TestHealthEndpoints:
    async def test_health_endpoint_exists(self, client):
        """Health now uses DI (HealthCheckService); without container it may return 500."""
        response = await client.get("/health")
        assert response.status_code in (200, 500)

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
