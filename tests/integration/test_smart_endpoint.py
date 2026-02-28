"""Integration tests for the smart query endpoint (mocked connectors)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from app.setup.app_factory import configure_app, create_app
from app.presentation.http.controllers.root_router import create_root_router


@pytest.fixture
def app():
    fast_app = create_app()
    root_router = create_root_router()
    configure_app(fast_app, root_router, environment="test")
    return fast_app


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


class TestSmartQueryEndpoint:
    async def test_returns_422_on_empty_body(self, client):
        response = await client.post("/api/v1/query/smart", json={})
        assert response.status_code == 422
        data = response.json()
        assert "error" in data

    async def test_returns_422_on_missing_question(self, client):
        response = await client.post(
            "/api/v1/query/smart",
            json={"user_email": "test@test.com"},
        )
        assert response.status_code == 422

    async def test_returns_405_on_get(self, client):
        response = await client.get("/api/v1/query/smart")
        assert response.status_code == 405
