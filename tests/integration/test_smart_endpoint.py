"""Integration tests for the smart query endpoint (mocked connectors)."""

from __future__ import annotations


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

    async def test_smart_query_route_is_post_only(self, app):
        route = next(r for r in app.routes if getattr(r, "path", None) == "/api/v1/query/smart")
        assert "POST" in route.methods
        assert "GET" not in route.methods
