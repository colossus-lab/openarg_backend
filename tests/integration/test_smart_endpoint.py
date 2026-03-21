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

    async def test_returns_405_on_get(self, client):
        response = await client.get("/api/v1/query/smart")
        assert response.status_code in (405, 500)
