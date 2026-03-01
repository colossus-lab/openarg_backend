"""Tests for the security headers middleware."""
from __future__ import annotations

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app.presentation.http.middleware.security_headers import SecurityHeadersMiddleware


@pytest.fixture
def app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(SecurityHeadersMiddleware)

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.get("/docs")
    async def docs():
        return {"docs": True}

    return app


@pytest.fixture
async def client(app: FastAPI) -> AsyncClient:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_security_headers_present(client: AsyncClient) -> None:
    response = await client.get("/health")
    assert response.status_code == 200
    assert response.headers["X-Content-Type-Options"] == "nosniff"
    assert response.headers["X-Frame-Options"] == "DENY"
    assert response.headers["X-XSS-Protection"] == "1; mode=block"
    assert response.headers["Referrer-Policy"] == "strict-origin-when-cross-origin"
    assert "camera=()" in response.headers["Permissions-Policy"]


@pytest.mark.asyncio
async def test_csp_restrictive_for_api(client: AsyncClient) -> None:
    response = await client.get("/health")
    csp = response.headers["Content-Security-Policy"]
    assert "frame-ancestors 'none'" in csp


@pytest.mark.asyncio
async def test_csp_relaxed_for_docs(client: AsyncClient) -> None:
    response = await client.get("/docs")
    csp = response.headers["Content-Security-Policy"]
    assert "cdn.jsdelivr.net" in csp


@pytest.mark.asyncio
async def test_no_hsts_on_http(client: AsyncClient) -> None:
    response = await client.get("/health")
    assert "Strict-Transport-Security" not in response.headers
