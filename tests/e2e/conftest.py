"""E2E test fixtures — real app, real DI, real database.

Connects to the staging database via SSH tunnel (in CI) or directly
(local dev with port-forward). All services are real: LLM, embeddings,
vector search, connectors.
"""

from __future__ import annotations

import os

import pytest
from dishka.integrations.fastapi import setup_dishka
from httpx import ASGITransport, AsyncClient

from app.presentation.http.controllers.root_router import create_root_router
from app.setup.app_factory import configure_app, create_app
from app.setup.config.settings import AppSettings
from app.setup.ioc.provider_registry import create_async_ioc_container, get_providers

# Mark all tests in this directory as e2e
pytestmark = pytest.mark.e2e


@pytest.fixture(scope="session")
def _e2e_env_check():
    """Fail fast if required env vars are missing."""
    required = ["DATABASE_URL", "REDIS_CACHE_URL"]
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        pytest.skip(f"E2E tests require env vars: {', '.join(missing)}")


@pytest.fixture(scope="session")
def e2e_settings(_e2e_env_check) -> AppSettings:
    """Load real application settings from environment."""
    return AppSettings()


@pytest.fixture
async def app(e2e_settings):
    """Create FastAPI app with real DI container (no mocks)."""
    fast_app = create_app()
    root_router = create_root_router()
    configure_app(fast_app, root_router, environment="e2e")

    providers = get_providers()
    container = create_async_ioc_container(providers, e2e_settings)
    setup_dishka(container=container, app=fast_app)

    yield fast_app

    await container.close()


@pytest.fixture(autouse=True)
def _reset_rate_limiter():
    """Reset rate limiter to avoid cross-test pollution."""
    from app.setup.app_factory import limiter

    storage = getattr(limiter, "_storage", None)
    if storage and hasattr(storage, "reset"):
        storage.reset()


@pytest.fixture
async def client(app):
    """Async HTTP client hitting the real app."""
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        timeout=120.0,  # Pipeline can take a while
    ) as c:
        yield c
