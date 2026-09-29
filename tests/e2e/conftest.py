"""E2E test fixtures — real app, real DI, real database.

Connects to the staging database via SSH tunnel (in CI) or directly
(local dev with port-forward). All services are real: LLM, embeddings,
vector search, connectors.
"""

from __future__ import annotations

import os

import pytest
import pytest_asyncio
from dishka.integrations.fastapi import setup_dishka
from httpx import ASGITransport, AsyncClient

from app.presentation.http.controllers.root_router import create_root_router
from app.setup.app_factory import configure_app, create_app
from app.setup.config.settings import AppSettings
from app.setup.ioc.provider_registry import create_async_ioc_container, get_providers

# Mark all tests in this directory as e2e
pytestmark = pytest.mark.e2e


def pytest_collection_modifyitems(items):
    """Un solo event loop para toda la suite E2E.

    En producción hay **un** loop por proceso: uvicorn levanta uno y el módulo
    del router se importa una vez. Por eso el checkpointer, el grafo compilado
    y sus locks se cachean a nivel de módulo.

    Con un loop por test, ese caché cruza loops y el `AsyncPostgresSaver` de
    LangGraph falla desde adentro con `<Lock> is bound to a different event
    loop` — que sale como un `500 PIPELINE_ERROR` genérico. Así caían 131 de
    135 tests el 2026-09-28.

    Compartir el loop no tapa un bug: reproduce la topología real. Un caché de
    proceso probado contra un loop por test estaba probando algo que no existe.
    """
    for item in items:
        item.add_marker(pytest.mark.asyncio(loop_scope="session"))


@pytest.fixture(scope="session", autouse=True)
def _e2e_env_check():
    """Fail fast if required env vars are missing.

    Also ensures SANDBOX_DATABASE_URL falls back to DATABASE_URL so the
    sandbox adapter reuses the same tunnel/connection in local E2E runs.
    """
    required = ["DATABASE_URL", "REDIS_CACHE_URL"]
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        pytest.skip(f"E2E tests require env vars: {', '.join(missing)}")

    # When no dedicated sandbox URL is configured, reuse the main DB URL
    # so the sandbox adapter connects through the same SSH tunnel.
    if not os.getenv("SANDBOX_DATABASE_URL"):
        os.environ["SANDBOX_DATABASE_URL"] = os.environ["DATABASE_URL"]


@pytest.fixture(scope="session")
def e2e_settings(_e2e_env_check) -> AppSettings:
    """Load real application settings from environment."""
    return AppSettings()


# `loop_scope="session"` para que el fixture viva en el mismo loop que los
# tests (ver `pytest_collection_modifyitems`). Sin esto, el fixture async
# corre en un loop de función y el test en el de sesión, y pytest-asyncio
# falla antes de llegar a la app.
@pytest_asyncio.fixture(loop_scope="session")
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


@pytest_asyncio.fixture(loop_scope="session")
async def client(app):
    """Async HTTP client hitting the real app."""
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        timeout=120.0,  # Pipeline can take a while
    ) as c:
        yield c
