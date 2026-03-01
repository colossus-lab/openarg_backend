"""Shared fixtures for integration tests.

Provides a FastAPI app with Dishka DI wired to mock providers,
so @inject endpoints work without real infrastructure.
"""
from unittest.mock import AsyncMock, MagicMock

import pytest
from dishka import Provider, Scope, make_async_container, provide
from dishka.integrations.fastapi import setup_dishka
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.ports.cache.cache_port import ICacheService
from app.domain.ports.connectors.argentina_datos import IArgentinaDatosConnector
from app.domain.ports.connectors.ckan_search import ICKANSearchConnector
from app.domain.ports.connectors.georef import IGeorefConnector
from app.domain.ports.connectors.series_tiempo import ISeriesTiempoConnector
from app.domain.ports.connectors.sesiones import ISesionesConnector
from app.domain.ports.llm.llm_provider import IEmbeddingProvider, ILLMProvider
from app.domain.ports.search.retrieval_evaluator import IRetrievalEvaluator
from app.domain.ports.search.vector_search import IVectorSearch
from app.infrastructure.adapters.cache.semantic_cache import SemanticCache
from app.infrastructure.adapters.connectors.ddjj_adapter import DDJJAdapter
from app.infrastructure.monitoring.health import HealthCheckService
from app.infrastructure.persistence_sqla.provider import MainAsyncSession
from app.presentation.http.controllers.root_router import create_root_router
from app.setup.app_factory import configure_app, create_app


class MockProvider(Provider):
    """Provides AsyncMock instances for all DI-injected interfaces."""

    scope = Scope.REQUEST

    @provide
    def llm(self) -> ILLMProvider:
        mock = AsyncMock()
        mock.chat.return_value = MagicMock(
            content="Mock response", tokens_used=10, model="test",
        )
        return mock

    @provide
    def embedding(self) -> IEmbeddingProvider:
        mock = AsyncMock()
        mock.embed.return_value = [0.1] * 1536
        return mock

    @provide
    def vector_search(self) -> IVectorSearch:
        mock = AsyncMock()
        mock.search_datasets_hybrid.return_value = []
        return mock

    @provide
    def cache(self) -> ICacheService:
        mock = AsyncMock()
        mock.get.return_value = None
        mock.set.return_value = None
        return mock

    @provide
    def series(self) -> ISeriesTiempoConnector:
        return AsyncMock()

    @provide
    def arg_datos(self) -> IArgentinaDatosConnector:
        return AsyncMock()

    @provide
    def georef(self) -> IGeorefConnector:
        return AsyncMock()

    @provide
    def ckan(self) -> ICKANSearchConnector:
        return AsyncMock()

    @provide
    def sesiones(self) -> ISesionesConnector:
        return AsyncMock()

    @provide
    def ddjj(self) -> DDJJAdapter:
        adapter = MagicMock(spec=DDJJAdapter)
        adapter._data = []
        return adapter

    @provide
    def semantic_cache(self) -> SemanticCache:
        mock = AsyncMock(spec=SemanticCache)
        mock.get.return_value = None
        mock.set.return_value = None
        return mock

    @provide
    def retrieval_evaluator(self) -> IRetrievalEvaluator:
        return AsyncMock()

    @provide
    def session(self) -> MainAsyncSession:
        return AsyncMock(spec=AsyncSession)

    @provide
    def health_service(self) -> HealthCheckService:
        mock = AsyncMock(spec=HealthCheckService)
        mock.check_all.return_value = {
            "status": "healthy",
            "components": {},
        }
        return mock


@pytest.fixture
def app():
    """Create test FastAPI app with mocked Dishka DI container."""
    fast_app = create_app()
    root_router = create_root_router()
    configure_app(fast_app, root_router, environment="test")

    container = make_async_container(MockProvider())
    setup_dishka(container=container, app=fast_app)

    return fast_app


@pytest.fixture(autouse=True)
def _reset_rate_limiter():
    """Reset the rate limiter storage to avoid cross-test pollution."""
    from app.setup.app_factory import limiter

    storage = getattr(limiter, "_storage", None)
    if storage and hasattr(storage, "reset"):
        storage.reset()


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app, raise_app_exceptions=False)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
