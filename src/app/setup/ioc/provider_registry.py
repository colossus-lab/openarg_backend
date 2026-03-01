from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterable

from dishka import Provider, Scope, make_async_container, provide
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.application.smart_query_service import SmartQueryService
from app.domain.ports.cache.cache_port import ICacheService
from app.domain.ports.chat.chat_repository import IChatRepository
from app.domain.ports.connectors.argentina_datos import IArgentinaDatosConnector
from app.domain.ports.connectors.ckan_search import ICKANSearchConnector
from app.domain.ports.connectors.georef import IGeorefConnector
from app.domain.ports.connectors.series_tiempo import ISeriesTiempoConnector
from app.domain.ports.connectors.sesiones import ISesionesConnector
from app.domain.ports.dataset.dataset_repository import IDatasetRepository
from app.domain.ports.llm.llm_provider import IEmbeddingProvider, ILLMProvider
from app.domain.ports.sandbox.sql_sandbox import ISQLSandbox
from app.domain.ports.search.retrieval_evaluator import IRetrievalEvaluator
from app.domain.ports.search.vector_search import IVectorSearch
from app.domain.ports.source.data_source import IDataSource
from app.domain.ports.user.user_repository import IUserRepository
from app.infrastructure.adapters.cache.redis_cache_adapter import RedisCacheAdapter
from app.infrastructure.adapters.chat.chat_repository_sqla import ChatRepositorySQLA
from app.infrastructure.adapters.connectors.argentina_datos_adapter import ArgentinaDatosAdapter
from app.infrastructure.adapters.connectors.ckan_search_adapter import CKANSearchAdapter
from app.infrastructure.adapters.cache.semantic_cache import SemanticCache
from app.infrastructure.adapters.connectors.ddjj_adapter import DDJJAdapter
from app.infrastructure.mcp.mcp_client import MCPClient
from app.infrastructure.monitoring.health import HealthCheckService
from app.infrastructure.adapters.connectors.georef_adapter import GeorefAdapter
from app.infrastructure.adapters.connectors.series_tiempo_adapter import SeriesTiempoAdapter
from app.infrastructure.adapters.connectors.sesiones_adapter import SesionesAdapter
from app.infrastructure.adapters.connectors.staff_adapter import StaffAdapter
from app.infrastructure.adapters.dataset.dataset_repository_sqla import DatasetRepositorySQLA
from app.infrastructure.adapters.llm.anthropic_adapter import AnthropicLLMAdapter
from app.infrastructure.adapters.llm.fallback_llm_adapter import FallbackLLMAdapter
from app.infrastructure.adapters.llm.gemini_adapter import GeminiLLMAdapter
from app.infrastructure.adapters.cache.cached_embedding_service import CachedEmbeddingService
from app.infrastructure.adapters.llm.gemini_embedding_adapter import GeminiEmbeddingAdapter
from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import PgSandboxAdapter
from app.infrastructure.adapters.search.pgvector_search_adapter import PgVectorSearchAdapter
from app.infrastructure.adapters.search.retrieval_evaluator import RetrievalEvaluator
from app.infrastructure.adapters.source.caba_adapter import CABADataAdapter
from app.infrastructure.adapters.source.datos_gob_ar_adapter import DatosGobArAdapter
from app.infrastructure.adapters.user.user_repository_sqla import UserRepositorySQLA
from app.infrastructure.persistence_sqla.config import PostgresDsn, SqlaEngineConfig
from app.infrastructure.persistence_sqla.provider import (
    MainAsyncSession,
    get_async_engine,
    get_async_session_factory,
    get_main_async_session,
)
from app.setup.config.settings import AppSettings


class SettingsProvider(Provider):
    scope = Scope.APP

    def __init__(self, settings: AppSettings) -> None:
        super().__init__()
        self._settings = settings

    @provide
    def settings(self) -> AppSettings:
        return self._settings


class DatabaseProvider(Provider):
    scope = Scope.APP

    @provide
    def dsn(self, settings: AppSettings) -> PostgresDsn:
        return PostgresDsn(url=settings.postgres.dsn)

    @provide
    def engine_config(self, settings: AppSettings) -> SqlaEngineConfig:
        return SqlaEngineConfig.from_settings(settings.postgres, settings.sqla)

    @provide
    async def engine(self, config: SqlaEngineConfig) -> AsyncEngine:
        return await get_async_engine(config)

    @provide
    def session_factory(self, engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
        return get_async_session_factory(engine)

    @provide(scope=Scope.REQUEST)
    async def session(
        self, factory: async_sessionmaker[AsyncSession]
    ) -> AsyncIterator[MainAsyncSession]:
        async for session in get_main_async_session(factory):
            yield session


class DatasetProvider(Provider):
    scope = Scope.REQUEST

    @provide
    def dataset_repository(self, session: MainAsyncSession) -> IDatasetRepository:
        return DatasetRepositorySQLA(session)

    @provide
    def vector_search(self, session: MainAsyncSession) -> IVectorSearch:
        return PgVectorSearchAdapter(session)


class LLMProvider(Provider):
    scope = Scope.REQUEST

    @provide
    def llm_provider(self, settings: AppSettings) -> ILLMProvider:
        return FallbackLLMAdapter(
            primary=GeminiLLMAdapter(
                api_key=settings.gemini.API_KEY,
                model=settings.gemini.MODEL,
            ),
            fallback=AnthropicLLMAdapter(
                api_key=settings.anthropic.API_KEY,
                model=settings.anthropic.MODEL,
            ),
        )

    @provide
    def embedding_provider(self, settings: AppSettings, cache: ICacheService) -> IEmbeddingProvider:
        base = GeminiEmbeddingAdapter(
            api_key=settings.gemini.API_KEY,
            model=settings.agents.EMBEDDING_MODEL,
            dimensions=settings.agents.EMBEDDING_DIMENSIONS,
        )
        return CachedEmbeddingService(base=base, cache=cache)


class DataSourceProvider(Provider):
    scope = Scope.REQUEST

    @provide
    def datos_gob_ar(self, settings: AppSettings) -> DatosGobArAdapter:
        return DatosGobArAdapter(base_url=settings.scraper.DATOS_GOB_AR_BASE_URL)

    @provide
    def caba(self, settings: AppSettings) -> CABADataAdapter:
        return CABADataAdapter(base_url=settings.scraper.CABA_BASE_URL)


class CacheProvider(Provider):
    scope = Scope.APP

    @provide
    def cache_service(self) -> ICacheService:
        redis_url = os.getenv("REDIS_CACHE_URL", "redis://localhost:6379/2")
        return RedisCacheAdapter(redis_url=redis_url)


class SandboxProvider(Provider):
    scope = Scope.REQUEST

    @provide
    def sql_sandbox(self) -> ISQLSandbox:
        return PgSandboxAdapter()


class UserProvider(Provider):
    scope = Scope.REQUEST

    @provide
    def user_repository(self, session: MainAsyncSession) -> IUserRepository:
        return UserRepositorySQLA(session)


class ChatProvider(Provider):
    scope = Scope.REQUEST

    @provide
    def chat_repository(self, session: MainAsyncSession) -> IChatRepository:
        return ChatRepositorySQLA(session)


class MCPProvider(Provider):
    scope = Scope.APP

    @provide
    def mcp_client(self, settings: AppSettings) -> MCPClient:
        servers = {
            "series_tiempo": os.getenv("MCP_SERIES_TIEMPO_URL", "http://localhost:8091"),
            "ckan": os.getenv("MCP_CKAN_URL", "http://localhost:8092"),
            "argentina_datos": os.getenv("MCP_ARGENTINA_DATOS_URL", "http://localhost:8093"),
            "sesiones": os.getenv("MCP_SESIONES_URL", "http://localhost:8094"),
        }
        return MCPClient(servers=servers, timeout=30.0)


class SemanticCacheProvider(Provider):
    scope = Scope.APP

    @provide
    def semantic_cache(
        self, session_factory: async_sessionmaker[AsyncSession]
    ) -> SemanticCache:
        return SemanticCache(session_factory=session_factory)


class MonitoringProvider(Provider):
    scope = Scope.APP

    @provide
    def health_check_service(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        ddjj: DDJJAdapter,
    ) -> HealthCheckService:
        redis_url = os.getenv("REDIS_CACHE_URL", "redis://localhost:6379/2")
        return HealthCheckService(
            session_factory=session_factory,
            redis_url=redis_url,
            ddjj=ddjj,
        )


class ConnectorProvider(Provider):
    scope = Scope.APP

    @provide
    def series_tiempo(self, mcp_client: MCPClient) -> ISeriesTiempoConnector:
        return SeriesTiempoAdapter(mcp_client=mcp_client)

    @provide
    def argentina_datos(self, mcp_client: MCPClient) -> IArgentinaDatosConnector:
        return ArgentinaDatosAdapter(mcp_client=mcp_client)

    @provide
    def georef(self, settings: AppSettings) -> IGeorefConnector:
        return GeorefAdapter(base_url=settings.scraper.GEOREF_BASE_URL)

    @provide
    def ckan_search(self, mcp_client: MCPClient) -> ICKANSearchConnector:
        return CKANSearchAdapter(mcp_client=mcp_client)

    @provide
    def sesiones(
        self,
        settings: AppSettings,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> ISesionesConnector:
        adapter = SesionesAdapter(
            session_factory=session_factory,
            gemini_api_key=settings.gemini.API_KEY,
        )
        adapter._ensure_loaded()
        return adapter

    @provide
    def ddjj(self) -> DDJJAdapter:
        adapter = DDJJAdapter()
        adapter._ensure_loaded()
        return adapter

    @provide
    def staff(
        self, session_factory: async_sessionmaker[AsyncSession],
    ) -> StaffAdapter:
        return StaffAdapter(session_factory=session_factory)


class RetrievalEvaluatorProvider(Provider):
    scope = Scope.REQUEST

    @provide
    def retrieval_evaluator(self, llm: ILLMProvider) -> IRetrievalEvaluator:
        return RetrievalEvaluator(llm=llm)


class ApplicationProvider(Provider):
    scope = Scope.REQUEST

    @provide
    def smart_query_service(
        self,
        llm: ILLMProvider,
        embedding: IEmbeddingProvider,
        vector_search: IVectorSearch,
        cache: ICacheService,
        series: ISeriesTiempoConnector,
        arg_datos: IArgentinaDatosConnector,
        georef: IGeorefConnector,
        ckan: ICKANSearchConnector,
        sesiones: ISesionesConnector,
        ddjj: DDJJAdapter,
        semantic_cache: SemanticCache,
        retrieval_evaluator: IRetrievalEvaluator,
        staff: StaffAdapter,
    ) -> SmartQueryService:
        return SmartQueryService(
            llm=llm,
            embedding=embedding,
            vector_search=vector_search,
            cache=cache,
            series=series,
            arg_datos=arg_datos,
            georef=georef,
            ckan=ckan,
            sesiones=sesiones,
            ddjj=ddjj,
            semantic_cache=semantic_cache,
            retrieval_evaluator=retrieval_evaluator,
            staff=staff,
        )


def get_providers() -> Iterable[Provider]:
    return (
        DatabaseProvider(),
        DatasetProvider(),
        LLMProvider(),
        DataSourceProvider(),
        CacheProvider(),
        SandboxProvider(),
        UserProvider(),
        ChatProvider(),
        ConnectorProvider(),
        MCPProvider(),
        SemanticCacheProvider(),
        MonitoringProvider(),
        RetrievalEvaluatorProvider(),
        ApplicationProvider(),
    )


def create_async_ioc_container(
    providers: tuple[Provider, ...],
    settings: AppSettings,
) -> object:
    all_providers = (SettingsProvider(settings), *providers)
    return make_async_container(*all_providers)
