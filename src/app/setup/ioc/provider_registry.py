from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterable

import httpx
from dishka import Provider, Scope, make_async_container, provide
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.application.pipeline.nodes import PipelineDeps
from app.application.smart_query_service import SmartQueryService
from app.domain.ports.cache.cache_port import ICacheService
from app.domain.ports.chat.chat_repository import IChatRepository
from app.domain.ports.connectors.argentina_datos import IArgentinaDatosConnector
from app.domain.ports.connectors.ckan_search import ICKANSearchConnector
from app.domain.ports.connectors.georef import IGeorefConnector
from app.domain.ports.connectors.series_tiempo import ISeriesTiempoConnector
from app.domain.ports.connectors.sesiones import ISesionesConnector
from app.domain.ports.connectors.staff import IStaffConnector
from app.domain.ports.dataset.dataset_repository import IDatasetRepository
from app.domain.ports.llm.llm_provider import IEmbeddingProvider, ILLMProvider
from app.domain.ports.sandbox.sql_sandbox import ISQLSandbox
from app.domain.ports.search.vector_search import IVectorSearch
from app.domain.ports.user.user_repository import IUserRepository
from app.infrastructure.adapters.cache.cached_embedding_service import CachedEmbeddingService
from app.infrastructure.adapters.cache.redis_cache_adapter import RedisCacheAdapter
from app.infrastructure.adapters.cache.semantic_cache import SemanticCache
from app.infrastructure.adapters.chat.chat_repository_sqla import ChatRepositorySQLA
from app.infrastructure.adapters.connectors.argentina_datos_adapter import ArgentinaDatosAdapter
from app.infrastructure.adapters.connectors.bcra_adapter import BCRAAdapter
from app.infrastructure.adapters.connectors.ckan_search_adapter import CKANSearchAdapter
from app.infrastructure.adapters.connectors.ddjj_adapter import DDJJAdapter
from app.infrastructure.adapters.connectors.georef_adapter import GeorefAdapter
from app.infrastructure.adapters.connectors.series_tiempo_adapter import SeriesTiempoAdapter
from app.infrastructure.adapters.connectors.sesiones_adapter import SesionesAdapter
from app.infrastructure.adapters.connectors.staff_adapter import StaffAdapter
from app.infrastructure.adapters.dataset.dataset_repository_sqla import DatasetRepositorySQLA
from app.infrastructure.adapters.llm.anthropic_adapter import AnthropicLLMAdapter
from app.infrastructure.adapters.llm.bedrock_embedding_adapter import BedrockEmbeddingAdapter
from app.infrastructure.adapters.llm.bedrock_llm_adapter import BedrockLLMAdapter
from app.infrastructure.adapters.llm.fallback_llm_adapter import FallbackLLMAdapter
from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import PgSandboxAdapter
from app.infrastructure.adapters.search.pgvector_search_adapter import PgVectorSearchAdapter
from app.infrastructure.adapters.source.caba_adapter import CABADataAdapter
from app.infrastructure.adapters.source.datos_gob_ar_adapter import DatosGobArAdapter
from app.infrastructure.adapters.user.user_repository_sqla import UserRepositorySQLA
from app.infrastructure.monitoring.health import HealthCheckService
from app.infrastructure.persistence_sqla.config import PostgresDsn, SqlaEngineConfig
from app.infrastructure.persistence_sqla.provider import (
    MainAsyncSession,
    get_async_engine,
    get_async_session_factory,
    get_main_async_session,
)
from app.setup.config.settings import AppSettings


class SettingsProvider(Provider):  # type: ignore[misc]
    scope = Scope.APP

    def __init__(self, settings: AppSettings) -> None:
        super().__init__()
        self._settings = settings

    @provide  # type: ignore[untyped-decorator]
    def settings(self) -> AppSettings:
        return self._settings


class DatabaseProvider(Provider):  # type: ignore[misc]
    scope = Scope.APP

    @provide  # type: ignore[untyped-decorator]
    def dsn(self, settings: AppSettings) -> PostgresDsn:
        return PostgresDsn(url=settings.postgres.dsn)

    @provide  # type: ignore[untyped-decorator]
    def engine_config(self, settings: AppSettings) -> SqlaEngineConfig:
        return SqlaEngineConfig.from_settings(settings.postgres, settings.sqla)

    @provide  # type: ignore[untyped-decorator]
    async def engine(self, config: SqlaEngineConfig) -> AsyncEngine:
        return await get_async_engine(config)  # type: ignore[no-any-return]

    @provide  # type: ignore[untyped-decorator]
    def session_factory(self, engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
        return get_async_session_factory(engine)  # type: ignore[no-any-return]

    @provide(scope=Scope.REQUEST)  # type: ignore[untyped-decorator]
    async def session(
        self, factory: async_sessionmaker[AsyncSession]
    ) -> AsyncIterator[MainAsyncSession]:
        async for session in get_main_async_session(factory):
            yield session


class DatasetProvider(Provider):  # type: ignore[misc]
    scope = Scope.REQUEST

    @provide  # type: ignore[untyped-decorator]
    def dataset_repository(self, session: MainAsyncSession) -> IDatasetRepository:
        return DatasetRepositorySQLA(session)

    @provide  # type: ignore[untyped-decorator]
    def vector_search(self, session: MainAsyncSession) -> IVectorSearch:
        return PgVectorSearchAdapter(session)


class LLMProvider(Provider):  # type: ignore[misc]
    scope = Scope.REQUEST

    @provide  # type: ignore[untyped-decorator]
    def llm_provider(self, settings: AppSettings) -> ILLMProvider:
        return FallbackLLMAdapter(
            primary=BedrockLLMAdapter(
                region=settings.bedrock.REGION,
                model=settings.bedrock.LLM_MODEL,
            ),
            fallback=AnthropicLLMAdapter(
                api_key=settings.anthropic.API_KEY,
                model=settings.anthropic.MODEL,
            ),
        )

    @provide  # type: ignore[untyped-decorator]
    def embedding_provider(self, settings: AppSettings, cache: ICacheService) -> IEmbeddingProvider:
        base = BedrockEmbeddingAdapter(
            region=settings.bedrock.REGION,
            model=settings.bedrock.EMBEDDING_MODEL,
            dimensions=settings.agents.EMBEDDING_DIMENSIONS,
        )
        return CachedEmbeddingService(base=base, cache=cache)


class DataSourceProvider(Provider):  # type: ignore[misc]
    scope = Scope.REQUEST

    @provide  # type: ignore[untyped-decorator]
    def datos_gob_ar(self, settings: AppSettings) -> DatosGobArAdapter:
        return DatosGobArAdapter(base_url=settings.scraper.DATOS_GOB_AR_BASE_URL)

    @provide  # type: ignore[untyped-decorator]
    def caba(self, settings: AppSettings) -> CABADataAdapter:
        return CABADataAdapter(base_url=settings.scraper.CABA_BASE_URL)


class CacheProvider(Provider):  # type: ignore[misc]
    scope = Scope.APP

    @provide  # type: ignore[untyped-decorator]
    def cache_service(self) -> ICacheService:
        redis_url = os.getenv("REDIS_CACHE_URL", "redis://localhost:6379/2")
        return RedisCacheAdapter(redis_url=redis_url)


class SandboxProvider(Provider):  # type: ignore[misc]
    scope = Scope.REQUEST

    @provide  # type: ignore[untyped-decorator]
    def sql_sandbox(self) -> ISQLSandbox:
        return PgSandboxAdapter()


class UserProvider(Provider):  # type: ignore[misc]
    scope = Scope.REQUEST

    @provide  # type: ignore[untyped-decorator]
    def user_repository(self, session: MainAsyncSession) -> IUserRepository:
        return UserRepositorySQLA(session)


class ChatProvider(Provider):  # type: ignore[misc]
    scope = Scope.REQUEST

    @provide  # type: ignore[untyped-decorator]
    def chat_repository(self, session: MainAsyncSession) -> IChatRepository:
        return ChatRepositorySQLA(session)


class HttpClientProvider(Provider):  # type: ignore[misc]
    scope = Scope.APP

    @provide  # type: ignore[untyped-decorator]
    def http_client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            timeout=15.0,
            follow_redirects=True,
            headers={"User-Agent": "OpenArg/1.0"},
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
        )


class SemanticCacheProvider(Provider):  # type: ignore[misc]
    scope = Scope.APP

    @provide  # type: ignore[untyped-decorator]
    def semantic_cache(self, session_factory: async_sessionmaker[AsyncSession]) -> SemanticCache:
        return SemanticCache(session_factory=session_factory)


class MonitoringProvider(Provider):  # type: ignore[misc]
    scope = Scope.APP

    @provide  # type: ignore[untyped-decorator]
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


class ConnectorProvider(Provider):  # type: ignore[misc]
    scope = Scope.APP

    @provide  # type: ignore[untyped-decorator]
    def series_tiempo(self, http_client: httpx.AsyncClient) -> ISeriesTiempoConnector:
        return SeriesTiempoAdapter(http_client=http_client)

    @provide  # type: ignore[untyped-decorator]
    def argentina_datos(self, http_client: httpx.AsyncClient) -> IArgentinaDatosConnector:
        return ArgentinaDatosAdapter(http_client=http_client)

    @provide  # type: ignore[untyped-decorator]
    def georef(self, settings: AppSettings) -> IGeorefConnector:
        return GeorefAdapter(base_url=settings.scraper.GEOREF_BASE_URL)

    @provide  # type: ignore[untyped-decorator]
    def ckan_search(self, http_client: httpx.AsyncClient) -> ICKANSearchConnector:
        return CKANSearchAdapter(http_client=http_client)

    @provide  # type: ignore[untyped-decorator]
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

    @provide  # type: ignore[untyped-decorator]
    def ddjj(self) -> DDJJAdapter:
        adapter = DDJJAdapter()
        adapter._ensure_loaded()
        return adapter

    @provide  # type: ignore[untyped-decorator]
    def staff(
        self,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> IStaffConnector:
        return StaffAdapter(session_factory=session_factory)

    @provide  # type: ignore[untyped-decorator]
    def bcra(self) -> BCRAAdapter:
        return BCRAAdapter()


class ApplicationProvider(Provider):  # type: ignore[misc]
    scope = Scope.REQUEST

    @provide  # type: ignore[untyped-decorator]
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
        staff: IStaffConnector,
        bcra: BCRAAdapter,
        sandbox: ISQLSandbox,
        chat_repo: IChatRepository,
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
            staff=staff,
            bcra=bcra,
            sandbox=sandbox,
            chat_repo=chat_repo,
        )


class LangGraphProvider(Provider):  # type: ignore[misc]
    """Provides PipelineDeps per-request and a compiled graph (APP-scoped).

    The graph topology is compiled once. Dependencies (LLM, cache, etc.)
    are injected per-request via PipelineDeps and set on the nodes module
    before each invocation.
    """

    scope = Scope.REQUEST

    @provide  # type: ignore[untyped-decorator]
    def pipeline_deps(
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
        staff: IStaffConnector,
        bcra: BCRAAdapter,
        sandbox: ISQLSandbox,
        chat_repo: IChatRepository,
    ) -> PipelineDeps:
        from app.infrastructure.monitoring.metrics import MetricsCollector

        return PipelineDeps(
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
            staff=staff,
            bcra=bcra,
            sandbox=sandbox,
            semantic_cache=semantic_cache,
            chat_repo=chat_repo,
            metrics=MetricsCollector(),
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
        HttpClientProvider(),
        ConnectorProvider(),
        SemanticCacheProvider(),
        MonitoringProvider(),
        ApplicationProvider(),
        LangGraphProvider(),
    )


def create_async_ioc_container(
    providers: tuple[Provider, ...],
    settings: AppSettings,
) -> object:
    all_providers = (SettingsProvider(settings), *providers)
    return make_async_container(*all_providers)
