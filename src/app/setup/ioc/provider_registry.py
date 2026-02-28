from __future__ import annotations

import os
from collections.abc import AsyncIterator, Iterable

from dishka import Provider, Scope, make_async_container, provide
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

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
from app.domain.ports.search.vector_search import IVectorSearch
from app.domain.ports.source.data_source import IDataSource
from app.domain.ports.user.user_repository import IUserRepository
from app.infrastructure.adapters.cache.redis_cache_adapter import RedisCacheAdapter
from app.infrastructure.adapters.chat.chat_repository_sqla import ChatRepositorySQLA
from app.infrastructure.adapters.connectors.argentina_datos_adapter import ArgentinaDatosAdapter
from app.infrastructure.adapters.connectors.ckan_search_adapter import CKANSearchAdapter
from app.infrastructure.adapters.connectors.ddjj_adapter import DDJJAdapter
from app.infrastructure.adapters.connectors.georef_adapter import GeorefAdapter
from app.infrastructure.adapters.connectors.series_tiempo_adapter import SeriesTiempoAdapter
from app.infrastructure.adapters.connectors.sesiones_adapter import SesionesAdapter
from app.infrastructure.adapters.dataset.dataset_repository_sqla import DatasetRepositorySQLA
from app.infrastructure.adapters.llm.anthropic_adapter import AnthropicLLMAdapter
from app.infrastructure.adapters.llm.gemini_adapter import GeminiLLMAdapter
from app.infrastructure.adapters.llm.openai_adapter import OpenAILLMAdapter
from app.infrastructure.adapters.llm.openai_embedding_adapter import OpenAIEmbeddingAdapter
from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import PgSandboxAdapter
from app.infrastructure.adapters.search.pgvector_search_adapter import PgVectorSearchAdapter
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
        provider = settings.agents.DEFAULT_LLM_PROVIDER
        if provider == "gemini":
            return GeminiLLMAdapter(api_key=settings.gemini.API_KEY)
        if provider == "openai":
            return OpenAILLMAdapter(api_key=settings.openai.API_KEY)
        return AnthropicLLMAdapter(api_key=settings.anthropic.API_KEY)

    @provide
    def embedding_provider(self, settings: AppSettings) -> IEmbeddingProvider:
        return OpenAIEmbeddingAdapter(
            api_key=settings.openai.API_KEY,
            model=settings.agents.EMBEDDING_MODEL,
            dimensions=settings.agents.EMBEDDING_DIMENSIONS,
        )


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


class ConnectorProvider(Provider):
    scope = Scope.APP

    @provide
    def series_tiempo(self, settings: AppSettings) -> ISeriesTiempoConnector:
        return SeriesTiempoAdapter(base_url=settings.scraper.SERIES_TIEMPO_BASE_URL)

    @provide
    def argentina_datos(self, settings: AppSettings) -> IArgentinaDatosConnector:
        return ArgentinaDatosAdapter(base_url=settings.scraper.ARGENTINA_DATOS_BASE_URL)

    @provide
    def georef(self, settings: AppSettings) -> IGeorefConnector:
        return GeorefAdapter(base_url=settings.scraper.GEOREF_BASE_URL)

    @provide
    def ckan_search(self) -> ICKANSearchConnector:
        return CKANSearchAdapter()

    @provide
    def sesiones(
        self,
        settings: AppSettings,
        session_factory: async_sessionmaker[AsyncSession],
    ) -> ISesionesConnector:
        adapter = SesionesAdapter(
            session_factory=session_factory,
            openai_api_key=settings.openai.API_KEY,
        )
        adapter._ensure_loaded()
        return adapter

    @provide
    def ddjj(self) -> DDJJAdapter:
        adapter = DDJJAdapter()
        adapter._ensure_loaded()
        return adapter


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
    )


def create_async_ioc_container(
    providers: tuple[Provider, ...],
    settings: AppSettings,
) -> object:
    all_providers = (SettingsProvider(settings), *providers)
    return make_async_container(*all_providers)
