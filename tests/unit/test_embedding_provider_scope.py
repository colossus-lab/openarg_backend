"""El proveedor de embeddings es uno por proceso, con timeouts cortos.

`buscar_datasets` midió en prod (api_usage, 2 y 3-oct) p50 1,4 s, p90 9,2 s y
un máximo de 45 s, mientras su SQL tardaba 2,5 ms dentro del índice. El
proveedor tenía scope REQUEST: cada búsqueda armaba un ``boto3.client`` nuevo,
sin reusar conexiones (un handshake TLS por pedido), con los defaults de
botocore: 60 s de connect y de read y reintentos "legacy".
"""

from __future__ import annotations

from types import SimpleNamespace

from dishka import Provider, Scope, make_async_container, provide

from app.domain.ports.cache.cache_port import ICacheService
from app.domain.ports.llm.llm_provider import IEmbeddingProvider
from app.infrastructure.adapters.cache.cached_embedding_service import CachedEmbeddingService
from app.infrastructure.adapters.llm.bedrock_embedding_adapter import BedrockEmbeddingAdapter
from app.setup.config.settings import AppSettings
from app.setup.ioc.provider_registry import LLMProvider, SettingsProvider


class _Cache(Provider):
    scope = Scope.APP

    @provide
    def cache(self) -> ICacheService:
        return SimpleNamespace()  # type: ignore[return-value]


def _settings() -> AppSettings:
    return SimpleNamespace(  # type: ignore[return-value]
        bedrock=SimpleNamespace(REGION="us-east-1", EMBEDDING_MODEL="cohere.embed-multilingual-v3"),
        agents=SimpleNamespace(EMBEDDING_DIMENSIONS=1024),
    )


async def test_two_requests_share_one_embedding_provider() -> None:
    container = make_async_container(SettingsProvider(_settings()), _Cache(), LLMProvider())
    try:
        async with container() as request_a:
            first = await request_a.get(IEmbeddingProvider)
        async with container() as request_b:
            second = await request_b.get(IEmbeddingProvider)
    finally:
        await container.close()

    assert first is second
    assert isinstance(first, CachedEmbeddingService)


def test_the_bedrock_client_does_not_wait_a_minute() -> None:
    adapter = BedrockEmbeddingAdapter(region="us-east-1")
    config = adapter._client.meta.config

    assert config.connect_timeout <= 5
    assert config.read_timeout <= 10
    assert config.retries["mode"] == "adaptive"
    assert config.retries["total_max_attempts"] == 3
    assert config.max_pool_connections >= 10
