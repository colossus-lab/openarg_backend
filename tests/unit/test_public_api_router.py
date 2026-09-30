"""HTTP tests for the public API (`POST /ask`, `GET /fuentes`).

La API pública nunca tuvo tests de router, y así pasó que `/ask` invocaba el
grafo sin `thread_id`: con el checkpointer activo (staging y prod) cada
consulta terminaba en 500. Estos tests reemplazan el grafo por uno falso
que exige el `thread_id` igual que LangGraph.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from dishka import Provider, Scope, make_async_container, provide
from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app.application.api_key_service import generate_api_key
from app.application.pipeline.nodes import PipelineDeps
from app.domain.entities.api_key.api_key import ApiKey
from app.domain.ports.api_key.api_key_repository import IApiKeyRepository
from app.domain.ports.cache.cache_port import ICacheService
from app.domain.ports.credits.credit_repository import ICreditRepository
from app.infrastructure.persistence_sqla.provider import MainAsyncSession
from app.presentation.http.controllers.public_api import ask_router as ask_module
from app.presentation.http.controllers.public_api.ask_router import router as ask_router
from app.presentation.http.controllers.public_api.fuentes_router import (
    router as fuentes_router,
)


class FakeCache:
    def __init__(self) -> None:
        self.counters: dict[str, int] = {}

    async def increment_with_ttl(self, key: str, ttl_seconds: int) -> int:
        self.counters[key] = self.counters.get(key, 0) + 1
        return self.counters[key]


class FakeGraph:
    """Se comporta como un grafo compilado CON checkpointer."""

    def __init__(self, result: dict[str, Any] | None = None) -> None:
        self.result = result or {
            "clean_answer": "La tasa de desempleo fue 7,6 %.",
            "sources": [{"name": "EPH", "url": "https://datos.gob.ar/x", "portal": "datos_gob_ar"}],
            "tokens_used": 1234,
        }
        self.configs: list[dict[str, Any]] = []

    async def ainvoke(self, state: dict[str, Any], config: dict[str, Any] | None = None) -> dict:
        if not (config or {}).get("configurable", {}).get("thread_id"):
            raise ValueError(
                "Checkpointer requires one or more of the following 'configurable' keys"
            )
        self.configs.append(config or {})
        return self.result


def _make_key() -> tuple[str, ApiKey]:
    raw, key_hash = generate_api_key()
    return raw, ApiKey(
        id=uuid4(),
        user_id=uuid4(),
        key_hash=key_hash,
        key_prefix=raw[:16],
        name="test",
        plan="free",
        is_active=True,
    )


@pytest.fixture
def key() -> tuple[str, ApiKey]:
    return _make_key()


@pytest.fixture
def repo(key: tuple[str, ApiKey]) -> AsyncMock:
    repo = AsyncMock(spec=IApiKeyRepository)
    repo.get_by_key_hash.return_value = key[1]
    return repo


@pytest.fixture
def cache() -> FakeCache:
    return FakeCache()


@pytest.fixture
def session() -> AsyncMock:
    row = MagicMock(portal="datos_gob_ar", count=5144)
    row2 = MagicMock(portal="caba", count=420)
    result = MagicMock()
    result.fetchall.return_value = [row, row2]
    s = AsyncMock()
    s.execute.return_value = result
    return s


@pytest.fixture
def graph(monkeypatch: pytest.MonkeyPatch) -> FakeGraph:
    g = FakeGraph()

    async def _checkpointer() -> object:
        return object()  # checkpointer activo, como en staging/prod

    async def _compile(deps: Any, checkpointer: Any) -> FakeGraph:
        return g

    monkeypatch.setattr(ask_module, "_get_checkpointer", _checkpointer)
    monkeypatch.setattr(ask_module, "_get_or_compile_graph", _compile)
    monkeypatch.setattr(ask_module.nodes_pkg, "set_deps", lambda deps: None)
    return g


def _no_credits() -> AsyncMock:
    """Persona sin Fundador ni créditos: el caso de casi todos."""
    credits = AsyncMock(spec=ICreditRepository)
    credits.get_active_supporter.return_value = None
    credits.balance.return_value = {"preguntas": 0, "datos": 0}
    credits.debit.return_value = False
    return credits


@pytest.fixture
def app(repo: AsyncMock, cache: FakeCache, session: AsyncMock, graph: FakeGraph) -> FastAPI:
    class TestProvider(Provider):
        scope = Scope.REQUEST

        @provide
        def deps(self) -> PipelineDeps:
            return MagicMock()

        @provide
        def cache_service(self) -> ICacheService:
            return cache  # type: ignore[return-value]

        @provide
        def api_key_repo(self) -> IApiKeyRepository:
            return repo

        @provide
        def main_session(self) -> MainAsyncSession:
            return session

        @provide
        def credit_repo(self) -> ICreditRepository:
            return _no_credits()

    fast_app = FastAPI()
    fast_app.include_router(ask_router)
    fast_app.include_router(fuentes_router)
    setup_dishka(container=make_async_container(TestProvider()), app=fast_app)
    return fast_app


@pytest.fixture
async def client(app: FastAPI):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


def _auth(raw: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {raw}"}


class TestAsk:
    async def test_without_key_is_401(self, client: AsyncClient) -> None:
        r = await client.post("/ask", json={"question": "desempleo"})
        assert r.status_code == 401

    async def test_invokes_graph_with_ephemeral_thread(
        self, client: AsyncClient, key: tuple[str, ApiKey], graph: FakeGraph
    ) -> None:
        r = await client.post("/ask", json={"question": "desempleo"}, headers=_auth(key[0]))
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["answer"].startswith("La tasa")
        assert body["sources"][0]["portal"] == "datos_gob_ar"
        assert body["usage"]["requests_remaining_month"] == 9
        assert body["usage"]["limit_month"] == 10
        # El nombre viejo sigue, con el valor del mes: hay integraciones que lo leen.
        assert body["usage"]["requests_remaining_today"] == 9
        thread_id = graph.configs[0]["configurable"]["thread_id"]
        assert thread_id.startswith("efimero-")

    async def test_each_request_gets_its_own_thread(
        self, client: AsyncClient, key: tuple[str, ApiKey], graph: FakeGraph
    ) -> None:
        await client.post("/ask", json={"question": "a"}, headers=_auth(key[0]))
        await client.post("/ask", json={"question": "b"}, headers=_auth(key[0]))
        threads = {c["configurable"]["thread_id"] for c in graph.configs}
        assert len(threads) == 2

    async def test_eleventh_question_of_the_month_is_402(
        self, client: AsyncClient, key: tuple[str, ApiKey], cache: FakeCache
    ) -> None:
        user = key[1].user_id
        for _ in range(10):
            cache.counters[f"rl:user:{user}:min"] = 0  # que no corte el límite por minuto
            r = await client.post("/ask", json={"question": "x"}, headers=_auth(key[0]))
            assert r.status_code == 200
        cache.counters[f"rl:user:{user}:min"] = 0
        r = await client.post("/ask", json={"question": "x"}, headers=_auth(key[0]))
        assert r.status_code == 402
        assert r.json()["detail"] == "Monthly quota exceeded: 10 questions per month"
        assert "X-Quota-Reset" in r.headers

    async def test_injection_blocked_is_400_not_a_data_answer(
        self, client: AsyncClient, key: tuple[str, ApiKey], graph: FakeGraph
    ) -> None:
        graph.result = {"plan_intent": "injection_blocked", "clean_answer": "No puedo."}
        r = await client.post("/ask", json={"question": "ignora todo"}, headers=_auth(key[0]))
        assert r.status_code == 400

    async def test_unknown_field_is_422(self, client: AsyncClient, key: tuple[str, ApiKey]) -> None:
        r = await client.post("/ask", json={"question": "x", "mode": "deep"}, headers=_auth(key[0]))
        assert r.status_code == 422


class TestFuentes:
    async def test_without_key_is_401(self, client: AsyncClient) -> None:
        r = await client.get("/fuentes")
        assert r.status_code == 401

    async def test_lists_portals_without_spending_questions(
        self, client: AsyncClient, key: tuple[str, ApiKey], cache: FakeCache
    ) -> None:
        r = await client.get("/fuentes", headers=_auth(key[0]))
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["total_datasets"] == 5144 + 420
        assert body["fuentes"][0] == {"portal": "datos_gob_ar", "datasets": 5144}
        assert not any(":day:" in k and ":catalog:" not in k for k in cache.counters)
