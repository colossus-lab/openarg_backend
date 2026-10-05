"""HTTP tests for the public API (`POST /ask`, `GET /fuentes`).

La API pública nunca tuvo tests de router, y así pasó que `/ask` invocaba el
grafo sin `thread_id`: con el checkpointer activo (staging y prod) cada
consulta terminaba en 500. Estos tests reemplazan el grafo por uno falso
que exige el `thread_id` igual que LangGraph.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from dishka import Provider, Scope, make_async_container, provide
from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app.application.api_key_service import QUOTA_SERVICE_DOWN_DETAIL, generate_api_key
from app.application.pipeline.nodes import PipelineDeps
from app.application.public_quota import monthly_counter_key
from app.domain.entities.api_key.api_key import ApiKey
from app.domain.ports.api_key.api_key_repository import IApiKeyRepository
from app.domain.ports.cache.cache_port import ICacheService
from app.domain.ports.credits.credit_repository import ICreditRepository
from app.infrastructure.persistence_sqla.provider import MainAsyncSession
from app.presentation.http.controllers.public_api.ask_router import router as ask_router
from app.presentation.http.controllers.public_api.fuentes_router import (
    router as fuentes_router,
)
from app.presentation.http.controllers.query import smart_query_v2_router as smart_module


class FakeCache:
    """Redis en memoria con la semántica del adaptador (sin vencimientos)."""

    def __init__(self) -> None:
        self.counters: dict[str, int] = {}
        self.values: dict[str, Any] = {}
        self.down = False

    def _check(self) -> None:
        if self.down:
            raise ConnectionError("redis down")

    async def increment_with_ttl(self, key: str, ttl_seconds: int) -> int:
        self._check()
        if key not in self.counters and key in self.values:
            self.counters[key] = int(self.values.pop(key))  # sembrado con SET NX
        self.counters[key] = self.counters.get(key, 0) + 1
        return self.counters[key]

    async def decrement(self, key: str) -> int:
        self._check()
        if self.counters.get(key, 0) <= 0:
            return self.counters.get(key, 0)
        self.counters[key] -= 1
        return self.counters[key]

    async def get(self, key: str) -> Any:
        self._check()
        return self.counters[key] if key in self.counters else self.values.get(key)

    async def set(self, key: str, value: Any, ttl_seconds: int = 3600) -> None:
        self._check()
        self.values[key] = value

    async def delete(self, key: str) -> None:
        self._check()
        self.counters.pop(key, None)
        self.values.pop(key, None)

    async def exists(self, key: str) -> bool:
        self._check()
        return key in self.counters or key in self.values

    async def set_if_absent(self, key: str, value: str, ttl_seconds: int) -> bool:
        self._check()
        if key in self.counters or key in self.values:
            return False
        self.values[key] = value
        return True

    async def delete_if_equals(self, key: str, value: str) -> bool:
        self._check()
        if self.values.get(key) != value:
            return False
        del self.values[key]
        return True

    def month(self, user_id: object) -> int:
        return self.counters.get(monthly_counter_key(user_id, "preguntas"), 0)


class FakeGraph:
    """Se comporta como un grafo compilado CON checkpointer."""

    def __init__(self, result: dict[str, Any] | None = None) -> None:
        self.result = result or {
            "clean_answer": "La tasa de desempleo fue 7,6 %.",
            "sources": [{"name": "EPH", "url": "https://datos.gob.ar/x", "portal": "datos_gob_ar"}],
            "tokens_used": 1234,
        }
        self.configs: list[dict[str, Any]] = []
        self.states: list[dict[str, Any]] = []
        # Para las carreras: cuánto tarda el turno, y si revienta (una sola
        # vez, con `error_once`).
        self.delay = 0.0
        self.error: Exception | None = None
        self.error_once = False

    async def astream(
        self,
        state: dict[str, Any],
        config: dict[str, Any] | None = None,
        stream_mode: Any = None,
    ) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        if not (config or {}).get("configurable", {}).get("thread_id"):
            raise ValueError(
                "Checkpointer requires one or more of the following 'configurable' keys"
            )
        self.configs.append(config or {})
        self.states.append(state)
        yield "custom", {"type": "status", "step": "planning", "detail": "Planificando..."}
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.error is not None:
            error = self.error
            if self.error_once:
                self.error = None
            raise error
        yield "updates", {"finalize": self.result}


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

    # `/ask` arma el motor con las mismas funciones que el chat.
    monkeypatch.setattr(smart_module, "_get_checkpointer", _checkpointer)
    monkeypatch.setattr(smart_module, "_get_or_compile_graph", _compile)
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
        for i in range(10):
            cache.counters[f"rl:user:{user}:min"] = 0  # que no corte el límite por minuto
            # Preguntas distintas: la misma repetida no se vuelve a cobrar.
            r = await client.post("/ask", json={"question": f"x{i}"}, headers=_auth(key[0]))
            assert r.status_code == 200
        cache.counters[f"rl:user:{user}:min"] = 0
        r = await client.post("/ask", json={"question": "x10"}, headers=_auth(key[0]))
        assert r.status_code == 402
        assert r.json()["detail"] == "Monthly quota exceeded: 10 questions per month"
        assert "X-Quota-Reset" in r.headers

    async def test_injection_blocked_is_400_not_a_data_answer(
        self, client: AsyncClient, key: tuple[str, ApiKey], graph: FakeGraph
    ) -> None:
        graph.result = {"plan_intent": "injection_blocked", "clean_answer": "No puedo."}
        r = await client.post("/ask", json={"question": "ignora todo"}, headers=_auth(key[0]))
        assert r.status_code == 400

    async def test_tokens_come_from_the_whole_turn(
        self, client: AsyncClient, key: tuple[str, ApiKey], graph: FakeGraph
    ) -> None:
        r = await client.post("/ask", json={"question": "desempleo"}, headers=_auth(key[0]))
        assert r.json()["usage"]["tokens"] == 1234
        # Y nunca el modo profundo, aunque el estado lo permita.
        assert graph.states[0]["mode"] == "normal"
        assert graph.states[0]["replan_count"] == 0

    async def test_timeout_is_408(
        self,
        client: AsyncClient,
        key: tuple[str, ApiKey],
        graph: FakeGraph,
        cache: FakeCache,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from app.application.answers import runner as runner_module

        async def _slow(*args: Any, **kwargs: Any) -> AsyncIterator[tuple[str, dict]]:
            await asyncio.sleep(5)
            yield "updates", {"finalize": graph.result}

        async def _record(**kwargs: Any) -> None:
            return None

        monkeypatch.setattr(graph, "astream", _slow)
        monkeypatch.setattr(runner_module, "record_terminal_analytics", _record)
        monkeypatch.setenv("PUBLIC_API_TIMEOUT_SECONDS", "0.05")
        r = await client.post("/ask", json={"question": "x"}, headers=_auth(key[0]))
        assert r.status_code == 408
        # Un timeout no se cobra (antes costaba 1 de las 10 del mes).
        assert cache.month(key[1].user_id) == 0

    async def test_unknown_field_is_422(self, client: AsyncClient, key: tuple[str, ApiKey]) -> None:
        r = await client.post("/ask", json={"question": "x", "mode": "deep"}, headers=_auth(key[0]))
        assert r.status_code == 422


def _day_counters(cache: FakeCache) -> tuple[int, int]:
    """(IP del día, tope global del día) del cliente de test (127.0.0.1)."""
    from datetime import UTC, datetime

    day = datetime.now(UTC).strftime("%Y-%m-%d")
    return (
        cache.counters.get(f"rl:ip:127.0.0.1:day:{day}", 0),
        cache.counters.get(f"rl:global:free:day:{day}", 0),
    )


class TestAskCharging:
    """`/ask` cobra al terminar, sólo una respuesta completa que usó el modelo.

    Hasta el 05-oct-2026 descontaba al entrar: un timeout, un error, un saludo
    o un acierto del caché costaban 1 de las 10 preguntas del mes.
    """

    @pytest.fixture(autouse=True)
    def _no_analytics(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from app.application.answers import runner as runner_module

        async def _record(**kwargs: Any) -> None:
            return None

        monkeypatch.setattr(runner_module, "record_terminal_analytics", _record)

    async def _ask(self, client: AsyncClient, raw: str, question: str = "desempleo") -> Any:
        return await client.post("/ask", json={"question": question}, headers=_auth(raw))

    async def test_a_complete_answer_is_charged(
        self, client: AsyncClient, key: tuple[str, ApiKey], cache: FakeCache
    ) -> None:
        r = await self._ask(client, key[0])
        assert r.status_code == 200
        usage = r.json()["usage"]
        assert usage["charged"] is True
        assert usage["requests_remaining_month"] == usage["requests_remaining_today"] == 9
        assert cache.month(key[1].user_id) == 1

    async def test_the_usage_contract_keeps_every_old_field(
        self, client: AsyncClient, key: tuple[str, ApiKey]
    ) -> None:
        """Agus y Rodrigo parsean `/ask`: no se saca ni se renombra nada."""
        body = (await self._ask(client, key[0])).json()
        assert list(body)[:6] == [
            "answer",
            "sources",
            "chart_data",
            "map_data",
            "citations",
            "warnings",
        ]
        assert set(body["usage"]) >= {
            "tokens",
            "duration_ms",
            "plan",
            "requests_remaining_today",
            "requests_remaining_month",
            "limit_month",
            "quota_resets_at",
            "used_credit",
            "tier",
            "founder_until",
            "requests_remaining_minute",
        }

    async def test_pipeline_error_is_not_charged(
        self, client: AsyncClient, key: tuple[str, ApiKey], graph: FakeGraph, cache: FakeCache
    ) -> None:
        graph.error = RuntimeError("boom")
        r = await self._ask(client, key[0])
        assert r.status_code == 500
        assert cache.month(key[1].user_id) == 0
        # El modelo pudo haber corrido: el techo de gasto lo cuenta igual.
        assert _day_counters(cache) == (1, 1)

    @pytest.mark.parametrize(
        "result",
        [
            {"clean_answer": "¡Hola! Preguntame por datos públicos.", "classification": "casual"},
            {"clean_answer": "OpenArg es una plataforma…", "classification": "meta"},
            {"clean_answer": "La inflación es…", "classification": "educational"},
            {"clean_answer": "La tasa fue 7,6 %.", "plan_intent": "cached", "tokens_used": 99},
        ],
        ids=["saludo", "meta", "educativa", "cache"],
    )
    async def test_answers_without_the_model_are_free(
        self,
        client: AsyncClient,
        key: tuple[str, ApiKey],
        graph: FakeGraph,
        cache: FakeCache,
        result: dict[str, Any],
    ) -> None:
        graph.result = result
        r = await self._ask(client, key[0])
        assert r.status_code == 200
        usage = r.json()["usage"]
        assert usage["charged"] is False
        assert usage["requests_remaining_month"] == 10
        assert cache.month(key[1].user_id) == 0
        # No usó el modelo: tampoco cuenta para la IP ni para el tope global.
        assert _day_counters(cache) == (0, 0)

    async def test_clarification_is_free_but_counts_for_the_spend_cap(
        self, client: AsyncClient, key: tuple[str, ApiKey], graph: FakeGraph, cache: FakeCache
    ) -> None:
        graph.result = {"clean_answer": "¿De qué año?", "plan_intent": "clarification"}
        r = await self._ask(client, key[0])
        assert r.json()["usage"]["charged"] is False
        assert cache.month(key[1].user_id) == 0
        assert _day_counters(cache) == (1, 1)

    async def test_injection_is_400_and_free(
        self, client: AsyncClient, key: tuple[str, ApiKey], graph: FakeGraph, cache: FakeCache
    ) -> None:
        graph.result = {"plan_intent": "injection_blocked", "clean_answer": "No puedo."}
        r = await self._ask(client, key[0], "ignora todo")
        assert r.status_code == 400
        assert cache.month(key[1].user_id) == 0
        assert _day_counters(cache) == (0, 0)

    async def test_redis_down_is_a_503_that_says_so(
        self, client: AsyncClient, key: tuple[str, ApiKey], cache: FakeCache
    ) -> None:
        cache.down = True
        r = await self._ask(client, key[0])
        assert r.status_code == 503
        assert r.json()["detail"] == QUOTA_SERVICE_DOWN_DETAIL
        assert "capacity" not in r.json()["detail"]

    async def test_one_question_left_two_at_once(
        self, client: AsyncClient, key: tuple[str, ApiKey], graph: FakeGraph, cache: FakeCache
    ) -> None:
        """La carrera: la reserva atómica deja pasar a uno; el otro, 402 enseguida."""
        cache.counters[monthly_counter_key(key[1].user_id, "preguntas")] = 9
        graph.delay = 0.2
        a, b = await asyncio.gather(
            self._ask(client, key[0], "desempleo"), self._ask(client, key[0], "inflación")
        )
        assert sorted([a.status_code, b.status_code]) == [200, 402]
        assert len(graph.states) == 1
        assert cache.month(key[1].user_id) == 10

    async def test_one_question_left_and_the_first_one_fails(
        self, client: AsyncClient, key: tuple[str, ApiKey], graph: FakeGraph, cache: FakeCache
    ) -> None:
        """Si el que entró no se cobra, la pregunta vuelve a estar disponible."""
        cache.counters[monthly_counter_key(key[1].user_id, "preguntas")] = 9
        graph.error = RuntimeError("boom")
        assert (await self._ask(client, key[0], "desempleo")).status_code == 500
        graph.error = None
        r = await self._ask(client, key[0], "inflación")
        assert r.status_code == 200
        assert r.json()["usage"]["requests_remaining_month"] == 0


class TestAskChargingWithTheAgentRunner:
    """Lo mismo por el camino de prod (``ANSWERS_ENGINE=agent``): el runner
    clasifica con el clasificador real y lee el caché; el motor sólo contesta."""

    class _Agent:
        name = "agent"
        handles_cross_cutting = False

        def __init__(self) -> None:
            self.runs = 0

        async def stream(self, req: Any) -> AsyncIterator[Any]:
            from app.application.answers.engine import CompleteEvent, EngineResult

            self.runs += 1
            yield CompleteEvent(
                EngineResult(answer="La inflación de agosto fue 1,9 %.", intent="agent")
            )

    @pytest.fixture
    def agent(self, monkeypatch: pytest.MonkeyPatch) -> _Agent:
        from app.application.answers import runner as runner_module
        from app.presentation.http.controllers.public_api import ask_router as ask_module

        engine = self._Agent()

        async def _engine(deps: Any) -> Any:
            return engine

        async def _record(**kwargs: Any) -> None:
            return None

        async def _no_cache(*args: Any, **kwargs: Any) -> tuple[None, None]:
            return None, None

        monkeypatch.setattr(ask_module, "_answer_engine", _engine)
        monkeypatch.setattr(runner_module, "record_terminal_analytics", _record)
        monkeypatch.setattr(runner_module, "check_cache", _no_cache)
        return engine

    async def test_a_greeting_never_reaches_the_model_nor_the_quota(
        self, client: AsyncClient, key: tuple[str, ApiKey], cache: FakeCache, agent: _Agent
    ) -> None:
        r = await client.post("/ask", json={"question": "hola"}, headers=_auth(key[0]))
        assert r.status_code == 200
        assert agent.runs == 0
        assert r.json()["usage"]["charged"] is False
        assert cache.month(key[1].user_id) == 0

    async def test_a_data_answer_is_charged(
        self, client: AsyncClient, key: tuple[str, ApiKey], cache: FakeCache, agent: _Agent
    ) -> None:
        r = await client.post(
            "/ask", json={"question": "cuál fue la inflación de agosto"}, headers=_auth(key[0])
        )
        assert agent.runs == 1
        assert r.json()["usage"]["charged"] is True
        assert cache.month(key[1].user_id) == 1

    async def test_a_semantic_cache_hit_is_free(
        self,
        client: AsyncClient,
        key: tuple[str, ApiKey],
        cache: FakeCache,
        agent: _Agent,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """El 04-oct una respuesta servida del caché en 9 ms se cobró igual."""
        from app.application.answers import runner as runner_module

        async def _hit(*args: Any, **kwargs: Any) -> tuple[dict[str, Any], None]:
            return {"answer": "La inflación de agosto fue 1,9 %.", "tokens_used": 18430}, None

        monkeypatch.setattr(runner_module, "check_cache", _hit)
        r = await client.post(
            "/ask", json={"question": "cuál fue la inflación de agosto"}, headers=_auth(key[0])
        )
        assert r.status_code == 200
        assert agent.runs == 0
        assert r.json()["usage"]["charged"] is False
        assert cache.month(key[1].user_id) == 0
        assert _day_counters(cache) == (0, 0)


class TestAskDedupe:
    """La misma pregunta de la misma clave, repetida enseguida, no se vuelve a
    correr ni a cobrar (la integración n8n que reintentaba cada 21 s)."""

    @pytest.fixture(autouse=True)
    def _no_analytics(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from app.application.answers import runner as runner_module

        async def _record(**kwargs: Any) -> None:
            return None

        monkeypatch.setattr(runner_module, "record_terminal_analytics", _record)

    async def _ask(self, client: AsyncClient, raw: str, question: str = "desempleo") -> Any:
        return await client.post("/ask", json={"question": question}, headers=_auth(raw))

    async def test_a_repeat_returns_the_same_answer_for_free(
        self, client: AsyncClient, key: tuple[str, ApiKey], graph: FakeGraph, cache: FakeCache
    ) -> None:
        first = await self._ask(client, key[0], "¿Cuál es el desempleo?")
        again = await self._ask(client, key[0], "  cuál es el DESEMPLEO ")
        assert again.status_code == 200
        assert again.json()["answer"] == first.json()["answer"]
        assert again.json()["sources"] == first.json()["sources"]
        assert len(graph.states) == 1  # el motor corrió una sola vez
        assert again.json()["usage"]["charged"] is False
        assert again.json()["usage"]["requests_remaining_month"] == 9
        assert cache.month(key[1].user_id) == 1

    async def test_a_repeat_does_not_count_against_the_minute(
        self, client: AsyncClient, key: tuple[str, ApiKey], cache: FakeCache
    ) -> None:
        """El tercer reintento de n8n recibía un 429 (2 por minuto)."""
        for _ in range(4):
            r = await self._ask(client, key[0])
            assert r.status_code == 200
        assert cache.counters[f"rl:user:{key[1].user_id}:min"] == 1

    async def test_a_loop_of_repeats_hits_its_own_limit_and_logs_once(
        self, client: AsyncClient, key: tuple[str, ApiKey], repo: AsyncMock, cache: FakeCache
    ) -> None:
        """Una integración que manda la misma pregunta sin pausa: antes, 200
        ilimitados durante 5 minutos y una fila de `api_usage` por pedido."""
        assert (await self._ask(client, key[0])).status_code == 200
        for _ in range(10):
            assert (await self._ask(client, key[0])).status_code == 200
        r = await self._ask(client, key[0])
        assert r.status_code == 429
        assert "minute" in r.json()["detail"]  # el MCP muestra "esperá un minuto"
        assert r.headers["Retry-After"] == "60"
        # El límite de las preguntas no se tocó: sigue el de la primera.
        assert cache.counters[f"rl:user:{key[1].user_id}:min"] == 1
        rows = [c.args[0] for c in repo.record_usage.await_args_list]
        # La pregunta, UNA repetición (no 10) y el 429 (que ya tenía su freno).
        assert [row.status_code for row in rows] == [200, 200, 429]
        assert rows[1].cost_usd == 0.0
        assert repo.update_last_used.await_count == 2

    async def test_a_waiter_with_no_time_left_does_not_run_it_again(
        self,
        client: AsyncClient,
        key: tuple[str, ApiKey],
        graph: FakeGraph,
        cache: FakeCache,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Antes, el que esperaba corría otro tope entero: ~2 × tope + 10 s, más
        que el timeout del MCP, y se cobraba una respuesta que nadie veía."""
        monkeypatch.setenv("PUBLIC_API_TIMEOUT_SECONDS", "0.5")
        graph.delay = 0.3
        graph.error = RuntimeError("boom")
        a, b = await asyncio.gather(self._ask(client, key[0]), self._ask(client, key[0]))
        assert sorted([a.status_code, b.status_code]) == [408, 500]
        assert len(graph.states) == 1  # el motor no volvió a correr
        assert cache.month(key[1].user_id) == 0

    async def test_a_waiter_runs_it_with_what_is_left_of_the_deadline(
        self,
        client: AsyncClient,
        key: tuple[str, ApiKey],
        graph: FakeGraph,
        cache: FakeCache,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from app.presentation.http.controllers.public_api import ask_router as ask_module

        deadlines: list[float] = []

        class _Spy(ask_module.EngineRunner):
            async def run(self, req: Any) -> Any:
                deadlines.append(req.deadline_s)
                return await super().run(req)

        monkeypatch.setattr(ask_module, "EngineRunner", _Spy)
        monkeypatch.setenv("PUBLIC_API_TIMEOUT_SECONDS", "12")
        graph.delay = 0.05
        graph.error = RuntimeError("boom")
        graph.error_once = True
        a, b = await asyncio.gather(self._ask(client, key[0]), self._ask(client, key[0]))
        assert sorted([a.status_code, b.status_code]) == [200, 500]
        assert deadlines[0] == 12
        assert 10 <= deadlines[1] < 12
        assert cache.month(key[1].user_id) == 1

    async def test_a_repeat_while_running_waits_for_the_same_run(
        self, client: AsyncClient, key: tuple[str, ApiKey], graph: FakeGraph, cache: FakeCache
    ) -> None:
        graph.delay = 0.3
        a, b = await asyncio.gather(self._ask(client, key[0]), self._ask(client, key[0]))
        assert (a.status_code, b.status_code) == (200, 200)
        assert len(graph.states) == 1
        assert {a.json()["usage"]["charged"], b.json()["usage"]["charged"]} == {True, False}
        assert cache.month(key[1].user_id) == 1

    async def test_a_failed_run_is_not_replayed(
        self, client: AsyncClient, key: tuple[str, ApiKey], graph: FakeGraph, cache: FakeCache
    ) -> None:
        graph.error = RuntimeError("boom")
        assert (await self._ask(client, key[0])).status_code == 500
        graph.error = None
        r = await self._ask(client, key[0])
        assert r.status_code == 200
        assert r.json()["usage"]["charged"] is True
        assert len(graph.states) == 2

    async def test_another_question_is_not_deduplicated(
        self, client: AsyncClient, key: tuple[str, ApiKey], graph: FakeGraph
    ) -> None:
        await self._ask(client, key[0], "desempleo 2024")
        await self._ask(client, key[0], "desempleo 2023")
        assert len(graph.states) == 2

    async def test_another_key_does_not_get_my_answer(
        self,
        client: AsyncClient,
        key: tuple[str, ApiKey],
        repo: AsyncMock,
        graph: FakeGraph,
    ) -> None:
        other_raw, other_key = _make_key()
        keys = {key[1].key_hash: key[1], other_key.key_hash: other_key}
        repo.get_by_key_hash.side_effect = lambda h: keys.get(h)
        await self._ask(client, key[0])
        r = await self._ask(client, other_raw)
        assert r.json()["usage"]["charged"] is True
        assert len(graph.states) == 2


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
