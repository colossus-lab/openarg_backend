"""El cobro de `/ask` contra Redis de verdad.

Lo que un test con dobles no ve:

- que el script de devolución (``RedisCacheAdapter.decrement``) no cree la
  clave, no baje de 0 y conserve el TTL, en Redis real;
- que la reserva del cupo sea atómica con pedidos simultáneos de verdad (el
  INCR decide cuál entra cuando queda 1 sola pregunta);
- que la respuesta guardada para los reintentos sobreviva la ida y vuelta por
  Redis (JSON) y que `/ask` entero cobre una vez sola la misma pregunta;
- que el candado de la dedupe sólo lo suelte su dueño (SET NX EX con token y
  compare-and-delete en Lua);
- que el crédito se decida al cobrar con el contador de cobradas, también con
  pedidos simultáneos de verdad.

Cada test usa claves propias y las borra al terminar.
"""

from __future__ import annotations

import asyncio
import os
import random
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from dishka import Provider, Scope, make_async_container, provide
from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI, HTTPException
from httpx import ASGITransport, AsyncClient

from app.application.api_key_service import (
    MinuteWindow,
    charged_counter_key,
    reserve_question,
    settle_question,
)
from app.application.ask_dedupe import (
    lead_or_wait,
    question_fingerprint,
    release,
    store_answer,
)
from app.application.pipeline.nodes import PipelineDeps
from app.application.public_quota import monthly_counter_key
from app.domain.entities.api_key.api_key import ApiKey
from app.domain.ports.api_key.api_key_repository import IApiKeyRepository
from app.domain.ports.cache.cache_port import ICacheService
from app.domain.ports.credits.credit_repository import ICreditRepository
from app.infrastructure.adapters.cache.redis_cache_adapter import RedisCacheAdapter


@pytest.fixture
async def redis_cache() -> AsyncIterator[RedisCacheAdapter]:
    url = os.getenv("REDIS_CACHE_URL", "")
    if not url:
        pytest.skip("REDIS_CACHE_URL not set — este test necesita un Redis real")
    cache = RedisCacheAdapter(url)
    try:
        await cache._redis.ping()
    except Exception as exc:  # pragma: no cover — environmental
        pytest.skip(f"Redis unreachable: {exc}")
    yield cache
    await cache._redis.aclose()


async def _cleanup(cache: RedisCacheAdapter, *patterns: str) -> None:
    for pattern in patterns:
        async for key in cache._redis.scan_iter(match=pattern):
            await cache._redis.delete(key)


class TestDecrementScript:
    async def test_missing_key_is_not_created(self, redis_cache: RedisCacheAdapter) -> None:
        key = f"test:refund:{uuid4()}"
        assert await redis_cache.decrement(key) == 0
        assert not await redis_cache._redis.exists(key)

    async def test_refund_keeps_the_ttl_and_never_goes_below_zero(
        self, redis_cache: RedisCacheAdapter
    ) -> None:
        key = f"test:refund:{uuid4()}"
        try:
            for _ in range(2):
                await redis_cache.increment_with_ttl(key, ttl_seconds=600)
            assert await redis_cache.decrement(key) == 1
            assert 0 < await redis_cache._redis.ttl(key) <= 600
            assert await redis_cache.decrement(key) == 0
            assert await redis_cache.decrement(key) == 0
            assert await redis_cache._redis.get(key) == "0"
            assert 0 < await redis_cache._redis.ttl(key) <= 600
        finally:
            await redis_cache._redis.delete(key)


class TestLockPrimitives:
    async def test_set_if_absent_is_set_nx_with_ttl(self, redis_cache: RedisCacheAdapter) -> None:
        key = f"test:lock:{uuid4()}"
        try:
            assert await redis_cache.set_if_absent(key, "a", ttl_seconds=60) is True
            assert await redis_cache.set_if_absent(key, "b", ttl_seconds=60) is False
            assert await redis_cache._redis.get(key) == "a"
            assert 0 < await redis_cache._redis.ttl(key) <= 60
        finally:
            await redis_cache._redis.delete(key)

    async def test_delete_if_equals_only_deletes_its_own(
        self, redis_cache: RedisCacheAdapter
    ) -> None:
        key = f"test:lock:{uuid4()}"
        try:
            await redis_cache.set_if_absent(key, "mine", ttl_seconds=60)
            assert await redis_cache.delete_if_equals(key, "someone-else") is False
            assert await redis_cache._redis.get(key) == "mine"
            assert await redis_cache.delete_if_equals(key, "mine") is True
            assert not await redis_cache._redis.exists(key)
            assert await redis_cache.delete_if_equals(key, "mine") is False  # ya no está
        finally:
            await redis_cache._redis.delete(key)


class TestReservationRace:
    async def test_twenty_at_once_with_one_question_left(
        self, redis_cache: RedisCacheAdapter
    ) -> None:
        user = uuid4()
        month_key = monthly_counter_key(user, "preguntas")
        try:
            await redis_cache._redis.set(month_key, 9, ex=600)
            # Plan pro: sin tope global (es una clave compartida por fecha).
            keys = [ApiKey(id=uuid4(), user_id=user, plan="pro") for _ in range(20)]
            minute = MinuteWindow(limit=100, count=1)

            async def one(api_key: ApiKey) -> Any:
                try:
                    return await reserve_question(api_key, redis_cache, minute=minute)
                except HTTPException as exc:
                    return exc.status_code

            outcomes = await asyncio.gather(*(one(k) for k in keys))
            admitted = [o for o in outcomes if not isinstance(o, int)]
            assert len(admitted) == 1
            assert sorted(o for o in outcomes if isinstance(o, int)) == [402] * 19
            assert await redis_cache._redis.get(month_key) == "10"

            # El que entró no se cobra: la pregunta vuelve.
            info = await settle_question(admitted[0], redis_cache, charge=False)
            assert info["remaining_month"] == 1
            assert await redis_cache._redis.get(month_key) == "9"
        finally:
            await redis_cache._redis.delete(month_key)

    async def test_a_freed_slot_is_not_paid_with_a_credit(
        self, redis_cache: RedisCacheAdapter
    ) -> None:
        """9 de 10 usadas: A reserva el 10 y B el 11; B termina antes y A da
        timeout. B ocupa el lugar 10: no gasta crédito."""
        user = uuid4()
        month_key = monthly_counter_key(user, "preguntas")
        charged_key = charged_counter_key(user)
        credits = AsyncMock(spec=ICreditRepository)
        credits.get_active_supporter.return_value = None
        credits.balance.return_value = {"preguntas": 3, "datos": 0}
        credits.debit.return_value = True
        try:
            await redis_cache._redis.set(month_key, 9, ex=600)
            await redis_cache._redis.set(charged_key, 9, ex=600)
            minute = MinuteWindow(limit=100, count=1)
            key = ApiKey(id=uuid4(), user_id=user, plan="pro")
            first, second = await asyncio.gather(
                reserve_question(key, redis_cache, credits=credits, minute=minute),
                reserve_question(key, redis_cache, credits=credits, minute=minute),
            )
            # Cuál reservó el 11 lo decide Redis: ése es B.
            a, b = (first, second) if second.needs_credit else (second, first)
            assert not a.needs_credit and b.needs_credit
            info = await settle_question(b, redis_cache, credits, charge=True)
            await settle_question(a, redis_cache, credits, charge=False)  # timeout
            assert info["used_credit"] is False
            credits.debit.assert_not_awaited()
            assert await redis_cache._redis.get(month_key) == "10"
            assert await redis_cache._redis.get(charged_key) == "10"
        finally:
            await redis_cache._redis.delete(month_key, charged_key)

    async def test_twenty_answers_at_once_past_the_month_spend_exact_credits(
        self, redis_cache: RedisCacheAdapter
    ) -> None:
        """Con 8 de 10 cobradas, 20 respuestas simultáneas: 2 entran en el mes
        y exactamente 18 gastan crédito (antes la cuenta dependía del orden)."""
        user = uuid4()
        month_key = monthly_counter_key(user, "preguntas")
        charged_key = charged_counter_key(user)
        credits = AsyncMock(spec=ICreditRepository)
        credits.get_active_supporter.return_value = None
        credits.balance.return_value = {"preguntas": 100, "datos": 0}
        credits.debit.return_value = True
        try:
            await redis_cache._redis.set(month_key, 8, ex=600)
            await redis_cache._redis.set(charged_key, 8, ex=600)
            minute = MinuteWindow(limit=100, count=1)
            key = ApiKey(id=uuid4(), user_id=user, plan="pro")
            reservations = await asyncio.gather(
                *(
                    reserve_question(key, redis_cache, credits=credits, minute=minute)
                    for _ in range(20)
                )
            )
            infos = await asyncio.gather(
                *(settle_question(r, redis_cache, credits, charge=True) for r in reservations)
            )
            assert sum(i["used_credit"] for i in infos) == 18
            assert credits.debit.await_count == 18
            assert await redis_cache._redis.get(charged_key) == "28"
        finally:
            await redis_cache._redis.delete(month_key, charged_key)


class TestUnbilledRunsCap:
    """H108 contra Redis de verdad: el tope diario de corridas no cobradas."""

    @staticmethod
    def _credits() -> AsyncMock:
        # Saldo de sobra: que el cupo del mes (402) no se meta en la cuenta.
        credits = AsyncMock(spec=ICreditRepository)
        credits.get_active_supporter.return_value = None
        credits.balance.return_value = {"preguntas": 100, "datos": 0}
        credits.debit.return_value = True
        return credits

    async def test_twenty_five_at_once_admit_exactly_the_cap(
        self, redis_cache: RedisCacheAdapter
    ) -> None:
        user = uuid4()
        runs_key = f"rl:user:{user}:runs:day:{datetime.now(UTC):%Y-%m-%d}"
        month_key = monthly_counter_key(user, "preguntas")
        credits = self._credits()
        try:
            minute = MinuteWindow(limit=100, count=1)
            # Plan pro: sin tope global (es una clave compartida por fecha).
            key = ApiKey(id=uuid4(), user_id=user, plan="pro")

            async def one() -> Any:
                try:
                    return await reserve_question(key, redis_cache, credits=credits, minute=minute)
                except HTTPException as exc:
                    return exc

            outcomes = await asyncio.gather(*(one() for _ in range(25)))
            admitted = [o for o in outcomes if not isinstance(o, HTTPException)]
            rejected = [o for o in outcomes if isinstance(o, HTTPException)]
            assert len(admitted) == 20
            assert [e.status_code for e in rejected] == [429] * 5
            assert all(0 < int((e.headers or {})["Retry-After"]) <= 86400 for e in rejected)
            assert await redis_cache._redis.get(runs_key) == "20"
            assert 0 < await redis_cache._redis.ttl(runs_key) <= 172800
            # Los rechazados devolvieron la reserva del mes.
            assert await redis_cache._redis.get(month_key) == "20"

            # Las 20 terminan sin cobrarse (p. ej. timeouts): quedan contadas.
            await asyncio.gather(
                *(settle_question(r, redis_cache, credits, charge=False) for r in admitted)
            )
            assert await redis_cache._redis.get(runs_key) == "20"
            assert await redis_cache._redis.get(month_key) == "0"
            assert isinstance(await one(), HTTPException)
            assert await redis_cache._redis.get(runs_key) == "20"
        finally:
            await _cleanup(redis_cache, f"rl:user:{user}:*")

    async def test_a_charged_answer_gives_the_run_back(
        self, redis_cache: RedisCacheAdapter
    ) -> None:
        user = uuid4()
        runs_key = f"rl:user:{user}:runs:day:{datetime.now(UTC):%Y-%m-%d}"
        credits = self._credits()
        try:
            minute = MinuteWindow(limit=100, count=1)
            key = ApiKey(id=uuid4(), user_id=user, plan="pro")
            reservation = await reserve_question(key, redis_cache, credits=credits, minute=minute)
            assert await redis_cache._redis.get(runs_key) == "1"  # en curso
            await settle_question(reservation, redis_cache, credits, charge=True)
            assert await redis_cache._redis.get(runs_key) == "0"
        finally:
            await _cleanup(redis_cache, f"rl:user:{user}:*")


class TestDedupeLock:
    async def test_one_leads_and_the_other_gets_its_answer(
        self, redis_cache: RedisCacheAdapter
    ) -> None:
        fp = f"test{uuid4().hex}"
        try:
            leader = await lead_or_wait(redis_cache, fp, lock_ttl=60, wait_s=1)
            assert leader.leader

            async def finish() -> None:
                await asyncio.sleep(0.1)
                await store_answer(redis_cache, fp, {"answer": "7,6 %", "sources": [{"a": 1}]})
                await release(redis_cache, fp, leader.token)

            waiter, _ = await asyncio.gather(
                lead_or_wait(redis_cache, fp, lock_ttl=60, wait_s=3, poll_s=0.02), finish()
            )
            assert not waiter.leader
            assert waiter.answer == {"answer": "7,6 %", "sources": [{"a": 1}]}
            assert not await redis_cache._redis.exists(f"ask:dedupe:{fp}:lock")
        finally:
            await _cleanup(redis_cache, f"ask:dedupe:{fp}:*")

    async def test_an_expired_lock_taken_by_another_is_not_freed(
        self, redis_cache: RedisCacheAdapter
    ) -> None:
        fp = f"test{uuid4().hex}"
        lock = f"ask:dedupe:{fp}:lock"
        try:
            a = await lead_or_wait(redis_cache, fp, lock_ttl=1, wait_s=1)
            await asyncio.sleep(1.2)  # venció en Redis de verdad
            assert not await redis_cache._redis.exists(lock)
            c = await lead_or_wait(redis_cache, fp, lock_ttl=60, wait_s=1)
            assert c.leader
            await release(redis_cache, fp, a.token)  # A termina tarde
            assert await redis_cache._redis.get(lock) == c.token
        finally:
            await _cleanup(redis_cache, f"ask:dedupe:{fp}:*")


class _Graph:
    """Un grafo compilado con checkpointer que tarda un poco."""

    def __init__(self) -> None:
        self.runs = 0

    async def astream(
        self, state: dict[str, Any], config: Any = None, stream_mode: Any = None
    ) -> AsyncIterator[tuple[str, dict[str, Any]]]:
        self.runs += 1
        await asyncio.sleep(0.3)
        yield (
            "updates",
            {
                "finalize": {
                    "clean_answer": "La desocupación fue 7,6 %.",
                    "sources": [{"name": "EPH", "url": "https://datos.gob.ar/x"}],
                    "chart_data": [{"periodo": "2025-T2", "valor": 7.6}],
                    "tokens_used": 1234,
                }
            },
        )


class TestAskEndToEnd:
    async def test_a_retry_storm_is_charged_once(
        self, redis_cache: RedisCacheAdapter, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """El patrón de n8n: la misma pregunta mientras corre y después de terminar."""
        from app.application.answers import runner as runner_module
        from app.application.api_key_service import generate_api_key
        from app.presentation.http.controllers.public_api.ask_router import router
        from app.presentation.http.controllers.query import smart_query_v2_router as smart

        graph = _Graph()

        async def _checkpointer() -> object:
            return object()

        async def _compile(deps: Any, checkpointer: Any) -> _Graph:
            return graph

        async def _record(**kwargs: Any) -> None:
            return None

        monkeypatch.setattr(smart, "_get_checkpointer", _checkpointer)
        monkeypatch.setattr(smart, "_get_or_compile_graph", _compile)
        monkeypatch.setattr(runner_module, "record_terminal_analytics", _record)

        raw, key_hash = generate_api_key()
        # Plan pro: sin tope global (es una clave compartida por fecha).
        api_key = ApiKey(id=uuid4(), user_id=uuid4(), key_hash=key_hash, plan="pro")
        repo = AsyncMock(spec=IApiKeyRepository)
        repo.get_by_key_hash.return_value = api_key
        credits = AsyncMock(spec=ICreditRepository)
        credits.get_active_supporter.return_value = None
        credits.balance.return_value = {"preguntas": 0, "datos": 0}

        class _Provider(Provider):
            scope = Scope.REQUEST

            @provide
            def deps(self) -> PipelineDeps:
                return MagicMock()

            @provide
            def cache(self) -> ICacheService:
                return redis_cache

            @provide
            def api_key_repo(self) -> IApiKeyRepository:
                return repo

            @provide
            def credit_repo(self) -> ICreditRepository:
                return credits

        app = FastAPI()
        app.include_router(router)
        setup_dishka(container=make_async_container(_Provider()), app=app)
        user = api_key.user_id
        # IP propia: el contador por IP del día es una clave real de Redis.
        ip = f"10.{random.randrange(256)}.{random.randrange(256)}.{random.randrange(1, 255)}"
        fp = question_fingerprint(api_key.id, "¿Desempleo?")
        transport = ASGITransport(app=app, client=(ip, 4321))
        try:
            async with AsyncClient(transport=transport, base_url="http://t") as c:

                async def ask() -> Any:
                    return await c.post(
                        "/ask",
                        json={"question": "¿Desempleo?"},
                        headers={"Authorization": f"Bearer {raw}"},
                    )

                first, second = await asyncio.gather(ask(), ask())
                third = await ask()
            assert [r.status_code for r in (first, second, third)] == [200, 200, 200]
            assert graph.runs == 1
            assert [r.json()["usage"]["charged"] for r in (first, second, third)].count(True) == 1
            assert third.json()["chart_data"] == [{"periodo": "2025-T2", "valor": 7.6}]
            assert third.json()["usage"]["requests_remaining_month"] == 9
            assert await redis_cache._redis.get(monthly_counter_key(user, "preguntas")) == "1"
            # El tercero no contó para el límite por minuto de las preguntas,
            # sino para el de las repeticiones.
            assert await redis_cache._redis.get(f"rl:user:{user}:min") == "2"
            assert await redis_cache._redis.get(f"rl:user:{user}:replay:min") == "1"
        finally:
            await _cleanup(redis_cache, f"rl:user:{user}:*", f"rl:ip:{ip}:*", f"ask:dedupe:{fp}:*")
