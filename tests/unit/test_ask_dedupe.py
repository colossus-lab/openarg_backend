"""La misma pregunta repetida enseguida en `/ask` (ver `app.application.ask_dedupe`)."""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

import pytest

from app.application.ask_dedupe import (
    ANSWER_TTL_SECONDS,
    cached_answer,
    lead_or_wait,
    normalize_question,
    question_fingerprint,
    release,
    store_answer,
)


class FakeCache:
    def __init__(self) -> None:
        self.counters: dict[str, int] = {}
        self.values: dict[str, Any] = {}
        self.ttls: dict[str, int] = {}
        self.down = False
        # Un parpadeo: las próximas N llamadas a Redis fallan.
        self.flaky = 0

    def _check(self) -> None:
        if self.flaky > 0:
            self.flaky -= 1
            raise ConnectionError("redis blip")
        if self.down:
            raise ConnectionError("redis down")

    async def increment_with_ttl(self, key: str, ttl_seconds: int) -> int:
        self._check()
        self.counters[key] = self.counters.get(key, 0) + 1
        self.ttls.setdefault(key, ttl_seconds)
        return self.counters[key]

    async def get(self, key: str) -> Any:
        self._check()
        return self.values.get(key)

    async def set(self, key: str, value: Any, ttl_seconds: int = 3600) -> None:
        self._check()
        self.values[key] = value
        self.ttls[key] = ttl_seconds

    async def delete(self, key: str) -> None:
        self._check()
        self.counters.pop(key, None)
        self.values.pop(key, None)
        self.ttls.pop(key, None)

    async def set_if_absent(self, key: str, value: str, ttl_seconds: int) -> bool:
        self._check()
        if key in self.values:
            return False
        self.values[key] = value
        self.ttls[key] = ttl_seconds
        return True

    async def delete_if_equals(self, key: str, value: str) -> bool:
        self._check()
        if self.values.get(key) != value:
            return False
        del self.values[key]
        self.ttls.pop(key, None)
        return True


_LOCK = "ask:dedupe:fp:lock"


class TestFingerprint:
    def test_retries_of_the_same_question_match(self) -> None:
        key = uuid4()
        variants = ["¿Cuál es el desempleo?", "cuál es el desempleo", "  CUÁL  es el\tdesempleo?? "]
        assert len({question_fingerprint(key, q) for q in variants}) == 1

    def test_accents_and_words_still_matter(self) -> None:
        """Conservadora: busca el reintento, no preguntas parecidas."""
        assert normalize_question("año 2024") != normalize_question("ano 2024")
        assert normalize_question("desempleo 2024") != normalize_question("desempleo 2023")

    def test_each_key_has_its_own(self) -> None:
        assert question_fingerprint(uuid4(), "x") != question_fingerprint(uuid4(), "x")

    def test_the_question_is_not_in_the_fingerprint(self) -> None:
        fp = question_fingerprint(uuid4(), "desempleo en Rosario")
        assert "rosario" not in fp and len(fp) == 40


class TestLeadOrWait:
    async def test_the_first_one_leads(self) -> None:
        cache = FakeCache()
        turn = await lead_or_wait(cache, "fp", lock_ttl=60, wait_s=1)  # type: ignore[arg-type]
        assert turn.leader and turn.answer is None and not turn.waited
        assert cache.values[_LOCK] == turn.token
        assert cache.ttls[_LOCK] == 60

    async def test_a_second_one_waits_and_gets_the_answer(self) -> None:
        cache = FakeCache()
        leader = await lead_or_wait(cache, "fp", lock_ttl=60, wait_s=1)  # type: ignore[arg-type]

        async def leader_finishes() -> None:
            await asyncio.sleep(0.05)
            await store_answer(cache, "fp", {"answer": "7,6 %"})  # type: ignore[arg-type]
            await release(cache, "fp", leader.token)  # type: ignore[arg-type]

        turn, _ = await asyncio.gather(
            lead_or_wait(cache, "fp", lock_ttl=60, wait_s=2, poll_s=0.01),  # type: ignore[arg-type]
            leader_finishes(),
        )
        assert not turn.leader and turn.waited
        assert turn.answer == {"answer": "7,6 %"}
        assert cache.ttls["ask:dedupe:fp:answer"] == ANSWER_TTL_SECONDS

    async def test_if_the_leader_fails_the_waiter_runs_it(self) -> None:
        cache = FakeCache()
        leader = await lead_or_wait(cache, "fp", lock_ttl=60, wait_s=1)  # type: ignore[arg-type]

        async def leader_fails() -> None:
            await asyncio.sleep(0.05)
            await release(cache, "fp", leader.token)  # type: ignore[arg-type]  # sin respuesta

        turn, _ = await asyncio.gather(
            lead_or_wait(cache, "fp", lock_ttl=60, wait_s=2, poll_s=0.01),  # type: ignore[arg-type]
            leader_fails(),
        )
        assert turn.leader and turn.waited  # esperó: corre con lo que le queda
        assert turn.token is not None and turn.token != leader.token

    async def test_answer_stored_just_before_taking_the_lock_is_used(self) -> None:
        """El anterior guardó y soltó entre la lectura y el INCR: no se corre dos veces."""
        cache = FakeCache()
        await store_answer(cache, "fp", {"answer": "ya está"})  # type: ignore[arg-type]
        turn = await lead_or_wait(cache, "fp", lock_ttl=60, wait_s=1)  # type: ignore[arg-type]
        assert not turn.leader and turn.answer == {"answer": "ya está"}
        assert _LOCK not in cache.values

    async def test_gives_up_after_the_wait(self) -> None:
        cache = FakeCache()
        await lead_or_wait(cache, "fp", lock_ttl=60, wait_s=1)  # type: ignore[arg-type]
        turn = await lead_or_wait(cache, "fp", lock_ttl=60, wait_s=0.05, poll_s=0.01)  # type: ignore[arg-type]
        assert turn.gave_up

    async def test_without_redis_every_request_runs(self) -> None:
        cache = FakeCache()
        cache.down = True
        turn = await lead_or_wait(cache, "fp", lock_ttl=60, wait_s=1)  # type: ignore[arg-type]
        assert turn.leader and turn.token is None
        assert await cached_answer(cache, "fp") is None  # type: ignore[arg-type]
        await store_answer(cache, "fp", {"answer": "x"})  # type: ignore[arg-type]  # no rompe
        await release(cache, "fp", turn.token)  # type: ignore[arg-type]


class TestTheLockIsOnlyReleasedByItsOwner:
    """Antes el candado era un INCR y se soltaba con un DEL sin mirar de quién
    era: un pedido podía borrárselo al que estaba corriendo, y la misma
    pregunta corría (y se cobraba) dos veces."""

    async def test_a_request_that_ran_on_a_redis_blip_does_not_free_the_lock(self) -> None:
        cache = FakeCache()
        a = await lead_or_wait(cache, "fp", lock_ttl=60, wait_s=1)  # type: ignore[arg-type]
        cache.flaky = 1  # el candado de B falla: B corre igual, sin candado
        b = await lead_or_wait(cache, "fp", lock_ttl=60, wait_s=1)  # type: ignore[arg-type]
        assert b.leader and b.token is None
        await release(cache, "fp", b.token)  # type: ignore[arg-type]  # B termina
        assert cache.values[_LOCK] == a.token  # el de A sigue
        # C llega mientras A corre: espera, no vuelve a correr la pregunta.
        c = await lead_or_wait(cache, "fp", lock_ttl=60, wait_s=0.05, poll_s=0.01)  # type: ignore[arg-type]
        assert c.gave_up

    async def test_an_expired_lock_taken_by_another_is_not_freed(self) -> None:
        cache = FakeCache()
        a = await lead_or_wait(cache, "fp", lock_ttl=60, wait_s=1)  # type: ignore[arg-type]
        del cache.values[_LOCK]  # venció: A tardó más que el tope (base lenta)
        c = await lead_or_wait(cache, "fp", lock_ttl=60, wait_s=1)  # type: ignore[arg-type]
        assert c.leader
        await release(cache, "fp", a.token)  # type: ignore[arg-type]  # A termina tarde
        assert cache.values[_LOCK] == c.token
        await release(cache, "fp", c.token)  # type: ignore[arg-type]
        assert _LOCK not in cache.values


@pytest.mark.parametrize("stored", ["texto suelto", 3, ["lista"]])
async def test_only_a_dict_is_a_stored_answer(stored: Any) -> None:
    cache = FakeCache()
    cache.values["ask:dedupe:fp:answer"] = stored
    assert await cached_answer(cache, "fp") is None  # type: ignore[arg-type]
