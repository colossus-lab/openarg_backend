"""Cupo mensual del chat web: 30 por mes (Fundador 100), créditos compartidos
con el MCP y sólo descuentan las respuestas completas."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest

from app.application.answers.engine import EngineResult
from app.application.public_quota import monthly_counter_key
from app.application.web_quota import (
    DAILY_CAP_REACHED,
    QUOTA_EXHAUSTED,
    consume_web_question,
    counts_against_quota,
    exhausted_message,
    web_counter_key,
    web_global_key,
    web_quota,
)
from app.domain.entities.credits.credits import Supporter
from app.presentation.http.controllers.query.smart_query_v2_router import (
    _web_quota_after,
    _web_quota_gate,
)


class FakeCache:
    def __init__(self) -> None:
        self.counters: dict[str, int] = {}
        self.down = False

    async def get(self, key: str) -> int | None:
        if self.down:
            raise ConnectionError("redis down")
        return self.counters.get(key)

    async def increment_with_ttl(self, key: str, ttl_seconds: int) -> int:
        if self.down:
            raise ConnectionError("redis down")
        self.counters[key] = self.counters.get(key, 0) + 1
        return self.counters[key]


class FakeCredits:
    def __init__(self, preguntas: int = 0, founder: bool = False) -> None:
        self.saldo = {"preguntas": preguntas, "datos": 0}
        self.founder = founder

    async def get_active_supporter(self, user_id):  # type: ignore[no-untyped-def]
        if not self.founder:
            return None
        return Supporter(
            user_id=user_id,
            nivel="fundador",
            desde=datetime(2026, 9, 30, tzinfo=UTC),
            hasta=None,
            origen="admin",
        )

    async def debit(self, user_id, tipo):  # type: ignore[no-untyped-def]
        if self.saldo[tipo] <= 0:
            return False
        self.saldo[tipo] -= 1
        return True

    async def balance(self, user_id):  # type: ignore[no-untyped-def]
        return dict(self.saldo)


class FakeUsers:
    def __init__(self, user_id) -> None:  # type: ignore[no-untyped-def]
        self.user = SimpleNamespace(id=user_id, email="ana@example.com")

    async def get_by_email(self, email: str):  # type: ignore[no-untyped-def]
        return self.user if email == self.user.email else None


def _answer(intent: str = "agent", answer: str = "La inflación fue **2,1 %**.") -> EngineResult:
    return EngineResult(answer=answer, intent=intent)


@pytest.fixture(autouse=True)
def _limits(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "PUBLIC_WEB_MONTHLY_PREGUNTAS",
        "PUBLIC_WEB_FOUNDER_PREGUNTAS",
        "PUBLIC_WEB_GLOBAL_DAILY_CAP",
    ):
        monkeypatch.delenv(name, raising=False)


# ── qué cuenta ─────────────────────────────────────────────


@pytest.mark.parametrize(
    "intent", ["casual", "meta", "educational", "clarification", "injection_blocked", "off_topic"]
)
def test_saludos_aclaraciones_y_bloqueos_no_descuentan(intent: str) -> None:
    assert not counts_against_quota(_answer(intent))


def test_una_respuesta_vacia_no_descuenta() -> None:
    assert not counts_against_quota(_answer(answer="  "))


@pytest.mark.parametrize("intent", ["agent", "cached", ""])
def test_una_respuesta_completa_descuenta(intent: str) -> None:
    assert counts_against_quota(_answer(intent))


# ── cupo y créditos ────────────────────────────────────────


async def test_gratis_tiene_30_por_mes() -> None:
    uid = uuid4()
    quota = await web_quota(uid, FakeCache(), FakeCredits())
    assert (quota.limite, quota.restantes, quota.fundador) == (30, 30, False)


async def test_fundador_tiene_100_por_mes() -> None:
    quota = await web_quota(uuid4(), FakeCache(), FakeCredits(founder=True))
    assert (quota.limite, quota.fundador) == (100, True)
    assert quota.to_wire()["fundador"] == {"hasta": None}


async def test_el_limite_se_ajusta_por_entorno(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PUBLIC_WEB_MONTHLY_PREGUNTAS", "50")
    assert (await web_quota(uuid4(), FakeCache(), FakeCredits())).limite == 50


async def test_descuenta_del_mes_mientras_queda() -> None:
    uid, cache, credits = uuid4(), FakeCache(), FakeCredits(preguntas=5)
    after = await consume_web_question(uid, cache, credits)
    assert after.usadas == 1
    assert cache.counters[web_counter_key(uid)] == 1
    assert credits.saldo["preguntas"] == 5


async def test_con_el_mes_agotado_gasta_un_credito() -> None:
    uid, cache, credits = uuid4(), FakeCache(), FakeCredits(preguntas=2)
    cache.counters[web_counter_key(uid)] = 30
    after = await consume_web_question(uid, cache, credits)
    assert credits.saldo["preguntas"] == 1
    assert after.creditos == 1
    assert cache.counters[web_counter_key(uid)] == 30


async def test_web_y_mcp_cuentan_aparte_pero_comparten_creditos() -> None:
    uid, cache, credits = uuid4(), FakeCache(), FakeCredits(preguntas=3)
    # El MCP ya usó sus 10 del mes: la web arranca de cero.
    cache.counters[monthly_counter_key(uid, "preguntas")] = 10
    quota = await web_quota(uid, cache, credits)
    assert quota.restantes == 30
    # Y el saldo que ve la web es el mismo que gasta el MCP.
    await credits.debit(uid, "preguntas")
    assert (await web_quota(uid, cache, credits)).creditos == 2


async def test_cada_respuesta_suma_al_tope_diario_de_la_web() -> None:
    uid, cache = uuid4(), FakeCache()
    await consume_web_question(uid, cache, FakeCredits())
    assert cache.counters[web_global_key()] == 1


async def test_con_redis_caido_no_bloquea() -> None:
    uid, cache = uuid4(), FakeCache()
    cache.down = True
    quota = await web_quota(uid, cache, FakeCredits())
    assert not quota.agotado


# ── la puerta del router ───────────────────────────────────


async def test_sin_cupo_ni_creditos_rechaza_con_el_estado() -> None:
    uid, cache = uuid4(), FakeCache()
    cache.counters[web_counter_key(uid)] = 30
    user_id, rejection = await _web_quota_gate(
        "ana@example.com", FakeUsers(uid), cache, FakeCredits()
    )
    assert user_id == uid
    assert rejection is not None
    assert rejection["code"] == QUOTA_EXHAUSTED
    assert rejection["quota"]["restantes"] == 0
    assert rejection["message"].startswith("Usaste tus 30 preguntas de este mes.")


async def test_sin_cupo_pero_con_creditos_deja_pasar() -> None:
    uid, cache = uuid4(), FakeCache()
    cache.counters[web_counter_key(uid)] = 30
    _, rejection = await _web_quota_gate(
        "ana@example.com", FakeUsers(uid), cache, FakeCredits(preguntas=1)
    )
    assert rejection is None


async def test_el_tope_diario_de_la_web_rechaza_sin_tocar_el_mes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PUBLIC_WEB_GLOBAL_DAILY_CAP", "2")
    uid, cache = uuid4(), FakeCache()
    cache.counters[web_global_key()] = 2
    _, rejection = await _web_quota_gate("ana@example.com", FakeUsers(uid), cache, FakeCredits())
    assert rejection is not None and rejection["code"] == DAILY_CAP_REACHED
    assert web_counter_key(uid) not in cache.counters


async def test_sin_usuario_conocido_no_aplica_cupo() -> None:
    user_id, rejection = await _web_quota_gate("", FakeUsers(uuid4()), FakeCache(), FakeCredits())
    assert (user_id, rejection) == (None, None)


async def test_despues_de_una_aclaracion_informa_el_cupo_sin_descontar() -> None:
    uid, cache = uuid4(), FakeCache()
    wire = await _web_quota_after(_answer("clarification"), uid, cache, FakeCredits())
    assert wire["usadas"] == 0
    assert web_counter_key(uid) not in cache.counters


async def test_despues_de_una_respuesta_informa_el_cupo_descontado() -> None:
    uid, cache = uuid4(), FakeCache()
    wire = await _web_quota_after(_answer(), uid, cache, FakeCredits())
    assert (wire["usadas"], wire["restantes"], wire["limite"]) == (1, 29, 30)


def test_el_mensaje_dice_cuando_se_renueva() -> None:
    from app.application.web_quota import WebQuota

    message = exhausted_message(WebQuota(usadas=30, limite=30, creditos=0, fundador=False))
    assert "Se renuevan el 1 de " in message


# ── el motor viejo también marca sus respuestas rápidas ────


@pytest.mark.parametrize("classification", ["casual", "meta", "educational"])
def test_una_respuesta_rapida_del_motor_viejo_no_descuenta(classification: str) -> None:
    from app.application.answers.legacy_engine import result_from_state

    # Lo que deja `fast_reply_node`: `plan_intent` vacío y la clasificación.
    state = {
        "clean_answer": "¡Hola! ¿Qué dato público buscás?",
        "plan_intent": "",
        "classification": classification,
    }
    assert not counts_against_quota(result_from_state(state))


def test_una_respuesta_con_datos_del_motor_viejo_descuenta() -> None:
    from app.application.answers.legacy_engine import result_from_state

    state = {
        "clean_answer": "La inflación fue 1,66 %.",
        "plan_intent": "series",
        "classification": None,
    }
    assert counts_against_quota(result_from_state(state))
