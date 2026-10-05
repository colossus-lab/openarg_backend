"""Unit tests for API key generation, hashing, and rate limiting."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from app.application.answers.engine import EngineResult
from app.application.api_key_service import (
    CATALOG_MINUTE_LIMIT,
    DAILY_CAPACITY_DETAIL,
    PLAN_LIMITS,
    QUOTA_SERVICE_DOWN_DETAIL,
    REPLAY_PER_MIN,
    answer_is_billable,
    answer_used_model,
    charged_counter_key,
    check_catalog_rate_limit,
    check_replay_rate,
    generate_api_key,
    global_free_daily_cap,
    hash_api_key,
    replay_per_min,
    reserve_question,
    settle_question,
    verify_api_key,
)
from app.application.public_quota import (
    MONTH_TTL,
    first_of_next_month_utc,
    free_tier,
    monthly_counter_key,
    seconds_until_next_month,
)
from app.domain.entities.api_key.api_key import ApiKey
from app.domain.entities.credits.credits import Supporter

# ── Key generation ───────────────────────────────────────────


class TestGenerateApiKey:
    def test_format(self) -> None:
        raw_key, key_hash = generate_api_key()
        assert raw_key.startswith("oarg_sk_")
        assert len(raw_key) > 30

    def test_hash_is_hex(self) -> None:
        _, key_hash = generate_api_key()
        assert len(key_hash) == 64  # SHA-256 hex
        int(key_hash, 16)  # Should not raise

    def test_unique_keys(self) -> None:
        keys = {generate_api_key()[0] for _ in range(10)}
        assert len(keys) == 10

    def test_hash_matches(self) -> None:
        raw_key, key_hash = generate_api_key()
        assert hash_api_key(raw_key) == key_hash

    def test_different_keys_different_hashes(self) -> None:
        _, h1 = generate_api_key()
        _, h2 = generate_api_key()
        assert h1 != h2


# ── Key verification ─────────────────────────────────────────


class TestVerifyApiKey:
    @pytest.fixture
    def mock_repo(self) -> AsyncMock:
        return AsyncMock()

    @pytest.fixture
    def valid_key(self) -> tuple[str, ApiKey]:
        raw_key, key_hash = generate_api_key()
        api_key = ApiKey(
            id=uuid4(),
            user_id=uuid4(),
            key_hash=key_hash,
            key_prefix=raw_key[:16],
            name="test",
            plan="free",
            is_active=True,
        )
        return raw_key, api_key

    @pytest.mark.asyncio
    async def test_valid_key(self, mock_repo: AsyncMock, valid_key: tuple) -> None:
        raw_key, api_key = valid_key
        mock_repo.get_by_key_hash.return_value = api_key
        result = await verify_api_key(raw_key, mock_repo)
        assert result.id == api_key.id

    @pytest.mark.asyncio
    async def test_wrong_prefix_rejected(self, mock_repo: AsyncMock) -> None:
        from fastapi import HTTPException

        with pytest.raises(HTTPException) as exc_info:
            await verify_api_key("wrong_prefix_abc123", mock_repo)
        assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_unknown_key_rejected(self, mock_repo: AsyncMock) -> None:
        from fastapi import HTTPException

        mock_repo.get_by_key_hash.return_value = None
        with pytest.raises(HTTPException) as exc_info:
            await verify_api_key("oarg_sk_nonexistent123456789012345678901234", mock_repo)
        assert exc_info.value.status_code == 401

    @pytest.mark.asyncio
    async def test_inactive_key_rejected(self, mock_repo: AsyncMock, valid_key: tuple) -> None:
        from fastapi import HTTPException

        raw_key, api_key = valid_key
        api_key.is_active = False
        mock_repo.get_by_key_hash.return_value = api_key
        with pytest.raises(HTTPException) as exc_info:
            await verify_api_key(raw_key, mock_repo)
        assert exc_info.value.status_code == 401

    # test_expired_key_rejected was deleted 2026-04-11 along with the
    # api_keys.expires_at column (Alembic 0030). It was testing a
    # behavior that never fired in production — the column was read
    # but never written, so every real key had expires_at=NULL and
    # the validation short-circuited on the None guard. See
    # specs/008-developers-keys/[DEBT-003].


# ── Rate limiting ────────────────────────────────────────────


class FakeCache:
    """Contadores en memoria con la semántica de `increment_with_ttl`."""

    def __init__(self) -> None:
        self.counters: dict[str, int] = {}
        self.ttls: dict[str, int] = {}
        self.down = False

    async def increment_with_ttl(self, key: str, ttl_seconds: int) -> int:
        if self.down:
            raise ConnectionError("redis down")
        self.counters[key] = self.counters.get(key, 0) + 1
        self.ttls.setdefault(key, ttl_seconds)  # EXPIRE NX
        return self.counters[key]

    async def decrement(self, key: str) -> int:
        """Como el script de Redis: no crea la clave ni baja de 0."""
        if self.down:
            raise ConnectionError("redis down")
        if self.counters.get(key, 0) <= 0:
            return self.counters.get(key, 0)
        self.counters[key] -= 1
        return self.counters[key]

    async def get(self, key: str) -> int | None:
        if self.down:
            raise ConnectionError("redis down")
        return self.counters.get(key)

    async def exists(self, key: str) -> bool:
        if self.down:
            raise ConnectionError("redis down")
        return key in self.counters

    async def set_if_absent(self, key: str, value: str, ttl_seconds: int) -> bool:
        if self.down:
            raise ConnectionError("redis down")
        if key in self.counters:
            return False
        self.counters[key] = int(value)
        self.ttls[key] = ttl_seconds
        return True


class FakeCredits:
    """`ICreditRepository` en memoria: Fundador opcional y saldo por tipo."""

    def __init__(
        self,
        founder_until: datetime | None = None,
        founder: bool = False,
        preguntas: int = 0,
        datos: int = 0,
    ) -> None:
        self.founder = founder or founder_until is not None
        self.founder_until = founder_until
        self.saldo = {"preguntas": preguntas, "datos": datos}
        self.debits: list[str] = []
        self.broken = False

    async def get_active_supporter(self, user_id):  # type: ignore[no-untyped-def]
        if self.broken:
            raise RuntimeError("db down")
        if not self.founder:
            return None
        return Supporter(
            user_id=user_id,
            nivel="fundador",
            desde=datetime(2026, 9, 30, tzinfo=UTC),
            hasta=self.founder_until,
            origen="test",
        )

    async def debit(self, user_id, tipo):  # type: ignore[no-untyped-def]
        if self.broken:
            raise RuntimeError("db down")
        if self.saldo[tipo] <= 0:
            return False
        self.saldo[tipo] -= 1
        self.debits.append(tipo)
        return True

    async def balance(self, user_id):  # type: ignore[no-untyped-def]
        if self.broken:
            raise RuntimeError("db down")
        return dict(self.saldo)


def _today() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")


def _month_key(user_id: object, tipo: str = "preguntas") -> str:
    return monthly_counter_key(user_id, tipo)  # type: ignore[arg-type]


def _month_used(cache: FakeCache, user_id: object, n: int) -> None:
    """El mes con `n` preguntas cobradas: reservas y cobradas en `n`."""
    cache.counters[_month_key(user_id)] = n
    cache.counters[charged_counter_key(user_id)] = n


def _free_key() -> ApiKey:
    return ApiKey(id=uuid4(), user_id=uuid4(), plan="free", is_active=True)


async def _admit(
    api_key: ApiKey,
    cache: FakeCache,
    client_ip: str = "",
    credits: FakeCredits | None = None,
) -> dict:
    """Una pregunta que pasa los controles y termina cobrada: reservar + cobrar.

    Los tests de los controles (orden, topes, 402/429/503) usan esto; los del
    cobro al terminar usan `reserve_question` y `settle_question` por separado.
    """
    reservation = await reserve_question(
        api_key,
        cache,  # type: ignore[arg-type]
        client_ip=client_ip,
        credits=credits,  # type: ignore[arg-type]
    )
    return await settle_question(reservation, cache, credits, charge=True)  # type: ignore[arg-type]


@pytest.fixture(autouse=True)
def _default_quotas(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "PUBLIC_API_MONTHLY_PREGUNTAS",
        "PUBLIC_API_MONTHLY_DATOS",
        "PUBLIC_API_FOUNDER_PREGUNTAS",
        "PUBLIC_API_FOUNDER_DATOS",
        "PUBLIC_API_GLOBAL_DAILY_CAP",
        "PUBLIC_API_IP_DAILY_LIMIT",
    ):
        monkeypatch.delenv(name, raising=False)


class TestRateLimit:
    @pytest.fixture
    def free_key(self) -> ApiKey:
        return _free_key()

    @pytest.fixture
    def cache(self) -> FakeCache:
        return FakeCache()

    @pytest.mark.asyncio
    async def test_first_request_allowed(self, free_key: ApiKey, cache: FakeCache) -> None:
        result = await _admit(free_key, cache, client_ip="1.2.3.4")  # type: ignore[arg-type]
        assert result["remaining_minute"] == PLAN_LIMITS["free"]["per_min"] - 1
        assert result["remaining_month"] == 9
        assert result["limit_month"] == 10
        assert result["tier"] == "gratis"
        # Nombres viejos, con el valor del mes, para no romper integraciones.
        assert result["remaining_day"] == 9 and result["limit_day"] == 10

    def test_free_allowance_is_ten_questions_and_200_data_per_month(self) -> None:
        tier = free_tier()
        assert (tier.preguntas, tier.datos) == (10, 200)

    @pytest.mark.asyncio
    async def test_minute_limit_exceeded(self, free_key: ApiKey, cache: FakeCache) -> None:
        for _ in range(PLAN_LIMITS["free"]["per_min"]):
            await _admit(free_key, cache)  # type: ignore[arg-type]
        with pytest.raises(HTTPException) as exc_info:
            await _admit(free_key, cache)  # type: ignore[arg-type]
        assert exc_info.value.status_code == 429
        assert "minute" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_minute_rejection_does_not_consume_the_month(
        self, free_key: ApiKey, cache: FakeCache
    ) -> None:
        per_min = PLAN_LIMITS["free"]["per_min"]
        for _ in range(per_min):
            await _admit(free_key, cache)  # type: ignore[arg-type]
        with pytest.raises(HTTPException):
            await _admit(free_key, cache)  # type: ignore[arg-type]
        assert cache.counters[_month_key(free_key.user_id)] == per_min

    @pytest.mark.asyncio
    async def test_month_exhausted_without_credits_is_402(
        self, free_key: ApiKey, cache: FakeCache
    ) -> None:
        cache.counters[_month_key(free_key.user_id)] = 10
        with pytest.raises(HTTPException) as exc_info:
            await _admit(free_key, cache, credits=FakeCredits())  # type: ignore[arg-type]
        assert exc_info.value.status_code == 402
        assert exc_info.value.detail == "Monthly quota exceeded: 10 questions per month"
        assert exc_info.value.headers["X-Quota-Reset"].endswith("+00:00")
        assert int(exc_info.value.headers["Retry-After"]) > 0

    @pytest.mark.asyncio
    async def test_402_does_not_touch_the_global_cap(
        self, free_key: ApiKey, cache: FakeCache
    ) -> None:
        """Si no, cada reintento de alguien sin cupo le comería lugar a los demás."""
        cache.counters[_month_key(free_key.user_id)] = 10
        with pytest.raises(HTTPException):
            await _admit(free_key, cache, client_ip="1.1.1.1")  # type: ignore[arg-type]
        assert f"rl:global:free:day:{_today()}" not in cache.counters
        assert f"rl:ip:1.1.1.1:day:{_today()}" not in cache.counters

    @pytest.mark.asyncio
    async def test_credits_are_spent_only_after_the_month(
        self, free_key: ApiKey, cache: FakeCache
    ) -> None:
        credits = FakeCredits(preguntas=2)
        for _ in range(2):  # dentro del cupo: no toca créditos
            cache.counters.pop(f"rl:user:{free_key.user_id}:min", None)
            result = await _admit(free_key, cache, credits=credits)  # type: ignore[arg-type]
            assert result["used_credit"] is False
        assert credits.debits == []
        _month_used(cache, free_key.user_id, 10)
        cache.counters.pop(f"rl:user:{free_key.user_id}:min", None)
        result = await _admit(free_key, cache, credits=credits)  # type: ignore[arg-type]
        assert result["used_credit"] is True
        assert credits.saldo["preguntas"] == 1

    @pytest.mark.asyncio
    async def test_credit_is_not_burnt_when_the_global_cap_rejects(
        self, free_key: ApiKey, cache: FakeCache, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PUBLIC_API_GLOBAL_DAILY_CAP", "1")
        cache.counters[f"rl:global:free:day:{_today()}"] = 1
        cache.counters[_month_key(free_key.user_id)] = 10
        credits = FakeCredits(preguntas=1)
        with pytest.raises(HTTPException) as exc_info:
            await _admit(free_key, cache, credits=credits)  # type: ignore[arg-type]
        assert exc_info.value.status_code == 503
        assert credits.saldo["preguntas"] == 1

    @pytest.mark.asyncio
    async def test_founder_gets_the_larger_allowance(
        self, free_key: ApiKey, cache: FakeCache
    ) -> None:
        until = datetime(2027, 3, 31, 23, 59, tzinfo=UTC)
        cache.counters[_month_key(free_key.user_id)] = 50
        result = await _admit(
            free_key,
            cache,
            credits=FakeCredits(founder_until=until),  # type: ignore[arg-type]
        )
        assert result["tier"] == "fundador"
        assert result["limit_month"] == 100
        assert result["remaining_month"] == 49
        assert result["founder_until"] == until.isoformat()

    @pytest.mark.asyncio
    async def test_founder_quota_in_the_402(self, free_key: ApiKey, cache: FakeCache) -> None:
        cache.counters[_month_key(free_key.user_id)] = 100
        with pytest.raises(HTTPException) as exc_info:
            await _admit(
                free_key,
                cache,
                credits=FakeCredits(founder=True),  # type: ignore[arg-type]
            )
        assert exc_info.value.detail == "Monthly quota exceeded: 100 questions per month"

    @pytest.mark.asyncio
    async def test_supporter_lookup_failure_falls_back_to_free(
        self, free_key: ApiKey, cache: FakeCache
    ) -> None:
        credits = FakeCredits(founder=True)
        credits.broken = True
        result = await _admit(free_key, cache, credits=credits)  # type: ignore[arg-type]
        assert result["tier"] == "gratis"

    @pytest.mark.asyncio
    async def test_monthly_counter_is_per_person_and_lasts_the_month(
        self, free_key: ApiKey, cache: FakeCache
    ) -> None:
        await _admit(free_key, cache)  # type: ignore[arg-type]
        key = _month_key(free_key.user_id)
        assert str(free_key.user_id) in key and datetime.now(UTC).strftime("%Y-%m") in key
        assert cache.ttls[key] == MONTH_TTL
        # Otra clave de la misma persona comparte el cupo.
        other = ApiKey(id=uuid4(), user_id=free_key.user_id, plan="free", is_active=True)
        cache.counters.pop(f"rl:user:{free_key.user_id}:min", None)
        await _admit(other, cache)  # type: ignore[arg-type]
        assert cache.counters[key] == 2

    @pytest.mark.asyncio
    async def test_ip_limit_from_env(
        self, free_key: ApiKey, cache: FakeCache, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PUBLIC_API_IP_DAILY_LIMIT", "1")
        await _admit(free_key, cache, client_ip="9.9.9.9")  # type: ignore[arg-type]
        with pytest.raises(HTTPException) as exc_info:
            await _admit(_free_key(), cache, client_ip="9.9.9.9")  # type: ignore[arg-type]
        assert exc_info.value.status_code == 429
        # Otra IP no comparte el bucket.
        await _admit(_free_key(), cache, client_ip="8.8.8.8")  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_global_cap_from_env_returns_503(
        self, cache: FakeCache, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PUBLIC_API_GLOBAL_DAILY_CAP", "3")
        for _ in range(3):
            await _admit(_free_key(), cache)  # type: ignore[arg-type]
        with pytest.raises(HTTPException) as exc_info:
            await _admit(_free_key(), cache)  # type: ignore[arg-type]
        assert exc_info.value.status_code == 503

    @pytest.mark.asyncio
    async def test_global_cap_fails_closed_when_cache_is_down(
        self, free_key: ApiKey, cache: FakeCache
    ) -> None:
        cache.down = True
        with pytest.raises(HTTPException) as exc_info:
            await _admit(free_key, cache)  # type: ignore[arg-type]
        assert exc_info.value.status_code == 503

    @pytest.mark.asyncio
    async def test_paid_plan_fails_open_when_cache_is_down(self, cache: FakeCache) -> None:
        cache.down = True
        pro_key = ApiKey(id=uuid4(), user_id=uuid4(), plan="pro", is_active=True)
        result = await _admit(pro_key, cache)  # type: ignore[arg-type]
        assert result["remaining_month"] == 10

    def test_invalid_env_falls_back_to_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PUBLIC_API_GLOBAL_DAILY_CAP", "muchas")
        assert global_free_daily_cap() == 300
        monkeypatch.setenv("PUBLIC_API_GLOBAL_DAILY_CAP", "-5")
        assert global_free_daily_cap() == 300
        monkeypatch.setenv("PUBLIC_API_GLOBAL_DAILY_CAP", "450")
        assert global_free_daily_cap() == 450

    def test_monthly_allowances_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PUBLIC_API_MONTHLY_PREGUNTAS", "3")
        monkeypatch.setenv("PUBLIC_API_MONTHLY_DATOS", "nada")
        assert (free_tier().preguntas, free_tier().datos) == (3, 200)

    @pytest.mark.asyncio
    async def test_catalog_limit_is_separate_from_questions(
        self, free_key: ApiKey, cache: FakeCache, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PUBLIC_API_MONTHLY_DATOS", "3")
        for _ in range(3):
            await check_catalog_rate_limit(free_key, cache)  # type: ignore[arg-type]
        with pytest.raises(HTTPException) as exc_info:
            await check_catalog_rate_limit(free_key, cache)  # type: ignore[arg-type]
        assert exc_info.value.status_code == 402
        assert exc_info.value.detail == "Monthly quota exceeded: 3 catalog requests per month"
        # Las preguntas no se tocaron.
        result = await _admit(free_key, cache)  # type: ignore[arg-type]
        assert result["remaining_month"] == 9

    @pytest.mark.asyncio
    async def test_catalog_spends_data_credits_after_the_month(
        self, free_key: ApiKey, cache: FakeCache
    ) -> None:
        cache.counters[_month_key(free_key.user_id, "datos")] = 200
        credits = FakeCredits(datos=1, preguntas=5)
        await check_catalog_rate_limit(free_key, cache, credits)  # type: ignore[arg-type]
        assert credits.debits == ["datos"]
        with pytest.raises(HTTPException) as exc_info:
            await check_catalog_rate_limit(free_key, cache, credits)  # type: ignore[arg-type]
        assert exc_info.value.status_code == 402
        assert credits.saldo["preguntas"] == 5  # nunca usa créditos del otro tipo

    @pytest.mark.asyncio
    async def test_catalog_has_a_per_minute_limit(self, free_key: ApiKey, cache: FakeCache) -> None:
        for _ in range(CATALOG_MINUTE_LIMIT):
            await check_catalog_rate_limit(free_key, cache)  # type: ignore[arg-type]
        with pytest.raises(HTTPException) as exc_info:
            await check_catalog_rate_limit(free_key, cache)  # type: ignore[arg-type]
        assert exc_info.value.status_code == 429
        assert "per minute" in exc_info.value.detail


class TestChargeOnlyWhenAnswered:
    """La pregunta se reserva al entrar y se cobra sólo si la respuesta cuenta.

    Antes `check_rate_limit` hacía el INCR del mes al entrar y nada lo
    devolvía: un timeout, un error, un saludo o un acierto del caché costaban
    1 de 10.
    """

    @pytest.fixture
    def free_key(self) -> ApiKey:
        return _free_key()

    @pytest.fixture
    def cache(self) -> FakeCache:
        return FakeCache()

    async def _reserve(self, key: ApiKey, cache: FakeCache, **kw: object):  # type: ignore[no-untyped-def]
        return await reserve_question(key, cache, **kw)  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_not_charged_gives_the_question_back(
        self, free_key: ApiKey, cache: FakeCache
    ) -> None:
        reservation = await self._reserve(free_key, cache)
        assert cache.counters[_month_key(free_key.user_id)] == 1  # reservada
        info = await settle_question(reservation, cache, charge=False)  # type: ignore[arg-type]
        assert cache.counters[_month_key(free_key.user_id)] == 0
        assert info["remaining_month"] == 10
        assert info["remaining_day"] == 10  # el nombre viejo, mismo valor
        assert info["used_credit"] is False

    @pytest.mark.asyncio
    async def test_charged_keeps_the_reservation(self, free_key: ApiKey, cache: FakeCache) -> None:
        reservation = await self._reserve(free_key, cache)
        info = await settle_question(reservation, cache, charge=True)  # type: ignore[arg-type]
        assert cache.counters[_month_key(free_key.user_id)] == 1
        assert info["remaining_month"] == 9

    @pytest.mark.asyncio
    async def test_settle_is_idempotent(self, free_key: ApiKey, cache: FakeCache) -> None:
        reservation = await self._reserve(free_key, cache)
        await settle_question(reservation, cache, charge=False)  # type: ignore[arg-type]
        await settle_question(reservation, cache, charge=False)  # type: ignore[arg-type]
        assert cache.counters[_month_key(free_key.user_id)] == 0
        assert reservation.settled

    @pytest.mark.asyncio
    async def test_ip_rejection_gives_the_month_back(
        self, cache: FakeCache, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Antes el 429 por IP saltaba después del INCR del mes y lo perdía."""
        monkeypatch.setenv("PUBLIC_API_IP_DAILY_LIMIT", "1")
        await _admit(_free_key(), cache, client_ip="9.9.9.9")
        other = _free_key()
        with pytest.raises(HTTPException) as exc_info:
            await self._reserve(other, cache, client_ip="9.9.9.9")
        assert exc_info.value.status_code == 429
        assert cache.counters[_month_key(other.user_id)] == 0
        assert cache.counters[f"rl:ip:9.9.9.9:day:{_today()}"] == 1

    @pytest.mark.asyncio
    async def test_global_cap_rejection_gives_month_and_ip_back(
        self, free_key: ApiKey, cache: FakeCache, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PUBLIC_API_GLOBAL_DAILY_CAP", "1")
        cache.counters[f"rl:global:free:day:{_today()}"] = 1
        with pytest.raises(HTTPException) as exc_info:
            await self._reserve(free_key, cache, client_ip="7.7.7.7")
        assert exc_info.value.status_code == 503
        assert exc_info.value.detail == DAILY_CAPACITY_DETAIL
        assert cache.counters[_month_key(free_key.user_id)] == 0
        assert cache.counters[f"rl:ip:7.7.7.7:day:{_today()}"] == 0
        assert cache.counters[f"rl:global:free:day:{_today()}"] == 1

    @pytest.mark.asyncio
    async def test_402_gives_the_reservation_back(self, free_key: ApiKey, cache: FakeCache) -> None:
        cache.counters[_month_key(free_key.user_id)] = 10
        with pytest.raises(HTTPException) as exc_info:
            await self._reserve(free_key, cache, credits=FakeCredits())
        assert exc_info.value.status_code == 402
        # Antes cada reintento lo subía (11, 12, …): el contador era de intentos.
        assert cache.counters[_month_key(free_key.user_id)] == 10

    @pytest.mark.asyncio
    async def test_cache_down_503_is_not_the_daily_cap(
        self, free_key: ApiKey, cache: FakeCache
    ) -> None:
        cache.down = True
        with pytest.raises(HTTPException) as exc_info:
            await self._reserve(free_key, cache)
        assert exc_info.value.status_code == 503
        assert exc_info.value.detail == QUOTA_SERVICE_DOWN_DETAIL
        assert exc_info.value.headers["Retry-After"] == "300"

    def test_the_two_503_details_are_told_apart_by_the_mcp(self) -> None:
        """El MCP distingue los dos 503 por el `detail` (ver mcp_publico/core.py)."""
        from mcp_publico import core

        down = core.error_message(503, QUOTA_SERVICE_DOWN_DETAIL)
        cap = core.error_message(503, DAILY_CAPACITY_DETAIL)
        assert "no responde" in down and "agotado" not in down
        assert "agotado" in cap

    @pytest.mark.asyncio
    async def test_two_at_once_with_one_question_left(
        self, free_key: ApiKey, cache: FakeCache
    ) -> None:
        """La carrera: con 1 restante, el INCR atómico deja entrar a uno solo."""
        cache.counters[_month_key(free_key.user_id)] = 9
        other_key = ApiKey(id=uuid4(), user_id=free_key.user_id, plan="free", is_active=True)
        first = await self._reserve(free_key, cache, credits=FakeCredits())
        with pytest.raises(HTTPException) as exc_info:
            await self._reserve(other_key, cache, credits=FakeCredits())
        assert exc_info.value.status_code == 402
        assert cache.counters[_month_key(free_key.user_id)] == 10
        # El que entró termina sin cobrarse (p. ej. un timeout): la pregunta
        # vuelve a estar disponible para el próximo pedido.
        await settle_question(first, cache, charge=False)  # type: ignore[arg-type]
        assert cache.counters[_month_key(free_key.user_id)] == 9
        cache.counters.pop(f"rl:user:{free_key.user_id}:min", None)
        again = await self._reserve(other_key, cache, credits=FakeCredits())
        assert not again.needs_credit

    @pytest.mark.asyncio
    async def test_credit_is_spent_only_when_charged(
        self, free_key: ApiKey, cache: FakeCache
    ) -> None:
        cache.counters[_month_key(free_key.user_id)] = 10
        credits = FakeCredits(preguntas=1)
        reservation = await self._reserve(free_key, cache, credits=credits)
        assert reservation.needs_credit
        assert credits.saldo["preguntas"] == 1  # al entrar sólo se verifica
        await settle_question(reservation, cache, credits, charge=False)  # type: ignore[arg-type]
        assert credits.saldo["preguntas"] == 1
        assert cache.counters[_month_key(free_key.user_id)] == 10

        cache.counters.pop(f"rl:user:{free_key.user_id}:min", None)
        reservation = await self._reserve(free_key, cache, credits=credits)
        info = await settle_question(reservation, cache, credits, charge=True)  # type: ignore[arg-type]
        assert info["used_credit"] is True
        assert credits.saldo["preguntas"] == 0

    @pytest.mark.asyncio
    async def test_last_credit_race_is_a_bounded_overdraft(
        self, free_key: ApiKey, cache: FakeCache
    ) -> None:
        """Con 1 crédito, dos pedidos simultáneos entran; el segundo débito falla
        y esa respuesta sale sin cobrarse (documentado en api_key_service)."""
        cache.counters[_month_key(free_key.user_id)] = 10
        credits = FakeCredits(preguntas=1)
        a = await self._reserve(free_key, cache, credits=credits)
        b = await self._reserve(free_key, cache, credits=credits)
        info_a = await settle_question(a, cache, credits, charge=True)  # type: ignore[arg-type]
        info_b = await settle_question(b, cache, credits, charge=True)  # type: ignore[arg-type]
        assert (info_a["used_credit"], info_b["used_credit"]) == (True, False)
        assert credits.saldo["preguntas"] == 0

    @pytest.mark.asyncio
    async def test_turn_without_model_gives_ip_and_global_back(
        self, free_key: ApiKey, cache: FakeCache
    ) -> None:
        reservation = await self._reserve(free_key, cache, client_ip="5.5.5.5")
        await settle_question(reservation, cache, charge=False, used_model=False)  # type: ignore[arg-type]
        assert cache.counters[f"rl:ip:5.5.5.5:day:{_today()}"] == 0
        assert cache.counters[f"rl:global:free:day:{_today()}"] == 0

    @pytest.mark.asyncio
    async def test_timeout_keeps_ip_and_global_counted(
        self, free_key: ApiKey, cache: FakeCache
    ) -> None:
        """El modelo corrió y Bedrock cobró: el techo de gasto lo cuenta igual."""
        reservation = await self._reserve(free_key, cache, client_ip="5.5.5.5")
        await settle_question(reservation, cache, charge=False, used_model=True)  # type: ignore[arg-type]
        assert cache.counters[_month_key(free_key.user_id)] == 0
        assert cache.counters[f"rl:ip:5.5.5.5:day:{_today()}"] == 1
        assert cache.counters[f"rl:global:free:day:{_today()}"] == 1

    @pytest.mark.asyncio
    async def test_minute_is_never_given_back(self, free_key: ApiKey, cache: FakeCache) -> None:
        reservation = await self._reserve(free_key, cache)
        await settle_question(reservation, cache, charge=False, used_model=False)  # type: ignore[arg-type]
        assert cache.counters[f"rl:user:{free_key.user_id}:min"] == 1


class TestCreditIsDecidedWhenCharged:
    """Si una respuesta gasta un crédito se decide al cobrar, con el contador
    de respuestas cobradas del mes, no al reservar.

    Antes `needs_credit` se decidía al reservar, contando reservas en curso
    que después podían devolverse: con 9 de 10 usadas, A reservaba el 10 y B
    el 11; si A terminaba en timeout, B gastaba un crédito igual aunque el
    lugar 10 había quedado libre (cobro doble: lugar del mes más crédito).
    """

    def _pro(self, user_id: object) -> ApiKey:
        # Plan pro: el límite por minuto (30) no se mete en estas carreras.
        return ApiKey(id=uuid4(), user_id=user_id, plan="pro", is_active=True)  # type: ignore[arg-type]

    async def _reserve(self, key: ApiKey, cache: FakeCache, credits: FakeCredits):  # type: ignore[no-untyped-def]
        return await reserve_question(key, cache, credits=credits)  # type: ignore[arg-type]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("b_finishes_first", [True, False], ids=["B-antes", "A-antes"])
    async def test_a_freed_slot_is_not_paid_with_a_credit(self, b_finishes_first: bool) -> None:
        cache, user, credits = FakeCache(), uuid4(), FakeCredits(preguntas=3)
        _month_used(cache, user, 9)
        a = await self._reserve(self._pro(user), cache, credits)
        b = await self._reserve(self._pro(user), cache, credits)
        assert not a.needs_credit and b.needs_credit  # B pasó el control de saldo
        if b_finishes_first:  # lo típico: A es la lenta que termina en timeout
            info = await settle_question(b, cache, credits, charge=True)  # type: ignore[arg-type]
            await settle_question(a, cache, credits, charge=False)  # type: ignore[arg-type]
        else:
            await settle_question(a, cache, credits, charge=False)  # type: ignore[arg-type]
            info = await settle_question(b, cache, credits, charge=True)  # type: ignore[arg-type]
        assert info["used_credit"] is False
        assert credits.saldo["preguntas"] == 3
        assert cache.counters[_month_key(user)] == 10
        assert cache.counters[charged_counter_key(user)] == 10

    @pytest.mark.asyncio
    @pytest.mark.parametrize("b_finishes_first", [True, False], ids=["B-antes", "A-antes"])
    async def test_both_answered_spend_exactly_one_credit(self, b_finishes_first: bool) -> None:
        cache, user, credits = FakeCache(), uuid4(), FakeCredits(preguntas=3)
        _month_used(cache, user, 9)
        a = await self._reserve(self._pro(user), cache, credits)
        b = await self._reserve(self._pro(user), cache, credits)
        order = (b, a) if b_finishes_first else (a, b)
        used = [
            (await settle_question(r, cache, credits, charge=True))["used_credit"]  # type: ignore[arg-type]
            for r in order
        ]
        # El último en cobrarse es la respuesta 11: ésa paga el crédito.
        assert used == [False, True]
        assert credits.saldo["preguntas"] == 2

    @pytest.mark.asyncio
    async def test_the_month_of_the_deploy_starts_from_the_reservations(self) -> None:
        """El mes del despliegue: las reservas de antes ya estaban cobradas."""
        cache, user, credits = FakeCache(), uuid4(), FakeCredits(preguntas=1)
        cache.counters[_month_key(user)] = 10  # sin contador de cobradas
        r = await self._reserve(self._pro(user), cache, credits)
        info = await settle_question(r, cache, credits, charge=True)  # type: ignore[arg-type]
        assert info["used_credit"] is True
        assert cache.counters[charged_counter_key(user)] == 11

    @pytest.mark.asyncio
    async def test_a_new_month_starts_the_charged_count_at_zero(self) -> None:
        cache, user, credits = FakeCache(), uuid4(), FakeCredits()
        r = await self._reserve(self._pro(user), cache, credits)
        assert cache.counters[charged_counter_key(user)] == 0  # sembrado al reservar
        assert cache.ttls[charged_counter_key(user)] == MONTH_TTL
        await settle_question(r, cache, credits, charge=True)  # type: ignore[arg-type]
        assert cache.counters[charged_counter_key(user)] == 1

    @pytest.mark.asyncio
    async def test_a_refund_never_touches_the_charged_count(self) -> None:
        cache, user, credits = FakeCache(), uuid4(), FakeCredits()
        _month_used(cache, user, 3)
        r = await self._reserve(self._pro(user), cache, credits)
        await settle_question(r, cache, credits, charge=False)  # type: ignore[arg-type]
        assert cache.counters[charged_counter_key(user)] == 3

    @pytest.mark.asyncio
    async def test_without_redis_at_the_end_the_entry_decision_stands(self) -> None:
        cache, user, credits = FakeCache(), uuid4(), FakeCredits(preguntas=1)
        _month_used(cache, user, 10)
        r = await self._reserve(self._pro(user), cache, credits)
        cache.down = True
        info = await settle_question(r, cache, credits, charge=True)  # type: ignore[arg-type]
        assert info["used_credit"] is True


class TestReplayRate:
    """Las repeticiones de una pregunta ya respondida tienen su propio límite."""

    def test_never_below_the_plan_limit(self) -> None:
        free = ApiKey(id=uuid4(), user_id=uuid4(), plan="free", is_active=True)
        pro = ApiKey(id=uuid4(), user_id=uuid4(), plan="pro", is_active=True)
        assert replay_per_min(free) == REPLAY_PER_MIN == 10
        assert replay_per_min(pro) == PLAN_LIMITS["pro"]["per_min"]

    @pytest.mark.asyncio
    async def test_the_eleventh_repeat_in_a_minute_is_429(self) -> None:
        cache, key = FakeCache(), _free_key()
        for _ in range(10):
            await check_replay_rate(key, cache)  # type: ignore[arg-type]
        with pytest.raises(HTTPException) as exc_info:
            await check_replay_rate(key, cache)  # type: ignore[arg-type]
        assert exc_info.value.status_code == 429
        assert "minute" in exc_info.value.detail
        # El de las preguntas no se toca.
        assert f"rl:user:{key.user_id}:min" not in cache.counters
        assert cache.ttls[f"rl:user:{key.user_id}:replay:min"] == 60

    @pytest.mark.asyncio
    async def test_fails_open_without_redis(self) -> None:
        cache, key = FakeCache(), _free_key()
        cache.down = True
        await check_replay_rate(key, cache)  # type: ignore[arg-type]


class TestBillable:
    """Qué respuesta de `/ask` descuenta: el criterio de la web más el caché."""

    @pytest.mark.parametrize("intent", ["", "agent", "consulta_datos"])
    def test_a_complete_model_answer_is_billable(self, intent: str) -> None:
        result = EngineResult(answer="La desocupación fue 7,6 %.", intent=intent)
        assert answer_is_billable(result)
        assert answer_used_model(result)

    @pytest.mark.parametrize(
        "intent",
        ["cached", "casual", "meta", "educational", "clarification", "injection_blocked"],
    )
    def test_these_are_not_billable(self, intent: str) -> None:
        assert not answer_is_billable(EngineResult(answer="algo", intent=intent))

    def test_an_empty_answer_is_not_billable(self) -> None:
        assert not answer_is_billable(EngineResult(answer="  ", intent="agent"))

    def test_clarification_used_the_model_but_cache_and_greetings_did_not(self) -> None:
        assert answer_used_model(EngineResult(answer="¿De qué año?", intent="clarification"))
        assert not answer_used_model(EngineResult(answer="x", intent="cached"))
        assert not answer_used_model(EngineResult(answer="¡Hola!", intent="casual"))


class TestMonthHelpers:
    def test_renewal_is_the_first_of_next_month_utc(self) -> None:
        assert first_of_next_month_utc(datetime(2026, 10, 31, 23, 59, tzinfo=UTC)) == datetime(
            2026, 11, 1, tzinfo=UTC
        )
        assert first_of_next_month_utc(datetime(2026, 12, 15, tzinfo=UTC)) == datetime(
            2027, 1, 1, tzinfo=UTC
        )

    def test_month_key_changes_at_midnight_utc(self) -> None:
        user = uuid4()
        before = monthly_counter_key(user, "preguntas", datetime(2026, 10, 31, 23, 59, tzinfo=UTC))
        after = monthly_counter_key(user, "preguntas", datetime(2026, 11, 1, 0, 0, tzinfo=UTC))
        assert before.endswith("2026-10") and after.endswith("2026-11")

    def test_questions_and_data_are_counted_apart(self) -> None:
        user = uuid4()
        assert monthly_counter_key(user, "preguntas") != monthly_counter_key(user, "datos")

    def test_seconds_until_next_month(self) -> None:
        assert seconds_until_next_month(datetime(2026, 10, 31, 23, 59, 0, tzinfo=UTC)) == 60


# ── Plan limits ──────────────────────────────────────────────


class TestPlanLimits:
    def test_all_plans_defined(self) -> None:
        assert "free" in PLAN_LIMITS
        assert "basic" in PLAN_LIMITS
        assert "pro" in PLAN_LIMITS

    def test_free_most_restrictive_per_minute(self) -> None:
        assert PLAN_LIMITS["free"]["per_min"] < PLAN_LIMITS["basic"]["per_min"]
        assert PLAN_LIMITS["pro"]["per_min"] > PLAN_LIMITS["basic"]["per_min"]
