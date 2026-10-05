"""Unit tests for API key generation, hashing, and rate limiting."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from app.application.api_key_service import (
    CATALOG_MINUTE_LIMIT,
    PLAN_LIMITS,
    check_catalog_rate_limit,
    check_rate_limit,
    generate_api_key,
    global_free_daily_cap,
    hash_api_key,
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
        # Segundos que le quedan a cada clave, si el test los fija.
        self.remaining: dict[str, int] = {}
        self.down = False

    async def increment_with_ttl(self, key: str, ttl_seconds: int) -> int:
        if self.down:
            raise ConnectionError("redis down")
        self.counters[key] = self.counters.get(key, 0) + 1
        self.ttls.setdefault(key, ttl_seconds)  # EXPIRE NX
        return self.counters[key]

    async def ttl(self, key: str) -> int | None:
        return self.remaining.get(key)


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


def _free_key() -> ApiKey:
    return ApiKey(id=uuid4(), user_id=uuid4(), plan="free", is_active=True)


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
        result = await check_rate_limit(free_key, cache, client_ip="1.2.3.4")  # type: ignore[arg-type]
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
            await check_rate_limit(free_key, cache)  # type: ignore[arg-type]
        with pytest.raises(HTTPException) as exc_info:
            await check_rate_limit(free_key, cache)  # type: ignore[arg-type]
        assert exc_info.value.status_code == 429
        assert "minute" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_minute_rejection_does_not_consume_the_month(
        self, free_key: ApiKey, cache: FakeCache
    ) -> None:
        per_min = PLAN_LIMITS["free"]["per_min"]
        for _ in range(per_min):
            await check_rate_limit(free_key, cache)  # type: ignore[arg-type]
        with pytest.raises(HTTPException):
            await check_rate_limit(free_key, cache)  # type: ignore[arg-type]
        assert cache.counters[_month_key(free_key.user_id)] == per_min

    @pytest.mark.asyncio
    async def test_month_exhausted_without_credits_is_402(
        self, free_key: ApiKey, cache: FakeCache
    ) -> None:
        cache.counters[_month_key(free_key.user_id)] = 10
        with pytest.raises(HTTPException) as exc_info:
            await check_rate_limit(free_key, cache, credits=FakeCredits())  # type: ignore[arg-type]
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
            await check_rate_limit(free_key, cache, client_ip="1.1.1.1")  # type: ignore[arg-type]
        assert f"rl:global:free:day:{_today()}" not in cache.counters
        assert f"rl:ip:1.1.1.1:day:{_today()}" not in cache.counters

    @pytest.mark.asyncio
    async def test_credits_are_spent_only_after_the_month(
        self, free_key: ApiKey, cache: FakeCache
    ) -> None:
        credits = FakeCredits(preguntas=2)
        for _ in range(2):  # dentro del cupo: no toca créditos
            cache.counters.pop(f"rl:user:{free_key.user_id}:min", None)
            result = await check_rate_limit(free_key, cache, credits=credits)  # type: ignore[arg-type]
            assert result["used_credit"] is False
        assert credits.debits == []
        cache.counters[_month_key(free_key.user_id)] = 10
        cache.counters.pop(f"rl:user:{free_key.user_id}:min", None)
        result = await check_rate_limit(free_key, cache, credits=credits)  # type: ignore[arg-type]
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
            await check_rate_limit(free_key, cache, credits=credits)  # type: ignore[arg-type]
        assert exc_info.value.status_code == 503
        assert credits.saldo["preguntas"] == 1

    @pytest.mark.asyncio
    async def test_founder_gets_the_larger_allowance(
        self, free_key: ApiKey, cache: FakeCache
    ) -> None:
        until = datetime(2027, 3, 31, 23, 59, tzinfo=UTC)
        cache.counters[_month_key(free_key.user_id)] = 50
        result = await check_rate_limit(
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
            await check_rate_limit(
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
        result = await check_rate_limit(free_key, cache, credits=credits)  # type: ignore[arg-type]
        assert result["tier"] == "gratis"

    @pytest.mark.asyncio
    async def test_monthly_counter_is_per_person_and_lasts_the_month(
        self, free_key: ApiKey, cache: FakeCache
    ) -> None:
        await check_rate_limit(free_key, cache)  # type: ignore[arg-type]
        key = _month_key(free_key.user_id)
        assert str(free_key.user_id) in key and datetime.now(UTC).strftime("%Y-%m") in key
        assert cache.ttls[key] == MONTH_TTL
        # Otra clave de la misma persona comparte el cupo.
        other = ApiKey(id=uuid4(), user_id=free_key.user_id, plan="free", is_active=True)
        cache.counters.pop(f"rl:user:{free_key.user_id}:min", None)
        await check_rate_limit(other, cache)  # type: ignore[arg-type]
        assert cache.counters[key] == 2

    @pytest.mark.asyncio
    async def test_ip_limit_from_env(
        self, free_key: ApiKey, cache: FakeCache, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PUBLIC_API_IP_DAILY_LIMIT", "1")
        await check_rate_limit(free_key, cache, client_ip="9.9.9.9")  # type: ignore[arg-type]
        with pytest.raises(HTTPException) as exc_info:
            await check_rate_limit(_free_key(), cache, client_ip="9.9.9.9")  # type: ignore[arg-type]
        assert exc_info.value.status_code == 429
        # Otra IP no comparte el bucket.
        await check_rate_limit(_free_key(), cache, client_ip="8.8.8.8")  # type: ignore[arg-type]

    @pytest.mark.asyncio
    async def test_global_cap_from_env_returns_503(
        self, cache: FakeCache, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PUBLIC_API_GLOBAL_DAILY_CAP", "3")
        for _ in range(3):
            await check_rate_limit(_free_key(), cache)  # type: ignore[arg-type]
        with pytest.raises(HTTPException) as exc_info:
            await check_rate_limit(_free_key(), cache)  # type: ignore[arg-type]
        assert exc_info.value.status_code == 503

    @pytest.mark.asyncio
    async def test_global_cap_fails_closed_when_cache_is_down(
        self, free_key: ApiKey, cache: FakeCache
    ) -> None:
        cache.down = True
        with pytest.raises(HTTPException) as exc_info:
            await check_rate_limit(free_key, cache)  # type: ignore[arg-type]
        assert exc_info.value.status_code == 503

    @pytest.mark.asyncio
    async def test_paid_plan_fails_open_when_cache_is_down(self, cache: FakeCache) -> None:
        cache.down = True
        pro_key = ApiKey(id=uuid4(), user_id=uuid4(), plan="pro", is_active=True)
        result = await check_rate_limit(pro_key, cache)  # type: ignore[arg-type]
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
        result = await check_rate_limit(free_key, cache)  # type: ignore[arg-type]
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

    @pytest.mark.asyncio
    async def test_retry_after_is_what_is_left_of_the_minute(
        self, free_key: ApiKey, cache: FakeCache
    ) -> None:
        """QW10: era 60 fijo, aunque la ventana se abriera en 5 segundos."""
        user = free_key.user_id
        cache.remaining = {f"rl:user:{user}:min": 23, f"rl:user:{user}:catalog:min": 7}
        cache.counters[f"rl:user:{user}:min"] = PLAN_LIMITS["free"]["per_min"]
        cache.counters[f"rl:user:{user}:catalog:min"] = CATALOG_MINUTE_LIMIT
        with pytest.raises(HTTPException) as preguntas:
            await check_rate_limit(free_key, cache)  # type: ignore[arg-type]
        with pytest.raises(HTTPException) as datos:
            await check_catalog_rate_limit(free_key, cache)  # type: ignore[arg-type]
        assert preguntas.value.headers["Retry-After"] == "23"
        assert datos.value.headers["Retry-After"] == "7"

    @pytest.mark.asyncio
    async def test_retry_after_falls_back_to_the_whole_minute(
        self, free_key: ApiKey, cache: FakeCache
    ) -> None:
        cache.counters[f"rl:user:{free_key.user_id}:catalog:min"] = CATALOG_MINUTE_LIMIT
        with pytest.raises(HTTPException) as exc_info:
            await check_catalog_rate_limit(free_key, cache)  # type: ignore[arg-type]
        assert exc_info.value.headers["Retry-After"] == "60"


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
