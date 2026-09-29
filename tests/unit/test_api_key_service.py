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
    catalog_daily_limit,
    check_catalog_rate_limit,
    check_rate_limit,
    generate_api_key,
    global_free_daily_cap,
    hash_api_key,
    verify_api_key,
)
from app.domain.entities.api_key.api_key import ApiKey

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


def _today() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%d")


def _free_key() -> ApiKey:
    return ApiKey(id=uuid4(), user_id=uuid4(), plan="free", is_active=True)


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
        assert result["remaining_day"] == PLAN_LIMITS["free"]["per_day"] - 1

    def test_free_plan_is_ten_per_day(self) -> None:
        assert PLAN_LIMITS["free"]["per_day"] == 10

    @pytest.mark.asyncio
    async def test_minute_limit_exceeded(self, free_key: ApiKey, cache: FakeCache) -> None:
        for _ in range(PLAN_LIMITS["free"]["per_min"]):
            await check_rate_limit(free_key, cache)  # type: ignore[arg-type]
        with pytest.raises(HTTPException) as exc_info:
            await check_rate_limit(free_key, cache)  # type: ignore[arg-type]
        assert exc_info.value.status_code == 429
        assert "minute" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_minute_rejection_does_not_consume_the_day(
        self, free_key: ApiKey, cache: FakeCache
    ) -> None:
        per_min = PLAN_LIMITS["free"]["per_min"]
        for _ in range(per_min):
            await check_rate_limit(free_key, cache)  # type: ignore[arg-type]
        with pytest.raises(HTTPException):
            await check_rate_limit(free_key, cache)  # type: ignore[arg-type]
        assert cache.counters[f"rl:user:{free_key.user_id}:day:{_today()}"] == per_min

    @pytest.mark.asyncio
    async def test_day_limit_exceeded(self, free_key: ApiKey, cache: FakeCache) -> None:
        cache.counters[f"rl:user:{free_key.user_id}:day:{_today()}"] = PLAN_LIMITS["free"][
            "per_day"
        ]
        with pytest.raises(HTTPException) as exc_info:
            await check_rate_limit(free_key, cache)  # type: ignore[arg-type]
        assert exc_info.value.status_code == 429
        assert "day" in exc_info.value.detail
        assert 0 < int(exc_info.value.headers["Retry-After"]) <= 86400

    @pytest.mark.asyncio
    async def test_day_rejection_does_not_touch_the_global_cap(
        self, free_key: ApiKey, cache: FakeCache
    ) -> None:
        cache.counters[f"rl:user:{free_key.user_id}:day:{_today()}"] = PLAN_LIMITS["free"][
            "per_day"
        ]
        with pytest.raises(HTTPException):
            await check_rate_limit(free_key, cache)  # type: ignore[arg-type]
        assert f"rl:global:free:day:{_today()}" not in cache.counters

    @pytest.mark.asyncio
    async def test_daily_counters_carry_the_utc_date_and_do_not_renew_ttl(
        self, free_key: ApiKey, cache: FakeCache
    ) -> None:
        await check_rate_limit(free_key, cache)  # type: ignore[arg-type]
        await check_rate_limit(free_key, cache)  # type: ignore[arg-type]
        user_day = f"rl:user:{free_key.user_id}:day:{_today()}"
        assert cache.counters[user_day] == 2
        assert cache.counters[f"rl:global:free:day:{_today()}"] == 2
        assert cache.ttls[user_day] == 172800

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
        assert result["remaining_day"] == PLAN_LIMITS["pro"]["per_day"]

    def test_invalid_env_falls_back_to_default(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PUBLIC_API_GLOBAL_DAILY_CAP", "muchas")
        assert global_free_daily_cap() == 300
        monkeypatch.setenv("PUBLIC_API_GLOBAL_DAILY_CAP", "-5")
        assert global_free_daily_cap() == 300
        monkeypatch.setenv("PUBLIC_API_GLOBAL_DAILY_CAP", "450")
        assert global_free_daily_cap() == 450

    @pytest.mark.asyncio
    async def test_pro_plan_higher_limits(self, cache: FakeCache) -> None:
        pro_key = ApiKey(id=uuid4(), user_id=uuid4(), plan="pro", is_active=True)
        result = await check_rate_limit(pro_key, cache)  # type: ignore[arg-type]
        assert result["limit_minute"] == PLAN_LIMITS["pro"]["per_min"]
        assert result["limit_day"] == PLAN_LIMITS["pro"]["per_day"]

    @pytest.mark.asyncio
    async def test_catalog_limit_is_separate_from_questions(
        self, free_key: ApiKey, cache: FakeCache, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PUBLIC_API_CATALOG_DAILY_LIMIT", "3")
        for _ in range(3):
            await check_catalog_rate_limit(free_key, cache)  # type: ignore[arg-type]
        with pytest.raises(HTTPException) as exc_info:
            await check_catalog_rate_limit(free_key, cache)  # type: ignore[arg-type]
        assert exc_info.value.status_code == 429
        assert "per day" in exc_info.value.detail
        # Las preguntas no se tocaron.
        result = await check_rate_limit(free_key, cache)  # type: ignore[arg-type]
        assert result["remaining_day"] == PLAN_LIMITS["free"]["per_day"] - 1

    @pytest.mark.asyncio
    async def test_catalog_has_a_per_minute_limit(self, free_key: ApiKey, cache: FakeCache) -> None:
        for _ in range(CATALOG_MINUTE_LIMIT):
            await check_catalog_rate_limit(free_key, cache)  # type: ignore[arg-type]
        with pytest.raises(HTTPException) as exc_info:
            await check_catalog_rate_limit(free_key, cache)  # type: ignore[arg-type]
        assert "per minute" in exc_info.value.detail

    def test_catalog_daily_default_is_200(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("PUBLIC_API_CATALOG_DAILY_LIMIT", raising=False)
        assert catalog_daily_limit() == 200


# ── Plan limits ──────────────────────────────────────────────


class TestPlanLimits:
    def test_all_plans_defined(self) -> None:
        assert "free" in PLAN_LIMITS
        assert "basic" in PLAN_LIMITS
        assert "pro" in PLAN_LIMITS

    def test_free_most_restrictive(self) -> None:
        assert PLAN_LIMITS["free"]["per_min"] < PLAN_LIMITS["basic"]["per_min"]
        assert PLAN_LIMITS["free"]["per_day"] < PLAN_LIMITS["basic"]["per_day"]

    def test_pro_most_generous(self) -> None:
        assert PLAN_LIMITS["pro"]["per_min"] > PLAN_LIMITS["basic"]["per_min"]
        assert PLAN_LIMITS["pro"]["per_day"] > PLAN_LIMITS["basic"]["per_day"]
