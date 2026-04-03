"""Unit tests for API key generation, hashing, and rate limiting."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from app.application.api_key_service import (
    PLAN_LIMITS,
    check_rate_limit,
    generate_api_key,
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
        assert "revoked" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_expired_key_rejected(self, mock_repo: AsyncMock, valid_key: tuple) -> None:
        from fastapi import HTTPException

        raw_key, api_key = valid_key
        api_key.expires_at = datetime.now(UTC) - timedelta(hours=1)
        mock_repo.get_by_key_hash.return_value = api_key
        with pytest.raises(HTTPException) as exc_info:
            await verify_api_key(raw_key, mock_repo)
        assert exc_info.value.status_code == 401
        assert "expired" in exc_info.value.detail


# ── Rate limiting ────────────────────────────────────────────


class TestRateLimit:
    @pytest.fixture
    def free_key(self) -> ApiKey:
        return ApiKey(id=uuid4(), user_id=uuid4(), plan="free", is_active=True)

    @pytest.fixture
    def mock_cache(self) -> AsyncMock:
        cache = AsyncMock()
        cache.get.return_value = None  # No prior usage
        return cache

    @pytest.mark.asyncio
    async def test_first_request_allowed(self, free_key: ApiKey, mock_cache: AsyncMock) -> None:
        result = await check_rate_limit(free_key, mock_cache)
        assert result["remaining_minute"] == PLAN_LIMITS["free"]["per_min"] - 1
        assert result["remaining_day"] == PLAN_LIMITS["free"]["per_day"] - 1

    @pytest.mark.asyncio
    async def test_minute_limit_exceeded(self, free_key: ApiKey, mock_cache: AsyncMock) -> None:
        from fastapi import HTTPException

        # Simulate per_min limit reached
        mock_cache.get.side_effect = lambda key: PLAN_LIMITS["free"]["per_min"] if "min" in key else 0
        with pytest.raises(HTTPException) as exc_info:
            await check_rate_limit(free_key, mock_cache)
        assert exc_info.value.status_code == 429
        assert "minute" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_day_limit_exceeded(self, free_key: ApiKey, mock_cache: AsyncMock) -> None:
        from fastapi import HTTPException

        # Simulate per_day limit reached (but per_min ok)
        mock_cache.get.side_effect = lambda key: (
            PLAN_LIMITS["free"]["per_day"] if "day" in key else 0
        )
        with pytest.raises(HTTPException) as exc_info:
            await check_rate_limit(free_key, mock_cache)
        assert exc_info.value.status_code == 429
        assert "day" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_increments_counters(self, free_key: ApiKey, mock_cache: AsyncMock) -> None:
        await check_rate_limit(free_key, mock_cache)
        # Should have called set twice (min + day)
        assert mock_cache.set.call_count == 2
        # Check TTLs
        calls = mock_cache.set.call_args_list
        min_call = [c for c in calls if "min" in str(c)][0]
        day_call = [c for c in calls if "day" in str(c)][0]
        assert min_call.kwargs.get("ttl_seconds") == 60
        assert day_call.kwargs.get("ttl_seconds") == 86400

    @pytest.mark.asyncio
    async def test_pro_plan_higher_limits(self, mock_cache: AsyncMock) -> None:
        pro_key = ApiKey(id=uuid4(), user_id=uuid4(), plan="pro", is_active=True)
        result = await check_rate_limit(pro_key, mock_cache)
        assert result["limit_minute"] == PLAN_LIMITS["pro"]["per_min"]
        assert result["limit_day"] == PLAN_LIMITS["pro"]["per_day"]


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
