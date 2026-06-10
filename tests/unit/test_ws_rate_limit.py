"""H8 fix — WebSocket per-minute rate limit (round v46).

Pins the contract that `_check_ws_rate_limit` calls the atomic
`increment_with_ttl` primitive and returns True iff the post-increment
counter exceeds the configured cap. The previous get→check→set logic
let two concurrent handshakes both observe count<cap and bump the
counter above the cap.
"""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from app.presentation.http.controllers.query.smart_query_v2_router import (
    _WS_RATE_LIMIT_PER_MINUTE,
    _check_ws_rate_limit,
)


@pytest.mark.asyncio
async def test_under_cap_returns_false():
    cache = AsyncMock()
    cache.increment_with_ttl = AsyncMock(return_value=_WS_RATE_LIMIT_PER_MINUTE - 1)
    assert await _check_ws_rate_limit(cache, "user@example.com") is False
    cache.increment_with_ttl.assert_awaited_once_with(
        "ws_rate:user@example.com", ttl_seconds=60
    )


@pytest.mark.asyncio
async def test_exactly_at_cap_still_allowed():
    """The cap is INCLUSIVE: count == cap is the last allowed hit."""
    cache = AsyncMock()
    cache.increment_with_ttl = AsyncMock(return_value=_WS_RATE_LIMIT_PER_MINUTE)
    assert await _check_ws_rate_limit(cache, "user@example.com") is False


@pytest.mark.asyncio
async def test_above_cap_returns_true():
    cache = AsyncMock()
    cache.increment_with_ttl = AsyncMock(return_value=_WS_RATE_LIMIT_PER_MINUTE + 1)
    assert await _check_ws_rate_limit(cache, "user@example.com") is True


@pytest.mark.asyncio
async def test_cache_failure_fails_open():
    """A Redis blip must NOT block legitimate WS handshakes — the
    cache layer is best-effort, and the AST gate + ownership check
    are the actual security boundaries.
    """
    cache = AsyncMock()
    cache.increment_with_ttl = AsyncMock(side_effect=RuntimeError("redis down"))
    assert await _check_ws_rate_limit(cache, "user@example.com") is False
