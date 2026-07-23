"""H3 fix — owner-scoped chat history loading (round v46).

These tests pin the contract that `load_chat_history` propagates
`owner_user_id` into `chat_repo.get_messages` as `user_id=`. The repo
implementation enforces the filter at the SQL layer; this test sits at
the application/port boundary so a future repo refactor that drops the
filter still requires us to explicitly bless the change here.
"""

from __future__ import annotations

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from app.application.pipeline.history import load_chat_history


@pytest.mark.asyncio
async def test_load_chat_history_passes_owner_user_id_to_repo():
    chat_repo = AsyncMock()
    chat_repo.get_messages = AsyncMock(return_value=[])

    conv_id = str(uuid4())
    owner_id = str(uuid4())

    await load_chat_history(conv_id, chat_repo, owner_user_id=owner_id)

    chat_repo.get_messages.assert_awaited_once()
    call = chat_repo.get_messages.await_args
    # Third positional or `user_id` kwarg — the helper uses kwarg form.
    assert "user_id" in call.kwargs
    # The repo receives UUIDs, not strings.
    from uuid import UUID

    assert call.kwargs["user_id"] == UUID(owner_id)


@pytest.mark.asyncio
async def test_load_chat_history_returns_empty_when_repo_returns_empty():
    """H3 invariant: a foreign conv_id resolves to [] at the repo and
    `load_chat_history` produces no LLM context — so a stolen conv_id
    cannot leak its history through the planner prompt.
    """
    chat_repo = AsyncMock()
    chat_repo.get_messages = AsyncMock(return_value=[])

    out = await load_chat_history(
        str(uuid4()), chat_repo, owner_user_id=str(uuid4())
    )
    assert out == ""


@pytest.mark.asyncio
async def test_load_chat_history_without_owner_uses_unscoped_lookup():
    """Backward compat: callers that did not opt into ownership (the
    conversations_router, which already checks ownership at the
    application layer) keep working.
    """
    chat_repo = AsyncMock()
    chat_repo.get_messages = AsyncMock(return_value=[])

    await load_chat_history(str(uuid4()), chat_repo, owner_user_id=None)

    call = chat_repo.get_messages.await_args
    assert call.kwargs.get("user_id") is None


@pytest.mark.asyncio
async def test_load_chat_history_empty_when_no_conv_or_repo():
    assert await load_chat_history("", AsyncMock()) == ""
    assert await load_chat_history(str(uuid4()), None) == ""
