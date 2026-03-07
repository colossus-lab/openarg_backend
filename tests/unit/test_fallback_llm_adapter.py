"""Tests for FallbackLLMAdapter — chained LLM services with automatic fallback."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

import pytest

from app.domain.ports.llm.llm_provider import ILLMProvider, LLMMessage, LLMResponse
from app.infrastructure.adapters.llm.fallback_llm_adapter import FallbackLLMAdapter


class FakeLLM(ILLMProvider):
    """Minimal LLM stub for testing."""

    def __init__(
        self, name: str, answer: str | None = None, error: Exception | None = None
    ):
        self._name = name
        self._answer = answer
        self._error = error

    async def chat(
        self,
        messages: list[LLMMessage],
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> LLMResponse:
        if self._error:
            raise self._error
        return LLMResponse(
            content=self._answer or f"{self._name} answer",
            tokens_used=10,
            model=self._name,
        )

    async def chat_stream(
        self,
        messages: list[LLMMessage],
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> AsyncIterator[str]:
        if self._error:
            raise self._error
        for chunk in [f"{self._name}", " stream"]:
            yield chunk


class TestChainFlattening:
    def test_flat_chain(self):
        a, b = FakeLLM("A"), FakeLLM("B")
        adapter = FallbackLLMAdapter(a, b)
        assert adapter.chain == [a, b]

    def test_nested_chain_flattened(self):
        a, b, c = FakeLLM("A"), FakeLLM("B"), FakeLLM("C")
        inner = FallbackLLMAdapter(b, c)
        outer = FallbackLLMAdapter(a, inner)
        assert outer.chain == [a, b, c]


class TestChat:
    async def test_primary_succeeds(self):
        primary = FakeLLM("primary", answer="ok")
        fallback = FakeLLM("fallback", answer="fallback ok")
        adapter = FallbackLLMAdapter(primary, fallback)
        result = await adapter.chat([])
        assert result.content == "ok"

    async def test_primary_fails_fallback_used(self):
        primary = FakeLLM("primary", error=RuntimeError("down"))
        fallback = FakeLLM("fallback", answer="fallback ok")
        adapter = FallbackLLMAdapter(primary, fallback)
        result = await adapter.chat([])
        assert result.content == "fallback ok"

    async def test_all_fail_raises_runtime_error(self):
        a = FakeLLM("A", error=RuntimeError("fail A"))
        b = FakeLLM("B", error=RuntimeError("fail B"))
        adapter = FallbackLLMAdapter(a, b)
        with pytest.raises(RuntimeError, match="All 2 LLM services failed"):
            await adapter.chat([])


class TestChatStream:
    async def test_primary_stream_succeeds(self):
        primary = FakeLLM("primary")
        fallback = FakeLLM("fallback")
        adapter = FallbackLLMAdapter(primary, fallback)
        chunks = [chunk async for chunk in adapter.chat_stream([])]
        assert chunks == ["primary", " stream"]

    async def test_primary_stream_fails_fallback_used(self):
        primary = FakeLLM("primary", error=RuntimeError("down"))
        fallback = FakeLLM("fallback")
        adapter = FallbackLLMAdapter(primary, fallback)
        chunks = [chunk async for chunk in adapter.chat_stream([])]
        assert chunks == ["fallback", " stream"]

    async def test_all_streams_fail(self):
        a = FakeLLM("A", error=RuntimeError("fail"))
        b = FakeLLM("B", error=RuntimeError("fail"))
        adapter = FallbackLLMAdapter(a, b)
        with pytest.raises(RuntimeError, match="All 2 LLM services failed to stream"):
            async for _ in adapter.chat_stream([]):
                pass
