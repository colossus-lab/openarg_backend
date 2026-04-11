from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass


@dataclass
class LLMMessage:
    role: str  # system, user, assistant
    content: str


@dataclass
class LLMResponse:
    content: str
    tokens_used: int
    model: str


class ILLMProvider(ABC):
    @abstractmethod
    async def chat(
        self,
        messages: list[LLMMessage],
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> LLMResponse: ...

    @abstractmethod
    async def chat_stream(
        self,
        messages: list[LLMMessage],
        temperature: float = 0.0,
        max_tokens: int = 4096,
        usage_out: dict[str, int] | None = None,
    ) -> AsyncIterator[str]:
        """Stream text chunks from the LLM.

        If ``usage_out`` is provided (a mutable dict), the adapter MUST
        populate it with token usage information when the stream ends:
        ``{"input_tokens": int, "output_tokens": int, "total_tokens": int}``.
        Callers can then read the dict after iterating the stream to
        attribute token costs to the request.

        FIX-006: tracks token usage in streaming mode (previously lost).
        """
        ...

    async def chat_json(
        self,
        messages: list[LLMMessage],
        json_schema: dict | None = None,
        temperature: float = 0.0,
        max_tokens: int = 512,
    ) -> LLMResponse:
        """Structured JSON output. Default: calls chat() and expects JSON."""
        return await self.chat(messages, temperature, max_tokens)


class IEmbeddingProvider(ABC):
    @abstractmethod
    async def embed(self, text: str) -> list[float]: ...

    @abstractmethod
    async def embed_batch(self, texts: list[str]) -> list[list[float]]: ...
