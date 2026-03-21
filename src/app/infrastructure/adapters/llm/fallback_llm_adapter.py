"""LLM adapter with automatic fallback chain."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator

from app.domain.ports.llm.llm_provider import ILLMProvider, LLMMessage, LLMResponse

logger = logging.getLogger(__name__)


class FallbackLLMAdapter(ILLMProvider):
    """Tries a chain of LLM services in order until one succeeds."""

    def __init__(self, primary: ILLMProvider, fallback: ILLMProvider) -> None:
        self.chain: list[ILLMProvider] = []
        for svc in (primary, fallback):
            if isinstance(svc, FallbackLLMAdapter):
                self.chain.extend(svc.chain)
            else:
                self.chain.append(svc)

    async def chat(
        self,
        messages: list[LLMMessage],
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> LLMResponse:
        last_exc: Exception | None = None
        for i, svc in enumerate(self.chain):
            try:
                return await svc.chat(messages, temperature, max_tokens)
            except Exception as exc:
                last_exc = exc
                name = type(svc).__name__
                remaining = len(self.chain) - i - 1
                if remaining > 0:
                    logger.warning(
                        "%s failed (%s), trying next fallback (%d remaining)...",
                        name,
                        exc,
                        remaining,
                    )
                else:
                    logger.error(
                        "All %d LLM services failed. Last error: %s",
                        len(self.chain),
                        exc,
                    )
        raise RuntimeError(f"All {len(self.chain)} LLM services failed") from last_exc

    async def chat_json(
        self,
        messages: list[LLMMessage],
        json_schema: dict | None = None,
        temperature: float = 0.0,
        max_tokens: int = 512,
    ) -> LLMResponse:
        last_exc: Exception | None = None
        for i, svc in enumerate(self.chain):
            try:
                return await svc.chat_json(messages, json_schema, temperature, max_tokens)
            except Exception as exc:
                last_exc = exc
                name = type(svc).__name__
                remaining = len(self.chain) - i - 1
                if remaining > 0:
                    logger.warning(
                        "%s chat_json failed (%s), trying next fallback (%d remaining)...",
                        name,
                        exc,
                        remaining,
                    )
                else:
                    logger.error(
                        "All %d LLM services failed chat_json. Last error: %s",
                        len(self.chain),
                        exc,
                    )
        raise RuntimeError(f"All {len(self.chain)} LLM services failed chat_json") from last_exc

    async def chat_stream(
        self,
        messages: list[LLMMessage],
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> AsyncIterator[str]:
        last_exc: Exception | None = None
        for i, svc in enumerate(self.chain):
            try:
                stream = svc.chat_stream(messages, temperature, max_tokens)
                first_chunk = await anext(stream)
                yield first_chunk
                async for chunk in stream:
                    yield chunk
                return
            except Exception as exc:
                last_exc = exc
                name = type(svc).__name__
                remaining = len(self.chain) - i - 1
                if remaining > 0:
                    logger.warning(
                        "%s stream failed (%s), trying next fallback (%d remaining)...",
                        name,
                        exc,
                        remaining,
                    )
                else:
                    logger.error(
                        "All %d LLM services failed to stream. Last error: %s",
                        len(self.chain),
                        exc,
                    )
        raise RuntimeError(f"All {len(self.chain)} LLM services failed to stream") from last_exc
