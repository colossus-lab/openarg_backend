"""LLM adapter with automatic fallback chain."""

from __future__ import annotations

import logging
import traceback
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
        names = [type(s).__name__ for s in self.chain]
        logger.info("FallbackLLMAdapter initialized with chain: %s", " → ".join(names))

    async def chat(
        self,
        messages: list[LLMMessage],
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> LLMResponse:
        last_exc: Exception | None = None
        for i, svc in enumerate(self.chain):
            name = type(svc).__name__
            try:
                result = await svc.chat(messages, temperature, max_tokens)
                logger.info(
                    "LLM chat served by %s (model=%s, tokens=%d)",
                    name,
                    result.model,
                    result.tokens_used,
                )
                return result
            except Exception as exc:
                last_exc = exc
                remaining = len(self.chain) - i - 1
                if remaining > 0:
                    logger.warning(
                        "%s chat FAILED: [%s] %s — falling back (%d remaining)...",
                        name,
                        type(exc).__name__,
                        exc,
                        remaining,
                    )
                    logger.debug("Traceback for %s failure:\n%s", name, traceback.format_exc())
                else:
                    logger.error(
                        "All %d LLM services failed chat. Last: [%s] %s",
                        len(self.chain),
                        type(exc).__name__,
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
            name = type(svc).__name__
            try:
                result = await svc.chat_json(messages, json_schema, temperature, max_tokens)
                logger.info(
                    "LLM chat_json served by %s (model=%s, tokens=%d)",
                    name,
                    result.model,
                    result.tokens_used,
                )
                return result
            except Exception as exc:
                last_exc = exc
                remaining = len(self.chain) - i - 1
                if remaining > 0:
                    logger.warning(
                        "%s chat_json FAILED: [%s] %s — falling back (%d remaining)...",
                        name,
                        type(exc).__name__,
                        exc,
                        remaining,
                    )
                    logger.debug("Traceback for %s failure:\n%s", name, traceback.format_exc())
                else:
                    logger.error(
                        "All %d LLM services failed chat_json. Last: [%s] %s",
                        len(self.chain),
                        type(exc).__name__,
                        exc,
                    )
        raise RuntimeError(f"All {len(self.chain)} LLM services failed chat_json") from last_exc

    async def chat_stream(
        self,
        messages: list[LLMMessage],
        temperature: float = 0.0,
        max_tokens: int = 4096,
        usage_out: dict[str, int] | None = None,
    ) -> AsyncIterator[str]:
        last_exc: Exception | None = None
        for i, svc in enumerate(self.chain):
            name = type(svc).__name__
            try:
                # FIX-006: forward usage_out to whichever adapter serves the stream
                stream = svc.chat_stream(
                    messages, temperature, max_tokens, usage_out=usage_out
                )
                first_chunk = await anext(stream)
                logger.info("LLM stream served by %s", name)
                yield first_chunk
                async for chunk in stream:
                    yield chunk
                return
            except Exception as exc:
                last_exc = exc
                remaining = len(self.chain) - i - 1
                if remaining > 0:
                    logger.warning(
                        "%s stream FAILED: [%s] %s — falling back (%d remaining)...",
                        name,
                        type(exc).__name__,
                        exc,
                        remaining,
                    )
                    logger.debug("Traceback for %s failure:\n%s", name, traceback.format_exc())
                else:
                    logger.error(
                        "All %d LLM services failed to stream. Last: [%s] %s",
                        len(self.chain),
                        type(exc).__name__,
                        exc,
                    )
        raise RuntimeError(f"All {len(self.chain)} LLM services failed to stream") from last_exc
