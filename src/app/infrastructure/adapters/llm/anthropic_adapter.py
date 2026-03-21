from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator

import anthropic

from app.domain.ports.llm.llm_provider import ILLMProvider, LLMMessage, LLMResponse

logger = logging.getLogger(__name__)

LLM_TIMEOUT_SECONDS = 120


class AnthropicLLMAdapter(ILLMProvider):
    def __init__(self, api_key: str, model: str = "claude-sonnet-4-20250514") -> None:
        self._client = anthropic.AsyncAnthropic(api_key=api_key)
        self._model = model

    def _build_messages(self, messages: list[LLMMessage]) -> tuple[str | None, list[dict]]:
        system_prompt = None
        api_messages = []
        for msg in messages:
            if msg.role == "system":
                system_prompt = msg.content
            else:
                api_messages.append({"role": msg.role, "content": msg.content})
        return system_prompt, api_messages

    async def chat(
        self,
        messages: list[LLMMessage],
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> LLMResponse:
        system_prompt, api_messages = self._build_messages(messages)

        kwargs: dict = {
            "model": self._model,
            "messages": api_messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        if system_prompt:
            kwargs["system"] = system_prompt

        try:
            response = await asyncio.wait_for(
                self._client.messages.create(**kwargs),
                timeout=LLM_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            logger.error("Anthropic chat timed out after %ds", LLM_TIMEOUT_SECONDS)
            raise

        content = ""
        for block in response.content:
            if block.type == "text":
                content += block.text

        return LLMResponse(
            content=content,
            tokens_used=response.usage.input_tokens + response.usage.output_tokens,
            model=response.model,
        )

    async def chat_stream(
        self,
        messages: list[LLMMessage],
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> AsyncIterator[str]:
        system_prompt, api_messages = self._build_messages(messages)

        kwargs: dict = {
            "model": self._model,
            "messages": api_messages,
            "max_tokens": max_tokens,
            "temperature": temperature,
        }
        if system_prompt:
            kwargs["system"] = system_prompt

        async with self._client.messages.stream(**kwargs) as stream:
            async for text in stream.text_stream:
                yield text
