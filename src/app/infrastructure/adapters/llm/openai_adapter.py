from __future__ import annotations

from collections.abc import AsyncIterator

import openai

from app.domain.ports.llm.llm_provider import ILLMProvider, LLMMessage, LLMResponse


class OpenAILLMAdapter(ILLMProvider):
    def __init__(self, api_key: str, model: str = "gpt-4o") -> None:
        self._client = openai.AsyncOpenAI(api_key=api_key)
        self._model = model

    async def chat(
        self,
        messages: list[LLMMessage],
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> LLMResponse:
        api_messages = [{"role": m.role, "content": m.content} for m in messages]

        response = await self._client.chat.completions.create(
            model=self._model,
            messages=api_messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )

        content = response.choices[0].message.content or ""
        tokens = response.usage.total_tokens if response.usage else 0

        return LLMResponse(content=content, tokens_used=tokens, model=self._model)

    async def chat_stream(
        self,
        messages: list[LLMMessage],
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> AsyncIterator[str]:
        api_messages = [{"role": m.role, "content": m.content} for m in messages]

        stream = await self._client.chat.completions.create(
            model=self._model,
            messages=api_messages,
            temperature=temperature,
            max_tokens=max_tokens,
            stream=True,
        )

        async for chunk in stream:
            if chunk.choices and chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content
