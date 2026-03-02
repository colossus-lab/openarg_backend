from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator

import google.generativeai as genai

from app.domain.ports.llm.llm_provider import ILLMProvider, LLMMessage, LLMResponse

logger = logging.getLogger(__name__)

LLM_TIMEOUT_SECONDS = 120


class GeminiLLMAdapter(ILLMProvider):
    def __init__(self, api_key: str, model: str = "gemini-2.5-flash") -> None:
        genai.configure(api_key=api_key)
        self._model_name = model
        self._model = genai.GenerativeModel(model)

    async def chat(
        self,
        messages: list[LLMMessage],
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> LLMResponse:
        system_parts = [m.content for m in messages if m.role == "system"]
        chat_messages = []
        for m in messages:
            if m.role == "system":
                continue
            role = "user" if m.role == "user" else "model"
            chat_messages.append({"role": role, "parts": [m.content]})

        model = self._model
        if system_parts:
            model = genai.GenerativeModel(
                self._model_name,
                system_instruction="\n".join(system_parts),
            )

        try:
            response = await asyncio.wait_for(
                model.generate_content_async(
                    chat_messages,
                    generation_config=genai.types.GenerationConfig(
                        temperature=temperature,
                        max_output_tokens=max_tokens,
                    ),
                ),
                timeout=LLM_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            logger.error("Gemini chat timed out after %ds", LLM_TIMEOUT_SECONDS)
            raise

        content = response.text or ""
        tokens = 0
        if response.usage_metadata:
            tokens = (
                response.usage_metadata.prompt_token_count
                + response.usage_metadata.candidates_token_count
            )

        return LLMResponse(content=content, tokens_used=tokens, model=self._model_name)

    async def chat_json(
        self,
        messages: list[LLMMessage],
        json_schema: dict | None = None,
        temperature: float = 0.0,
        max_tokens: int = 512,
    ) -> LLMResponse:
        system_parts = [m.content for m in messages if m.role == "system"]
        chat_messages = []
        for m in messages:
            if m.role == "system":
                continue
            role = "user" if m.role == "user" else "model"
            chat_messages.append({"role": role, "parts": [m.content]})

        model = self._model
        if system_parts:
            model = genai.GenerativeModel(
                self._model_name,
                system_instruction="\n".join(system_parts),
            )

        gen_config = genai.types.GenerationConfig(
            temperature=temperature,
            max_output_tokens=max_tokens,
            response_mime_type="application/json",
            **({"response_schema": json_schema} if json_schema else {}),
        )

        try:
            response = await asyncio.wait_for(
                model.generate_content_async(chat_messages, generation_config=gen_config),
                timeout=10,
            )
        except TimeoutError:
            logger.error("Gemini chat_json timed out after 10s")
            raise

        content = response.text or ""
        tokens = 0
        if response.usage_metadata:
            tokens = (
                response.usage_metadata.prompt_token_count
                + response.usage_metadata.candidates_token_count
            )
        return LLMResponse(content=content, tokens_used=tokens, model=self._model_name)

    async def chat_stream(
        self,
        messages: list[LLMMessage],
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> AsyncIterator[str]:
        system_parts = [m.content for m in messages if m.role == "system"]
        chat_messages = []
        for m in messages:
            if m.role == "system":
                continue
            role = "user" if m.role == "user" else "model"
            chat_messages.append({"role": role, "parts": [m.content]})

        model = self._model
        if system_parts:
            model = genai.GenerativeModel(
                self._model_name,
                system_instruction="\n".join(system_parts),
            )

        response = await model.generate_content_async(
            chat_messages,
            generation_config=genai.types.GenerationConfig(
                temperature=temperature,
                max_output_tokens=max_tokens,
            ),
            stream=True,
        )

        async for chunk in response:
            if chunk.text:
                yield chunk.text
