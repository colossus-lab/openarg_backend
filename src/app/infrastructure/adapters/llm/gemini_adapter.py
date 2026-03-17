from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from typing import Any

import google.generativeai as genai

from app.domain.ports.llm.llm_provider import ILLMProvider, LLMMessage, LLMResponse  # type: ignore[import-not-found]

logger = logging.getLogger(__name__)

LLM_TIMEOUT_SECONDS = 120


class GeminiLLMAdapter(ILLMProvider):  # type: ignore[misc]
    def __init__(self, api_key: str, model: str = "gemini-2.5-flash") -> None:
        genai.configure(api_key=api_key)
        self._model_name = model
        self._model = genai.GenerativeModel(model)

    def _prepare_call(
        self, messages: list[LLMMessage],
    ) -> tuple[genai.GenerativeModel, list[dict[str, Any]]]:
        """Extract system instructions, build chat messages, return (model, messages)."""
        system_parts = [m.content for m in messages if m.role == "system"]
        chat_messages = [
            {"role": "user" if m.role == "user" else "model", "parts": [m.content]}
            for m in messages if m.role != "system"
        ]
        model = self._model
        if system_parts:
            model = genai.GenerativeModel(
                self._model_name,
                system_instruction="\n".join(system_parts),
            )
        return model, chat_messages

    async def chat(
        self,
        messages: list[LLMMessage],
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> LLMResponse:
        model, chat_messages = self._prepare_call(messages)

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

        try:
            content = response.text or ""
        except (ValueError, AttributeError):
            # Gemini may return empty response (safety filter, no parts)
            logger.warning("Gemini returned empty response (finish_reason=%s)", getattr(response.candidates[0] if response.candidates else None, "finish_reason", "unknown"))
            content = ""

        if not content:
            raise RuntimeError("Gemini returned empty response")

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
        json_schema: dict[str, Any] | None = None,
        temperature: float = 0.0,
        max_tokens: int = 512,
    ) -> LLMResponse:
        model, chat_messages = self._prepare_call(messages)

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

        try:
            content = response.text or ""
        except (ValueError, AttributeError):
            logger.warning("Gemini chat_json returned empty response")
            content = ""

        if not content:
            raise RuntimeError("Gemini returned empty response")

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
        model, chat_messages = self._prepare_call(messages)

        response = await model.generate_content_async(
            chat_messages,
            generation_config=genai.types.GenerationConfig(
                temperature=temperature,
                max_output_tokens=max_tokens,
            ),
            stream=True,
        )

        yielded_any = False
        async for chunk in response:
            try:
                if chunk.text:
                    yield chunk.text
                    yielded_any = True
            except (ValueError, AttributeError):
                continue
        if not yielded_any:
            raise RuntimeError("Gemini stream returned no content")
