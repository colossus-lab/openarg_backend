"""LLM adapter using AWS Bedrock with Claude Haiku 3.5."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from typing import Any

import boto3

from app.domain.ports.llm.llm_provider import ILLMProvider, LLMMessage, LLMResponse

logger = logging.getLogger(__name__)

LLM_TIMEOUT_SECONDS = 120


class BedrockLLMAdapter(ILLMProvider):
    """ILLMProvider implementation backed by Claude on AWS Bedrock (Converse API).

    Uses boto3 with default AWS credentials (env vars or instance profile).
    Default model: Claude 3.5 Haiku.
    """

    def __init__(
        self,
        region: str = "us-east-1",
        model: str = "us.anthropic.claude-3-5-haiku-20241022-v1:0",
    ) -> None:
        self._client = boto3.client("bedrock-runtime", region_name=region)
        self._model = model

    def _build_messages(
        self, messages: list[LLMMessage]
    ) -> tuple[list[dict[str, Any]] | None, list[dict[str, Any]]]:
        """Separate system prompt and convert messages to Bedrock Converse format."""
        system_parts: list[dict[str, Any]] = []
        api_messages: list[dict[str, Any]] = []

        for msg in messages:
            if msg.role == "system":
                system_parts.append({"text": msg.content})
            else:
                api_messages.append({"role": msg.role, "content": [{"text": msg.content}]})

        return system_parts or None, api_messages

    def _converse_sync(
        self,
        messages: list[LLMMessage],
        temperature: float,
        max_tokens: int,
    ) -> dict[str, Any]:
        """Synchronous Bedrock converse call."""
        system_parts, api_messages = self._build_messages(messages)

        kwargs: dict[str, Any] = {
            "modelId": self._model,
            "messages": api_messages,
            "inferenceConfig": {
                "temperature": temperature,
                "maxTokens": max_tokens,
            },
        }
        if system_parts:
            kwargs["system"] = system_parts

        return self._client.converse(**kwargs)

    async def chat(
        self,
        messages: list[LLMMessage],
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> LLMResponse:
        try:
            response = await asyncio.wait_for(
                asyncio.to_thread(self._converse_sync, messages, temperature, max_tokens),
                timeout=LLM_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            logger.error("Bedrock chat timed out after %ds", LLM_TIMEOUT_SECONDS)
            raise

        content = response["output"]["message"]["content"][0]["text"]
        usage = response.get("usage", {})
        tokens = usage.get("inputTokens", 0) + usage.get("outputTokens", 0)

        return LLMResponse(content=content, tokens_used=tokens, model=self._model)

    async def chat_json(
        self,
        messages: list[LLMMessage],
        json_schema: dict[str, Any] | None = None,
        temperature: float = 0.0,
        max_tokens: int = 512,
    ) -> LLMResponse:
        """Structured JSON output via system prompt instruction."""
        json_instruction = (
            "You MUST respond with valid JSON only. No markdown, no explanation, "
            "no code fences. Output raw JSON."
        )
        if json_schema:
            json_instruction += f"\n\nExpected JSON schema:\n{json.dumps(json_schema, indent=2)}"

        # Prepend JSON instruction as a system message
        augmented: list[LLMMessage] = [LLMMessage(role="system", content=json_instruction)]
        augmented.extend(messages)

        return await self.chat(augmented, temperature, max_tokens)

    async def chat_stream(
        self,
        messages: list[LLMMessage],
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> AsyncIterator[str]:
        system_parts, api_messages = self._build_messages(messages)

        kwargs: dict[str, Any] = {
            "modelId": self._model,
            "messages": api_messages,
            "inferenceConfig": {
                "temperature": temperature,
                "maxTokens": max_tokens,
            },
        }
        if system_parts:
            kwargs["system"] = system_parts

        response = await asyncio.to_thread(self._client.converse_stream, **kwargs)
        stream = response["stream"]

        for event in stream:
            if "contentBlockDelta" in event:
                delta = event["contentBlockDelta"].get("delta", {})
                text = delta.get("text", "")
                if text:
                    yield text
