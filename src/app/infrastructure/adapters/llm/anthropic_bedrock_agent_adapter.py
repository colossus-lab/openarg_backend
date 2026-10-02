"""IAgentLLM on AWS Bedrock through the Anthropic SDK (``AsyncAnthropicBedrock``).

Why not the Converse adapter (``bedrock_llm_adapter.py``):

- it is text-in/text-out, with no tool blocks;
- it runs boto3 in a thread (``asyncio.to_thread``) and its streaming path
  iterates the boto3 event stream on the event loop, blocking it;
- it reports no cache tokens, so a turn's real cost cannot be computed.

This adapter is async end to end. Prompt caching uses explicit breakpoints on
the system prompt and on the last tool definition: the tool list and the
system prompt are identical across turns and requests, so every request after
the first reads them from cache. (The InvokeModel path on Bedrock rejects the
top-level automatic ``cache_control``; explicit breakpoints are the supported
form there.)
"""

from __future__ import annotations

import logging
from collections.abc import AsyncGenerator
from typing import Any

from anthropic import AsyncAnthropicBedrock

from app.domain.ports.llm.agent_llm import (
    AgentTool,
    AgentTurn,
    AgentUsage,
    IAgentLLM,
    TextDelta,
    ToolCall,
)

logger = logging.getLogger(__name__)

_CACHE = {"type": "ephemeral"}


class AnthropicBedrockAgentAdapter(IAgentLLM):
    def __init__(self, *, region: str, model: str, timeout_s: float = 120.0) -> None:
        self._model = model
        # Two retries on 429/5xx/connection errors (SDK default). Credentials
        # come from the default AWS chain, same as the boto3 clients.
        self._client = AsyncAnthropicBedrock(aws_region=region, timeout=timeout_s)

    @property
    def model(self) -> str:
        return self._model

    @staticmethod
    def _tools(tools: list[AgentTool]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = [
            {"name": t.name, "description": t.description, "input_schema": t.input_schema}
            for t in tools
        ]
        if out:
            out[-1] = {**out[-1], "cache_control": _CACHE}
        return out

    async def stream_turn(
        self,
        *,
        system: str,
        messages: list[dict[str, Any]],
        tools: list[AgentTool],
        max_tokens: int = 4096,
        allow_tools: bool = True,
    ) -> AsyncGenerator[TextDelta | AgentTurn, None]:
        kwargs: dict[str, Any] = {
            "model": self._model,
            "max_tokens": max_tokens,
            "system": [{"type": "text", "text": system, "cache_control": _CACHE}],
            "messages": messages,
        }
        if tools:
            kwargs["tools"] = self._tools(tools)
            if not allow_tools:
                kwargs["tool_choice"] = {"type": "none"}

        async with self._client.messages.stream(**kwargs) as stream:
            async for event in stream:
                if event.type == "text":
                    yield TextDelta(event.text)
            final = await stream.get_final_message()

        usage = final.usage
        calls = [
            ToolCall(id=b.id, name=b.name, input=dict(b.input) if isinstance(b.input, dict) else {})
            for b in final.content
            if b.type == "tool_use"
        ]
        yield AgentTurn(
            text="".join(b.text for b in final.content if b.type == "text"),
            tool_calls=calls,
            stop_reason=final.stop_reason or "",
            usage=AgentUsage(
                input_tokens=usage.input_tokens or 0,
                output_tokens=usage.output_tokens or 0,
                cache_read_tokens=getattr(usage, "cache_read_input_tokens", 0) or 0,
                cache_write_tokens=getattr(usage, "cache_creation_input_tokens", 0) or 0,
            ),
            content=[b.model_dump(exclude_none=True) for b in final.content],
        )
