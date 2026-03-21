"""LangGraph node: load conversational memory and chat history."""

from __future__ import annotations

import logging

from langgraph.config import get_stream_writer

import app.application.pipeline.nodes as nodes_pkg
from app.application.pipeline.history import load_chat_history
from app.application.pipeline.state import OpenArgState
from app.infrastructure.adapters.connectors.memory_agent import (
    build_memory_context_prompt,
    load_memory,
)

logger = logging.getLogger(__name__)


async def load_memory_node(state: OpenArgState) -> dict:
    """Load session memory from Redis and chat history from the DB.

    Produces three context strings consumed downstream:
    - *memory_ctx*: full memory context (used by planner when no chat history)
    - *memory_ctx_analyst*: lightweight variant (used by analyst to avoid data bleed)
    - *planner_ctx*: chat_history if available, otherwise memory_ctx
    """
    writer = get_stream_writer()
    writer({"type": "status", "step": "loading_context", "detail": "Cargando contexto..."})
    deps = nodes_pkg.get_deps()

    try:
        conversation_id = state.get("conversation_id", "")
        session_id = conversation_id or ""

        memory = await load_memory(deps.cache, session_id)
        memory_ctx = build_memory_context_prompt(memory)
        memory_ctx_analyst = build_memory_context_prompt(memory, for_analyst=True)

        chat_history = await load_chat_history(conversation_id, deps.chat_repo)
        planner_ctx = chat_history or memory_ctx

        return {
            "memory": memory,
            "memory_ctx": memory_ctx,
            "memory_ctx_analyst": memory_ctx_analyst,
            "planner_ctx": planner_ctx,
        }
    except Exception:
        logger.exception("load_memory_node failed")
        return {
            "memory": None,
            "memory_ctx": "",
            "memory_ctx_analyst": "",
            "planner_ctx": "",
        }
