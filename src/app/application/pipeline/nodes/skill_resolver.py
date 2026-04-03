"""LangGraph node: detect and resolve active skill from user query."""

from __future__ import annotations

import logging

from langgraph.config import get_stream_writer

from app.application.pipeline.state import OpenArgState
from app.application.skills.registry import SkillRegistry

logger = logging.getLogger(__name__)

_registry = SkillRegistry()


async def skill_resolver_node(state: OpenArgState) -> dict:
    """Detect if the query matches a known skill pattern.

    Sets *active_skill* and *skill_context* when a match is found.
    Both remain ``None`` when no skill applies — pipeline continues normally.
    """
    try:
        question = state.get("preprocessed_query", state["question"])
        skill = _registry.match_auto(question)

        if not skill:
            return {"active_skill": None, "skill_context": None}

        logger.info("Skill detected: %s (query=%r)", skill.name, question[:80])

        writer = get_stream_writer()
        writer({"type": "status", "step": "skill", "detail": skill.description})

        return {
            "active_skill": skill.name,
            "skill_context": {
                "planner": skill.planner_injection,
                "analyst": skill.analyst_injection,
            },
        }
    except Exception:
        logger.exception("skill_resolver_node failed")
        return {"active_skill": None, "skill_context": None}
