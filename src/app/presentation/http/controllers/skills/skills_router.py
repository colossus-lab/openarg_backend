"""Skills listing endpoint."""

from __future__ import annotations

from fastapi import APIRouter

from app.application.skills.registry import SkillRegistry

router = APIRouter(tags=["skills"])

_registry = SkillRegistry()


@router.get("/skills")
async def list_skills() -> list[dict[str, str]]:
    """Return all available skills with their names and descriptions."""
    return [{"name": s.name, "description": s.description} for s in _registry.list_all()]
