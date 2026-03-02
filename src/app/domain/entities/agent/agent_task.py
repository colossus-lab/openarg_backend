from __future__ import annotations

from dataclasses import dataclass

from app.domain.entities.base import BaseEntity


@dataclass
class AgentTask(BaseEntity):
    """Tarea ejecutada por un agente específico."""

    query_id: str = ""
    agent_type: str = ""  # planner, collector, analyst, memory
    status: str = "pending"  # pending, running, completed, error
    input_json: str = ""
    output_json: str = ""
    error_message: str = ""
    tokens_used: int = 0
    duration_ms: int = 0
