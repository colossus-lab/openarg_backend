from __future__ import annotations

from dataclasses import dataclass

from app.domain.entities.base import BaseEntity


@dataclass
class UserQuery(BaseEntity):
    """Consulta de un usuario al sistema."""

    user_id: str | None = None
    question: str = ""
    status: str = "pending"  # pending, planning, collecting, analyzing, completed, error
    plan_json: str = ""  # JSON con el plan del Planner agent
    datasets_used: str = ""  # JSON con IDs de datasets utilizados
    analysis_result: str = ""  # resultado final del Analyst
    sources_json: str = ""  # JSON con las fuentes citadas
    error_message: str = ""
    tokens_used: int = 0
    duration_ms: int = 0


@dataclass
class QueryDatasetLink(BaseEntity):
    """Relación entre una query y los datasets que usó."""

    query_id: str = ""
    dataset_id: str = ""
    relevance_score: float = 0.0
