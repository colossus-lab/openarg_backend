from __future__ import annotations

from sqlalchemy import (
    Column,
    DateTime,
    Integer,
    String,
    Table,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from app.domain.entities.agent.agent_task import AgentTask
from app.infrastructure.persistence_sqla.registry import mapping_registry

_mapped = False


def map_agent_tables() -> None:
    global _mapped
    if _mapped:
        return

    agent_tasks_table = Table(
        "agent_tasks",
        mapping_registry.metadata,
        Column(
            "id", PG_UUID(as_uuid=True), primary_key=True,
            server_default=text("gen_random_uuid()"),
        ),
        Column("query_id", PG_UUID(as_uuid=True), nullable=False, index=True),
        Column("agent_type", String(50), nullable=False, index=True),
        Column("status", String(50), server_default="pending"),
        Column("input_json", Text, nullable=True),
        Column("output_json", Text, nullable=True),
        Column("error_message", Text, nullable=True),
        Column("tokens_used", Integer, server_default="0"),
        Column("duration_ms", Integer, server_default="0"),
        Column("created_at", DateTime(timezone=True), server_default=func.now()),
        Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
    )

    mapping_registry.map_imperatively(AgentTask, agent_tasks_table)

    _mapped = True
