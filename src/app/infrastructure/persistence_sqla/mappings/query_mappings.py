from __future__ import annotations

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Integer,
    String,
    Table,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from app.domain.entities.query.query import QueryDatasetLink, UserQuery
from app.infrastructure.persistence_sqla.registry import mapping_registry

_mapped = False


def map_query_tables() -> None:
    global _mapped
    if _mapped:
        return

    user_queries_table = Table(
        "user_queries",
        mapping_registry.metadata,
        Column(
            "id", PG_UUID(as_uuid=True), primary_key=True,
            server_default=text("gen_random_uuid()"),
        ),
        Column("user_id", Text, nullable=True, index=True),
        Column("question", Text, nullable=False),
        Column("status", String(50), server_default="pending", index=True),
        Column("plan_json", Text, nullable=True),
        Column("datasets_used", Text, nullable=True),
        Column("analysis_result", Text, nullable=True),
        Column("sources_json", Text, nullable=True),
        Column("error_message", Text, nullable=True),
        Column("tokens_used", Integer, server_default="0"),
        Column("duration_ms", Integer, server_default="0"),
        Column("created_at", DateTime(timezone=True), server_default=func.now()),
        Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
    )

    query_dataset_links_table = Table(
        "query_dataset_links",
        mapping_registry.metadata,
        Column(
            "id", PG_UUID(as_uuid=True), primary_key=True,
            server_default=text("gen_random_uuid()"),
        ),
        Column("query_id", PG_UUID(as_uuid=True), nullable=False, index=True),
        Column("dataset_id", PG_UUID(as_uuid=True), nullable=False, index=True),
        Column("relevance_score", Float, server_default="0.0"),
        Column("created_at", DateTime(timezone=True), server_default=func.now()),
        Column("updated_at", DateTime(timezone=True), server_default=func.now()),
    )

    mapping_registry.map_imperatively(UserQuery, user_queries_table)
    mapping_registry.map_imperatively(QueryDatasetLink, query_dataset_links_table)

    _mapped = True
