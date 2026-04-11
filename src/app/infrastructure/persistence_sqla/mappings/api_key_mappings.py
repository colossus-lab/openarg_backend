from __future__ import annotations

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Table,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from app.domain.entities.api_key.api_key import ApiKey, ApiUsage
from app.infrastructure.persistence_sqla.registry import mapping_registry

_mapped = False


def map_api_key_tables() -> None:
    global _mapped  # noqa: PLW0603
    if _mapped:
        return

    api_keys_table = Table(
        "api_keys",
        mapping_registry.metadata,
        Column(
            "id",
            PG_UUID(as_uuid=True),
            primary_key=True,
            server_default=text("gen_random_uuid()"),
        ),
        Column(
            "user_id",
            PG_UUID(as_uuid=True),
            ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False,
        ),
        Column("key_hash", String(255), nullable=False, unique=True),
        Column("key_prefix", String(32), nullable=False),
        Column("name", String(200), nullable=False),
        Column("plan", String(20), nullable=False, server_default="free"),
        Column("is_active", Boolean, nullable=False, server_default=text("true")),
        Column("last_used_at", DateTime(timezone=True), nullable=True),
        # expires_at was dropped 2026-04-11 (Alembic 0030) — see
        # specs/008-developers-keys/[DEBT-003] for rationale.
        Column("created_at", DateTime(timezone=True), server_default=func.now()),
        Column(
            "updated_at",
            DateTime(timezone=True),
            server_default=func.now(),
            onupdate=func.now(),
        ),
    )

    api_usage_table = Table(
        "api_usage",
        mapping_registry.metadata,
        Column(
            "id",
            PG_UUID(as_uuid=True),
            primary_key=True,
            server_default=text("gen_random_uuid()"),
        ),
        Column(
            "api_key_id",
            PG_UUID(as_uuid=True),
            ForeignKey("api_keys.id", ondelete="CASCADE"),
            nullable=False,
        ),
        Column("endpoint", String(255), nullable=False),
        Column("question", String(500), nullable=True),
        Column("status_code", Integer, nullable=False),
        Column("tokens_used", Integer, server_default="0"),
        Column("duration_ms", Integer, server_default="0"),
        Column("created_at", DateTime(timezone=True), server_default=func.now()),
    )

    mapping_registry.map_imperatively(ApiKey, api_keys_table)
    mapping_registry.map_imperatively(ApiUsage, api_usage_table)

    _mapped = True
