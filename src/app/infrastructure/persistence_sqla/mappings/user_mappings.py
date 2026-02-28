from __future__ import annotations

from sqlalchemy import (
    Column,
    DateTime,
    String,
    Table,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from app.domain.entities.user.user import User
from app.infrastructure.persistence_sqla.registry import mapping_registry

_mapped = False


def map_user_tables() -> None:
    global _mapped
    if _mapped:
        return

    users_table = Table(
        "users",
        mapping_registry.metadata,
        Column(
            "id", PG_UUID(as_uuid=True), primary_key=True,
            server_default=text("gen_random_uuid()"),
        ),
        Column("email", String(320), nullable=False, unique=True),
        Column("name", String(500), nullable=False, server_default=""),
        Column("image_url", String(2000), nullable=True),
        Column("created_at", DateTime(timezone=True), server_default=func.now()),
        Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
    )

    mapping_registry.map_imperatively(User, users_table)

    _mapped = True
