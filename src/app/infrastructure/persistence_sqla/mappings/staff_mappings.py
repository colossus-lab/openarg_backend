from __future__ import annotations

from sqlalchemy import Column, Date, DateTime, String, Table, func, text
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from app.domain.entities.staff.staff import StaffChange, StaffMember
from app.infrastructure.persistence_sqla.registry import mapping_registry

_mapped = False


def map_staff_tables() -> None:
    global _mapped
    if _mapped:
        return

    staff_snapshots_table = Table(
        "staff_snapshots",
        mapping_registry.metadata,
        Column(
            "id",
            PG_UUID(as_uuid=True),
            primary_key=True,
            server_default=text("gen_random_uuid()"),
        ),
        Column("legajo", String(50), nullable=False),
        Column("apellido", String(300), nullable=False),
        Column("nombre", String(300), nullable=False),
        Column("escalafon", String(200), nullable=True),
        Column("area_desempeno", String(500), nullable=False),
        Column("convenio", String(200), nullable=True),
        Column("snapshot_date", Date, nullable=False),
        Column("created_at", DateTime(timezone=True), server_default=func.now()),
        Column(
            "updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
        ),
    )

    staff_changes_table = Table(
        "staff_changes",
        mapping_registry.metadata,
        Column(
            "id",
            PG_UUID(as_uuid=True),
            primary_key=True,
            server_default=text("gen_random_uuid()"),
        ),
        Column("legajo", String(50), nullable=False),
        Column("apellido", String(300), nullable=False),
        Column("nombre", String(300), nullable=False),
        Column("area_desempeno", String(500), nullable=False),
        Column("tipo", String(10), nullable=False),
        Column("detected_at", DateTime(timezone=True), server_default=func.now()),
        Column("created_at", DateTime(timezone=True), server_default=func.now()),
        Column(
            "updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
        ),
    )

    mapping_registry.map_imperatively(StaffMember, staff_snapshots_table)
    mapping_registry.map_imperatively(StaffChange, staff_changes_table)

    _mapped = True
