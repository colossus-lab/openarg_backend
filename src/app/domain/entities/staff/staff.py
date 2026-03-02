from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime

from app.domain.entities.base import BaseEntity, _utcnow


@dataclass
class StaffMember(BaseEntity):
    """A single employee record from a weekly HCDN staff snapshot."""

    legajo: str = ""
    apellido: str = ""
    nombre: str = ""
    escalafon: str | None = None
    area_desempeno: str = ""
    convenio: str | None = None
    snapshot_date: date = field(default_factory=date.today)


@dataclass
class StaffChange(BaseEntity):
    """An alta/baja detected by diffing consecutive snapshots."""

    legajo: str = ""
    apellido: str = ""
    nombre: str = ""
    area_desempeno: str = ""
    tipo: str = ""  # "alta" | "baja"
    detected_at: datetime = field(default_factory=_utcnow)
