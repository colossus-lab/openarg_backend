"""Fundadores y créditos de la API pública.

Un Fundador es alguien que sostiene OpenArg con un aporte a la Fundación
Colossus Lab (o a quien se le dio por cortesía): tiene cupo mensual ampliado.
Los créditos son un saldo extra que se usa sólo cuando se terminó el cupo del
mes; salen de donaciones o se cargan desde el admin, y no vencen.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal
from uuid import UUID

CreditType = Literal["preguntas", "datos"]
CREDIT_TYPES: tuple[CreditType, ...] = ("preguntas", "datos")


@dataclass(frozen=True)
class Supporter:
    """Un Fundador. `hasta` nulo = sin vencimiento."""

    user_id: UUID
    nivel: str
    desde: datetime
    hasta: datetime | None
    origen: str
