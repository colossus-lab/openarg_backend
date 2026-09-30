"""Puerto de Fundadores y créditos de la API pública."""

from __future__ import annotations

from abc import ABC, abstractmethod
from uuid import UUID

from app.domain.entities.credits.credits import CreditType, Supporter


class ICreditRepository(ABC):
    """Lo que necesita el control de cupos en cada pedido."""

    @abstractmethod
    async def get_active_supporter(self, user_id: UUID) -> Supporter | None:
        """El Fundador vigente (sin `hasta` o con `hasta` en el futuro), si hay."""

    @abstractmethod
    async def debit(self, user_id: UUID, tipo: CreditType) -> bool:
        """Descuenta un crédito de forma atómica. False si no quedaba saldo."""

    @abstractmethod
    async def balance(self, user_id: UUID) -> dict[str, int]:
        """Saldo actual: `{"preguntas": n, "datos": n}` (ceros si no tiene)."""
