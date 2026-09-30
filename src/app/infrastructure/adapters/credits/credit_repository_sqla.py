"""SQLAlchemy adapter for supporters (Fundadores) and API credits.

Tables come from Alembic 0063: `api_supporters`, `api_credit_balances`,
`api_credit_movements`. Raw SQL on purpose: the debit must be a single
conditional UPDATE so two concurrent requests cannot both spend the last
credit, which an ORM read-modify-write would allow.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import text

from app.domain.entities.credits.credits import CREDIT_TYPES, CreditType, Supporter
from app.domain.ports.credits.credit_repository import ICreditRepository

if TYPE_CHECKING:
    from uuid import UUID

    from app.infrastructure.persistence_sqla.provider import MainAsyncSession


class CreditRepositorySQLA(ICreditRepository):
    def __init__(self, session: MainAsyncSession) -> None:
        self._session = session

    async def get_active_supporter(self, user_id: UUID) -> Supporter | None:
        row = (
            await self._session.execute(
                text(
                    "SELECT user_id, nivel, desde, hasta, COALESCE(origen, '') AS origen "
                    "FROM public.api_supporters "
                    "WHERE user_id = :u AND (hasta IS NULL OR hasta > NOW())"
                ),
                {"u": user_id},
            )
        ).first()
        if row is None:
            return None
        return Supporter(
            user_id=row.user_id,
            nivel=row.nivel,
            desde=row.desde,
            hasta=row.hasta,
            origen=row.origen,
        )

    async def debit(self, user_id: UUID, tipo: CreditType) -> bool:
        if tipo not in CREDIT_TYPES:
            raise ValueError(f"Unknown credit type: {tipo}")
        # `tipo` is validated against a closed set above, so interpolating the
        # column name is safe; values still travel as bound parameters.
        row = (
            await self._session.execute(
                text(
                    f"UPDATE public.api_credit_balances SET {tipo} = {tipo} - 1, updated_at = NOW() "  # noqa: S608
                    f"WHERE user_id = :u AND {tipo} > 0 RETURNING {tipo}"
                ),
                {"u": user_id},
            )
        ).first()
        if row is None:
            # Nothing was updated, so there is nothing to undo. No rollback on
            # purpose: this is the request-scoped session, and a rollback here
            # could discard unrelated pending work.
            return False
        await self._session.execute(
            text(
                "INSERT INTO public.api_credit_movements (user_id, tipo, delta, motivo) "
                "VALUES (:u, :t, -1, 'consumo')"
            ),
            {"u": user_id, "t": tipo},
        )
        await self._session.commit()
        return True

    async def balance(self, user_id: UUID) -> dict[str, int]:
        row = (
            await self._session.execute(
                text("SELECT preguntas, datos FROM public.api_credit_balances WHERE user_id = :u"),
                {"u": user_id},
            )
        ).first()
        if row is None:
            return {"preguntas": 0, "datos": 0}
        return {"preguntas": int(row.preguntas), "datos": int(row.datos)}
