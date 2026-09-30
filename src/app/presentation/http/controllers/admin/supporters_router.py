"""Admin: Fundadores y créditos de la API pública (tablas de Alembic 0063).

Reemplaza el script manual de Tomi (`despliegue/fundador.py`): el tablero de
openarg.org/admin/mcp marca Fundadores y carga créditos desde acá, y cada
acción queda con quién la hizo (`X-Admin-Actor`, el mail de la sesión admin
que el proxy del frontend reenvía) y, en los créditos, con un movimiento.

Protegido con `X-Admin-Key` como el resto de `/admin`; Caddy bloquea
`/api/v1/admin/*` desde internet.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, time
from typing import Any, Literal

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, ConfigDict, EmailStr, Field, model_validator
from sqlalchemy import text

from app.infrastructure.celery.tasks._db import get_sync_engine
from app.presentation.http.controllers.admin.tasks_router import verify_admin_key

router = APIRouter(prefix="/admin", tags=["admin-supporters"])

_ACTOR_MAX = 255


def _actor(x_admin_actor: str | None) -> str | None:
    return (x_admin_actor or "").strip()[:_ACTOR_MAX] or None


class SupporterRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    email: EmailStr
    # Fecha (inclusive): la marca vale hasta el final de ese día, hora UTC.
    hasta: date | None = None
    origen: str = Field(default="", max_length=200)
    nota: str = Field(default="", max_length=1000)


class CreditsRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    email: EmailStr
    preguntas: int = Field(default=0, ge=0, le=100_000)
    datos: int = Field(default=0, ge=0, le=1_000_000)
    motivo: Literal["admin", "donacion", "ajuste"] = "admin"
    referencia: str | None = Field(default=None, max_length=255)

    @model_validator(mode="after")
    def _something_to_grant(self) -> CreditsRequest:
        if self.preguntas == 0 and self.datos == 0:
            raise ValueError("Cargá al menos una pregunta o una consulta de datos.")
        return self


def _user_id(conn: Any, email: str) -> Any:
    row = conn.execute(
        text("SELECT id FROM public.users WHERE LOWER(email) = LOWER(:e)"), {"e": email}
    ).first()
    if row is None:
        raise HTTPException(
            status_code=404,
            detail="No hay una cuenta con ese mail: la persona tiene que entrar a openarg.org primero.",
        )
    return row.id


def _iso(value: Any) -> str | None:
    return value.isoformat() if value else None


@router.get("/supporters", dependencies=[Depends(verify_admin_key)])
def list_supporters() -> dict[str, Any]:
    """Fundadores (vigentes y vencidos) y personas con créditos."""
    engine = get_sync_engine()
    try:
        with engine.connect() as conn:
            fundadores = conn.execute(
                text(
                    """
                    SELECT u.email, s.nivel, s.desde, s.hasta, s.origen, s.nota, s.created_by,
                           (s.hasta IS NULL OR s.hasta > NOW()) AS activo,
                           COALESCE(b.preguntas, 0) AS creditos_preguntas,
                           COALESCE(b.datos, 0) AS creditos_datos
                    FROM public.api_supporters s
                    JOIN public.users u ON u.id = s.user_id
                    LEFT JOIN public.api_credit_balances b ON b.user_id = s.user_id
                    ORDER BY activo DESC, s.hasta NULLS FIRST, u.email
                    """
                )
            ).fetchall()
            creditos = conn.execute(
                text(
                    """
                    SELECT u.email, b.preguntas, b.datos, b.updated_at
                    FROM public.api_credit_balances b
                    JOIN public.users u ON u.id = b.user_id
                    WHERE b.preguntas > 0 OR b.datos > 0
                    ORDER BY b.updated_at DESC
                    """
                )
            ).fetchall()
    finally:
        engine.dispose()
    return {
        "fundadores": [
            {
                "email": r.email,
                "nivel": r.nivel,
                "desde": _iso(r.desde),
                "hasta": _iso(r.hasta),
                "activo": bool(r.activo),
                "origen": r.origen,
                "nota": r.nota,
                "creado_por": r.created_by,
                "creditos": {"preguntas": r.creditos_preguntas, "datos": r.creditos_datos},
            }
            for r in fundadores
        ],
        "creditos": [
            {
                "email": r.email,
                "preguntas": r.preguntas,
                "datos": r.datos,
                "actualizado": _iso(r.updated_at),
            }
            for r in creditos
        ],
    }


@router.post("/supporters", dependencies=[Depends(verify_admin_key)])
def upsert_supporter(
    body: SupporterRequest,
    x_admin_actor: str | None = Header(default=None, alias="X-Admin-Actor"),
) -> dict[str, Any]:
    """Marca (o actualiza) un Fundador. Repetirlo no duplica nada."""
    hasta = datetime.combine(body.hasta, time.max, tzinfo=UTC) if body.hasta else None
    engine = get_sync_engine()
    try:
        with engine.begin() as conn:
            user_id = _user_id(conn, body.email)
            conn.execute(
                text(
                    """
                    INSERT INTO public.api_supporters (user_id, hasta, origen, nota, created_by)
                    VALUES (:u, :hasta, :origen, :nota, :actor)
                    ON CONFLICT (user_id) DO UPDATE
                    SET hasta = EXCLUDED.hasta, origen = EXCLUDED.origen,
                        nota = EXCLUDED.nota, created_by = EXCLUDED.created_by,
                        updated_at = NOW()
                    """
                ),
                {
                    "u": user_id,
                    "hasta": hasta,
                    "origen": body.origen or None,
                    "nota": body.nota or None,
                    "actor": _actor(x_admin_actor),
                },
            )
    finally:
        engine.dispose()
    return {"ok": True, "email": body.email, "hasta": _iso(hasta)}


@router.delete("/supporters/{email}", dependencies=[Depends(verify_admin_key)])
def delete_supporter(email: EmailStr) -> dict[str, Any]:
    engine = get_sync_engine()
    try:
        with engine.begin() as conn:
            user_id = _user_id(conn, email)
            deleted = conn.execute(
                text("DELETE FROM public.api_supporters WHERE user_id = :u"), {"u": user_id}
            ).rowcount
    finally:
        engine.dispose()
    if not deleted:
        raise HTTPException(status_code=404, detail="Esa persona no es Fundador.")
    return {"ok": True, "email": email}


@router.post("/credits", dependencies=[Depends(verify_admin_key)])
def grant_credits(
    body: CreditsRequest,
    x_admin_actor: str | None = Header(default=None, alias="X-Admin-Actor"),
) -> dict[str, Any]:
    """Carga créditos: suma al saldo y deja un movimiento por tipo, en una transacción.

    Con `referencia` (p. ej. el id del pago de Mercado Pago) la carga es
    idempotente: repetirla da 409 y no suma dos veces.
    """
    actor = _actor(x_admin_actor)
    engine = get_sync_engine()
    try:
        with engine.begin() as conn:
            user_id = _user_id(conn, body.email)
            if (
                body.referencia
                and conn.execute(
                    text(
                        "SELECT 1 FROM public.api_credit_movements WHERE motivo = :m AND referencia = :r LIMIT 1"
                    ),
                    {"m": body.motivo, "r": body.referencia},
                ).first()
            ):
                raise HTTPException(status_code=409, detail="Esa referencia ya se acreditó antes.")
            balance = conn.execute(
                text(
                    """
                    INSERT INTO public.api_credit_balances (user_id, preguntas, datos)
                    VALUES (:u, :p, :d)
                    ON CONFLICT (user_id) DO UPDATE
                    SET preguntas = api_credit_balances.preguntas + EXCLUDED.preguntas,
                        datos = api_credit_balances.datos + EXCLUDED.datos,
                        updated_at = NOW()
                    RETURNING preguntas, datos
                    """
                ),
                {"u": user_id, "p": body.preguntas, "d": body.datos},
            ).one()
            for tipo, delta in (("preguntas", body.preguntas), ("datos", body.datos)):
                if delta:
                    conn.execute(
                        text(
                            """
                            INSERT INTO public.api_credit_movements
                                (user_id, tipo, delta, motivo, referencia, created_by)
                            VALUES (:u, :t, :d, :m, :r, :a)
                            """
                        ),
                        {
                            "u": user_id,
                            "t": tipo,
                            "d": delta,
                            "m": body.motivo,
                            "r": body.referencia,
                            "a": actor,
                        },
                    )
    finally:
        engine.dispose()
    return {
        "ok": True,
        "email": body.email,
        "saldo": {"preguntas": balance.preguntas, "datos": balance.datos},
    }
