"""Admin: uso de la API pública y del MCP (mcp.openarg.org), sobre `api_usage`.

Protegido con `X-Admin-Key` como el resto de `/admin`, y Caddy bloquea
`/api/v1/admin/*` desde internet: lo consume el tablero de openarg.org/admin/mcp
a través de un proxy del frontend que corre en la red interna.

Filas anteriores a la migración 0062 no tienen `mode`: se infiere del endpoint.
Los días son UTC, que es cuando se renuevan los cupos (21:00 en Argentina).
"""

from __future__ import annotations

import os
from typing import Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text

from app.application.api_key_service import global_free_daily_cap
from app.infrastructure.celery.tasks._db import get_sync_engine
from app.presentation.http.controllers.admin.tasks_router import verify_admin_key

router = APIRouter(prefix="/admin/analytics/mcp", tags=["admin-analytics"])

_DEFAULT_COST_PER_ANSWER_USD = 0.034  # medido en CloudWatch, septiembre 2026

# `mode` explícito, o inferido para las filas viejas (sólo `/ask` se registraba).
_MODE = "COALESCE(u.mode, CASE WHEN u.endpoint = '/api/v1/ask' THEN 'respuestas' ELSE 'datos' END)"
# Rechazo por cupo: 429 (por minuto o IP), 402 (cupo del mes sin créditos) y
# el 503 del modo respuestas, que es el tope global. En el modo datos un 503 es
# una falla (p. ej. la búsqueda sin embeddings), no un rechazo.
_REJECTED = "(status_code IN (402, 429) OR (status_code = 503 AND mode = 'respuestas'))"
_ERROR = f"((status_code >= 500 OR status_code = 408) AND NOT {_REJECTED})"
_EXECUTED = f"(NOT {_REJECTED})"

# Base común: el uso del período, con el modo resuelto.
_WINDOW = f"""
    WITH w AS (
        SELECT u.api_key_id, u.endpoint, u.question, u.status_code, u.tokens_used,
               u.duration_ms, u.tool, u.via, u.client, u.created_at,
               {_MODE} AS mode
        FROM api_usage u
        WHERE u.created_at > NOW() - make_interval(days => :days)
    )
"""


def _days() -> Any:
    return Query(30, ge=1, le=365, description="Ventana en días")


def cost_per_answer_usd() -> float:
    try:
        value = float(os.getenv("PUBLIC_API_COST_PER_ANSWER_USD", ""))
    except ValueError:
        return _DEFAULT_COST_PER_ANSWER_USD
    return value if value >= 0 else _DEFAULT_COST_PER_ANSWER_USD


def _rows(sql: str, params: dict | None = None) -> list[dict[str, Any]]:
    """Run a read-only SELECT and return rows as dicts."""
    engine = get_sync_engine()
    try:
        with engine.connect() as conn:
            res = conn.execute(text(sql), params or {})
            cols = list(res.keys())
            return [dict(zip(cols, row, strict=False)) for row in res.fetchall()]
    finally:
        engine.dispose()


def _num(value: Any) -> float | int | None:
    """Decimal de Postgres a número JSON; None se queda None."""
    if value is None:
        return None
    as_float = float(value)
    return int(as_float) if as_float.is_integer() else round(as_float, 2)


@router.get("/overview", dependencies=[Depends(verify_admin_key)])
def overview(days: int = _days()) -> dict[str, Any]:
    """KPIs de adopción, uso, salud y costo del período."""
    usage = _rows(
        _WINDOW
        + f"""
        SELECT
            COUNT(*) FILTER (WHERE mode = 'datos' AND {_EXECUTED})       AS pedidos_datos,
            COUNT(*) FILTER (WHERE mode = 'respuestas' AND {_EXECUTED})  AS preguntas,
            COUNT(*) FILTER (WHERE mode = 'respuestas' AND status_code = 200)
                                                                          AS preguntas_ok,
            COUNT(*) FILTER (WHERE {_ERROR})                              AS errores,
            COUNT(*) FILTER (WHERE {_REJECTED})                           AS rechazos,
            COUNT(DISTINCT api_key_id) FILTER (WHERE {_EXECUTED})         AS claves_activas,
            COALESCE(SUM(tokens_used), 0)                                 AS tokens,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms)
                FILTER (WHERE mode = 'datos' AND status_code = 200)       AS p50_datos_ms,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms)
                FILTER (WHERE mode = 'datos' AND status_code = 200)       AS p95_datos_ms,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms)
                FILTER (WHERE mode = 'respuestas' AND status_code = 200)  AS p50_respuestas_ms,
            percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms)
                FILTER (WHERE mode = 'respuestas' AND status_code = 200)  AS p95_respuestas_ms
        FROM w
        """,
        {"days": days},
    )[0]

    keys = _rows(
        f"""
        SELECT
            (SELECT COUNT(*) FROM api_keys WHERE is_active)                AS claves_total,
            (SELECT COUNT(*) FROM api_keys
              WHERE created_at > NOW() - make_interval(days => :days))    AS claves_nuevas,
            (SELECT COUNT(DISTINCT api_key_id) FROM api_usage u
              WHERE u.created_at > NOW() - INTERVAL '1 day'
                AND u.status_code NOT IN (402, 429, 503))     AS activas_1d,
            (SELECT COUNT(DISTINCT api_key_id) FROM api_usage u
              WHERE u.created_at > NOW() - INTERVAL '7 days'
                AND u.status_code NOT IN (402, 429, 503))     AS activas_7d,
            (SELECT COUNT(DISTINCT api_key_id) FROM api_usage u
              WHERE u.created_at > NOW() - INTERVAL '30 days'
                AND u.status_code NOT IN (402, 429, 503))     AS activas_30d,
            (SELECT COUNT(*) FROM (
                SELECT api_key_id FROM api_usage u
                 WHERE u.created_at > NOW() - make_interval(days => :days)
                 GROUP BY api_key_id
                HAVING COUNT(DISTINCT (u.created_at AT TIME ZONE 'UTC')::date) >= 2
            ) r)                                                           AS claves_recurrentes,
            (SELECT COUNT(*) FROM api_usage u
              WHERE (u.created_at AT TIME ZONE 'UTC')::date = (NOW() AT TIME ZONE 'UTC')::date
                AND {_MODE} = 'respuestas'
                AND u.status_code NOT IN (402, 429, 503))     AS preguntas_hoy
        """,
        {"days": days},
    )[0]

    cost = cost_per_answer_usd()
    preguntas_ok = int(usage["preguntas_ok"] or 0)
    return {
        "days": days,
        "adopcion": {
            k: int(keys[k] or 0)
            for k in (
                "claves_total",
                "claves_nuevas",
                "activas_1d",
                "activas_7d",
                "activas_30d",
                "claves_recurrentes",
            )
        },
        "uso": {
            "pedidos_datos": int(usage["pedidos_datos"] or 0),
            "preguntas": int(usage["preguntas"] or 0),
            "preguntas_ok": preguntas_ok,
            "claves_activas": int(usage["claves_activas"] or 0),
            "tokens": int(usage["tokens"] or 0),
        },
        "salud": {
            "errores": int(usage["errores"] or 0),
            "rechazos": int(usage["rechazos"] or 0),
            "p50_datos_ms": _num(usage["p50_datos_ms"]),
            "p95_datos_ms": _num(usage["p95_datos_ms"]),
            "p50_respuestas_ms": _num(usage["p50_respuestas_ms"]),
            "p95_respuestas_ms": _num(usage["p95_respuestas_ms"]),
        },
        "cupo_global_hoy": {
            "usado": int(keys["preguntas_hoy"] or 0),
            "tope": global_free_daily_cap(),
        },
        "costo": {
            "estimado_usd": round(preguntas_ok * cost, 2),
            "usd_por_respuesta": cost,
            "nota": "Estimado. El costo real está en CloudWatch (AWS/Bedrock).",
        },
    }


@router.get("/timeline", dependencies=[Depends(verify_admin_key)])
def timeline(days: int = _days()) -> list[dict[str, Any]]:
    """Una fila por día UTC, con días en cero incluidos."""
    rows = _rows(
        _WINDOW
        + f""",
        d AS (
            SELECT generate_series(
                ((NOW() AT TIME ZONE 'UTC') - make_interval(days => :days - 1))::date,
                (NOW() AT TIME ZONE 'UTC')::date,
                INTERVAL '1 day'
            )::date AS dia
        ),
        uso AS (
            SELECT (created_at AT TIME ZONE 'UTC')::date AS dia,
                   COUNT(*) FILTER (WHERE mode = 'datos' AND {_EXECUTED})      AS pedidos_datos,
                   COUNT(*) FILTER (WHERE mode = 'respuestas' AND {_EXECUTED}) AS preguntas,
                   COUNT(*) FILTER (WHERE {_REJECTED})                          AS rechazos,
                   COUNT(*) FILTER (WHERE {_ERROR})                             AS errores,
                   COUNT(DISTINCT api_key_id) FILTER (WHERE {_EXECUTED})        AS claves_activas
            FROM w GROUP BY 1
        ),
        altas AS (
            SELECT (created_at AT TIME ZONE 'UTC')::date AS dia, COUNT(*) AS claves_nuevas
            FROM api_keys
            WHERE created_at > NOW() - make_interval(days => :days)
            GROUP BY 1
        )
        SELECT d.dia,
               COALESCE(a.claves_nuevas, 0)   AS claves_nuevas,
               COALESCE(u.claves_activas, 0)  AS claves_activas,
               COALESCE(u.pedidos_datos, 0)   AS pedidos_datos,
               COALESCE(u.preguntas, 0)       AS preguntas,
               COALESCE(u.rechazos, 0)        AS rechazos,
               COALESCE(u.errores, 0)         AS errores
        FROM d
        LEFT JOIN uso u   ON u.dia = d.dia
        LEFT JOIN altas a ON a.dia = d.dia
        ORDER BY d.dia
        """,
        {"days": days},
    )
    return [
        {**{k: int(v or 0) for k, v in r.items() if k != "dia"}, "dia": r["dia"].isoformat()}
        for r in rows
    ]


@router.get("/breakdown", dependencies=[Depends(verify_admin_key)])
def breakdown(days: int = _days()) -> dict[str, Any]:
    """Por herramienta (volumen, errores, latencia) y por cliente y vía."""
    tools = _rows(
        _WINDOW
        + f"""
        SELECT COALESCE(tool, CASE WHEN mode = 'respuestas'
                                   THEN 'consultar_datos_publicos' ELSE endpoint END) AS herramienta,
               mode AS modo,
               COUNT(*) FILTER (WHERE {_EXECUTED}) AS pedidos,
               COUNT(*) FILTER (WHERE {_ERROR})    AS errores,
               COUNT(*) FILTER (WHERE {_REJECTED}) AS rechazos,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms)
                   FILTER (WHERE status_code = 200) AS p50_ms,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms)
                   FILTER (WHERE status_code = 200) AS p95_ms
        FROM w GROUP BY 1, 2 ORDER BY 3 DESC
        """,
        {"days": days},
    )
    clients = _rows(
        _WINDOW
        + f"""
        SELECT COALESCE(client, 'sin dato') AS cliente,
               COUNT(*) FILTER (WHERE {_EXECUTED})                       AS pedidos,
               COUNT(DISTINCT api_key_id) FILTER (WHERE {_EXECUTED})     AS claves
        FROM w GROUP BY 1 ORDER BY 2 DESC
        """,
        {"days": days},
    )
    vias = _rows(
        _WINDOW
        + f"""
        SELECT COALESCE(via, 'sin dato') AS via,
               COUNT(*) FILTER (WHERE {_EXECUTED}) AS pedidos
        FROM w GROUP BY 1 ORDER BY 2 DESC
        """,
        {"days": days},
    )
    return {
        "days": days,
        "herramientas": [
            {**t, "p50_ms": _num(t["p50_ms"]), "p95_ms": _num(t["p95_ms"])} for t in tools
        ],
        "clientes": clients,
        "vias": vias,
    }


@router.get("/keys", dependencies=[Depends(verify_admin_key)])
def top_keys(days: int = _days(), limit: int = Query(20, ge=1, le=100)) -> list[dict[str, Any]]:
    """Las claves que más usaron el servicio en el período."""
    rows = _rows(
        _WINDOW
        + f"""
        SELECT us.email,
               k.key_prefix,
               k.is_active                                             AS activa,
               k.created_at                                            AS alta,
               k.last_used_at                                          AS ultimo_uso,
               COUNT(*) FILTER (WHERE w.mode = 'datos' AND {_EXECUTED})      AS pedidos_datos,
               COUNT(*) FILTER (WHERE w.mode = 'respuestas' AND {_EXECUTED}) AS preguntas,
               COUNT(*) FILTER (WHERE {_REJECTED})                            AS rechazos,
               COUNT(DISTINCT (w.created_at AT TIME ZONE 'UTC')::date)        AS dias_activos
        FROM w
        JOIN api_keys k ON k.id = w.api_key_id
        JOIN users us   ON us.id = k.user_id
        GROUP BY us.email, k.key_prefix, k.is_active, k.created_at, k.last_used_at
        ORDER BY COUNT(*) DESC
        LIMIT :limit
        """,
        {"days": days, "limit": limit},
    )
    return [
        {
            **r,
            "alta": r["alta"].isoformat() if r["alta"] else None,
            "ultimo_uso": r["ultimo_uso"].isoformat() if r["ultimo_uso"] else None,
        }
        for r in rows
    ]


@router.get("/questions", dependencies=[Depends(verify_admin_key)])
def questions(days: int = _days()) -> dict[str, Any]:
    """Qué pregunta la gente (sólo el modo respuestas guarda el texto)."""
    top = _rows(
        _WINDOW
        + f"""
        SELECT LOWER(TRIM(question))                   AS pregunta,
               COUNT(*)                                AS veces,
               COUNT(*) FILTER (WHERE status_code = 200) AS respondidas,
               MAX(created_at)                         AS ultima
        FROM w
        WHERE mode = 'respuestas' AND question IS NOT NULL AND {_EXECUTED}
        GROUP BY 1 ORDER BY 2 DESC, 4 DESC
        LIMIT 30
        """,
        {"days": days},
    )
    failed = _rows(
        _WINDOW
        + f"""
        SELECT w.question AS pregunta, w.status_code AS estado, w.duration_ms,
               w.created_at AS fecha, k.key_prefix
        FROM w JOIN api_keys k ON k.id = w.api_key_id
        WHERE w.mode = 'respuestas' AND w.status_code <> 200 AND {_EXECUTED}
        ORDER BY w.created_at DESC
        LIMIT 30
        """,
        {"days": days},
    )
    return {
        "days": days,
        "top": [{**r, "ultima": r["ultima"].isoformat()} for r in top],
        "fallidas": [{**r, "fecha": r["fecha"].isoformat()} for r in failed],
    }
