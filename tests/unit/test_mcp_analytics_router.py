"""Tablero de uso del MCP (`admin/mcp_analytics_router.py`).

El SQL se prueba contra una base real antes de desplegar; acá se cubre lo que
no depende de Postgres: que todas las rutas pidan la clave de admin, la forma
de cada respuesta y el costo configurable.
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import Any

import pytest

from app.presentation.http.controllers.admin import mcp_analytics_router as mod


@pytest.fixture
def rows(monkeypatch: pytest.MonkeyPatch):
    """Reemplaza `_rows` por respuestas fijas, en orden de llamada."""
    queue: list[list[dict[str, Any]]] = []
    seen: list[str] = []

    def fake(sql: str, params: dict | None = None) -> list[dict[str, Any]]:
        seen.append(sql)
        return queue.pop(0)

    monkeypatch.setattr(mod, "_rows", fake)
    return queue, seen


def test_every_route_requires_the_admin_key() -> None:
    for route in mod.router.routes:
        deps = [d.dependency.__name__ for d in route.dependencies]  # type: ignore[attr-defined]
        assert "verify_admin_key" in deps, route.path  # type: ignore[attr-defined]


def test_old_rows_get_their_mode_from_the_endpoint() -> None:
    """Antes de la migración 0062 sólo se registraba `/ask`."""
    assert "'/api/v1/ask' THEN 'respuestas' ELSE 'datos'" in mod._MODE


def test_global_cap_503_is_a_rejection_but_a_data_mode_503_is_an_error() -> None:
    assert "status_code = 503 AND mode = 'respuestas'" in mod._REJECTED
    assert "NOT" in mod._ERROR


@pytest.mark.parametrize(
    ("env", "expected"), [(None, 0.034), ("0.05", 0.05), ("nada", 0.034), ("-1", 0.034)]
)
def test_cost_per_answer(monkeypatch: pytest.MonkeyPatch, env: str | None, expected: float) -> None:
    if env is None:
        monkeypatch.delenv("PUBLIC_API_COST_PER_ANSWER_USD", raising=False)
    else:
        monkeypatch.setenv("PUBLIC_API_COST_PER_ANSWER_USD", env)
    assert mod.cost_per_answer_usd() == expected


def test_overview_shape(rows, monkeypatch: pytest.MonkeyPatch) -> None:
    queue, _ = rows
    monkeypatch.delenv("PUBLIC_API_COST_PER_ANSWER_USD", raising=False)
    queue.append(
        [
            {
                "pedidos_datos": 120,
                "preguntas": 11,
                "preguntas_ok": 10,
                "errores": 1,
                "rechazos": 3,
                "claves_activas": 4,
                "tokens": 5000,
                "p50_datos_ms": 850.0,
                "p95_datos_ms": 1900.5,
                "p50_respuestas_ms": None,
                "p95_respuestas_ms": None,
            }
        ]
    )
    queue.append(
        [
            {
                "claves_total": 9,
                "claves_nuevas": 5,
                "activas_1d": 2,
                "activas_7d": 4,
                "activas_30d": 6,
                "claves_recurrentes": 3,
                "preguntas_hoy": 7,
            }
        ]
    )
    result = mod.overview(days=30)

    assert result["uso"]["pedidos_datos"] == 120
    assert result["adopcion"]["claves_recurrentes"] == 3
    assert result["salud"]["p50_datos_ms"] == 850
    assert result["salud"]["p95_datos_ms"] == 1900.5
    assert result["salud"]["p50_respuestas_ms"] is None
    assert result["cupo_global_hoy"]["usado"] == 7
    assert result["costo"]["estimado_usd"] == 0.34


def test_overview_on_an_empty_database(rows) -> None:
    queue, _ = rows
    queue.append(
        [
            dict.fromkeys(
                (
                    "pedidos_datos",
                    "preguntas",
                    "preguntas_ok",
                    "errores",
                    "rechazos",
                    "claves_activas",
                    "tokens",
                    "p50_datos_ms",
                    "p95_datos_ms",
                    "p50_respuestas_ms",
                    "p95_respuestas_ms",
                )
            )
        ]
    )
    queue.append(
        [
            dict.fromkeys(
                (
                    "claves_total",
                    "claves_nuevas",
                    "activas_1d",
                    "activas_7d",
                    "activas_30d",
                    "claves_recurrentes",
                    "preguntas_hoy",
                )
            )
        ]
    )
    result = mod.overview(days=7)
    assert result["uso"]["preguntas"] == 0
    assert result["costo"]["estimado_usd"] == 0


def test_timeline_serialises_the_day(rows) -> None:
    queue, _ = rows
    queue.append(
        [
            {
                "dia": date(2026, 9, 29),
                "claves_nuevas": 1,
                "claves_activas": 2,
                "pedidos_datos": 30,
                "preguntas": 3,
                "rechazos": 0,
                "errores": None,
            }
        ]
    )
    assert mod.timeline(days=7) == [
        {
            "dia": "2026-09-29",
            "claves_nuevas": 1,
            "claves_activas": 2,
            "pedidos_datos": 30,
            "preguntas": 3,
            "rechazos": 0,
            "errores": 0,
        }
    ]


def test_keys_serialise_dates(rows) -> None:
    queue, _ = rows
    when = datetime(2026, 9, 29, 12, 0, tzinfo=UTC)
    queue.append(
        [
            {
                "email": "alguien@example.com",
                "key_prefix": "oarg_sk_abcd",
                "activa": True,
                "alta": when,
                "ultimo_uso": None,
                "pedidos_datos": 40,
                "preguntas": 2,
                "rechazos": 1,
                "dias_activos": 2,
            }
        ]
    )
    [row] = mod.top_keys(days=30, limit=20)
    assert row["alta"] == when.isoformat()
    assert row["ultimo_uso"] is None
    assert row["email"] == "alguien@example.com"


def test_users_lists_keys_without_usage_too(rows) -> None:
    """La lista parte de `api_keys`, no del registro: una clave sin uso aparece."""
    queue, seen = rows
    when = datetime(2026, 9, 29, 12, 0, tzinfo=UTC)
    queue.append(
        [
            {
                "email": "usa@example.com",
                "nombre": "Usa",
                "key_prefix": "oarg_sk_abcd",
                "activa": True,
                "claves": 1,
                "alta": when,
                "ultimo_uso": when,
                "datos_mes": 12,
                "preguntas_mes": 3,
                "pedidos_total": 80,
                "rechazos_total": 1,
                "fundador_hasta": datetime(2027, 3, 31, 23, 59, tzinfo=UTC),
                "fundador": True,
                "creditos_preguntas": 0,
                "creditos_datos": 5,
            },
            {
                "email": "nunca@example.com",
                "nombre": None,
                "key_prefix": "oarg_sk_efgh",
                "activa": True,
                "claves": 2,
                "alta": when,
                "ultimo_uso": None,
                "datos_mes": 0,
                "preguntas_mes": 0,
                "pedidos_total": 0,
                "rechazos_total": 0,
                "fundador_hasta": None,
                "fundador": False,
                "creditos_preguntas": 0,
                "creditos_datos": 0,
            },
        ]
    )
    usa, nunca = mod.users(limit=1000)
    assert usa["ultimo_uso"] == when.isoformat()
    assert usa["fundador_hasta"].startswith("2027-03-31")
    assert nunca["ultimo_uso"] is None and nunca["fundador_hasta"] is None

    sql = seen[0]
    assert "FROM k\n" in sql and "LEFT JOIN uso" in sql
    # Tablas nombradas con esquema: el search_path por defecto es `raw, public`.
    for table in ("api_usage", "api_keys", "users", "api_supporters", "api_credit_balances"):
        assert f"public.{table}" in sql
    # El último uso no depende sólo de last_used_at, que quedó vacío en claves usadas.
    assert "GREATEST(uso.ultimo_registro, k.last_used_at)" in sql


def test_questions_only_look_at_the_answers_mode(rows) -> None:
    queue, seen = rows
    when = datetime(2026, 9, 29, tzinfo=UTC)
    queue.append([{"pregunta": "badlar", "veces": 3, "respondidas": 3, "ultima": when}])
    queue.append([])
    result = mod.questions(days=30)
    assert result["top"][0]["ultima"] == when.isoformat()
    assert all("mode = 'respuestas'" in sql for sql in seen)


def test_breakdown_shape(rows) -> None:
    queue, _ = rows
    queue.append(
        [
            {
                "herramienta": "buscar_datasets",
                "modo": "datos",
                "pedidos": 5,
                "errores": 0,
                "rechazos": 0,
                "p50_ms": 700.0,
                "p95_ms": None,
            }
        ]
    )
    queue.append([{"cliente": "claude-code", "pedidos": 5, "claves": 1}])
    queue.append([{"via": "mcp", "pedidos": 5}])
    result = mod.breakdown(days=30)
    assert result["herramientas"][0]["p50_ms"] == 700
    assert result["clientes"][0]["cliente"] == "claude-code"
    assert result["vias"] == [{"via": "mcp", "pedidos": 5}]
