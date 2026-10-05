"""Los límites que el MCP le anuncia a los clientes son los que aplica el backend.

El contenedor del MCP no importa `app` (sólo biblioteca estándar y `mcp`), así
que los números están escritos dos veces: en `mcp_publico/core.py`, para las
`instructions` del servidor, y en el backend, que es el que los hace cumplir.
Este test corre en el job de unit tests, que tiene los dos.
"""

from __future__ import annotations

import pytest
from mcp_publico import core

from app.application.answers.aggregates import MAX_LIMIT as MAX_AGG_LIMIT
from app.application.api_key_service import CATALOG_MINUTE_LIMIT, PLAN_LIMITS
from app.application.public_catalog import MAX_LIMIT
from app.application.public_quota import founder_tier, free_tier


@pytest.fixture(autouse=True)
def _defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "PUBLIC_API_MONTHLY_PREGUNTAS",
        "PUBLIC_API_MONTHLY_DATOS",
        "PUBLIC_API_FOUNDER_PREGUNTAS",
        "PUBLIC_API_FOUNDER_DATOS",
    ):
        monkeypatch.delenv(name, raising=False)


def test_los_limites_del_mcp_son_los_del_backend() -> None:
    free, founder = free_tier(), founder_tier(None)
    assert core.LIMITE_DATOS_POR_MINUTO == CATALOG_MINUTE_LIMIT
    assert core.LIMITE_DATOS_POR_MES == free.datos
    assert core.LIMITE_DATOS_FUNDADOR == founder.datos
    assert core.LIMITE_PREGUNTAS_POR_MINUTO == PLAN_LIMITS["free"]["per_min"]
    assert core.LIMITE_PREGUNTAS_POR_MES == free.preguntas
    assert core.LIMITE_PREGUNTAS_FUNDADOR == founder.preguntas
    assert core.LIMITE_FILAS_POR_PEDIDO == MAX_LIMIT


def test_el_tope_de_grupos_de_agregar_datos_es_el_del_backend() -> None:
    """`agregar_datos` acota `limite` antes de mandarlo; el backend rechaza más."""
    assert core.LIMITE_GRUPOS_AGREGAR == MAX_AGG_LIMIT
