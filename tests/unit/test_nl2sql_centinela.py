"""La fila centinela de seguridad del NL2SQL no es un dato.

Caso Pinamar (staging, 2026-10-01): ya con el estudio de discapacidad del
INDEC en el universo, el NL2SQL no pudo filtrar por Pinamar (la encuesta es
nacional, sin columnas geográficas) y respondió con la salida que
`nl2sql.txt` reserva para intentos de manipulación:
`SELECT 'operación no permitida' AS error`. Esa fila llegaba como resultado:
el estudio quedaba listado como fuente y el analista le habló al usuario de un
"error de acceso".
"""

from __future__ import annotations

from typing import Any

from app.application.pipeline.subgraphs.nl2sql import (
    _is_refusal,
    format_result_node,
    nl2sql_runtime,
)
from app.domain.ports.sandbox.sql_sandbox import SandboxResult

_TABLE = "raw.datos_gob_ar__estudio_nacional_sobre_el_perfil_de__08ca74a9__v1"


class _Sandbox:
    async def get_table_sources(self, _names: list[str]) -> dict:
        return {}

    async def execute_readonly(self, _sql: str, timeout_seconds: int = 10) -> SandboxResult:
        return SandboxResult(columns=[], rows=[], row_count=0, truncated=False)


def _result(rows: list[dict[str, Any]]) -> SandboxResult:
    return SandboxResult(
        columns=list(rows[0]) if rows else [], rows=rows, row_count=len(rows), truncated=False
    )


async def _format(result: SandboxResult, sql: str) -> Any:
    state = {"nl_query": "¿Cuántas personas con discapacidad hay en Pinamar?", "generated_sql": sql}
    state["result"] = result
    with nl2sql_runtime(llm=None, sandbox=_Sandbox(), embedding=None, semantic_cache=None):
        out = await format_result_node(state)  # type: ignore[arg-type]
    return out["data_results"][0]


def test_reconoce_la_fila_centinela() -> None:
    assert _is_refusal(_result([{"error": "operación no permitida"}]))
    assert _is_refusal(_result([{"error": " Operación no permitida "}]))
    # Un dato que se llama "error" no es la centinela, ni una fila con más columnas.
    assert not _is_refusal(_result([{"error": "3,2 %"}]))
    assert not _is_refusal(_result([{"error": "operación no permitida", "n": 1}]))
    assert not _is_refusal(_result([{"error": "operación no permitida"}] * 2))
    assert not _is_refusal(_result([]))


async def test_la_centinela_sale_como_consulta_fallida_y_no_como_dato() -> None:
    dr = await _format(
        _result([{"error": "operación no permitida"}]), "SELECT 'operación no permitida' AS error"
    )

    assert dr.records == []
    assert dr.metadata["error"] == "nl2sql_refused"
    # Sin filas no es fuente (finalize._extract_sources) ni dato principal.
    assert "Estudio" not in dr.dataset_title


async def test_un_resultado_normal_no_cambia() -> None:
    dr = await _format(
        _result([{"nivel_geografico": "nacional", "personas": 3675564}]),
        f"SELECT 'nacional' AS nivel_geografico, SUM(pondera) AS personas FROM {_TABLE}",
    )

    assert dr.records == [{"nivel_geografico": "nacional", "personas": 3675564}]
    assert "error" not in dr.metadata


def test_el_prompt_reserva_la_centinela_para_manipulacion() -> None:
    from app.prompts import load_prompt

    # Con las mismas variables que el código: si el texto nuevo trajera llaves
    # sueltas, `.format()` rompería acá igual que en producción.
    prompt = load_prompt("nl2sql", tables_context="(tablas)", few_shot_block="")
    assert "NEVER use it because the tables cannot answer the exact question" in prompt
    assert "MISSING GEOGRAPHIC LEVEL" in prompt
    assert "nivel_geografico" in prompt
