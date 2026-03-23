"""Connector: DDJJ (declaraciones juradas patrimoniales)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from app.domain.entities.connectors.data_result import DataResult, PlanStep

if TYPE_CHECKING:
    from app.infrastructure.adapters.connectors.ddjj_adapter import DDJJAdapter


def _not_found_result(searched_name: str) -> DataResult:
    """Return an informative DataResult when a person is not found in the DDJJ dataset.

    This ensures the analyst gets explicit "not found" info instead of
    receiving zero results and hallucinating that data exists.
    """
    return DataResult(
        source="ddjj:oficina_anticorrupcion",
        portal_name="Declaraciones Juradas Patrimoniales — Oficina Anticorrupción",
        portal_url="https://www.argentina.gob.ar/anticorrupcion/consultar-declaraciones-juradas-de-funcionarios-publicos",
        dataset_title=f'DDJJ de "{searched_name}" — NO ENCONTRADO',
        format="json",
        records=[
            {
                "nombre_buscado": searched_name,
                "resultado": "NO ENCONTRADO",
                "nota": (
                    f'No se encontró a "{searched_name}" en el dataset de DDJJ. '
                    "Este dataset contiene ÚNICAMENTE las 195 declaraciones juradas "
                    "de Diputados Nacionales (ejercicio 2024). No incluye senadores, "
                    "ex-presidentes, gobernadores ni otros funcionarios."
                ),
            }
        ],
        metadata={
            "total_records": 0,
            "fetched_at": datetime.now(UTC).isoformat(),
            "description": (
                f"Búsqueda de '{searched_name}' sin resultados. "
                "El dataset solo cubre Diputados Nacionales."
            ),
            "not_found": True,
        },
    )


def execute_ddjj_step(
    step: PlanStep,
    ddjj: DDJJAdapter,
) -> list[DataResult]:
    params = step.params
    action = params.get("action", "search")

    if action == "ranking":
        result = ddjj.ranking(
            sort_by=params.get("sortBy", "patrimonio"),
            top=params.get("top", 10),
            order=params.get("order", "desc"),
        )
        position = params.get("position")
        if position and result.records and len(result.records) >= position:
            result = DataResult(
                source=result.source,
                portal_name=result.portal_name,
                portal_url=result.portal_url,
                dataset_title=result.dataset_title,
                format=result.format,
                records=[result.records[position - 1]],
                metadata={**result.metadata, "position": position},
            )
        return [result] if result.records else []

    if action == "stats":
        result = ddjj.stats()
        return [result] if result.records else []

    # Name-based searches: return explicit "not found" result so the analyst
    # can tell the user *why* (dataset scope) instead of hallucinating.
    searched_name = params.get("nombre", params.get("query", ""))
    if action == "detail" or params.get("nombre"):
        result = ddjj.get_by_name(params.get("nombre", ""))
    else:
        result = ddjj.search(params.get("query", params.get("nombre", "")))

    if result.records:
        return [result]

    # Person not found — return an informative result instead of empty list
    if searched_name:
        return [_not_found_result(searched_name)]
    return []
