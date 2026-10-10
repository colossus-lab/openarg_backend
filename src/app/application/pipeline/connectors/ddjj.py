"""Connector: DDJJ (declaraciones juradas patrimoniales)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING

from app.domain.entities.connectors.data_result import DataResult, PlanStep

if TYPE_CHECKING:
    from app.infrastructure.adapters.connectors.ddjj_adapter import DDJJAdapter


def _not_found_result(searched_name: str, cobertura: str) -> DataResult:
    """Return an informative DataResult when a person is not found in the DDJJ tables.

    This ensures the analyst gets explicit "not found" info instead of
    receiving zero results and hallucinating that data exists. The coverage
    comes from the tables themselves, so it stays right when a new year lands.
    """
    return DataResult(
        source="ddjj:todas",
        portal_name="Declaraciones Juradas Patrimoniales",
        portal_url="https://datos.jus.gob.ar/dataset/declaraciones-juradas-patrimoniales-integrales",
        dataset_title=f'DDJJ de "{searched_name}" — NO ENCONTRADO',
        format="json",
        records=[
            {
                "nombre_buscado": searched_name,
                "resultado": "NO ENCONTRADO",
                "nota": (
                    f'No se encontró a "{searched_name}" en las declaraciones juradas cargadas: '
                    f"{cobertura}. No incluye jueces en general ni funcionarios provinciales."
                ),
            }
        ],
        metadata={
            "total_records": 0,
            "fetched_at": datetime.now(UTC).isoformat(),
            "description": f"Búsqueda de '{searched_name}' sin resultados. Cobertura: {cobertura}.",
            "not_found": True,
        },
    )


async def execute_ddjj_step(
    step: PlanStep,
    ddjj: DDJJAdapter,
) -> list[DataResult]:
    params = step.params
    action = params.get("action", "search")
    anio = params.get("anio") or params.get("year")

    if action == "ranking":
        result = await ddjj.ranking(
            sort_by=params.get("sortBy", "patrimonio"),
            top=params.get("top", 10),
            order=params.get("order", "desc"),
            anio=anio,
            cargo=params.get("cargo"),
            organismo=params.get("organismo"),
            altos_cargos=params.get("altos_cargos") is True,
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
        result = await ddjj.stats(
            anio=anio, cargo=params.get("cargo"), altos_cargos=params.get("altos_cargos") is True
        )
        return [result] if result.records else []

    # Name-based searches: return explicit "not found" result so the analyst
    # can tell the user *why* (dataset scope) instead of hallucinating.
    searched_name = params.get("nombre", params.get("query", ""))
    if action == "detail" or params.get("nombre"):
        result = await ddjj.get_by_name(params.get("nombre", ""))
    else:
        result = await ddjj.search(params.get("query", params.get("nombre", "")))

    if result.records:
        return [result]

    # Person not found — return an informative result instead of empty list
    if searched_name:
        cobertura = ddjj.describir_cobertura(await ddjj.cobertura())
        return [_not_found_result(searched_name, cobertura)]
    return []
