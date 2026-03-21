"""Connector: DDJJ (declaraciones juradas patrimoniales)."""

from __future__ import annotations

from typing import TYPE_CHECKING

from app.domain.entities.connectors.data_result import DataResult, PlanStep

if TYPE_CHECKING:
    from app.infrastructure.adapters.connectors.ddjj_adapter import DDJJAdapter


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
    elif action == "stats":
        result = ddjj.stats()
    elif action == "detail" or params.get("nombre"):
        result = ddjj.get_by_name(params.get("nombre", ""))
    else:
        result = ddjj.search(params.get("query", params.get("nombre", "")))

    return [result] if result.records else []
