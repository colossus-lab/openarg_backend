"""Connector: Staff (personal de Cámaras legislativas)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from app.domain.entities.connectors.data_result import DataResult, PlanStep

if TYPE_CHECKING:
    from app.domain.ports.connectors.staff import IStaffConnector

logger = logging.getLogger(__name__)


async def execute_staff_step(
    step: PlanStep,
    staff: IStaffConnector | None,
) -> list[DataResult]:
    if not staff:
        logger.warning("Staff adapter not configured, skipping step %s", step.id)
        return []
    params = step.params
    action = params.get("action", "search")
    name = params.get("name", "")

    if (action in ("get_by_legislator", "count") and name) or name:
        result = await staff.get_by_legislator(name)
    elif action == "changes":
        result = await staff.get_changes(name=name or None)
    elif action == "stats":
        result = await staff.stats()
    else:
        result = await staff.search(params.get("query", name or step.description))

    return [result] if result.records else []
