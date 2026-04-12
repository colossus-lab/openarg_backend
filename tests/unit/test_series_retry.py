"""Unit tests for the series connector retry-without-date-range path.

FR-009 / FIX-013: when a catalog-matched fetch returns empty AND the
request carried an explicit date range, ``execute_series_step`` retries
the fetch once without ``start_date``/``end_date``. This handles
questions about months the upstream API has not yet published.
"""

from __future__ import annotations

from typing import Any

import pytest

from app.application.pipeline.connectors.series import execute_series_step
from app.domain.entities.connectors.data_result import DataResult, PlanStep
from app.domain.exceptions.connector_errors import ConnectorError


class _FakeSeriesConnector:
    """Records fetch() calls and returns pre-programmed responses."""

    def __init__(self, responses: list[DataResult | None]) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    async def search(self, query: str, limit: int = 10) -> list[dict]:
        return []

    async def fetch(
        self,
        series_ids: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
        collapse: str | None = None,
        representation: str | None = None,
        limit: int = 1000,
    ) -> DataResult | None:
        self.calls.append(
            {
                "series_ids": list(series_ids),
                "start_date": start_date,
                "end_date": end_date,
                "collapse": collapse,
                "representation": representation,
            }
        )
        if not self._responses:
            return None
        return self._responses.pop(0)


def _data_result(ids: list[str]) -> DataResult:
    return DataResult(
        source="series_tiempo",
        portal_name="Series de Tiempo",
        portal_url=f"https://apis.datos.gob.ar/series/api/series?ids={','.join(ids)}",
        dataset_title="IPC Nacional",
        format="time_series",
        records=[{"fecha": "2026-01-01", "valor": 2.88}],
        metadata={"total_records": 1, "unit": "percent"},
    )


def _inflation_step(*, start_date: str | None, end_date: str | None) -> PlanStep:
    return PlanStep(
        id="step_1",
        action="query_series",
        params={
            "query": "inflacion mensual",
            "startDate": start_date,
            "endDate": end_date,
        },
        description="Obtener IPC mensual",
    )


@pytest.mark.asyncio
async def test_retry_triggers_when_fetch_returns_empty_with_date_range() -> None:
    """FR-009 happy path: first fetch empty (narrow range) → retry wide → hit."""
    connector = _FakeSeriesConnector(responses=[None, _data_result(["148.3_INIVELNAL_DICI_M_26"])])
    step = _inflation_step(start_date="2026-02-01", end_date="2026-02-28")

    results = await execute_series_step(step, connector)  # type: ignore[arg-type]

    assert len(results) == 1
    assert results[0].records[0]["valor"] == 2.88
    assert len(connector.calls) == 2
    # First call carried the narrow range
    assert connector.calls[0]["start_date"] == "2026-02-01"
    assert connector.calls[0]["end_date"] == "2026-02-28"
    # Retry cleared start/end
    assert connector.calls[1]["start_date"] is None
    assert connector.calls[1]["end_date"] is None


@pytest.mark.asyncio
async def test_retry_not_triggered_when_first_fetch_succeeds() -> None:
    """If the first fetch already returned data, do not retry."""
    connector = _FakeSeriesConnector(responses=[_data_result(["148.3_INIVELNAL_DICI_M_26"])])
    step = _inflation_step(start_date="2026-01-01", end_date="2026-01-31")

    await execute_series_step(step, connector)  # type: ignore[arg-type]

    assert len(connector.calls) == 1


@pytest.mark.asyncio
async def test_retry_not_triggered_when_no_original_date_range() -> None:
    """A fetch with no user-supplied date range hits once and raises if empty."""
    connector = _FakeSeriesConnector(responses=[None])
    step = PlanStep(
        id="step_1",
        action="query_series",
        params={"query": "inflacion"},
        description="Obtener IPC",
    )

    with pytest.raises(ConnectorError):
        await execute_series_step(step, connector)  # type: ignore[arg-type]

    # Only one call — the retry did not fire because the user did not
    # ask for a specific range.
    assert len(connector.calls) == 1


@pytest.mark.asyncio
async def test_raises_when_both_fetches_return_empty() -> None:
    """If even the wide fetch is empty, the step raises ConnectorError."""
    connector = _FakeSeriesConnector(responses=[None, None])
    step = _inflation_step(start_date="2026-02-01", end_date="2026-02-28")

    with pytest.raises(ConnectorError):
        await execute_series_step(step, connector)  # type: ignore[arg-type]

    assert len(connector.calls) == 2
