from __future__ import annotations

from abc import ABC, abstractmethod

from app.domain.entities.connectors.data_result import DataResult


class ISeriesTiempoConnector(ABC):
    @abstractmethod
    async def search(self, query: str, limit: int = 10) -> list[dict]:
        """Search for available time series by keyword."""
        ...

    @abstractmethod
    async def fetch(
        self,
        series_ids: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
        collapse: str | None = None,
        representation: str | None = None,
        limit: int = 1000,
        collapse_aggregation: str | None = None,
    ) -> DataResult | None:
        """Fetch time series data and return as DataResult.

        Returns the MOST RECENT ``limit`` observations of the requested range,
        in ascending order. ``collapse_aggregation`` (avg, sum, end_of_period,
        max, min) says how ``collapse`` aggregates; the API averages by
        default, which is wrong for flows such as exports (a yearly total is a
        sum). The result's metadata carries the freshness contract
        (``ultima_observacion``, ``frecuencia``, ``fecha_fin_fuente``,
        ``actualizada_en_fuente``, ``total_fuente``, ``truncada``, ``unidad``,
        ``oficial``).
        """
        ...
