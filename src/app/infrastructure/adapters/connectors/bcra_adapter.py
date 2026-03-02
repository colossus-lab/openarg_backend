"""Adapter for BCRA public API — exchange rates and monetary variables."""
from __future__ import annotations

import logging
from datetime import UTC, datetime

import httpx

from app.domain.entities.connectors.data_result import DataResult
from app.infrastructure.resilience.retry import with_retry

logger = logging.getLogger(__name__)


class BCRAAdapter:
    """Adapter para API del BCRA — cotizaciones y variables monetarias."""

    BASE_URL = "https://api.bcra.gob.ar"

    def __init__(self) -> None:
        self._client: httpx.AsyncClient | None = None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=30.0,
                headers={
                    "User-Agent": "OpenArg/1.0",
                    "Authorization": "Bearer BCRA",
                },
            )
        return self._client

    @with_retry(max_retries=2)
    async def get_cotizaciones(
        self,
        moneda: str = "USD",
        fecha_desde: str | None = None,
        fecha_hasta: str | None = None,
    ) -> DataResult:
        """Get exchange rate quotes from BCRA."""
        client = self._get_client()
        url = f"{self.BASE_URL}/estadisticascambiarias/v1.0/Cotizaciones"
        params: dict[str, str] = {}
        if fecha_desde:
            params["fechaDesde"] = fecha_desde
        if fecha_hasta:
            params["fechaHasta"] = fecha_hasta
        if moneda:
            params["moneda"] = moneda

        resp = await client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()

        records = data.get("results", data) if isinstance(data, dict) else data
        if isinstance(records, dict):
            records = [records]

        return DataResult(
            source="bcra",
            portal_name="Banco Central de la República Argentina",
            portal_url="https://www.bcra.gob.ar",
            dataset_title=f"Cotizaciones Cambiarias - {moneda}",
            format="json",
            records=records if isinstance(records, list) else [],
            metadata={
                "fetched_at": datetime.now(UTC).isoformat(),
                "moneda": moneda,
            },
        )

    @with_retry(max_retries=2)
    async def get_principales_variables(self) -> DataResult:
        """Get main monetary variables from BCRA."""
        client = self._get_client()
        url = f"{self.BASE_URL}/estadisticas/v2.0/PrincipalesVariables"

        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()

        records = data.get("results", data) if isinstance(data, dict) else data
        if isinstance(records, dict):
            records = [records]

        return DataResult(
            source="bcra",
            portal_name="Banco Central de la República Argentina",
            portal_url="https://www.bcra.gob.ar",
            dataset_title="Principales Variables Monetarias",
            format="json",
            records=records if isinstance(records, list) else [],
            metadata={
                "fetched_at": datetime.now(UTC).isoformat(),
            },
        )

    @with_retry(max_retries=2)
    async def get_variable_historica(
        self,
        id_variable: int,
        fecha_desde: str,
        fecha_hasta: str,
    ) -> DataResult:
        """Get historical data for a specific BCRA variable."""
        client = self._get_client()
        url = f"{self.BASE_URL}/estadisticas/v2.0/DatosVariable/{id_variable}/{fecha_desde}/{fecha_hasta}"

        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()

        records = data.get("results", data) if isinstance(data, dict) else data
        if isinstance(records, dict):
            records = [records]

        return DataResult(
            source="bcra",
            portal_name="Banco Central de la República Argentina",
            portal_url="https://www.bcra.gob.ar",
            dataset_title=f"Variable BCRA #{id_variable}",
            format="json",
            records=records if isinstance(records, list) else [],
            metadata={
                "fetched_at": datetime.now(UTC).isoformat(),
                "id_variable": id_variable,
                "fecha_desde": fecha_desde,
                "fecha_hasta": fecha_hasta,
            },
        )

    async def search(self, query: str) -> DataResult:
        """Search BCRA data by returning main variables (no search API available)."""
        return await self.get_principales_variables()
