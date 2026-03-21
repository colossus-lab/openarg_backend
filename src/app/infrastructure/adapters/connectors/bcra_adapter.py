"""Adapter for BCRA public API — exchange rates and monetary variables."""

from __future__ import annotations

import logging
from datetime import UTC, datetime

import httpx

from app.domain.entities.connectors.data_result import DataResult
from app.domain.exceptions.connector_errors import ConnectorError
from app.domain.exceptions.error_codes import ErrorCode
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
        moneda: str | None = None,
        fecha_desde: str | None = None,
        fecha_hasta: str | None = None,
    ) -> DataResult:
        """Get exchange rate quotes from BCRA.

        The API no longer accepts the ``moneda`` query-parameter — calling
        ``/Cotizaciones`` without it returns *all* currencies for the latest
        date.  We filter client-side when ``moneda`` is provided.
        """
        try:
            client = self._get_client()
            url = f"{self.BASE_URL}/estadisticascambiarias/v1.0/Cotizaciones"
            params: dict[str, str] = {}
            if fecha_desde:
                params["fechaDesde"] = fecha_desde
            if fecha_hasta:
                params["fechaHasta"] = fecha_hasta

            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            results = data.get("results", data) if isinstance(data, dict) else data

            # The API returns {"fecha": "...", "detalle": [...]}
            if isinstance(results, dict) and "detalle" in results:
                records = results["detalle"]
                if moneda:
                    records = [r for r in records if r.get("codigoMoneda") == moneda]
            elif isinstance(results, list):
                records = results
            else:
                records = [results] if results else []

            return DataResult(
                source="bcra",
                portal_name="Banco Central de la República Argentina",
                portal_url="https://www.bcra.gob.ar",
                dataset_title=f"Cotizaciones Cambiarias{f' - {moneda}' if moneda else ''}",
                format="json",
                records=records if isinstance(records, list) else [],
                metadata={
                    "fetched_at": datetime.now(UTC).isoformat(),
                    "moneda": moneda or "todas",
                },
            )
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(
                error_code=ErrorCode.CN_BCRA_UNAVAILABLE,
                details={"action": "get_cotizaciones", "reason": str(exc)},
            ) from exc

    @with_retry(max_retries=2)
    async def get_principales_variables(self) -> DataResult:
        """Get main monetary variables from BCRA.

        Returns all exchange-rate master data (``/Maestros/Divisas``) since
        the former ``/estadisticas/v2.0/PrincipalesVariables`` endpoint was
        deprecated.
        """
        try:
            client = self._get_client()
            # v2 PrincipalesVariables was deprecated — use Maestros/Divisas + Cotizaciones
            url = f"{self.BASE_URL}/estadisticascambiarias/v1.0/Cotizaciones"

            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()

            results = data.get("results", data) if isinstance(data, dict) else data
            if isinstance(results, dict) and "detalle" in results:
                records = results["detalle"]
            elif isinstance(results, list):
                records = results
            else:
                records = [results] if results else []

            return DataResult(
                source="bcra",
                portal_name="Banco Central de la República Argentina",
                portal_url="https://www.bcra.gob.ar",
                dataset_title="Cotizaciones Cambiarias — Todas las monedas",
                format="json",
                records=records if isinstance(records, list) else [],
                metadata={
                    "fetched_at": datetime.now(UTC).isoformat(),
                },
            )
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(
                error_code=ErrorCode.CN_BCRA_UNAVAILABLE,
                details={"action": "get_principales_variables", "reason": str(exc)},
            ) from exc

    @with_retry(max_retries=2)
    async def get_variable_historica(
        self,
        id_variable: int,
        fecha_desde: str,
        fecha_hasta: str,
    ) -> DataResult:
        """Get historical data for a specific BCRA variable."""
        try:
            client = self._get_client()
            url = f"{self.BASE_URL}/estadisticascambiarias/v1.0/Cotizaciones"
            params = {"fechaDesde": fecha_desde, "fechaHasta": fecha_hasta}

            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            results = data.get("results", data) if isinstance(data, dict) else data
            if isinstance(results, dict) and "detalle" in results:
                records = results["detalle"]
            elif isinstance(results, list):
                records = results
            else:
                records = [results] if results else []

            return DataResult(
                source="bcra",
                portal_name="Banco Central de la República Argentina",
                portal_url="https://www.bcra.gob.ar",
                dataset_title=f"Cotizaciones BCRA ({fecha_desde} a {fecha_hasta})",
                format="json",
                records=records if isinstance(records, list) else [],
                metadata={
                    "fetched_at": datetime.now(UTC).isoformat(),
                    "id_variable": id_variable,
                    "fecha_desde": fecha_desde,
                    "fecha_hasta": fecha_hasta,
                },
            )
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(
                error_code=ErrorCode.CN_BCRA_UNAVAILABLE,
                details={"action": "get_variable_historica", "reason": str(exc)},
            ) from exc

    async def search(self, query: str) -> DataResult:
        """Search BCRA data by returning all current quotes."""
        try:
            return await self.get_cotizaciones()
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(
                error_code=ErrorCode.CN_BCRA_UNAVAILABLE,
                details={"action": "search", "reason": str(exc)},
            ) from exc
