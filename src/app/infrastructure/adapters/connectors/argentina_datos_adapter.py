from __future__ import annotations

import logging
from datetime import UTC, datetime

import httpx

from app.domain.entities.connectors.data_result import DataResult
from app.domain.ports.connectors.argentina_datos import IArgentinaDatosConnector
from app.infrastructure.resilience.retry import with_retry

logger = logging.getLogger(__name__)


class ArgentinaDatosAdapter(IArgentinaDatosConnector):
    def __init__(self, base_url: str = "https://api.argentinadatos.com/v1") -> None:
        self._base_url = base_url
        self._client = httpx.AsyncClient(
            timeout=10.0,
            headers={"User-Agent": "OpenArg/1.0"},
        )

    @with_retry(max_retries=2, base_delay=1.0, service_name="argentina_datos")
    async def _get_json_internal(self, path: str) -> list | dict:
        resp = await self._client.get(f"{self._base_url}{path}")
        resp.raise_for_status()
        return resp.json()

    async def _get_json(self, path: str) -> list | dict | None:
        try:
            return await self._get_json_internal(path)
        except Exception:
            logger.warning("ArgentinaDatos request failed: %s", path, exc_info=True)
            return None

    async def fetch_dolar(self, casa: str | None = None) -> DataResult | None:
        path = f"/cotizaciones/dolares/{casa}" if casa else "/cotizaciones/dolares"
        data = await self._get_json(path)
        if not data or not isinstance(data, list):
            return None

        recent = data[-60:]
        casa_label = casa.capitalize() if casa else "todas las casas"
        return DataResult(
            source="argentina_datos",
            portal_name="ArgentinaDatos API",
            portal_url="https://argentinadatos.com",
            dataset_title=f"Cotización Dólar {casa_label}",
            format="time_series",
            records=[
                {"fecha": d["fecha"], "casa": d.get("casa", casa or ""), "compra": d.get("compra"), "venta": d.get("venta")}
                for d in recent
            ],
            metadata={
                "total_records": len(recent),
                "fetched_at": datetime.now(UTC).isoformat(),
                "description": f"Cotización histórica del dólar {casa_label}",
            },
        )

    async def fetch_riesgo_pais(self, ultimo: bool = False) -> DataResult | None:
        path = "/finanzas/indices/riesgo-pais/ultimo" if ultimo else "/finanzas/indices/riesgo-pais"
        data = await self._get_json(path)
        if data is None:
            return None

        # /ultimo returns a single object, normalize to list
        items = [data] if isinstance(data, dict) else data
        if not items:
            return None

        recent = items[-60:]
        return DataResult(
            source="argentina_datos",
            portal_name="ArgentinaDatos API",
            portal_url="https://argentinadatos.com",
            dataset_title="Riesgo País — EMBI+ Argentina",
            format="time_series",
            records=[{"fecha": d["fecha"], "riesgo_pais": d["valor"]} for d in recent],
            metadata={
                "total_records": len(recent),
                "fetched_at": datetime.now(UTC).isoformat(),
                "description": "Índice de Riesgo País (EMBI+ Argentina, puntos básicos)",
            },
        )

    async def fetch_inflacion(self) -> DataResult | None:
        data = await self._get_json("/finanzas/indices/inflacion")
        if not data or not isinstance(data, list):
            return None

        return DataResult(
            source="argentina_datos",
            portal_name="ArgentinaDatos API",
            portal_url="https://argentinadatos.com",
            dataset_title="Inflación Mensual — ArgentinaDatos",
            format="time_series",
            records=[{"fecha": d["fecha"], "inflacion": d["valor"]} for d in data],
            metadata={
                "total_records": len(data),
                "fetched_at": datetime.now(UTC).isoformat(),
                "description": "Inflación mensual (variación % IPC)",
            },
        )
