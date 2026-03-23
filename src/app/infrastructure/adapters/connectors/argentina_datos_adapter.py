from __future__ import annotations

import logging
from datetime import UTC, datetime

import httpx

from app.domain.entities.connectors.data_result import DataResult
from app.domain.exceptions.connector_errors import ConnectorError
from app.domain.exceptions.error_codes import ErrorCode
from app.domain.ports.connectors.argentina_datos import IArgentinaDatosConnector

logger = logging.getLogger(__name__)

BASE_URL = "https://api.argentinadatos.com/v1"

# Allowlist of valid exchange rate types (SEC-06 audit fix)
_ALLOWED_CASAS = frozenset(
    {
        "oficial",
        "blue",
        "bolsa",
        "contadoconliqui",
        "cripto",
        "mayorista",
        "solidario",
        "tarjeta",
    }
)


class ArgentinaDatosAdapter(IArgentinaDatosConnector):
    def __init__(self, http_client: httpx.AsyncClient) -> None:
        self._http = http_client

    async def fetch_dolar(self, casa: str | None = None) -> DataResult | None:
        try:
            if casa and casa.lower() not in _ALLOWED_CASAS:
                raise ConnectorError(
                    error_code=ErrorCode.CN_ARGENTINA_DATOS_UNAVAILABLE,
                    details={
                        "action": "fetch_dolar",
                        "reason": f"Invalid casa: {casa}. Allowed: {sorted(_ALLOWED_CASAS)}",
                    },
                )
            path = f"/cotizaciones/dolares/{casa}" if casa else "/cotizaciones/dolares"
            resp = await self._http.get(f"{BASE_URL}{path}")
            resp.raise_for_status()
            data = resp.json()

            if not data or not isinstance(data, list):
                return None

            recent = data[-60:]
            records = [
                {
                    "fecha": d["fecha"],
                    "casa": d.get("casa", casa or ""),
                    "compra": d.get("compra"),
                    "venta": d.get("venta"),
                }
                for d in recent
            ]
            if not records:
                return None

            casa_label = casa.capitalize() if casa else "todas las casas"
            return DataResult(
                source="argentina_datos",
                portal_name="ArgentinaDatos API",
                portal_url="https://argentinadatos.com",
                dataset_title=f"Cotización Dólar {casa_label}",
                format="time_series",
                records=records,
                metadata={
                    "total_records": len(records),
                    "fetched_at": datetime.now(UTC).isoformat(),
                    "description": f"Cotización histórica del dólar {casa_label}",
                },
            )
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(
                error_code=ErrorCode.CN_ARGENTINA_DATOS_UNAVAILABLE,
                details={"action": "fetch_dolar", "reason": str(exc)},
            ) from exc

    async def fetch_riesgo_pais(self, ultimo: bool = False) -> DataResult | None:
        try:
            path = (
                "/finanzas/indices/riesgo-pais/ultimo"
                if ultimo
                else "/finanzas/indices/riesgo-pais"
            )
            resp = await self._http.get(f"{BASE_URL}{path}")
            resp.raise_for_status()
            data = resp.json()

            items = [data] if isinstance(data, dict) else data
            if not items:
                return None

            recent = items[-60:]
            records = [{"fecha": d["fecha"], "riesgo_pais": d["valor"]} for d in recent]
            if not records:
                return None

            return DataResult(
                source="argentina_datos",
                portal_name="ArgentinaDatos API",
                portal_url="https://argentinadatos.com",
                dataset_title="Riesgo País — EMBI+ Argentina",
                format="time_series",
                records=records,
                metadata={
                    "total_records": len(records),
                    "fetched_at": datetime.now(UTC).isoformat(),
                    "description": "Índice de Riesgo País (EMBI+ Argentina, puntos básicos)",
                },
            )
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(
                error_code=ErrorCode.CN_ARGENTINA_DATOS_UNAVAILABLE,
                details={"action": "fetch_riesgo_pais", "reason": str(exc)},
            ) from exc

    async def fetch_inflacion(self) -> DataResult | None:
        try:
            resp = await self._http.get(f"{BASE_URL}/finanzas/indices/inflacion")
            resp.raise_for_status()
            data = resp.json()

            if not data or not isinstance(data, list):
                return None

            records = [{"fecha": d["fecha"], "inflacion": d["valor"]} for d in data]
            if not records:
                return None

            return DataResult(
                source="argentina_datos",
                portal_name="ArgentinaDatos API",
                portal_url="https://argentinadatos.com",
                dataset_title="Inflación Mensual — ArgentinaDatos",
                format="time_series",
                records=records,
                metadata={
                    "total_records": len(records),
                    "fetched_at": datetime.now(UTC).isoformat(),
                    "description": "Inflación mensual (variación % IPC)",
                },
            )
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(
                error_code=ErrorCode.CN_ARGENTINA_DATOS_UNAVAILABLE,
                details={"action": "fetch_inflacion", "reason": str(exc)},
            ) from exc
