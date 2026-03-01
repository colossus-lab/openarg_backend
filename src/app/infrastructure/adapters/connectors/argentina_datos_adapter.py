from __future__ import annotations

import logging
from datetime import UTC, datetime

from app.domain.entities.connectors.data_result import DataResult
from app.domain.exceptions.connector_errors import ConnectorError
from app.domain.exceptions.error_codes import ErrorCode
from app.domain.ports.connectors.argentina_datos import IArgentinaDatosConnector
from app.infrastructure.mcp.exceptions import MCPServerError
from app.infrastructure.mcp.mcp_client import MCPClient

logger = logging.getLogger(__name__)


class ArgentinaDatosAdapter(IArgentinaDatosConnector):
    def __init__(self, mcp_client: MCPClient) -> None:
        self._mcp = mcp_client

    async def fetch_dolar(self, casa: str | None = None) -> DataResult | None:
        try:
            params: dict = {}
            if casa:
                params["casa"] = casa
            result = await self._mcp.call_tool(
                "argentina_datos", "get_dolar", params
            )
            records = result.get("records", [])
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
                    "total_records": result.get("total_records", len(records)),
                    "fetched_at": datetime.now(UTC).isoformat(),
                    "description": f"Cotización histórica del dólar {casa_label}",
                },
            )
        except MCPServerError as exc:
            raise ConnectorError(
                error_code=ErrorCode.CN_ARGENTINA_DATOS_UNAVAILABLE,
                details={"action": "fetch_dolar", "reason": str(exc)},
            ) from exc
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(
                error_code=ErrorCode.CN_ARGENTINA_DATOS_UNAVAILABLE,
                details={"action": "fetch_dolar", "reason": str(exc)},
            ) from exc

    async def fetch_riesgo_pais(self, ultimo: bool = False) -> DataResult | None:
        try:
            result = await self._mcp.call_tool(
                "argentina_datos", "get_riesgo_pais", {"ultimo": ultimo}
            )
            records = result.get("records", [])
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
                    "total_records": result.get("total_records", len(records)),
                    "fetched_at": datetime.now(UTC).isoformat(),
                    "description": "Índice de Riesgo País (EMBI+ Argentina, puntos básicos)",
                },
            )
        except MCPServerError as exc:
            raise ConnectorError(
                error_code=ErrorCode.CN_ARGENTINA_DATOS_UNAVAILABLE,
                details={"action": "fetch_riesgo_pais", "reason": str(exc)},
            ) from exc
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(
                error_code=ErrorCode.CN_ARGENTINA_DATOS_UNAVAILABLE,
                details={"action": "fetch_riesgo_pais", "reason": str(exc)},
            ) from exc

    async def fetch_inflacion(self) -> DataResult | None:
        try:
            result = await self._mcp.call_tool(
                "argentina_datos", "get_inflacion", {}
            )
            records = result.get("records", [])
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
                    "total_records": result.get("total_records", len(records)),
                    "fetched_at": datetime.now(UTC).isoformat(),
                    "description": "Inflación mensual (variación % IPC)",
                },
            )
        except MCPServerError as exc:
            raise ConnectorError(
                error_code=ErrorCode.CN_ARGENTINA_DATOS_UNAVAILABLE,
                details={"action": "fetch_inflacion", "reason": str(exc)},
            ) from exc
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(
                error_code=ErrorCode.CN_ARGENTINA_DATOS_UNAVAILABLE,
                details={"action": "fetch_inflacion", "reason": str(exc)},
            ) from exc
