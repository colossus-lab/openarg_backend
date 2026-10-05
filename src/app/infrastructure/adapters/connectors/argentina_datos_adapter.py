"""ArgentinaDatos y DolarApi: agregadores no oficiales de cotizaciones.

Los dos son del mismo autor y no son fuentes oficiales. DolarApi sirve el
último valor (la casa "oficial" es la pizarra del Banco Nación) y
ArgentinaDatos la historia. El dólar oficial del BCRA sale de
``BCRAAdapter.get_variable`` (variables 4 y 5 de la API v4); estos resultados
llevan ``metadata["oficial"] = False`` y lo dicen en el título.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, date, datetime, timedelta, timezone
from typing import Any

import httpx

from app.domain.entities.connectors.data_result import DataResult
from app.domain.exceptions.connector_errors import ConnectorError
from app.domain.exceptions.error_codes import ErrorCode
from app.domain.ports.connectors.argentina_datos import IArgentinaDatosConnector

logger = logging.getLogger(__name__)

# Argentina no tiene horario de verano desde 2009.
_AR = timezone(timedelta(hours=-3), "ART")

ARGENTINA_DATOS_BASE_URL = "https://api.argentinadatos.com/v1"
DOLARAPI_BASE_URL = "https://dolarapi.com/v1"

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


def _today_ar() -> date:
    return datetime.now(_AR).date()


def _drop_future(items: list[Any], today: date) -> list[Any]:
    """Saca las filas con fecha posterior a hoy (en Argentina).

    El histórico de ArgentinaDatos trae días que todavía no llegaron,
    rellenados con el último valor: el domingo 04-oct terminaba en el lunes
    05-oct a 1.490/1.540. Con esas filas el modelo podía presentar
    "05/10/2026" como el último dato. Una fila sin fecha legible se deja: la
    descarta quien la lee.
    """
    kept: list[Any] = []
    for d in items:
        raw = (d.get("fechaActualizacion") or d.get("fecha")) if isinstance(d, dict) else None
        try:
            if raw and date.fromisoformat(str(raw)[:10]) > today:
                continue
        except ValueError:
            pass
        kept.append(d)
    return kept


def _dolar_title(casa: str | None, ultimo: bool) -> str:
    """El título de la fuente: dice de dónde sale y que no es oficial."""
    if casa == "oficial":
        if ultimo:
            return "Pizarra del Banco Nación vía DolarApi (no oficial)"
        return "Pizarra del Banco Nación, histórico vía ArgentinaDatos (no oficial)"
    via = "DolarApi" if ultimo else "ArgentinaDatos"
    label = f"Dólar {casa.capitalize()}" if casa else "Dólar todas las casas"
    kind = "actual" if ultimo else "histórica"
    return f"Cotización {kind} {label} vía {via} (no oficial)"


class ArgentinaDatosAdapter(IArgentinaDatosConnector):
    def __init__(
        self, http_client: httpx.AsyncClient, today: Callable[[], date] | None = None
    ) -> None:
        self._http = http_client
        self._today = today or _today_ar

    async def fetch_dolar(
        self,
        casa: str | None = None,
        ultimo: bool = False,
    ) -> DataResult | None:
        try:
            if casa and casa.lower() not in _ALLOWED_CASAS:
                raise ConnectorError(
                    error_code=ErrorCode.CN_ARGENTINA_DATOS_UNAVAILABLE,
                    details={
                        "action": "fetch_dolar",
                        "reason": f"Invalid casa: {casa}. Allowed: {sorted(_ALLOWED_CASAS)}",
                    },
                )
            if ultimo:
                path = f"/dolares/{casa}" if casa else "/dolares"
                resp = await self._http.get(f"{DOLARAPI_BASE_URL}{path}")
            else:
                path = f"/cotizaciones/dolares/{casa}" if casa else "/cotizaciones/dolares"
                resp = await self._http.get(f"{ARGENTINA_DATOS_BASE_URL}{path}")
            resp.raise_for_status()
            data = resp.json()

            items = [data] if isinstance(data, dict) else data
            if not items or not isinstance(items, list):
                return None

            items = _drop_future(items, self._today())
            recent = items if ultimo else items[-60:]
            records = []
            for d in recent:
                fecha = d.get("fechaActualizacion") or d.get("fecha")
                if not fecha:
                    continue
                records.append(
                    {
                        "fecha": fecha,
                        "casa": d.get("casa", casa or ""),
                        "compra": d.get("compra"),
                        "venta": d.get("venta"),
                        "nombre": d.get("nombre"),
                    }
                )
            if not records:
                return None

            casa_label = casa.capitalize() if casa else "todas las casas"
            source = "dolarapi" if ultimo else "argentina_datos"
            portal_name = (
                "DolarApi (agregador no oficial)"
                if ultimo
                else "ArgentinaDatos (agregador no oficial)"
            )
            portal_url = "https://dolarapi.com" if ultimo else "https://argentinadatos.com"
            description = (
                f"Cotización actual del dólar {casa_label}"
                if ultimo
                else f"Cotización histórica del dólar {casa_label}"
            )
            if casa == "oficial":
                description += (
                    ": pizarra del Banco Nación, no la referencia oficial del BCRA "
                    "(minorista promedio vendedor y mayorista Com. A 3500)"
                )
            return DataResult(
                source=source,
                portal_name=portal_name,
                portal_url=portal_url,
                dataset_title=_dolar_title(casa, ultimo),
                format="time_series",
                records=records,
                metadata={
                    "total_records": len(records),
                    "fetched_at": datetime.now(UTC).isoformat(),
                    "description": description,
                    "last_updated": records[-1]["fecha"],
                    "ultima_observacion": max(str(r["fecha"])[:10] for r in records),
                    "frecuencia": "diaria",
                    "realtime": ultimo,
                    "oficial": False,
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
            resp = await self._http.get(f"{ARGENTINA_DATOS_BASE_URL}{path}")
            resp.raise_for_status()
            data = resp.json()

            items = [data] if isinstance(data, dict) else data
            if not items:
                return None

            recent = _drop_future(items, self._today())[-60:]
            records = [{"fecha": d["fecha"], "riesgo_pais": d["valor"]} for d in recent]
            if not records:
                return None

            return DataResult(
                source="argentina_datos",
                portal_name="ArgentinaDatos (agregador no oficial)",
                portal_url="https://argentinadatos.com",
                dataset_title="Riesgo País — EMBI+ Argentina",
                format="time_series",
                records=records,
                metadata={
                    "total_records": len(records),
                    "fetched_at": datetime.now(UTC).isoformat(),
                    "description": "Índice de Riesgo País (EMBI+ Argentina, puntos básicos)",
                    "ultima_observacion": str(records[-1]["fecha"])[:10],
                    "frecuencia": "diaria",
                    "oficial": False,
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
            resp = await self._http.get(f"{ARGENTINA_DATOS_BASE_URL}/finanzas/indices/inflacion")
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
