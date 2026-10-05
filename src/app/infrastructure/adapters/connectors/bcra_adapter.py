"""Adapter de la API pública del BCRA: cotizaciones y variables monetarias.

Son dos APIs, las dos sin token:

- **Estadísticas cambiarias v1.0** (``/estadisticascambiarias/v1.0``): las
  cotizaciones de unas 39 monedas por día. La fecha viene en la cabecera
  (``results.fecha``) y se copia a cada registro; antes se descartaba y la
  tabla del snapshot no decía de qué día era cada cotización (ítem 2.4 de la
  auditoría del 04-oct). Los parámetros válidos son ``?fecha=`` para un día y
  ``/Cotizaciones/{moneda}?fechadesde=&fechahasta=`` para la historia; los
  ``fechaDesde``/``fechaHasta`` que se mandaban daban 400.
- **Estadísticas monetarias v4.0** (``/estadisticas/v4.0/monetarias``):
  reservas, tipo de cambio minorista y mayorista de referencia (Com. A 3500),
  tasas, base monetaria. Devuelve las observaciones de la más nueva a la más
  vieja, paginadas con ``limit`` (máximo 3000) y ``offset``. La v2 y la v3
  responden 410.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, date, datetime
from typing import Any

import httpx

from app.domain.entities.connectors.data_result import DataResult
from app.domain.exceptions.connector_errors import ConnectorError
from app.domain.exceptions.error_codes import ErrorCode
from app.domain.ports.connectors.bcra import IBCRAConnector
from app.infrastructure.resilience.retry import with_retry

logger = logging.getLogger(__name__)

PORTAL_NAME = "Banco Central de la República Argentina"
PORTAL_URL = "https://www.bcra.gob.ar"
VARIABLES_URL = "https://www.bcra.gob.ar/principales-variables/"

# Tope de observaciones por pedido a la v4 (lo dice la propia API: "El límite
# no puede superar los 3000 registros") y tope total de una consulta paginada.
# La serie diaria más larga del catálogo curado (reservas, desde 1996) tiene
# unas 7.600 observaciones.
V4_PAGE_LIMIT = 3000
V4_MAX_ROWS = 10_000
# Cotizaciones históricas: la API exige un `limit` entre 10 y 1000.
CAMBIARIAS_PAGE_LIMIT = 1000
CAMBIARIAS_MAX_ROWS = 20_000
# El catálogo de la v4 cambia poco (descripciones, unidades). Se cachea para
# no pedir 1.600 variables en cada consulta del agente.
CATALOG_TTL_S = 6 * 3600

_FREQUENCIES = {
    "D": "diaria",
    "S": "semanal",
    "M": "mensual",
    "T": "trimestral",
    "A": "anual",
}


def _iso_date(value: str | None, name: str) -> str | None:
    """``value`` como AAAA-MM-DD, o ConnectorError si no es una fecha ISO.

    La v4 interpreta en silencio formatos que no son ISO ("01/09/2026" lo leyó
    como otra fecha): mejor no mandárselos.
    """
    if value is None or value == "":
        return None
    try:
        return date.fromisoformat(str(value)[:10]).isoformat()
    except ValueError:
        raise ConnectorError(
            error_code=ErrorCode.CN_BCRA_UNAVAILABLE,
            details={"reason": f"{name} no es una fecha AAAA-MM-DD: {value!r}"},
        ) from None


class BCRAAdapter(IBCRAConnector):
    """Adapter para la API del BCRA — cotizaciones y variables monetarias."""

    BASE_URL = "https://api.bcra.gob.ar"

    def __init__(self, client: httpx.AsyncClient | None = None) -> None:
        self._client: httpx.AsyncClient | None = client
        self._catalog: list[dict[str, Any]] | None = None
        self._catalog_at = 0.0
        self._catalog_lock: asyncio.Lock | None = None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=20.0,
                headers={
                    "User-Agent": "OpenArg/1.0",
                    "Authorization": "Bearer BCRA",
                },
            )
        return self._client

    @with_retry(max_retries=2, service_name="bcra_api")
    async def _get_json(self, url: str, params: dict[str, str] | None = None) -> Any:
        """GET con reintentos ante 5xx/429 y cortes de red. Un 400 no se reintenta."""
        resp = await self._get_client().get(url, params=params or None)
        resp.raise_for_status()
        return resp.json()

    # ── cotizaciones (estadísticas cambiarias v1.0) ─────────────

    async def get_cotizaciones(
        self,
        moneda: str | None = None,
        fecha_desde: str | None = None,
        fecha_hasta: str | None = None,
    ) -> DataResult:
        """Cotizaciones del BCRA, cada una con la fecha del día publicado.

        - Sin fechas: el último día publicado, todas las monedas (filtradas por
          ``moneda`` del lado nuestro: la API ya no acepta ese parámetro acá).
        - Con un rango y una moneda: la historia de esa moneda.
        - Con una fecha y sin moneda: ese día, todas las monedas. Un sábado o un
          feriado no tiene cotización y vuelve vacío.
        """
        try:
            desde = _iso_date(fecha_desde, "fecha_desde")
            hasta = _iso_date(fecha_hasta, "fecha_hasta")
            if moneda and (desde or hasta):
                return await self.get_cotizaciones_historicas(
                    moneda, desde or hasta or "", hasta or desde or ""
                )

            params: dict[str, str] = {}
            dia = hasta or desde
            if dia:
                params["fecha"] = dia
            data = await self._get_json(
                f"{self.BASE_URL}/estadisticascambiarias/v1.0/Cotizaciones", params
            )

            results = data.get("results", data) if isinstance(data, dict) else data
            fecha: str | None = None
            # La API devuelve {"fecha": "AAAA-MM-DD", "detalle": [...]}.
            if isinstance(results, dict) and "detalle" in results:
                fecha = results.get("fecha")
                records = [{"fecha": fecha, **r} for r in (results.get("detalle") or [])]
            elif isinstance(results, list):
                records = list(results)
            else:
                records = [results] if results else []
            if moneda:
                records = [r for r in records if r.get("codigoMoneda") == moneda]

            metadata: dict[str, Any] = {
                "fetched_at": datetime.now(UTC).isoformat(),
                "moneda": moneda or "todas",
                "total_records": len(records),
                "oficial": True,
                "frecuencia": "diaria",
            }
            if fecha:
                metadata["last_updated"] = fecha
                metadata["ultima_observacion"] = fecha
            return DataResult(
                source="bcra",
                portal_name=PORTAL_NAME,
                portal_url=PORTAL_URL,
                dataset_title=f"Cotizaciones Cambiarias{f' - {moneda}' if moneda else ''}",
                format="json",
                records=records,
                metadata=metadata,
            )
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(
                error_code=ErrorCode.CN_BCRA_UNAVAILABLE,
                details={"action": "get_cotizaciones", "reason": str(exc)},
            ) from exc

    async def get_cotizaciones_historicas(self, moneda: str, desde: str, hasta: str) -> DataResult:
        """Historia diaria de una moneda, en orden cronológico."""
        try:
            code = (moneda or "").strip().upper()
            if not code.isalnum() or len(code) > 5:
                raise ConnectorError(
                    error_code=ErrorCode.CN_BCRA_UNAVAILABLE,
                    details={"reason": f"moneda inválida: {moneda!r}"},
                )
            start = _iso_date(desde, "desde")
            end = _iso_date(hasta, "hasta")
            if not start or not end:
                raise ConnectorError(
                    error_code=ErrorCode.CN_BCRA_UNAVAILABLE,
                    details={"reason": "la historia pide desde y hasta"},
                )
            url = f"{self.BASE_URL}/estadisticascambiarias/v1.0/Cotizaciones/{code}"
            records: list[dict[str, Any]] = []
            offset = 0
            total: int | None = None
            while len(records) < CAMBIARIAS_MAX_ROWS:
                data = await self._get_json(
                    url,
                    {
                        "fechadesde": start,
                        "fechahasta": end,
                        "limit": str(CAMBIARIAS_PAGE_LIMIT),
                        "offset": str(offset),
                    },
                )
                if total is None:
                    total = _resultset_count(data)
                days = data.get("results") if isinstance(data, dict) else None
                if not days:
                    break
                for day in days:
                    for r in day.get("detalle") or []:
                        records.append({"fecha": day.get("fecha"), **r})
                offset += len(days)
                if total is not None and offset >= total:
                    break
            records.sort(key=lambda r: str(r.get("fecha") or ""))
            last = records[-1]["fecha"] if records else None
            return DataResult(
                source="bcra",
                portal_name=PORTAL_NAME,
                portal_url=PORTAL_URL,
                dataset_title=f"Cotizaciones Cambiarias - {code}",
                format="time_series",
                records=records,
                metadata={
                    "fetched_at": datetime.now(UTC).isoformat(),
                    "moneda": code,
                    "total_records": len(records),
                    "total_fuente": total,
                    "truncada": total is not None and len(records) < total,
                    "last_updated": last,
                    "ultima_observacion": last,
                    "oficial": True,
                    "frecuencia": "diaria",
                },
            )
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(
                error_code=ErrorCode.CN_BCRA_UNAVAILABLE,
                details={"action": "get_cotizaciones_historicas", "reason": str(exc)},
            ) from exc

    # ── variables monetarias (estadísticas v4.0) ────────────────

    async def list_variables(self) -> list[dict[str, Any]]:
        """El catálogo v4 completo (unas 1.600 variables, en dos páginas)."""
        if self._catalog is not None and time.monotonic() - self._catalog_at < CATALOG_TTL_S:
            return self._catalog
        if self._catalog_lock is None:
            self._catalog_lock = asyncio.Lock()
        async with self._catalog_lock:
            if self._catalog is not None and time.monotonic() - self._catalog_at < CATALOG_TTL_S:
                return self._catalog
            try:
                url = f"{self.BASE_URL}/estadisticas/v4.0/monetarias"
                variables: list[dict[str, Any]] = []
                offset = 0
                while True:
                    data = await self._get_json(
                        url, {"limit": str(V4_PAGE_LIMIT), "offset": str(offset)}
                    )
                    page = data.get("results") if isinstance(data, dict) else None
                    if not page:
                        break
                    variables.extend(page)
                    offset += len(page)
                    total = _resultset_count(data)
                    if total is None or offset >= total:
                        break
            except ConnectorError:
                raise
            except Exception as exc:
                raise ConnectorError(
                    error_code=ErrorCode.CN_BCRA_UNAVAILABLE,
                    details={"action": "list_variables", "reason": str(exc)},
                ) from exc
            self._catalog = variables
            self._catalog_at = time.monotonic()
            return variables

    async def _catalog_entry(self, id_variable: int) -> dict[str, Any] | None:
        """La ficha de la variable en el catálogo, o None si no se pudo leer.

        Sólo da el título y la unidad: que el catálogo no responda no tiene que
        tirar abajo una consulta que sí trajo los datos.
        """
        try:
            catalog = await self.list_variables()
        except ConnectorError:
            logger.info("BCRA: catálogo v4 no disponible; sigo sin título", exc_info=True)
            return None
        return next((v for v in catalog if v.get("idVariable") == id_variable), None)

    async def get_variable(
        self,
        id_variable: int,
        desde: str | None = None,
        hasta: str | None = None,
        *,
        limit: int | None = None,
        title: str | None = None,
    ) -> DataResult:
        """Observaciones de una variable v4, de la más vieja a la más nueva.

        - Con ``desde``: todo el rango (paginado), hasta ``V4_MAX_ROWS``.
        - Sin ``desde``: las ``limit`` más recientes (por defecto 60) hasta
          ``hasta`` o hasta hoy.
        """
        try:
            start = _iso_date(desde, "desde")
            end = _iso_date(hasta, "hasta")
            if start and end and start > end:
                raise ConnectorError(
                    error_code=ErrorCode.CN_BCRA_UNAVAILABLE,
                    details={"reason": f"desde {start} es posterior a hasta {end}"},
                )
            url = f"{self.BASE_URL}/estadisticas/v4.0/monetarias/{int(id_variable)}"
            base: dict[str, str] = {}
            if start:
                base["desde"] = start
            if end:
                base["hasta"] = end
            wanted = V4_MAX_ROWS if start else max(10, min(limit or 60, V4_PAGE_LIMIT))

            entry_task = asyncio.create_task(self._catalog_entry(int(id_variable)))
            rows: list[dict[str, Any]] = []
            total: int | None = None
            offset = 0
            try:
                while len(rows) < wanted:
                    page_limit = min(V4_PAGE_LIMIT, wanted - len(rows))
                    data = await self._get_json(
                        url,
                        {**base, "limit": str(max(10, page_limit)), "offset": str(offset)},
                    )
                    if total is None:
                        total = _resultset_count(data)
                    page = _v4_detail(data)
                    if not page:
                        break
                    rows.extend(page)
                    offset += len(page)
                    if total is not None and offset >= total:
                        break
            finally:
                entry = await entry_task
            rows = rows[:wanted]

            records: list[dict[str, Any]] = sorted(
                (
                    {"fecha": str(r.get("fecha"))[:10], "valor": r.get("valor")}
                    for r in rows
                    if r.get("fecha")
                ),
                key=lambda r: str(r["fecha"]),
            )
            newest_fetched = records[-1]["fecha"] if records else None
            source_end: str | None
            if end is None:
                # Sin `hasta`, la primera fila de la primera página es la última
                # que publicó el BCRA: es más fresca que el catálogo cacheado.
                source_end = newest_fetched or (entry or {}).get("ultFechaInformada")
            else:
                source_end = (entry or {}).get("ultFechaInformada") or newest_fetched
            units = (entry or {}).get("unidadExpresion") or ""
            descripcion = ((entry or {}).get("descripcion") or "").strip()
            frecuencia = _FREQUENCIES.get(str((entry or {}).get("periodicidad") or "").upper())
            metadata: dict[str, Any] = {
                "fetched_at": datetime.now(UTC).isoformat(),
                "id_variable": int(id_variable),
                "total_records": len(records),
                "description": descripcion,
                "units": units,
                "last_updated": newest_fetched,
                "ultima_observacion": newest_fetched,
                "frecuencia": frecuencia,
                "fecha_fin_fuente": source_end,
                "actualizada_en_fuente": None,
                "total_fuente": total,
                # Truncada = el rango pedido no vino completo. Sin `desde` el
                # rango son las últimas N observaciones, que sí vienen enteras.
                "truncada": bool(start) and total is not None and len(records) < total,
                "oficial": True,
                "api": "estadisticas/v4.0/monetarias",
            }
            if "porcentaje" in units.lower():
                # Los valores ya vienen en puntos porcentuales (23,19 = 23,19 %).
                metadata["unidad"] = "porcentaje"
            return DataResult(
                source="bcra",
                portal_name=PORTAL_NAME,
                portal_url=VARIABLES_URL,
                dataset_title=title or descripcion or f"Variable {int(id_variable)} del BCRA",
                format="time_series",
                records=records,
                metadata=metadata,
            )
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(
                error_code=ErrorCode.CN_BCRA_UNAVAILABLE,
                details={
                    "action": "get_variable",
                    "id_variable": id_variable,
                    "reason": str(exc),
                },
            ) from exc

    # FIX-002 (mayo): se borraron get_principales_variables() y
    # get_variable_historica(), que llamaban a /Cotizaciones con otro nombre
    # porque la v2 se había deprecado. get_variable() es la v4 de verdad.


def _resultset_count(data: Any) -> int | None:
    try:
        count = data["metadata"]["resultset"]["count"]
    except (KeyError, TypeError):
        return None
    return int(count) if isinstance(count, int | float) else None


def _v4_detail(data: Any) -> list[dict[str, Any]]:
    """Las observaciones de una respuesta v4: ``results[0].detalle``."""
    results = data.get("results") if isinstance(data, dict) else None
    if not results:
        return []
    first = results[0] if isinstance(results, list) else results
    detail = first.get("detalle") if isinstance(first, dict) else None
    return [r for r in (detail or []) if isinstance(r, dict)]
