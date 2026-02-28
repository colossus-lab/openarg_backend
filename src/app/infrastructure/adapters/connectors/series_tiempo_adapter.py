from __future__ import annotations

import logging
import unicodedata
from datetime import UTC, datetime

import httpx

from app.domain.entities.connectors.data_result import DataResult
from app.domain.ports.connectors.series_tiempo import ISeriesTiempoConnector
from app.infrastructure.resilience.retry import with_retry

logger = logging.getLogger(__name__)

# Curated catalog of verified Series de Tiempo IDs.
# These IDs were validated against the live API and return real data.
SERIES_CATALOG: dict[str, dict] = {
    "presupuesto": {
        "ids": ["451.3_GPNGPN_0_0_3_30"],
        "description": "Gasto público nacional en millones de pesos (anual, desde 1980)",
        "keywords": ["presupuesto", "gasto", "gasto publico", "gasto nacional", "presupuesto nacional", "fiscal"],
    },
    "inflacion": {
        "ids": ["103.1_I2N_2016_M_19"],
        "description": "IPC Nivel General (índice base dic-2016=100). Usar con representation=percent_change para variación % mensual.",
        "keywords": ["inflacion", "ipc", "precios", "indice de precios", "costo de vida"],
        "default_collapse": "month",
        "default_representation": "percent_change",
    },
    "tipo_cambio": {
        "ids": ["92.2_TIPO_CAMBIION_0_0_21_24"],
        "description": "Tipo de cambio peso/dólar de valuación BCRA (diario, desde 2003)",
        "keywords": ["dolar", "tipo de cambio", "cambio", "divisa", "cotizacion"],
        "default_collapse": "month",
    },
    "ipc_regional": {
        "ids": ["103.1_I2N_2016_M_19", "148.3_INIVELNOA_DICI_M_21", "145.3_INGCUYUYO_DICI_M_11"],
        "description": "IPC Nivel General: GBA, NOA, y Cuyo (mensual)",
        "keywords": ["ipc regional", "precios regionales", "inflacion regional"],
        "default_collapse": "month",
    },
    "reservas": {
        "ids": ["174.1_RRVAS_IDOS_0_0_36"],
        "description": "Reservas internacionales del BCRA en millones de dólares (mensual)",
        "keywords": ["reservas", "reservas internacionales", "bcra reservas", "reservas bcra", "dolares bcra", "reservas del banco central"],
        "default_collapse": "month",
    },
    "base_monetaria": {
        "ids": ["331.1_SALDO_BASERIA__15"],
        "description": "Base monetaria — saldo en millones de pesos (mensual)",
        "keywords": ["base monetaria", "emision", "emision monetaria", "dinero en circulacion", "masa monetaria", "agregados monetarios"],
        "default_collapse": "month",
    },
    "leliq_pases": {
        "ids": ["331.1_PASES_REDELIQ_M_MONE_0_24_24"],
        "description": "LELIQ y pases del BCRA en millones de pesos (mensual)",
        "keywords": ["leliq", "pases", "letras de liquidez", "pases pasivos", "deuda bcra", "pasivos remunerados", "tasa de politica monetaria"],
        "default_collapse": "month",
    },
    "emae": {
        "ids": ["143.3_NO_PR_2004_A_21"],
        "description": "EMAE — Estimador Mensual de Actividad Económica, índice base 2004 (mensual, desde 2004)",
        "keywords": ["emae", "actividad economica", "pbi mensual", "crecimiento", "recesion", "producto bruto"],
        "default_collapse": "month",
    },
    "desempleo": {
        "ids": ["45.2_ECTDT_0_T_33"],
        "description": "Tasa de desempleo total en porcentaje (trimestral, desde 2003)",
        "keywords": ["desempleo", "desocupacion", "tasa de desempleo", "empleo", "mercado laboral", "trabajo"],
    },
    "salarios": {
        "ids": ["149.1_TL_INDIIOS_OCTU_0_21"],
        "description": "Índice de Salarios nivel general, base oct-2016=100 (mensual)",
        "keywords": ["salarios", "sueldos", "indice de salarios", "remuneraciones", "salario real", "paritarias"],
        "default_collapse": "month",
    },
    "canasta_basica": {
        "ids": ["150.1_LA_POBREZA_0_D_13"],
        "description": "Canasta Básica Total (CBT) / Línea de pobreza por adulto equivalente en pesos (mensual, desde 2016)",
        "keywords": ["canasta basica", "cbt", "linea de pobreza", "pobreza", "costo de vida"],
        "default_collapse": "month",
    },
    "canasta_alimentaria": {
        "ids": ["150.1_LA_INDICIA_0_D_16"],
        "description": "Canasta Básica Alimentaria (CBA) / Línea de indigencia por adulto equivalente en pesos (mensual, desde 2016)",
        "keywords": ["canasta alimentaria", "cba", "linea de indigencia", "indigencia", "alimentos basicos"],
        "default_collapse": "month",
    },
    "exportaciones": {
        "ids": ["74.3_IET_0_M_16"],
        "description": "Exportaciones totales en millones de dólares (mensual, desde 1992)",
        "keywords": ["exportaciones", "expo", "ventas externas", "comercio exterior"],
        "default_collapse": "month",
    },
    "importaciones": {
        "ids": ["74.3_IIT_0_M_25"],
        "description": "Importaciones totales en millones de dólares (mensual, desde 1992)",
        "keywords": ["importaciones", "impo", "compras externas"],
        "default_collapse": "month",
    },
    "balanza_comercial": {
        "ids": ["74.3_IET_0_M_16", "74.3_IIT_0_M_25"],
        "description": "Balanza comercial: exportaciones e importaciones totales en millones de dólares (mensual)",
        "keywords": ["balanza comercial", "saldo comercial", "comercio exterior", "intercambio comercial"],
        "default_collapse": "month",
    },
    "actividad_industrial": {
        "ids": ["11.3_AGCS_2004_M_41"],
        "description": "EMAE Sector Industrial — Comercio mayorista/minorista y reparaciones, índice base 2004 (mensual)",
        "keywords": ["industria", "produccion industrial", "actividad industrial", "manufactura", "emi", "fabrica"],
        "default_collapse": "month",
    },
}


def _strip_accents(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn"
    )


def find_catalog_match(query: str) -> dict | None:
    """Find a catalog entry matching the query by keyword matching."""
    normalized = _strip_accents(query.lower())
    for entry in SERIES_CATALOG.values():
        for keyword in entry["keywords"]:
            if _strip_accents(keyword) in normalized:
                return entry
    return None


class SeriesTiempoAdapter(ISeriesTiempoConnector):
    def __init__(self, base_url: str = "https://apis.datos.gob.ar/series/api") -> None:
        self._base_url = base_url
        self._client = httpx.AsyncClient(
            timeout=15.0,
            headers={"User-Agent": "OpenArg/1.0"},
        )

    async def search(self, query: str, limit: int = 10) -> list[dict]:
        try:
            return await self._search_internal(query, limit)
        except Exception:
            logger.warning("Series de Tiempo search failed", exc_info=True)
            return []

    @with_retry(max_retries=2, base_delay=1.0, service_name="series_tiempo")
    async def _search_internal(self, query: str, limit: int) -> list[dict]:
        resp = await self._client.get(
            f"{self._base_url}/search",
            params={"q": query, "limit": limit},
        )
        resp.raise_for_status()
        data = resp.json()
        if not data.get("data"):
            return []
        return [
            {
                "id": item["field"]["id"],
                "title": item["field"].get("title") or item["field"].get("description", ""),
                "description": item["field"].get("description", ""),
                "units": item["field"].get("units", ""),
                "frequency": item["field"].get("frequency", ""),
                "dataset_title": item["dataset"].get("title", ""),
                "source": item["dataset"].get("source", ""),
            }
            for item in data["data"]
        ]

    @with_retry(max_retries=2, base_delay=1.0, service_name="series_tiempo")
    async def _fetch_internal(
        self,
        series_ids: list[str],
        start_date: str | None,
        end_date: str | None,
        collapse: str | None,
        representation: str | None,
        limit: int,
    ) -> dict:
        params: dict[str, str] = {
            "ids": ",".join(series_ids),
            "format": "json",
            "limit": str(limit),
        }
        if start_date:
            params["start_date"] = start_date
        if end_date:
            params["end_date"] = end_date
        if representation:
            params["representation_mode"] = representation
        if collapse:
            params["collapse"] = collapse

        resp = await self._client.get(f"{self._base_url}/series", params=params)
        resp.raise_for_status()
        return resp.json()

    async def fetch(
        self,
        series_ids: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
        collapse: str | None = None,
        representation: str | None = None,
        limit: int = 1000,
    ) -> DataResult | None:
        try:
            result = await self._fetch_internal(
                series_ids, start_date, end_date, collapse, representation, limit
            )

            if not result or not result.get("data"):
                return None

            meta = result.get("meta", [])
            field_meta = meta[0] if meta else None

            # Transform percent_change values (multiply by 100)
            is_percent = representation == "percent_change"

            records = []
            for row in result["data"]:
                record: dict = {"fecha": row[0]}
                for idx, sid in enumerate(series_ids):
                    val = row[idx + 1]
                    if val is not None and is_percent:
                        val = round(val * 100, 2)
                    record[sid] = val
                records.append(record)

            description = ""
            units = ""
            dataset_title = ", ".join(series_ids)
            if field_meta:
                description = field_meta.get("field", {}).get("description", "")
                units = field_meta.get("field", {}).get("units", "")
                dataset_title = field_meta.get("dataset", {}).get("title", dataset_title)

            return DataResult(
                source="series_tiempo",
                portal_name="API de Series de Tiempo",
                portal_url=f"https://datos.gob.ar/series/api/series/?ids={','.join(series_ids)}",
                dataset_title=dataset_title,
                format="time_series",
                records=records,
                metadata={
                    "total_records": len(records),
                    "fetched_at": datetime.now(UTC).isoformat(),
                    "description": description,
                    "units": units,
                },
            )
        except Exception:
            logger.warning("Series de Tiempo fetch failed for %s", series_ids, exc_info=True)
            return None
