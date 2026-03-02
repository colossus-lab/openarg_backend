"""Standalone MCP Server for Series de Tiempo API."""
from __future__ import annotations

import logging
import os
import unicodedata

import httpx

from app.infrastructure.mcp.base_server import MCPServer

logger = logging.getLogger(__name__)

BASE_URL = os.getenv("SERIES_TIEMPO_BASE_URL", "https://apis.datos.gob.ar/series/api")

SERIES_CATALOG: dict[str, dict] = {
    "presupuesto": {
        "ids": ["451.3_GPNGPN_0_0_3_30"],
        "description": "Gasto público nacional en millones de pesos (anual, desde 1980)",
        "keywords": ["presupuesto", "gasto", "gasto publico", "gasto nacional", "fiscal"],
    },
    "inflacion": {
        "ids": ["103.1_I2N_2016_M_19"],
        "description": "IPC Nivel General (índice base dic-2016=100)",
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
    "reservas": {
        "ids": ["174.1_RRVAS_IDOS_0_0_36"],
        "description": "Reservas internacionales del BCRA en millones de dólares (mensual)",
        "keywords": ["reservas", "reservas internacionales", "bcra reservas"],
        "default_collapse": "month",
    },
    "base_monetaria": {
        "ids": ["331.1_SALDO_BASERIA__15"],
        "description": "Base monetaria — saldo en millones de pesos (mensual)",
        "keywords": ["base monetaria", "emision", "emision monetaria"],
        "default_collapse": "month",
    },
    "emae": {
        "ids": ["143.3_NO_PR_2004_A_21"],
        "description": "EMAE — Estimador Mensual de Actividad Económica (mensual, desde 2004)",
        "keywords": ["emae", "actividad economica", "pbi mensual", "crecimiento"],
        "default_collapse": "month",
    },
    "desempleo": {
        "ids": ["45.2_ECTDT_0_T_33"],
        "description": "Tasa de desempleo total (trimestral, desde 2003)",
        "keywords": ["desempleo", "desocupacion", "tasa de desempleo", "empleo"],
    },
    "exportaciones": {
        "ids": ["74.3_IET_0_M_16"],
        "description": "Exportaciones totales en millones de dólares (mensual)",
        "keywords": ["exportaciones", "ventas externas", "comercio exterior"],
        "default_collapse": "month",
    },
    "importaciones": {
        "ids": ["74.3_IIT_0_M_25"],
        "description": "Importaciones totales en millones de dólares (mensual)",
        "keywords": ["importaciones", "compras externas"],
        "default_collapse": "month",
    },
}


def _strip_accents(text: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn"
    )


def _find_catalog_match(query: str) -> dict | None:
    normalized = _strip_accents(query.lower())
    for entry in SERIES_CATALOG.values():
        for keyword in entry["keywords"]:
            if _strip_accents(keyword) in normalized:
                return entry
    return None


_client = httpx.AsyncClient(timeout=15.0, headers={"User-Agent": "OpenArg-MCP/1.0"})


async def search_series(query: str, limit: int = 10) -> dict:
    resp = await _client.get(f"{BASE_URL}/search", params={"q": query, "limit": limit})
    resp.raise_for_status()
    data = resp.json()
    if not data.get("data"):
        return {"results": []}
    return {
        "results": [
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
    }


async def fetch_series(
    series_ids: list[str],
    start_date: str | None = None,
    end_date: str | None = None,
    collapse: str | None = None,
    representation: str | None = None,
    limit: int = 1000,
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

    resp = await _client.get(f"{BASE_URL}/series", params=params)
    resp.raise_for_status()
    raw = resp.json()

    if not raw or not raw.get("data"):
        return {"result": None}

    is_percent = representation == "percent_change"
    records = []
    for row in raw["data"]:
        record: dict = {"fecha": row[0]}
        for idx, sid in enumerate(series_ids):
            val = row[idx + 1]
            if val is not None and is_percent:
                val = round(val * 100, 2)
            record[sid] = val
        records.append(record)

    return {"records": records, "total_records": len(records)}


def create_server() -> MCPServer:
    server = MCPServer(
        server_name="series_tiempo",
        description="Series de Tiempo API — Argentine macroeconomic time series",
        port=int(os.getenv("MCP_PORT", "8091")),
    )

    server.register_tool(
        name="search_series",
        description="Search time series from Argentina's Series de Tiempo API",
        parameters={
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query"},
                "limit": {"type": "integer", "default": 10},
            },
            "required": ["query"],
        },
        handler=search_series,
    )

    server.register_tool(
        name="fetch_series",
        description="Fetch data from specific time series by IDs",
        parameters={
            "type": "object",
            "properties": {
                "series_ids": {"type": "array", "items": {"type": "string"}},
                "start_date": {"type": "string"},
                "end_date": {"type": "string"},
                "collapse": {"type": "string"},
                "representation": {"type": "string"},
                "limit": {"type": "integer", "default": 1000},
            },
            "required": ["series_ids"],
        },
        handler=fetch_series,
    )

    return server


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    srv = create_server()
    srv.run()
