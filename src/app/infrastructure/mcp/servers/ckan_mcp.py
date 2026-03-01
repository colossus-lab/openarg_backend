"""Standalone MCP Server for CKAN open data portals."""
from __future__ import annotations

import asyncio
import csv
import io
import logging
import os

import httpx

from app.infrastructure.mcp.base_server import MCPServer

logger = logging.getLogger(__name__)

MAX_CSV_BYTES = 2 * 1024 * 1024
MAX_CSV_ROWS = 500

PORTALS: list[dict] = [
    # Nacionales
    {"id": "nacional", "name": "Portal Nacional", "base_url": "https://datos.gob.ar", "api_path": "/api/3/action", "category": "nacional"},
    {"id": "diputados", "name": "Cámara de Diputados", "base_url": "https://datos.hcdn.gob.ar", "api_path": "/api/3/action", "category": "nacional"},
    {"id": "justicia", "name": "Justicia (incl. Anticorrupción)", "base_url": "https://datos.jus.gob.ar", "api_path": "/api/3/action", "category": "nacional"},
    # Nacionales sectoriales
    {"id": "energia", "name": "Energía", "base_url": "https://datos.energia.gob.ar", "api_path": "/api/3/action", "category": "nacional"},
    {"id": "transporte", "name": "Transporte", "base_url": "https://datos.transporte.gob.ar", "api_path": "/api/3/action", "category": "nacional"},
    {"id": "salud", "name": "Salud", "base_url": "https://datos.salud.gob.ar", "api_path": "/api/3/action", "category": "nacional"},
    {"id": "cultura", "name": "Cultura", "base_url": "https://datos.cultura.gob.ar", "api_path": "/api/3/action", "category": "nacional"},
    {"id": "agroindustria", "name": "Agroindustria", "base_url": "https://datos.agroindustria.gob.ar", "api_path": "/api/3/action", "category": "nacional"},
    {"id": "produccion", "name": "Producción", "base_url": "https://datos.produccion.gob.ar", "api_path": "/api/3/action", "category": "nacional"},
    # CABA
    {"id": "caba", "name": "Buenos Aires Ciudad", "base_url": "https://data.buenosaires.gob.ar", "api_path": "/api/3/action", "category": "caba"},
    # Provincias
    {"id": "pba", "name": "Provincia de Buenos Aires", "base_url": "https://catalogo.datos.gba.gob.ar", "api_path": "/api/3/action", "category": "provincia"},
    {"id": "cordoba_prov", "name": "Córdoba Provincia", "base_url": "https://datosgestionabierta.cba.gov.ar", "api_path": "/api/3/action", "category": "provincia"},
    {"id": "santafe", "name": "Santa Fe", "base_url": "https://datos.santafe.gob.ar", "api_path": "/api/3/action", "category": "provincia"},
    {"id": "mendoza", "name": "Mendoza", "base_url": "https://datosabiertos.mendoza.gov.ar", "api_path": "/api/3/action", "category": "provincia"},
    {"id": "entrerios", "name": "Entre Ríos", "base_url": "https://datos.entrerios.gov.ar", "api_path": "/api/3/action", "category": "provincia"},
    {"id": "neuquen_legislatura", "name": "Legislatura de Neuquén", "base_url": "https://datos.legislaturaneuquen.gob.ar", "api_path": "/api/3/action", "category": "provincia"},
    {"id": "tucuman", "name": "Tucumán", "base_url": "https://datos.tucuman.gob.ar", "api_path": "/api/3/action", "category": "provincia"},
    {"id": "misiones", "name": "Misiones", "base_url": "https://datos.misiones.gob.ar", "api_path": "/api/3/action", "category": "provincia"},
    {"id": "chaco", "name": "Chaco", "base_url": "https://datos.chaco.gob.ar", "api_path": "/api/3/action", "category": "provincia"},
    # Municipios
    {"id": "rosario", "name": "Rosario", "base_url": "https://datos.rosario.gob.ar", "api_path": "/api/3/action", "category": "municipio"},
    {"id": "bahia_blanca", "name": "Bahía Blanca", "base_url": "https://datos.bahiablanca.gob.ar", "api_path": "/api/3/action", "category": "municipio"},
]

_client = httpx.AsyncClient(timeout=15.0, headers={"User-Agent": "OpenArg-MCP/1.0"})


def _find_portal(portal_id: str) -> dict | None:
    for p in PORTALS:
        if p["id"] == portal_id:
            return p
    return None


def _parse_csv_text(text: str) -> list[dict]:
    if not text.strip():
        return []
    if text[0] == "\ufeff":
        text = text[1:]
    lines = text.split("\n")
    if len(lines) < 2:
        return []
    header = lines[0]
    delimiter = ";" if header.count(";") > header.count(",") else ","
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    records: list[dict] = []
    for i, row in enumerate(reader):
        if i >= MAX_CSV_ROWS:
            break
        record: dict = {}
        for k, v in row.items():
            if k is None:
                continue
            k = k.strip()
            if v is None:
                record[k] = None
                continue
            v = v.strip()
            try:
                record[k] = float(v) if "." in v else int(v)
            except ValueError:
                record[k] = v
        records.append(record)
    return records


async def _search_portal(portal: dict, query: str, rows: int) -> list[dict]:
    url = f"{portal['base_url']}{portal['api_path']}/package_search"
    resp = await _client.get(url, params={"q": query, "rows": rows})
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        return []
    return data["result"]["results"]


async def _fetch_datastore(portal: dict, resource_id: str, q: str | None, limit: int) -> list[dict]:
    url = f"{portal['base_url']}{portal['api_path']}/datastore_search"
    params: dict[str, str | int] = {"resource_id": resource_id, "limit": limit}
    if q:
        params["q"] = q
    resp = await _client.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("success"):
        return []
    return data["result"]["records"]


async def _fetch_csv(url: str) -> list[dict]:
    resp = await _client.get(url, follow_redirects=True)
    resp.raise_for_status()
    text = resp.text
    if len(text) > MAX_CSV_BYTES:
        text = text[:MAX_CSV_BYTES]
    return _parse_csv_text(text)


async def search_datasets(query: str, portal_id: str | None = None, rows: int = 10) -> dict:
    target_portals = [p for p in PORTALS if p["id"] == portal_id] if portal_id else PORTALS
    if not target_portals:
        return {"results": []}

    search_query = "*:*" if query in ("*", "*:*") else query

    async def _search_one(portal: dict) -> list[dict]:
        try:
            datasets = await _search_portal(portal, search_query, rows)
            results = []
            for ds in datasets:
                # Try datastore first
                suitable = [r for r in ds.get("resources", []) if r.get("format", "").upper() in ("CSV", "JSON")]
                records = None
                for resource in suitable[:2]:
                    try:
                        records = await _fetch_datastore(portal, resource["id"], None, 50)
                        if records:
                            break
                    except Exception:
                        continue

                if not records:
                    csv_resources = [r for r in ds.get("resources", []) if r.get("format", "").upper() == "CSV"]
                    for resource in csv_resources[:1]:
                        try:
                            records = await _fetch_csv(resource["url"])
                            if records:
                                break
                        except Exception:
                            continue

                results.append({
                    "portal": portal["name"],
                    "portal_id": portal["id"],
                    "title": ds.get("title", ""),
                    "description": ds.get("notes", ""),
                    "url": f"{portal['base_url']}/dataset/{ds.get('name', '')}",
                    "records": records[:50] if records else [],
                    "record_count": len(records) if records else 0,
                })
            return results
        except Exception:
            logger.warning("CKAN search failed for %s", portal["name"], exc_info=True)
            return []

    portal_results = await asyncio.gather(
        *[_search_one(p) for p in target_portals],
        return_exceptions=True,
    )

    all_results: list[dict] = []
    for result in portal_results:
        if isinstance(result, list):
            all_results.extend(result)
    return {"results": all_results}


async def query_datastore(
    portal_id: str, resource_id: str, q: str | None = None, limit: int = 100
) -> dict:
    portal = _find_portal(portal_id)
    if not portal:
        return {"records": [], "error": f"Unknown portal: {portal_id}"}
    records = await _fetch_datastore(portal, resource_id, q, limit)
    return {"records": records}


def create_server() -> MCPServer:
    server = MCPServer(
        server_name="ckan",
        description="CKAN Open Data — Search datasets across Argentine government portals",
        port=int(os.getenv("MCP_PORT", "8092")),
    )

    server.register_tool(
        name="search_datasets",
        description="Search datasets across Argentine CKAN open data portals",
        parameters={
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query"},
                "portal_id": {"type": "string", "description": "Portal filter (nacional, caba, pba, etc.)"},
                "rows": {"type": "integer", "default": 10},
            },
            "required": ["query"],
        },
        handler=search_datasets,
    )

    server.register_tool(
        name="query_datastore",
        description="Query a specific CKAN datastore resource",
        parameters={
            "type": "object",
            "properties": {
                "portal_id": {"type": "string"},
                "resource_id": {"type": "string"},
                "q": {"type": "string"},
                "limit": {"type": "integer", "default": 100},
            },
            "required": ["portal_id", "resource_id"],
        },
        handler=query_datastore,
    )

    return server


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    srv = create_server()
    srv.run()
