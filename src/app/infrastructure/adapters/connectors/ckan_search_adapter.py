from __future__ import annotations

import asyncio
import csv
import io
import logging
from datetime import UTC, datetime

import httpx

from app.domain.entities.connectors.data_result import DataResult
from app.domain.ports.connectors.ckan_search import ICKANSearchConnector

logger = logging.getLogger(__name__)

MAX_CSV_BYTES = 2 * 1024 * 1024
MAX_CSV_ROWS = 500

PORTALS: list[dict] = [
    {"id": "nacional", "name": "Portal Nacional", "base_url": "https://datos.gob.ar", "api_path": "/api/3/action"},
    {"id": "caba", "name": "Buenos Aires Ciudad", "base_url": "https://data.buenosaires.gob.ar", "api_path": "/api/3/action"},
    {"id": "pba", "name": "Provincia de Buenos Aires", "base_url": "https://catalogo.datos.gba.gob.ar", "api_path": "/api/3/action"},
    {"id": "cordoba", "name": "Córdoba", "base_url": "https://gobiernoabierto.cordoba.gob.ar", "api_path": "/api/3/action"},
    {"id": "santafe", "name": "Santa Fe", "base_url": "https://datos.santafe.gob.ar", "api_path": "/api/3/action"},
    {"id": "mendoza", "name": "Mendoza", "base_url": "https://datosabiertos.mendoza.gov.ar", "api_path": "/api/3/action"},
    {"id": "entrerios", "name": "Entre Ríos", "base_url": "https://datos.entrerios.gov.ar", "api_path": "/api/3/action"},
    {"id": "neuquen", "name": "Neuquén (Ejecutivo)", "base_url": "https://portaldatosabiertos.neuquen.gov.ar", "api_path": "/api/3/action"},
    {"id": "neuquen_legislatura", "name": "Legislatura de Neuquén", "base_url": "https://datos.legislaturaneuquen.gob.ar", "api_path": "/api/3/action"},
    {"id": "diputados", "name": "Cámara de Diputados de la Nación", "base_url": "https://datos.hcdn.gob.ar", "api_path": "/api/3/action"},
]


def _find_portal(portal_id: str) -> dict | None:
    for p in PORTALS:
        if p["id"] == portal_id:
            return p
    return None


def _parse_csv_text(text: str) -> list[dict]:
    """Lightweight CSV parser with auto-detection of delimiter."""
    if not text.strip():
        return []

    # Strip BOM
    if text[0] == "\ufeff":
        text = text[1:]

    lines = text.split("\n")
    if len(lines) < 2:
        return []

    # Auto-detect delimiter
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


class CKANSearchAdapter(ICKANSearchConnector):
    def __init__(self) -> None:
        self._client = httpx.AsyncClient(
            timeout=15.0,
            headers={"User-Agent": "OpenArg/1.0"},
        )

    async def search_datasets(
        self,
        query: str,
        portal_id: str | None = None,
        rows: int = 10,
    ) -> list[DataResult]:
        target_portals = (
            [p for p in PORTALS if p["id"] == portal_id]
            if portal_id
            else PORTALS
        )
        if not target_portals:
            return []

        search_query = "*:*" if query in ("*", "*:*") else query

        # Search all portals in parallel (like frontend Promise.allSettled)
        async def _search_one_portal(portal: dict) -> list[DataResult]:
            try:
                datasets = await self._search_portal(portal, search_query, rows)
                results = []
                for ds in datasets:
                    dr = await self._dataset_to_data_result(portal, ds)
                    results.append(dr)
                return results
            except Exception:
                logger.warning("CKAN search failed for %s", portal["name"], exc_info=True)
                return []

        portal_results = await asyncio.gather(
            *[_search_one_portal(p) for p in target_portals],
            return_exceptions=True,
        )

        all_results: list[DataResult] = []
        for result in portal_results:
            if isinstance(result, list):
                all_results.extend(result)
        return all_results

    async def _search_portal(self, portal: dict, query: str, rows: int) -> list[dict]:
        url = f"{portal['base_url']}{portal['api_path']}/package_search"
        resp = await self._client.get(url, params={"q": query, "rows": rows})
        resp.raise_for_status()
        data = resp.json()
        if not data.get("success"):
            return []
        return data["result"]["results"]

    async def _dataset_to_data_result(self, portal: dict, ds: dict) -> DataResult:
        """Try datastore → CSV download → metadata fallback."""
        now = datetime.now(UTC).isoformat()

        # Try datastore for JSON/CSV resources
        suitable = [r for r in ds.get("resources", []) if r.get("format", "").upper() in ("CSV", "JSON")]
        for resource in suitable[:2]:
            try:
                records = await self._query_datastore_internal(portal, resource["id"], limit=50)
                if records:
                    return DataResult(
                        source=f"ckan:{portal['id']}",
                        portal_name=portal["name"],
                        portal_url=f"{portal['base_url']}/dataset/{ds.get('name', '')}",
                        dataset_title=ds.get("title", ""),
                        format="json",
                        records=records,
                        metadata={
                            "total_records": len(records),
                            "fetched_at": now,
                            "description": ds.get("notes") or "",
                        },
                    )
            except Exception:
                continue

        # Fallback: CSV download
        csv_resources = [r for r in ds.get("resources", []) if r.get("format", "").upper() == "CSV"]
        for resource in csv_resources[:2]:
            try:
                records = await self._fetch_csv(resource["url"])
                if records:
                    return DataResult(
                        source=f"ckan:{portal['id']}",
                        portal_name=portal["name"],
                        portal_url=f"{portal['base_url']}/dataset/{ds.get('name', '')}",
                        dataset_title=ds.get("title", ""),
                        format="csv",
                        records=records,
                        metadata={
                            "total_records": len(records),
                            "fetched_at": now,
                            "description": ds.get("notes") or "",
                        },
                    )
            except Exception:
                continue

        # Last resort: resource metadata
        return DataResult(
            source=f"ckan:{portal['id']}",
            portal_name=portal["name"],
            portal_url=f"{portal['base_url']}/dataset/{ds.get('name', '')}",
            dataset_title=ds.get("title", ""),
            format="json",
            records=[
                {
                    "_type": "resource_metadata",
                    "resource_name": r.get("name", ""),
                    "resource_url": r.get("url", ""),
                    "format": r.get("format", ""),
                    "description": r.get("description", ""),
                }
                for r in ds.get("resources", [])
            ],
            metadata={
                "total_records": len(ds.get("resources", [])),
                "fetched_at": now,
                "description": ds.get("notes") or "",
            },
        )

    async def _query_datastore_internal(
        self, portal: dict, resource_id: str, q: str | None = None, limit: int = 100
    ) -> list[dict]:
        url = f"{portal['base_url']}{portal['api_path']}/datastore_search"
        params: dict[str, str | int] = {"resource_id": resource_id, "limit": limit}
        if q:
            params["q"] = q
        resp = await self._client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
        if not data.get("success"):
            return []
        return data["result"]["records"]

    async def query_datastore(
        self,
        portal_id: str,
        resource_id: str,
        q: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        portal = _find_portal(portal_id)
        if not portal:
            return []
        return await self._query_datastore_internal(portal, resource_id, q=q, limit=limit)

    async def _fetch_csv(self, url: str) -> list[dict]:
        resp = await self._client.get(url, follow_redirects=True)
        resp.raise_for_status()
        content_length = resp.headers.get("content-length")
        if content_length and int(content_length) > MAX_CSV_BYTES:
            return []
        text = resp.text
        if len(text) > MAX_CSV_BYTES:
            text = text[:MAX_CSV_BYTES]
        return _parse_csv_text(text)
