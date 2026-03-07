from __future__ import annotations

import logging

import httpx

from app.domain.ports.source.data_source import CatalogEntry, DownloadedData, IDataSource

logger = logging.getLogger(__name__)


class CABADataAdapter(IDataSource):
    def __init__(
        self, base_url: str = "https://data.buenosaires.gob.ar/api/3/action"
    ) -> None:
        self._base_url = base_url
        self._client = httpx.AsyncClient(timeout=60.0)

    @property
    def portal_name(self) -> str:
        return "caba"

    async def fetch_catalog(
        self, limit: int = 100, offset: int = 0
    ) -> list[CatalogEntry]:
        url = f"{self._base_url}/package_search"
        params = {"rows": limit, "start": offset}

        response = await self._client.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        entries = []
        for pkg in data.get("result", {}).get("results", []):
            for resource in pkg.get("resources", []):
                fmt = resource.get("format", "").lower()
                if fmt not in ("csv", "json", "xlsx", "xls", "geojson", "txt", "ods", "zip", "xml"):
                    continue

                entries.append(
                    CatalogEntry(
                        source_id=resource.get("id", ""),
                        title=pkg.get("title", ""),
                        description=pkg.get("notes", ""),
                        organization=pkg.get("organization", {}).get("title", ""),
                        url=f"https://data.buenosaires.gob.ar/dataset/{pkg.get('name', '')}",
                        download_url=resource.get("url", ""),
                        format=fmt,
                        tags=[t.get("name", "") for t in pkg.get("tags", [])],
                        columns=[],
                        last_updated=resource.get("last_modified", ""),
                    )
                )

        return entries

    async def fetch_catalog_count(self) -> int:
        url = f"{self._base_url}/package_search"
        response = await self._client.get(url, params={"rows": 0})
        response.raise_for_status()
        data = response.json()
        return data.get("result", {}).get("count", 0)

    async def download_dataset(self, download_url: str) -> DownloadedData:
        response = await self._client.get(download_url, follow_redirects=True)
        response.raise_for_status()

        filename = download_url.split("/")[-1] or "dataset"
        content_type = response.headers.get("content-type", "")
        fmt = "csv"
        if "json" in content_type:
            fmt = "json"
        elif "spreadsheet" in content_type:
            fmt = "xlsx"

        return DownloadedData(
            content=response.content,
            format=fmt,
            filename=filename,
            size_bytes=len(response.content),
        )
