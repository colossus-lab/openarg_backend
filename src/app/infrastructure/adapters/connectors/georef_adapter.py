from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from urllib.parse import quote

import httpx

from app.domain.entities.connectors.data_result import DataResult
from app.domain.ports.connectors.georef import IGeorefConnector

logger = logging.getLogger(__name__)


class GeorefAdapter(IGeorefConnector):
    def __init__(self, base_url: str = "https://apis.datos.gob.ar/georef/api") -> None:
        self._base_url = base_url
        self._client = httpx.AsyncClient(
            timeout=10.0,
            headers={"User-Agent": "OpenArg/1.0"},
        )

    async def _get_entities(
        self, entity_type: str, params: dict[str, str]
    ) -> list[dict]:
        try:
            resp = await self._client.get(
                f"{self._base_url}/{entity_type}", params=params
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get(entity_type, [])
        except Exception:
            logger.warning("Georef %s request failed", entity_type, exc_info=True)
            return []

    async def get_provincias(self, nombre: str | None = None) -> list[dict]:
        params: dict[str, str] = {"max": "24"}
        if nombre:
            params["nombre"] = nombre
        return await self._get_entities("provincias", params)

    async def get_departamentos(
        self, provincia: str | None = None, nombre: str | None = None
    ) -> list[dict]:
        params: dict[str, str] = {"max": "100"}
        if provincia:
            params["provincia"] = provincia
        if nombre:
            params["nombre"] = nombre
        return await self._get_entities("departamentos", params)

    async def get_municipios(
        self, provincia: str | None = None, nombre: str | None = None
    ) -> list[dict]:
        params: dict[str, str] = {"max": "100"}
        if provincia:
            params["provincia"] = provincia
        if nombre:
            params["nombre"] = nombre
        return await self._get_entities("municipios", params)

    async def get_localidades(
        self, provincia: str | None = None, nombre: str | None = None
    ) -> list[dict]:
        params: dict[str, str] = {"max": "100"}
        if provincia:
            params["provincia"] = provincia
        if nombre:
            params["nombre"] = nombre
        return await self._get_entities("localidades", params)

    async def normalize_location(self, query: str) -> DataResult | None:
        # Fire all requests in parallel
        results = await asyncio.gather(
            self.get_provincias(nombre=query),
            self.get_departamentos(nombre=query),
            self.get_municipios(nombre=query),
            self.get_localidades(nombre=query),
            return_exceptions=True,
        )

        provincias = results[0] if isinstance(results[0], list) else []
        departamentos = results[1] if isinstance(results[1], list) else []
        municipios = results[2] if isinstance(results[2], list) else []
        localidades = results[3] if isinstance(results[3], list) else []

        # Priority: provincia > departamento > municipio > localidad
        if provincias:
            entity_type = "provincias"
            entities = provincias
        elif departamentos:
            entity_type = "departamentos"
            entities = departamentos
        elif municipios:
            entity_type = "municipios"
            entities = municipios
        elif localidades:
            entity_type = "localidades"
            entities = localidades
        else:
            return None

        return DataResult(
            source="georef",
            portal_name="API de Georef Argentina",
            portal_url=f"{self._base_url}/{entity_type}?nombre={quote(query)}",
            dataset_title=f"Entidades geográficas: {entity_type}",
            format="geo",
            records=[dict(e) for e in entities],
            metadata={
                "total_records": len(entities),
                "fetched_at": datetime.now(UTC).isoformat(),
                "description": f'Resultados de {entity_type} para "{query}"',
            },
        )
