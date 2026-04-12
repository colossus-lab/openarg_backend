from __future__ import annotations

import asyncio
import csv
import io
import logging
from datetime import UTC, datetime

import httpx

from app.domain.entities.connectors.data_result import DataResult
from app.domain.exceptions.connector_errors import ConnectorError
from app.domain.exceptions.error_codes import ErrorCode
from app.domain.ports.connectors.ckan_search import ICKANSearchConnector

logger = logging.getLogger(__name__)

MAX_CSV_BYTES = 2 * 1024 * 1024
MAX_CSV_ROWS = 500

PORTALS: list[dict] = [
    # ── Nacionales ──
    {
        "id": "nacional",
        "name": "Portal Nacional",
        "base_url": "https://datos.gob.ar",
        "api_path": "/api/3/action",
        "category": "nacional",
    },
    {
        "id": "diputados",
        "name": "Cámara de Diputados",
        "base_url": "https://datos.hcdn.gob.ar",
        "api_path": "/api/3/action",
        "category": "nacional",
    },
    {
        "id": "justicia",
        "name": "Justicia (incl. Anticorrupción)",
        "base_url": "https://datos.jus.gob.ar",
        "api_path": "/api/3/action",
        "category": "nacional",
    },
    {
        "id": "energia",
        "name": "Energía",
        "base_url": "http://datos.energia.gob.ar",
        "api_path": "/api/3/action",
        "category": "nacional",
    },
    {
        "id": "transporte",
        "name": "Transporte",
        "base_url": "https://datos.transporte.gob.ar",
        "api_path": "/api/3/action",
        "category": "nacional",
    },
    {
        "id": "salud",
        "name": "Salud",
        "base_url": "https://datos.salud.gob.ar",
        "api_path": "/api/3/action",
        "category": "nacional",
    },
    {
        "id": "cultura",
        "name": "Cultura",
        "base_url": "https://datos.cultura.gob.ar",
        "api_path": "/api/3/action",
        "category": "nacional",
    },
    {
        "id": "produccion",
        "name": "Producción",
        "base_url": "https://datos.produccion.gob.ar",
        "api_path": "/api/3/action",
        "category": "nacional",
    },
    {
        "id": "magyp",
        "name": "Agricultura, Ganadería y Pesca",
        "base_url": "https://datos.magyp.gob.ar",
        "api_path": "/api/3/action",
        "category": "nacional",
    },
    {
        "id": "arsat",
        "name": "ARSAT",
        "base_url": "https://datos.arsat.com.ar",
        "api_path": "/api/3/action",
        "category": "nacional",
    },
    {
        "id": "acumar",
        "name": "ACUMAR",
        "base_url": "https://datos.acumar.gob.ar",
        "api_path": "/api/3/action",
        "category": "nacional",
    },
    {
        "id": "mininterior",
        "name": "Interior",
        "base_url": "https://datos.mininterior.gob.ar",
        "api_path": "/api/3/action",
        "category": "nacional",
    },
    {
        "id": "pami",
        "name": "PAMI",
        "base_url": "https://datos.pami.org.ar",
        "api_path": "/api/3/action",
        "category": "nacional",
    },
    {
        "id": "desarrollo_social",
        "name": "Desarrollo Social",
        "base_url": "https://datosabiertos.desarrollosocial.gob.ar",
        "api_path": "/api/3/action",
        "category": "nacional",
    },
    {
        "id": "turismo",
        "name": "Turismo (Yvera)",
        "base_url": "http://datos.yvera.gob.ar",
        "api_path": "/api/3/action",
        "category": "nacional",
    },
    {
        "id": "ssn",
        "name": "Superintendencia de Seguros",
        "base_url": "https://datosabiertos.ssn.gob.ar",
        "api_path": "/api/3/action",
        "category": "nacional",
    },
    # ── CABA ──
    {
        "id": "caba",
        "name": "Buenos Aires Ciudad",
        "base_url": "https://data.buenosaires.gob.ar",
        "api_path": "/api/3/action",
        "category": "caba",
    },
    {
        "id": "legislatura_caba",
        "name": "Legislatura CABA",
        "base_url": "http://datos.legislatura.gob.ar",
        "api_path": "/api/3/action",
        "category": "caba",
    },
    # ── Provincias ──
    {
        "id": "pba",
        "name": "Provincia de Buenos Aires",
        "base_url": "https://catalogo.datos.gba.gob.ar",
        "api_path": "/api/3/action",
        "category": "provincia",
    },
    {
        "id": "cordoba_prov",
        "name": "Córdoba Provincia",
        "base_url": "https://datosgestionabierta.cba.gov.ar",
        "api_path": "/api/3/action",
        "category": "provincia",
    },
    {
        "id": "cordoba_estadistica",
        "name": "Córdoba Estadística",
        "base_url": "https://datosestadistica.cba.gov.ar",
        "api_path": "/api/3/action",
        "category": "provincia",
    },
    {
        "id": "mendoza",
        "name": "Mendoza",
        "base_url": "https://datosabiertos.mendoza.gov.ar",
        "api_path": "/api/3/action",
        "category": "provincia",
    },
    {
        "id": "entrerios",
        "name": "Entre Ríos",
        "base_url": "https://datos.entrerios.gov.ar",
        "api_path": "/api/3/action",
        "category": "provincia",
    },
    {
        "id": "neuquen_legislatura",
        "name": "Legislatura de Neuquén",
        "base_url": "https://datos.legislaturaneuquen.gob.ar",
        "api_path": "/api/3/action",
        "category": "provincia",
    },
    {
        "id": "tucuman",
        "name": "Tucumán",
        "base_url": "https://sep.tucuman.gob.ar",
        "api_path": "/api/3/action",
        "category": "provincia",
    },
    {
        "id": "misiones",
        "name": "Misiones",
        "base_url": "https://datos.misiones.gob.ar",
        "api_path": "/api/3/action",
        "category": "provincia",
    },
    {
        "id": "chaco",
        "name": "Chaco",
        "base_url": "https://datosabiertos.chaco.gob.ar",
        "api_path": "/api/3/action",
        "category": "provincia",
    },
    # ── Municipios ──
    {
        "id": "ciudad_mendoza",
        "name": "Ciudad de Mendoza",
        "base_url": "https://datos.ciudaddemendoza.gov.ar",
        "api_path": "/api/3/action",
        "category": "municipio",
    },
    {
        "id": "corrientes",
        "name": "Ciudad de Corrientes",
        "base_url": "https://datos.ciudaddecorrientes.gov.ar",
        "api_path": "/api/3/action",
        "category": "municipio",
    },
]

# Portales verificados offline — Mar 2026.
# No se scrapean. Revisar periódicamente por si vuelven.
DEAD_PORTALS = {
    "santafe": {
        "url": "https://datos.santafe.gob.ar",
        "reason": "CKAN Andino en mantenimiento (página estática, SSL chain incompleto). No migró — revisar periódicamente",
    },
    "modernizacion": {
        "url": "https://datos.modernizacion.gob.ar",
        "reason": "Ministerio disuelto, DNS muerto. Datos absorbidos por datos.gob.ar",
    },
    "ambiente": {
        "url": "https://datos.ambiente.gob.ar",
        "reason": "Redirige a datos.gob.ar (ya cubierto por datos_gob_ar)",
    },
    "agroindustria": {
        "url": "https://datos.agroindustria.gob.ar",
        "reason": "DNS muerto. Reemplazado por magyp",
    },
    "rio_negro": {
        "url": "https://datos.rionegro.gov.ar",
        "reason": "DNS muerto, sin reemplazo CKAN",
    },
    "jujuy": {
        "url": "https://datos.jujuy.gob.ar",
        "reason": "DNS muerto. Migró a DKAN (datos.gajujuy.gob.ar) — incompatible",
    },
    "salta": {
        "url": "https://datos.salta.gob.ar",
        "reason": "DNS muerto, sin portal de datos abiertos",
    },
    "la_plata": {
        "url": "https://datos.laplata.gob.ar",
        "reason": "DNS muerto, portal caído desde 2024",
    },
    "cordoba_muni": {
        "url": "https://gobiernoabierto.cordoba.gob.ar/data",
        "reason": "CKAN reemplazado por API Django REST (774 datasets, no compatible)",
    },
    "rosario": {
        "url": "https://datos.rosario.gob.ar",
        "reason": "Migró a DKAN (datosabiertos.rosario.gob.ar) — incompatible",
    },
    "bahia_blanca": {
        "url": "https://datos.bahiablanca.gob.ar",
        "reason": "URL vieja muerta. Portal nuevo en datos.bahia.gob.ar (221 datasets) — pendiente agregar",
    },
    "csjn": {"url": "https://datos.csjn.gov.ar", "reason": "API no responde desde Mar 2026"},
}


def _find_portal(portal_id: str) -> dict | None:
    for p in PORTALS:
        if p["id"] == portal_id:
            return p
    return None


def _parse_csv_text(text: str) -> tuple[list[dict], dict | None]:
    """Parse CSV text into records, returning truncation info per FR-011.

    Returns ``(records, truncation)`` where ``truncation`` is either
    ``None`` (the whole file fit) or a dict with keys
    ``{"truncated": True, "truncation_reason": "max_rows_exceeded",
    "truncation_limit": MAX_CSV_ROWS}`` when we stopped reading rows at
    the ``MAX_CSV_ROWS`` cap. Byte-level truncation is detected one
    layer up in ``_fetch_csv`` — see FR-011b for why both reasons exist
    as separate codes.
    """
    if not text.strip():
        return [], None
    if text[0] == "\ufeff":
        text = text[1:]
    lines = text.split("\n")
    if len(lines) < 2:
        return [], None
    header = lines[0]
    delimiter = ";" if header.count(";") > header.count(",") else ","
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    records: list[dict] = []
    truncated_rows = False
    for i, row in enumerate(reader):
        if i >= MAX_CSV_ROWS:
            truncated_rows = True
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
    if truncated_rows:
        return records, {
            "truncated": True,
            "truncation_reason": "max_rows_exceeded",
            "truncation_limit": MAX_CSV_ROWS,
        }
    return records, None


class CKANSearchAdapter(ICKANSearchConnector):
    def __init__(self, http_client: httpx.AsyncClient) -> None:
        self._http = http_client

    async def _search_portal(self, portal: dict, query: str, rows: int) -> list[dict]:
        url = f"{portal['base_url']}{portal['api_path']}/package_search"
        resp = await self._http.get(url, params={"q": query, "rows": rows})
        resp.raise_for_status()
        data = resp.json()
        if not data.get("success"):
            return []
        return data["result"]["results"]

    async def _fetch_datastore(
        self, portal: dict, resource_id: str, q: str | None, limit: int
    ) -> tuple[list[dict], dict | None]:
        """Fetch a datastore page, returning pagination truncation info per FR-012.

        Returns ``(records, truncation)``. ``truncation`` is None when the
        full dataset fits in one page (``total <= limit``) and a dict
        with ``truncated=True``, ``truncation_reason="datastore_pagination_limit"``,
        ``truncation_limit``, and ``total_available`` when the page is
        smaller than the upstream total. The connector still only
        fetches one page — this FR is about making the partial view
        visible, not about complete data retrieval. See
        ``specs/002-connectors/002d-ckan-search/spec.md`` FR-012 for
        the contract.
        """
        url = f"{portal['base_url']}{portal['api_path']}/datastore_search"
        params: dict[str, str | int] = {"resource_id": resource_id, "limit": limit}
        if q:
            params["q"] = q
        resp = await self._http.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
        if not data.get("success"):
            return [], None
        result = data.get("result") or {}
        records = result.get("records") or []
        # CKAN returns the true row count in `result.total`. When it is
        # strictly greater than the page we requested, we are looking at
        # the tip of a larger dataset — mark it so downstream consumers
        # can tell a partial view from a complete one.
        total = result.get("total")
        try:
            total_int = int(total) if total is not None else None
        except (TypeError, ValueError):
            total_int = None
        if total_int is not None and total_int > limit and len(records) >= limit:
            return records, {
                "truncated": True,
                "truncation_reason": "datastore_pagination_limit",
                "truncation_limit": limit,
                "total_available": total_int,
            }
        return records, None

    async def _fetch_csv(self, url: str) -> tuple[list[dict], dict | None]:
        """Fetch and parse a CSV resource, returning truncation info per FR-011.

        Returns ``(records, truncation)``. Byte-level truncation (the CSV
        exceeds ``MAX_CSV_BYTES``) takes precedence over row-level
        truncation in the reason code, because if we chopped the bytes
        we may also have cut the last record mid-line and the row count
        is no longer meaningful as an "I read the whole file" signal.
        """
        resp = await self._http.get(url, follow_redirects=True)
        resp.raise_for_status()
        text = resp.text
        byte_truncated = len(text) > MAX_CSV_BYTES
        if byte_truncated:
            text = text[:MAX_CSV_BYTES]
        records, row_trunc = _parse_csv_text(text)
        if byte_truncated:
            return records, {
                "truncated": True,
                "truncation_reason": "max_bytes_exceeded",
                "truncation_limit": MAX_CSV_BYTES,
            }
        return records, row_trunc

    async def search_datasets(
        self,
        query: str,
        portal_id: str | None = None,
        rows: int = 10,
    ) -> list[DataResult]:
        try:
            target_portals = [p for p in PORTALS if p["id"] == portal_id] if portal_id else PORTALS
            if not target_portals:
                return []

            search_query = "*:*" if query in ("*", "*:*") else query

            async def _search_one(portal: dict) -> list[DataResult]:
                try:
                    datasets = await self._search_portal(portal, search_query, rows)
                    results: list[DataResult] = []
                    now = datetime.now(UTC).isoformat()
                    for ds in datasets:
                        # Try datastore first
                        suitable = [
                            r
                            for r in ds.get("resources", [])
                            if r.get("format", "").upper() in ("CSV", "JSON")
                        ]
                        records: list[dict] | None = None
                        datastore_truncation: dict | None = None
                        for resource in suitable[:2]:
                            try:
                                records, datastore_truncation = await self._fetch_datastore(
                                    portal, resource["id"], None, 50
                                )
                                if records:
                                    break
                            except Exception:
                                continue

                        csv_truncation: dict | None = None
                        if not records:
                            csv_resources = [
                                r
                                for r in ds.get("resources", [])
                                if r.get("format", "").upper() == "CSV"
                            ]
                            for resource in csv_resources[:1]:
                                try:
                                    records, csv_truncation = await self._fetch_csv(resource["url"])
                                    if records:
                                        break
                                except Exception:
                                    continue

                        # FR-011 / FR-012: surface truncation metadata whether
                        # the partial view came from the CSV path (byte / row
                        # limit) or the datastore path (pagination limit).
                        # Only one of the two can be non-None for any given
                        # result — we try datastore first, fall back to CSV.
                        metadata: dict = {
                            "total_records": len(records) if records else 0,
                            "fetched_at": now,
                            "description": ds.get("notes", ""),
                        }
                        if datastore_truncation is not None:
                            metadata.update(datastore_truncation)
                        elif csv_truncation is not None:
                            metadata.update(csv_truncation)

                        results.append(
                            DataResult(
                                source=f"ckan:{portal['id']}",
                                portal_name=portal["name"],
                                portal_url=f"{portal['base_url']}/dataset/{ds.get('name', '')}",
                                dataset_title=ds.get("title", ""),
                                format="json",
                                records=(records[:50] if records else []),
                                metadata=metadata,
                            )
                        )
                    return results
                except Exception:
                    logger.warning("CKAN search failed for %s", portal["name"], exc_info=True)
                    return []

            portal_results = await asyncio.gather(
                *[_search_one(p) for p in target_portals],
                return_exceptions=True,
            )

            all_results: list[DataResult] = []
            for result in portal_results:
                if isinstance(result, list):
                    all_results.extend(result)
            return all_results
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(
                error_code=ErrorCode.CN_CKAN_UNAVAILABLE,
                details={"query": query[:100], "reason": str(exc)},
            ) from exc

    async def query_datastore(
        self,
        portal_id: str,
        resource_id: str,
        q: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        try:
            portal = _find_portal(portal_id)
            if not portal:
                return []
            # The port contract for query_datastore returns raw records
            # without truncation metadata — the DataResult-wrapping path
            # (search_datasets) is the one FR-012 targets. Discard the
            # truncation tuple element here.
            records, _ = await self._fetch_datastore(portal, resource_id, q, limit)
            return records
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(
                error_code=ErrorCode.CN_CKAN_UNAVAILABLE,
                details={"portal_id": portal_id, "reason": str(exc)},
            ) from exc
