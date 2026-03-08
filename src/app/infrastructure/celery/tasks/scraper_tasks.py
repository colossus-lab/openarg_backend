"""
Scraper Worker — Agente de indexación del catálogo.

Recorre los portales de datos abiertos, extrae metadata de cada dataset
y la guarda en PostgreSQL. Luego dispara el embedding de cada uno.
"""
from __future__ import annotations

import json
import logging
import os
import random
import time
from datetime import UTC, datetime

import httpx
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text

from app.infrastructure.adapters.connectors.ckan_search_adapter import PORTALS
from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine
from app.infrastructure.resilience.circuit_breaker import get_circuit_breaker

logger = logging.getLogger(__name__)

# Browser-like User-Agent (some portals like CABA block default httpx UA)
_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

# Generado automáticamente desde PORTALS (ckan_search_adapter.py) — fuente única de verdad.
# Aliases: el scraper guarda datos en la DB con estos IDs, que difieren del search adapter.
_SCRAPER_ID_ALIASES = {
    "nacional": "datos_gob_ar",
    "pba": "buenos_aires_prov",
    "entrerios": "entre_rios",
}
PORTAL_URLS = {
    _SCRAPER_ID_ALIASES.get(p["id"], p["id"]): f'{p["base_url"]}{p["api_path"]}'
    for p in PORTALS
}


PORTALS_SKIP_SSL = {"salud"}

class PortalUnavailableError(Exception):
    """Portal is down/maintenance — do NOT retry."""


class PortalHTMLResponseError(PortalUnavailableError):
    """Portal returned HTML instead of JSON (maintenance page, WAF block, etc.)."""


def _retry_countdown(attempt: int, base: float = 30.0, cap: float = 300.0) -> float:
    """Exponential backoff with full jitter: uniform(0, min(base * 2^attempt, cap))."""
    return random.uniform(0, min(base * (2 ** attempt), cap))


def _check_html_response(resp, portal: str) -> None:
    """Detect HTML maintenance pages or WAF blocks that arrive with 200 status."""
    ct = resp.headers.get("content-type", "")
    if "text/html" in ct:
        snippet = resp.text[:300] if resp.text else "(empty)"
        raise PortalHTMLResponseError(
            f"Portal {portal} returned HTML instead of JSON "
            f"(likely maintenance or WAF block): {snippet}"
        )
    # Some broken servers return raw HTML without proper Content-Type
    body = resp.content[:50] if resp.content else b""
    if body.lstrip().startswith(b"<html") or body.lstrip().startswith(b"<!DOCTYPE"):
        snippet = resp.text[:300] if resp.text else "(empty)"
        raise PortalHTMLResponseError(
            f"Portal {portal} returned raw HTML (no proper HTTP headers): {snippet}"
        )


@celery_app.task(name="openarg.scrape_catalog", bind=True, max_retries=2, soft_time_limit=900, time_limit=1080)
def scrape_catalog(self, portal: str = "datos_gob_ar", batch_size: int = 100):
    """
    Scrapea el catálogo completo de un portal de datos.
    Guarda metadata en tabla datasets. Dispara embedding para cada uno.
    """
    logger.info(f"Starting catalog scrape for portal: {portal} (attempt {self.request.retries + 1}/{self.max_retries + 1})")

    base_url = PORTAL_URLS.get(portal)
    if not base_url:
        logger.error(f"Unknown portal: {portal}")
        return {"error": f"Unknown portal: {portal}"}

    # Circuit breaker: skip portal if it has been failing repeatedly
    cb = get_circuit_breaker(f"scraper:{portal}", failure_threshold=3, recovery_timeout=300.0)
    if cb.is_open:
        logger.warning(
            "Circuit OPEN for portal %s — skipping scrape (will recover in ~5 min)", portal
        )
        return {"portal": portal, "skipped": True, "reason": "circuit_open"}

    engine = get_sync_engine()
    client = httpx.Client(
        timeout=httpx.Timeout(connect=20.0, read=60.0, write=30.0, pool=30.0),
        headers={"User-Agent": _USER_AGENT},
        follow_redirects=True,
        verify=portal not in PORTALS_SKIP_SSL,
    )

    def _safe_json(resp, label: str) -> dict:
        """Parse JSON response, handling empty/HTML bodies gracefully."""
        _check_html_response(resp, portal)
        if not resp.content or not resp.content.strip():
            logger.warning("Empty response body from %s (HTTP %d)", label, resp.status_code)
            return {}
        try:
            return resp.json()
        except Exception:
            logger.warning("Invalid JSON from %s: %s", label, resp.text[:200])
            return {}

    try:
        # Get total count
        count_resp = client.get(f"{base_url}/package_search", params={"rows": 0})
        count_resp.raise_for_status()
        data = _safe_json(count_resp, f"{portal}/count")
        total = data.get("result", {}).get("count", 0)
        if not total:
            logger.warning("Portal %s returned 0 packages or bad response — skipping", portal)
            return {"portal": portal, "total_packages": 0, "datasets_indexed": 0}
        logger.info(f"Portal {portal} has {total} packages")

        indexed = 0
        offset = 0
        consecutive_page_errors = 0
        max_consecutive_page_errors = 3

        while offset < total:
            try:
                resp = client.get(
                    f"{base_url}/package_search",
                    params={"rows": batch_size, "start": offset},
                )
                resp.raise_for_status()
                page_data = _safe_json(resp, f"{portal}/page?start={offset}")
                packages = page_data.get("result", {}).get("results", [])
                consecutive_page_errors = 0  # reset on success
            except PortalUnavailableError:
                raise  # propagate immediately — no retry
            except Exception as page_exc:
                consecutive_page_errors += 1
                logger.warning(
                    "Page fetch failed for %s offset=%d (%d/%d consecutive): %s",
                    portal, offset, consecutive_page_errors, max_consecutive_page_errors, page_exc,
                )
                if consecutive_page_errors >= max_consecutive_page_errors:
                    logger.error(
                        "Too many consecutive page errors for %s — aborting with partial results",
                        portal,
                    )
                    break
                offset += batch_size
                continue

            # Collect resources for batch upsert
            batch_rows = []
            for pkg in packages:
                for resource in pkg.get("resources", []):
                    fmt = resource.get("format", "").lower()
                    if fmt not in ("csv", "json", "xlsx", "xls", "geojson", "txt", "ods", "zip", "xml"):
                        continue

                    columns_list = []
                    if resource.get("attributesDescription"):
                        try:
                            attrs = json.loads(resource["attributesDescription"])
                            columns_list = list(attrs.keys()) if isinstance(attrs, dict) else []
                        except (json.JSONDecodeError, TypeError):
                            pass

                    # Extract last-updated timestamp: prefer resource-level, fallback to package
                    last_updated = None
                    for ts_field in (
                        resource.get("last_modified"),
                        pkg.get("metadata_modified"),
                        pkg.get("revision_timestamp"),
                    ):
                        if ts_field:
                            try:
                                last_updated = datetime.fromisoformat(
                                    ts_field.replace("Z", "+00:00")
                                )
                                break
                            except (ValueError, AttributeError):
                                continue

                    batch_rows.append({
                        "sid": resource.get("id", ""),
                        "title": pkg.get("title", ""),
                        "desc": pkg.get("notes", ""),
                        "org": pkg.get("organization", {}).get("title", ""),
                        "portal": portal,
                        "url": pkg.get("url", ""),
                        "dl": resource.get("url", ""),
                        "fmt": fmt,
                        "cols": json.dumps(columns_list),
                        "tags": ",".join(t.get("name", "") for t in pkg.get("tags", [])),
                        "now": datetime.now(UTC),
                        "last_upd": last_updated,
                    })

            # Batch upsert using ON CONFLICT (eliminates N+1 SELECT per resource)
            if batch_rows:
                source_ids = [r["sid"] for r in batch_rows]
                with engine.begin() as conn:
                    conn.execute(
                        text("""
                            INSERT INTO datasets
                                (source_id, title, description, organization,
                                 portal, url, download_url, format, columns, tags,
                                 last_updated_at)
                            VALUES
                                (:sid, :title, :desc, :org, :portal, :url, :dl, :fmt, :cols, :tags,
                                 :last_upd)
                            ON CONFLICT (source_id, portal) DO UPDATE SET
                                title = EXCLUDED.title, description = EXCLUDED.description,
                                organization = EXCLUDED.organization, url = EXCLUDED.url,
                                download_url = EXCLUDED.download_url, format = EXCLUDED.format,
                                columns = EXCLUDED.columns, tags = EXCLUDED.tags,
                                last_updated_at = COALESCE(EXCLUDED.last_updated_at, datasets.last_updated_at),
                                updated_at = :now
                        """),
                        batch_rows,
                    )
                    # Fetch IDs separately (RETURNING doesn't work with executemany)
                    result = conn.execute(
                        text(
                            "SELECT CAST(id AS text) FROM datasets "
                            "WHERE source_id = ANY(:sids) AND portal = :portal"
                        ),
                        {"sids": source_ids, "portal": portal},
                    )
                    dataset_ids = [row[0] for row in result.fetchall()]

                for dataset_id in dataset_ids:
                    index_dataset_embedding.delay(dataset_id)
                    indexed += 1

            offset += batch_size
            logger.info(f"Scraped {offset}/{total} packages from {portal}")

            # Rate-limit page requests for portals with WAF (e.g. CABA)
            if portal == "caba" and offset < total:
                time.sleep(2.0)

        cb.record_success()
        return {"portal": portal, "total_packages": total, "datasets_indexed": indexed}

    except SoftTimeLimitExceeded:
        logger.error(f"Scraper timed out for portal {portal}")
        cb.record_failure()
        raise
    except PortalUnavailableError as exc:
        # Portal is down/maintenance/WAF — do NOT retry, fail immediately
        cb.record_failure()
        logger.error(f"Portal {portal} unavailable (no retry): {exc}")
        return {"portal": portal, "error": str(exc), "retryable": False}
    except (httpx.ConnectTimeout, httpx.ConnectError) as exc:
        # Connection-level failure — retry with backoff but limited
        cb.record_failure()
        logger.warning(f"Connection failed for {portal}: {exc}")
        countdown = _retry_countdown(self.request.retries)
        raise self.retry(exc=exc, countdown=countdown)
    except httpx.RemoteProtocolError as exc:
        # Broken HTTP response (e.g., HTTP/0.9 maintenance page) — no retry
        cb.record_failure()
        logger.error(f"Protocol error from {portal} (no retry): {exc}")
        return {"portal": portal, "error": str(exc), "retryable": False}
    except Exception as exc:
        cb.record_failure()
        logger.exception(f"Scraper failed for {portal}")
        countdown = _retry_countdown(self.request.retries)
        raise self.retry(exc=exc, countdown=countdown)
    finally:
        client.close()
        engine.dispose()


@celery_app.task(name="openarg.scrape_all_portals")
def scrape_all_portals():
    """Dispara scrape_catalog para todos los portales, escalonado 10s entre cada uno."""
    portals = list(PORTAL_URLS.keys())
    for i, portal in enumerate(portals):
        # Stagger dispatches by 10 seconds to avoid overwhelming the worker
        scrape_catalog.apply_async(args=[portal], countdown=i * 10)
    logger.info(f"Dispatched scrape for {len(portals)} portals (staggered)")
    return {"dispatched": portals}


def _portal_display_name(portal: str) -> str:
    """Nombre legible del portal para chunks. Usa PORTALS como fuente.

    Soporta tanto IDs de search adapter (nacional, pba, entrerios)
    como IDs de DB/scraper (datos_gob_ar, buenos_aires_prov, entre_rios).
    """
    _names = {p["id"]: p["name"] for p in PORTALS}
    if portal in _names:
        return _names[portal]
    # Reverse alias: DB ID → search adapter ID → name
    _reverse = {v: k for k, v in _SCRAPER_ID_ALIASES.items()}
    canonical = _reverse.get(portal)
    if canonical and canonical in _names:
        return _names[canonical]
    return portal


def _get_data_statistics(engine, dataset_id: str) -> str | None:
    """
    Extrae estadísticas reales de los datos cacheados en la tabla cache_*.
    Retorna un texto resumen o None si no hay datos cacheados.
    """
    try:
        with engine.begin() as conn:
            cache_row = conn.execute(
                text(
                    "SELECT table_name, row_count, columns_json "
                    "FROM cached_datasets "
                    "WHERE dataset_id = CAST(:did AS uuid) AND status = 'ready'"
                ),
                {"did": dataset_id},
            ).fetchone()

        if not cache_row or not cache_row.table_name:
            return None

        table_name = cache_row.table_name
        # Validate table name to prevent injection
        import re
        if not re.match(r"^cache_[a-z0-9_]{1,100}$", table_name):
            return None

        cols_json = cache_row.columns_json or "[]"
        try:
            columns = json.loads(cols_json) if isinstance(cols_json, str) else cols_json
        except (json.JSONDecodeError, TypeError):
            columns = []

        if not columns:
            return None

        stats_parts = [
            f"Estadísticas de datos reales ({cache_row.row_count or '?'} registros):",
        ]

        with engine.begin() as conn:
            # Sample up to 5 columns for statistics
            sample_cols = columns[:5]
            for col_name in sample_cols:
                # Sanitize column name
                safe_col = col_name.replace('"', '""')
                try:
                    # Get column type and basic stats
                    info = conn.execute(
                        text(f'SELECT pg_typeof("{safe_col}") as dtype FROM "{table_name}" LIMIT 1'),
                    ).fetchone()
                    dtype = str(info[0]) if info else "unknown"

                    if dtype in ("integer", "bigint", "numeric", "double precision", "real"):
                        agg = conn.execute(
                            text(
                                f'SELECT MIN("{safe_col}"), MAX("{safe_col}"), '
                                f'ROUND(AVG("{safe_col}")::numeric, 2) '
                                f'FROM "{table_name}" WHERE "{safe_col}" IS NOT NULL'
                            ),
                        ).fetchone()
                        if agg and agg[0] is not None:
                            stats_parts.append(
                                f"- {col_name} (numérico): min={agg[0]}, max={agg[1]}, promedio={agg[2]}"
                            )
                    elif dtype in ("date", "timestamp without time zone", "timestamp with time zone"):
                        agg = conn.execute(
                            text(
                                f'SELECT MIN("{safe_col}"), MAX("{safe_col}") '
                                f'FROM "{table_name}" WHERE "{safe_col}" IS NOT NULL'
                            ),
                        ).fetchone()
                        if agg and agg[0] is not None:
                            stats_parts.append(
                                f"- {col_name} (fecha): desde {agg[0]} hasta {agg[1]}"
                            )
                    else:
                        # Text/categorical: count distinct + sample values
                        agg = conn.execute(
                            text(
                                f'SELECT COUNT(DISTINCT "{safe_col}") '
                                f'FROM "{table_name}" WHERE "{safe_col}" IS NOT NULL'
                            ),
                        ).fetchone()
                        n_distinct = agg[0] if agg else 0
                        samples = conn.execute(
                            text(
                                f'SELECT DISTINCT "{safe_col}" FROM "{table_name}" '
                                f'WHERE "{safe_col}" IS NOT NULL LIMIT 5'
                            ),
                        ).fetchall()
                        sample_vals = [str(s[0])[:50] for s in samples]
                        stats_parts.append(
                            f"- {col_name} (texto, {n_distinct} valores únicos): "
                            f"ej. {', '.join(sample_vals)}"
                        )
                except Exception:
                    continue

            # Show remaining column names if any
            if len(columns) > 5:
                remaining = columns[5:]
                stats_parts.append(f"Otras columnas: {', '.join(str(c) for c in remaining)}")

        return "\n".join(stats_parts) if len(stats_parts) > 1 else None

    except Exception:
        logger.debug("Could not extract data statistics for %s", dataset_id, exc_info=True)
        return None


def _get_sample_rows_text(engine, dataset_id: str, title: str, portal_name: str) -> str | None:
    """Genera texto natural de las primeras filas para embedding."""
    import re

    try:
        with engine.begin() as conn:
            cache_row = conn.execute(
                text(
                    "SELECT table_name, columns_json FROM cached_datasets "
                    "WHERE dataset_id = CAST(:did AS uuid) AND status = 'ready'"
                ),
                {"did": dataset_id},
            ).fetchone()

        if not cache_row or not cache_row.table_name:
            return None

        table_name = cache_row.table_name
        # Validate table name to prevent injection
        if not re.match(r"^cache_[a-z0-9_]{1,100}$", table_name):
            return None

        with engine.begin() as conn:
            rows = conn.execute(
                text(f'SELECT * FROM "{table_name}" LIMIT 15')
            ).fetchall()
            if not rows:
                return None
            col_desc = conn.execute(
                text(f'SELECT * FROM "{table_name}" LIMIT 0')
            ).cursor.description
            cols = [desc[0] for desc in col_desc]

        parts = [f"Muestra de datos reales del dataset '{title}' ({portal_name}):"]
        for i, r in enumerate(rows[:15], 1):
            row_parts = []
            for col, val in zip(cols, r, strict=False):
                if val is not None:
                    row_parts.append(f"{col}: {str(val)[:100]}")
            parts.append(f"Registro {i}: {', '.join(row_parts)}")

        return "\n".join(parts)

    except Exception:
        logger.debug("Could not extract sample rows for %s", dataset_id, exc_info=True)
        return None


@celery_app.task(name="openarg.index_dataset", bind=True, max_retries=3, soft_time_limit=120, time_limit=180)
def index_dataset_embedding(self, dataset_id: str):
    """
    Genera múltiples chunks de embedding por dataset:
    1. Chunk principal: título + descripción + organización + tags + jurisdicción
    2. Chunk de columnas: lista de columnas con contexto semántico
    3. Chunk de contexto: uso potencial, preguntas que responde
    4. Chunk de estadísticas: datos reales (rangos, promedios, valores de ejemplo)

    Esto mejora la búsqueda vectorial al tener más puntos de entrada semánticos.
    """
    import google.generativeai as genai

    logger.info(f"Generating embeddings for dataset: {dataset_id}")
    engine = get_sync_engine()

    try:
        with engine.begin() as conn:
            row = conn.execute(
                text(
                    "SELECT title, description, columns, tags, organization, portal, "
                    "format, download_url, is_cached, row_count "
                    "FROM datasets WHERE id = CAST(:id AS uuid)"
                ),
                {"id": dataset_id},
            ).fetchone()

        if not row:
            logger.warning(f"Dataset {dataset_id} not found")
            return

        portal_name = _portal_display_name(row.portal)

        # Build multiple chunks for richer semantic search
        chunks = []

        # Chunk 1: Main metadata (catches general queries)
        main_parts = [
            f"Dataset: {row.title}",
            f"Descripción: {row.description or 'Sin descripción'}",
            f"Organización: {row.organization or 'No especificada'}",
            f"Portal: {portal_name} ({row.portal})",
            f"Formato: {row.format or 'desconocido'}",
        ]
        if row.tags:
            main_parts.append(f"Temas: {row.tags}")
        if row.row_count:
            main_parts.append(f"Cantidad de registros: {row.row_count}")
        if row.download_url:
            main_parts.append(f"URL de descarga: {row.download_url}")
        chunks.append("\n".join(main_parts))

        # Chunk 2: Columns focus (catches column-specific queries)
        columns_str = row.columns or "[]"
        try:
            cols = json.loads(columns_str) if isinstance(columns_str, str) else columns_str
        except (json.JSONDecodeError, TypeError):
            cols = []

        if cols and len(cols) > 0:
            col_parts = [
                f"Dataset '{row.title}' de {portal_name} contiene las siguientes columnas/variables:",
                ", ".join(str(c) for c in cols),
                f"Publicado por: {row.organization or 'No especificada'}",
                f"Este dataset permite analizar: {row.description or row.title}",
            ]
            if row.row_count:
                col_parts.append(f"Total de registros disponibles: {row.row_count}")
            chunks.append("\n".join(col_parts))

        # Chunk 3: Contextual/use-case chunk (catches "how to" queries)
        context_parts = [
            f"Para consultar datos sobre {row.title.lower()} en Argentina,",
            f"existe el dataset '{row.title}' publicado por {row.organization or 'el gobierno'}.",
            f"Disponible en formato {row.format or 'CSV'} desde {portal_name}.",
        ]
        if row.description and len(row.description) > 20:
            context_parts.append(f"Detalle: {row.description[:500]}")
        if row.is_cached:
            context_parts.append(
                f"Los datos están cacheados localmente ({row.row_count or '?'} filas) "
                f"para consulta SQL directa."
            )
        if row.tags:
            context_parts.append(f"Palabras clave: {row.tags}")
        chunks.append("\n".join(context_parts))

        # Chunk 4: Data statistics (catches queries about specific values/ranges)
        data_stats = _get_data_statistics(engine, dataset_id)
        if data_stats:
            stats_chunk_parts = [
                f"Datos reales del dataset '{row.title}' ({portal_name}):",
                data_stats,
            ]
            if cols:
                stats_chunk_parts.append(f"Columnas: {', '.join(str(c) for c in cols[:10])}")
            chunks.append("\n".join(stats_chunk_parts))

        # Chunk 5: Sample rows (rich data context for semantic search)
        if row.is_cached:
            sample_text = _get_sample_rows_text(engine, dataset_id, row.title, portal_name)
            if sample_text:
                chunks.append(sample_text)

        # Generate embeddings in batch
        genai.configure(api_key=os.getenv("GEMINI_API_KEY", ""))
        resp = genai.embed_content(
            model="models/gemini-embedding-001",
            content=chunks,
            output_dimensionality=768,
        )
        embeddings = resp["embedding"]

        with engine.begin() as conn:
            # Delete old chunks
            conn.execute(
                text("DELETE FROM dataset_chunks WHERE dataset_id = :did"),
                {"did": dataset_id},
            )
            # Insert all chunks
            for i, emb in enumerate(embeddings):
                embedding_str = "[" + ",".join(str(v) for v in emb) + "]"
                conn.execute(
                    text("""
                        INSERT INTO dataset_chunks (dataset_id, content, embedding)
                        VALUES (:did, :content, CAST(:embedding AS vector))
                    """),
                    {"did": dataset_id, "content": chunks[i], "embedding": embedding_str},
                )

        logger.info(f"Indexed {len(chunks)} chunks for dataset: {dataset_id}")
        return {"dataset_id": dataset_id, "chunks_created": len(chunks)}

    except SoftTimeLimitExceeded:
        logger.error(f"Embedding timed out for dataset {dataset_id}")
        raise
    except Exception as exc:
        logger.exception(f"Embedding failed for dataset {dataset_id}")
        countdown = _retry_countdown(self.request.retries, base=15.0, cap=120.0)
        raise self.retry(exc=exc, countdown=countdown)
    finally:
        engine.dispose()
