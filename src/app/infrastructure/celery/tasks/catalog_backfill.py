"""Backfill `catalog_resources` from existing datasets + cached_datasets (WS2/WS4).

Reads `datasets` JOIN LEFT `cached_datasets` and produces one
`catalog_resources` row per existing dataset using the deterministic
title_extractor + physical_namer.

Idempotent by `resource_identity`. Safe to re-run — UPSERTs.

Usage:
  - As Celery task (`openarg.catalog_backfill`)
  - One-shot: `python -m app.infrastructure.celery.tasks.catalog_backfill --dry-run`

Doesn't touch serving — population only. WS3 wires discovery later.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys

import boto3
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.application.catalog.physical_namer import PhysicalNamer
from app.application.catalog.title_extractor import TitleExtractor
from app.domain.entities.dataset.catalog_resource import (
    MATERIALIZATION_FAILED,
    MATERIALIZATION_NON_TABULAR,
    MATERIALIZATION_PENDING,
    MATERIALIZATION_READY,
    RESOURCE_KIND_DOCUMENT_BUNDLE,
    RESOURCE_KIND_FILE,
)
from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)
_EMBEDDING_MODEL = os.getenv("BEDROCK_EMBEDDING_MODEL", "cohere.embed-multilingual-v3")
_EMBEDDING_BATCH_LIMIT = 96
_CATALOG_BACKFILL_LOCK_KEY = 156002
_CATALOG_PARSER_VERSION = "phase4-v1"
_CATALOG_NORMALIZATION_VERSION = "phase4-v1"
# When backfilling, datasets that already had `cached_status` set were
# parsed with whatever parser was alive when they were first ingested —
# possibly `phase3` or earlier. Marking them as `phase4-v1` would lie:
# the metadata would say "parsed with the new parser" without any
# reingest. We use a distinct sentinel so dashboards / reingest filters
# (`WHERE parser_version != 'phase4-v1'`) can tell apart genuine
# phase4-v1 rows from backfilled rows of unknown provenance.
_BACKFILL_LEGACY_PARSER_VERSION = "legacy:unknown"
_BACKFILL_LEGACY_NORMALIZATION_VERSION = "legacy:unknown"


def _resource_identity(portal: str, source_id: str, sub_path: str | None = None) -> str:
    """Stable key. Same inputs always yield same identity."""
    base = f"{portal or ''}::{source_id or ''}"
    if sub_path:
        return f"{base}::{sub_path}"
    return base


def _get_bedrock_client():
    return boto3.client(
        "bedrock-runtime",
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )


def _build_catalog_embedding_text(display_name: str | None, canonical_title: str | None) -> str:
    parts: list[str] = []
    seen: set[str] = set()
    for raw in (display_name, canonical_title):
        cleaned = (raw or "").strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        parts.append(cleaned)
    return " - ".join(parts)


def _embed_texts(texts: list[str]) -> list[list[float]]:
    bedrock = _get_bedrock_client()
    resp = bedrock.invoke_model(
        modelId=_EMBEDDING_MODEL,
        body=json.dumps(
            {
                "texts": texts,
                "input_type": "search_document",
                "truncate": "END",
            }
        ),
    )
    result = json.loads(resp["body"].read())
    return result["embeddings"]


# Same pattern as collector_tasks._HELD_ADVISORY_LOCKS — keep the
# acquiring connection open between acquire and release so the session
# lock survives.
_HELD_BACKFILL_LOCK: dict[int, "object"] = {}


def _try_backfill_lock(engine: Engine) -> bool:
    if _CATALOG_BACKFILL_LOCK_KEY in _HELD_BACKFILL_LOCK:
        return False
    conn = engine.connect()
    try:
        acquired = bool(
            conn.execute(
                text("SELECT pg_try_advisory_lock(:key)"),
                {"key": _CATALOG_BACKFILL_LOCK_KEY},
            ).scalar()
        )
        conn.rollback()
    except Exception:
        logger.warning("Could not probe catalog_backfill lock", exc_info=True)
        conn.close()
        return False

    if not acquired:
        conn.close()
        return False

    _HELD_BACKFILL_LOCK[_CATALOG_BACKFILL_LOCK_KEY] = conn
    return True


def _release_backfill_lock(engine: Engine) -> None:
    """`engine` is accepted for backward compat but ignored; the cached
    connection from `_try_backfill_lock` is the one that holds the lock."""
    conn = _HELD_BACKFILL_LOCK.pop(_CATALOG_BACKFILL_LOCK_KEY, None)
    if conn is None:
        return
    try:
        if conn.in_transaction():
            conn.rollback()
        conn.execute(text("SELECT pg_advisory_unlock(:key)"), {"key": _CATALOG_BACKFILL_LOCK_KEY})
        conn.rollback()
    except Exception:
        # Lock release failure is a real concern — it means the session
        # lock will linger until the connection is GC'd. Warning so it
        # surfaces in dashboards instead of being swallowed at debug.
        logger.warning("Could not release catalog_backfill advisory lock", exc_info=True)
    finally:
        try:
            conn.close()
        except Exception:
            pass


_UPSERT_SQL = text(
    """
    INSERT INTO catalog_resources (
        resource_identity, dataset_id, portal, source_id,
        s3_key, filename, sub_path,
        raw_title, canonical_title, display_name,
        title_source, title_confidence,
        resource_kind, materialization_status, materialized_table_name,
        layout_profile, header_quality,
        parser_version, normalization_version,
        created_at, updated_at
    ) VALUES (
        :resource_identity, CAST(NULLIF(:dataset_id, '') AS uuid),
        :portal, :source_id,
        :s3_key, :filename, :sub_path,
        :raw_title, :canonical_title, :display_name,
        :title_source, :title_confidence,
        :resource_kind, :materialization_status, :materialized_table_name,
        :layout_profile, :header_quality,
        :parser_version, :normalization_version,
        NOW(), NOW()
    )
    ON CONFLICT (resource_identity) DO UPDATE SET
        dataset_id = EXCLUDED.dataset_id,
        s3_key = EXCLUDED.s3_key,
        filename = EXCLUDED.filename,
        canonical_title = EXCLUDED.canonical_title,
        display_name = EXCLUDED.display_name,
        title_source = EXCLUDED.title_source,
        title_confidence = EXCLUDED.title_confidence,
        materialization_status = EXCLUDED.materialization_status,
        materialized_table_name = EXCLUDED.materialized_table_name,
        layout_profile = EXCLUDED.layout_profile,
        header_quality = EXCLUDED.header_quality,
        parser_version = EXCLUDED.parser_version,
        normalization_version = EXCLUDED.normalization_version,
        updated_at = NOW()
    """
)


_QUERY_SQL = text(
    """
    SELECT d.id::text AS dataset_id,
           d.portal,
           d.source_id,
           d.title AS raw_title,
           d.organization,
           d.url,
           d.format,
           cd.table_name,
           cd.s3_key,
           cd.status AS cached_status,
           cd.error_message,
           cd.layout_profile,
           cd.header_quality
    FROM datasets d
    LEFT JOIN LATERAL (
        -- A dataset can have several cached_datasets rows when retries hit
        -- different schema variants. Pick the most authoritative one for
        -- catalog purposes: prefer ready over everything else, then most
        -- recently updated. Avoids duplicate output rows that would
        -- otherwise toggle the same `resource_identity` UPSERT back and
        -- forth on each backfill iteration.
        SELECT cd.table_name, cd.s3_key, cd.status, cd.error_message, cd.updated_at,
               cd.layout_profile, cd.header_quality
        FROM cached_datasets cd
        WHERE cd.dataset_id = d.id
        ORDER BY (cd.status = 'ready') DESC,
                 cd.updated_at DESC NULLS LAST
        LIMIT 1
    ) cd ON true
    ORDER BY d.created_at NULLS LAST
    LIMIT :limit OFFSET :offset
    """
)

_EMBEDDING_QUERY_SQL = text(
    """
    SELECT id::text AS id,
           display_name,
           canonical_title
    FROM catalog_resources
    WHERE embedding IS NULL
      AND (COALESCE(display_name, '') <> '' OR COALESCE(canonical_title, '') <> '')
    ORDER BY updated_at NULLS FIRST, created_at NULLS FIRST
    LIMIT :limit
    """
)

_EMBEDDING_UPDATE_SQL = text(
    """
    UPDATE catalog_resources
    SET embedding = CAST(:embedding AS vector),
        updated_at = NOW()
    WHERE id = CAST(:id AS uuid)
    """
)

_CONNECTOR_UPSERT_SQL = text(
    """
    INSERT INTO catalog_resources (
        resource_identity, portal, source_id,
        raw_title, canonical_title, display_name,
        title_source, title_confidence,
        resource_kind, materialization_status,
        domain, subdomain, taxonomy_key,
        created_at, updated_at
    ) VALUES (
        :resource_identity, :portal, :source_id,
        :raw_title, :canonical_title, :display_name,
        'manual', 1.0,
        'connector_endpoint', 'live_api',
        :domain, :subdomain, :taxonomy_key,
        NOW(), NOW()
    )
    ON CONFLICT (resource_identity) DO UPDATE SET
        raw_title = EXCLUDED.raw_title,
        canonical_title = EXCLUDED.canonical_title,
        display_name = EXCLUDED.display_name,
        resource_kind = EXCLUDED.resource_kind,
        materialization_status = EXCLUDED.materialization_status,
        domain = EXCLUDED.domain,
        subdomain = EXCLUDED.subdomain,
        taxonomy_key = EXCLUDED.taxonomy_key,
        updated_at = NOW()
    """
)

_CONNECTOR_ENDPOINTS: tuple[dict[str, str], ...] = (
    {
        "portal": "bcra",
        "source_id": "query_bcra",
        "display_name": "BCRA",
        "canonical_title": "BCRA - cotizaciones y variables monetarias",
        "domain": "economia",
        "subdomain": "monetario",
        "taxonomy_key": "query_bcra",
    },
    {
        "portal": "series_tiempo",
        "source_id": "query_series",
        "display_name": "Series de Tiempo",
        "canonical_title": "Series de Tiempo - indicadores economicos",
        "domain": "economia",
        "subdomain": "indicadores",
        "taxonomy_key": "query_series",
    },
    {
        "portal": "argentina_datos",
        "source_id": "query_argentina_datos",
        "display_name": "Argentina Datos",
        "canonical_title": "Argentina Datos - datasets y APIs publicas",
        "domain": "datos_abiertos",
        "subdomain": "federal",
        "taxonomy_key": "query_argentina_datos",
    },
    {
        "portal": "georef",
        "source_id": "query_georef",
        "display_name": "GeoRef",
        "canonical_title": "GeoRef - referencia geografica argentina",
        "domain": "geografia",
        "subdomain": "referencias",
        "taxonomy_key": "query_georef",
    },
    {
        "portal": "ckan_search",
        "source_id": "search_ckan",
        "display_name": "Busqueda CKAN",
        "canonical_title": "Busqueda CKAN - catalogos de datos abiertos",
        "domain": "datos_abiertos",
        "subdomain": "catalogos",
        "taxonomy_key": "search_ckan",
    },
    {
        "portal": "sesiones",
        "source_id": "query_sesiones",
        "display_name": "Sesiones del Congreso",
        "canonical_title": "Sesiones del Congreso - textos y debates",
        "domain": "legislativo",
        "subdomain": "sesiones",
        "taxonomy_key": "query_sesiones",
    },
    {
        "portal": "ddjj",
        "source_id": "query_ddjj",
        "display_name": "Declaraciones Juradas",
        "canonical_title": "Declaraciones Juradas - patrimonio publico",
        "domain": "transparencia",
        "subdomain": "ddjj",
        "taxonomy_key": "query_ddjj",
    },
    {
        "portal": "staff",
        "source_id": "query_staff",
        "display_name": "Personal Legislativo",
        "canonical_title": "Personal Legislativo - staff y cambios",
        "domain": "legislativo",
        "subdomain": "staff",
        "taxonomy_key": "query_staff",
    },
    {
        "portal": "sandbox",
        "source_id": "query_sandbox",
        "display_name": "Sandbox SQL",
        "canonical_title": "Sandbox SQL - consulta estructurada sobre cache local",
        "domain": "sandbox",
        "subdomain": "sql",
        "taxonomy_key": "query_sandbox",
    },
)


_NON_TABULAR_ERROR_PREFIXES = (
    "zip_document_bundle",
    "zip_no_parseable_file",
)


def _materialization_status(cached_status: str | None, error_message: str | None = None) -> str:
    if cached_status == "ready":
        return MATERIALIZATION_READY
    if cached_status == "permanently_failed":
        msg = (error_message or "").strip().lower()
        if any(msg.startswith(prefix) for prefix in _NON_TABULAR_ERROR_PREFIXES):
            return MATERIALIZATION_NON_TABULAR
        return MATERIALIZATION_FAILED
    if cached_status == "error":
        return MATERIALIZATION_FAILED
    return MATERIALIZATION_PENDING


def _resource_kind(cached_status: str | None, error_message: str | None = None) -> str:
    if cached_status == "permanently_failed":
        msg = (error_message or "").strip().lower()
        if msg.startswith("zip_document_bundle"):
            return RESOURCE_KIND_DOCUMENT_BUNDLE
    return RESOURCE_KIND_FILE


def _derived_layout_profile(cached_layout_profile: str | None, cached_status: str | None) -> str | None:
    """Return the layout_profile to project into `catalog_resources`.

    Earlier versions defaulted to `simple_tabular` when status='ready' even
    if the row had no real `layout_profile` from the parser. That made
    backfilled rows look like they had been processed by the phase4 parser,
    inflating dashboards that filter by layout_profile and hiding the
    `IS NOT NULL` cohort that needs reingestion. Returning None for rows
    without an explicit profile keeps the catalog honest: dashboards can
    distinguish "really phase4-tagged" from "we don't know yet".
    """
    return cached_layout_profile


def _derived_header_quality(
    cached_header_quality: str | None,
    cached_status: str | None,
    error_message: str | None,
) -> str | None:
    """Same shape as `_derived_layout_profile`: only return what's
    explicitly known. The `error_message` -> 'degraded' rule is the only
    derived value left, because the message itself is hard evidence that
    the parser flagged the header. Defaulting `ready` to 'good' lied:
    plenty of cached_datasets ready rows from before phase4 were never
    parsed by the new pipeline, so we can't claim 'good' for them."""
    if cached_header_quality:
        return cached_header_quality
    msg = (error_message or "").lower()
    if "header_quality:degraded" in msg:
        return "degraded"
    return None


def _filename_from_url(url: str | None, fmt: str | None) -> str | None:
    if not url:
        return None
    candidate = url.rsplit("/", 1)[-1] or None
    if not candidate and fmt:
        return f"{fmt}"
    return candidate


def backfill_batch(
    engine: Engine,
    *,
    offset: int,
    limit: int,
    dry_run: bool,
    extractor: TitleExtractor,
    namer: PhysicalNamer,
) -> tuple[int, int]:
    """Process one batch. Returns (read, written)."""
    with engine.connect() as conn:
        rows = conn.execute(_QUERY_SQL, {"limit": limit, "offset": offset}).fetchall()
    if not rows:
        return 0, 0
    written = 0
    for row in rows:
        portal = row.portal or ""
        source_id = row.source_id or ""
        if not portal or not source_id:
            continue
        identity = _resource_identity(portal, source_id)
        ctx = {
            "portal": portal,
            "dataset_title": row.raw_title or "",
            "organization": row.organization or "",
            "filename": _filename_from_url(row.url, row.format),
            "source_id": source_id,
        }
        extraction = extractor.extract(ctx)
        # If we have a materialized table, keep its name; otherwise compute
        # the deterministic candidate so future materializations can land on
        # the same physical name.
        physical_name = row.table_name or namer.build(
            portal, source_id, slug_hint=row.raw_title or source_id
        ).table_name
        materialization = _materialization_status(row.cached_status, row.error_message)
        resource_kind = _resource_kind(row.cached_status, row.error_message)
        params = {
            "resource_identity": identity,
            "dataset_id": row.dataset_id or "",
            "portal": portal,
            "source_id": source_id,
            "s3_key": row.s3_key,
            "filename": ctx["filename"],
            "sub_path": None,
            "raw_title": (row.raw_title or "")[:1000],
            "canonical_title": extraction.canonical_title[:1000],
            "display_name": extraction.display_name[:1000],
            "title_source": extraction.title_source.value,
            "title_confidence": extraction.title_confidence,
            "resource_kind": resource_kind,
            "materialization_status": materialization,
            "materialized_table_name": physical_name,
            "layout_profile": _derived_layout_profile(row.layout_profile, row.cached_status),
            "header_quality": _derived_header_quality(
                row.header_quality, row.cached_status, row.error_message
            ),
            "parser_version": _BACKFILL_LEGACY_PARSER_VERSION if row.cached_status else None,
            "normalization_version": _BACKFILL_LEGACY_NORMALIZATION_VERSION if row.cached_status else None,
        }
        if dry_run:
            logger.debug("dry-run upsert %s", identity)
            written += 1
            continue
        try:
            with engine.begin() as conn:
                conn.execute(_UPSERT_SQL, params)
            written += 1
        except Exception:
            logger.exception("Failed upserting catalog_resource %s", identity)
    return len(rows), written


def run_backfill(
    *,
    dry_run: bool = False,
    batch_size: int = 500,
    max_batches: int | None = None,
) -> dict:
    engine = get_sync_engine()
    extractor = TitleExtractor()
    namer = PhysicalNamer()
    offset = 0
    total_read = 0
    total_written = 0
    batches = 0
    try:
        while True:
            read, written = backfill_batch(
                engine,
                offset=offset,
                limit=batch_size,
                dry_run=dry_run,
                extractor=extractor,
                namer=namer,
            )
            if read == 0:
                break
            total_read += read
            total_written += written
            offset += batch_size
            batches += 1
            if max_batches is not None and batches >= max_batches:
                break
    except SoftTimeLimitExceeded:
        logger.warning(
            "catalog backfill hit soft time limit at batch %d (read=%d)",
            batches,
            total_read,
        )
    summary = {
        "read": total_read,
        "written": total_written,
        "batches": batches,
        "dry_run": dry_run,
    }
    logger.info("catalog_backfill done: %s", summary)
    return summary


def _load_embedding_batch(engine: Engine, *, limit: int) -> list[dict]:
    with engine.connect() as conn:
        rows = conn.execute(_EMBEDDING_QUERY_SQL, {"limit": limit}).fetchall()
    out: list[dict] = []
    for row in rows:
        mapped = dict(row._mapping)
        text_to_embed = _build_catalog_embedding_text(
            mapped.get("display_name"),
            mapped.get("canonical_title"),
        )
        if text_to_embed:
            mapped["text_to_embed"] = text_to_embed
            out.append(mapped)
    return out


def _persist_embedding_batch(engine: Engine, rows: list[dict], embeddings: list[list[float]]) -> int:
    payload = [
        {
            "id": row["id"],
            "embedding": "[" + ",".join(str(v) for v in emb) + "]",
        }
        for row, emb in zip(rows, embeddings, strict=True)
    ]
    if not payload:
        return 0
    with engine.begin() as conn:
        conn.execute(_EMBEDDING_UPDATE_SQL, payload)
    return len(payload)


def run_populate_catalog_embeddings(
    *,
    dry_run: bool = False,
    batch_size: int = _EMBEDDING_BATCH_LIMIT,
    max_batches: int | None = None,
) -> dict:
    engine = get_sync_engine()
    effective_batch_size = max(1, min(batch_size, _EMBEDDING_BATCH_LIMIT))
    total_selected = 0
    total_updated = 0
    batches = 0
    try:
        while True:
            batch = _load_embedding_batch(engine, limit=effective_batch_size)
            if not batch:
                break
            total_selected += len(batch)
            if not dry_run:
                embeddings = _embed_texts([row["text_to_embed"] for row in batch])
                total_updated += _persist_embedding_batch(engine, batch, embeddings)
            batches += 1
            if max_batches is not None and batches >= max_batches:
                break
    except SoftTimeLimitExceeded:
        logger.warning(
            "populate_catalog_embeddings hit soft time limit at batch %d (selected=%d)",
            batches,
            total_selected,
        )
    finally:
        engine.dispose()
    summary = {
        "selected": total_selected,
        "updated": total_updated if not dry_run else total_selected,
        "batches": batches,
        "dry_run": dry_run,
        "batch_size": effective_batch_size,
    }
    logger.info("populate_catalog_embeddings done: %s", summary)
    return summary


def run_seed_connector_endpoints(*, dry_run: bool = False) -> dict:
    engine = get_sync_engine()
    written = 0
    try:
        for item in _CONNECTOR_ENDPOINTS:
            params = {
                "resource_identity": _resource_identity(item["portal"], item["source_id"]),
                "portal": item["portal"],
                "source_id": item["source_id"],
                "raw_title": item["display_name"],
                "canonical_title": item["canonical_title"],
                "display_name": item["display_name"],
                "domain": item["domain"],
                "subdomain": item["subdomain"],
                "taxonomy_key": item["taxonomy_key"],
            }
            if dry_run:
                written += 1
                continue
            with engine.begin() as conn:
                conn.execute(_CONNECTOR_UPSERT_SQL, params)
            written += 1
    finally:
        engine.dispose()
    summary = {"written": written, "dry_run": dry_run}
    logger.info("seed_connector_endpoints done: %s", summary)
    return summary


@celery_app.task(
    name="openarg.catalog_backfill",
    bind=True,
    soft_time_limit=900,
    time_limit=1080,
)
def catalog_backfill_task(self, *, dry_run: bool = False, max_batches: int | None = None) -> dict:
    engine = get_sync_engine()
    # NOTE: do NOT engine.dispose() between acquire and release. The
    # advisory lock connection is checked out from the pool and stashed
    # in `_HELD_BACKFILL_LOCK` (kept open by `_try_backfill_lock`).
    # Calling dispose() invalidates the pool but the checked-out
    # connection survives — still, removing dispose() avoids confusing
    # readers and any future regression where dispose() ends up
    # closing held connections directly.
    lock_acquired = _try_backfill_lock(engine)
    if not lock_acquired:
        logger.info("catalog_backfill skipped because another run already holds the lock")
        return {"status": "skipped_already_running"}

    try:
        return run_backfill(dry_run=dry_run, max_batches=max_batches)
    finally:
        # `engine` is ignored by `_release_backfill_lock` (the cached
        # connection from acquire is what holds the lock); we pass it
        # for signature compatibility.
        _release_backfill_lock(engine)


@celery_app.task(
    name="openarg.populate_catalog_embeddings",
    bind=True,
    soft_time_limit=900,
    time_limit=1080,
)
def populate_catalog_embeddings_task(
    self,
    *,
    dry_run: bool = False,
    batch_size: int = _EMBEDDING_BATCH_LIMIT,
    max_batches: int | None = None,
) -> dict:
    return run_populate_catalog_embeddings(
        dry_run=dry_run,
        batch_size=batch_size,
        max_batches=max_batches,
    )


@celery_app.task(
    name="openarg.seed_connector_endpoints",
    bind=True,
    soft_time_limit=120,
    time_limit=180,
)
def seed_connector_endpoints_task(self, *, dry_run: bool = False) -> dict:
    return run_seed_connector_endpoints(dry_run=dry_run)


def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="WS2/WS4 catalog backfill")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--max-batches", type=int, default=None)
    args = parser.parse_args(argv)
    summary = run_backfill(
        dry_run=args.dry_run,
        batch_size=args.batch_size,
        max_batches=args.max_batches,
    )
    print(summary)
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(_cli())
