"""Censo 2022 ingest — applies the WS5 hierarchical pattern (cuadro-by-cuadro).

Builds the inventory in `catalog_resources` first (one entry per cuadro), with
deterministic naming, then materialises selectively. Reuses:
  - `parse_hierarchical_headers` (WS5 parser)
  - `physical_table_name` / `extract_title` (WS4)
  - WS0 detectors via `validate_post_parse`

The actual download URLs / portal endpoints for Censo 2022 are configured via
environment variables so this stays generic until wired to the real portal.
Until then it acts as a catalog-only seeder for the cuadro list provided in
`config/censo2022_cuadros.json`.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

from sqlalchemy import text

from app.application.catalog.physical_namer import PhysicalNamer
from app.application.catalog.title_extractor import TitleExtractor
from app.domain.entities.dataset.catalog_resource import (
    MATERIALIZATION_PENDING,
    RESOURCE_KIND_CUADRO,
)
from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)


def _config_path() -> Path:
    raw = os.getenv(
        "OPENARG_CENSO2022_CONFIG",
        "config/censo2022_cuadros.json",
    )
    return Path(raw)


def _load_cuadros() -> list[dict]:
    path = _config_path()
    if not path.exists():
        logger.info("Censo 2022 config not found at %s — nothing to seed", path)
        return []
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("Failed to parse Censo 2022 config %s", path)
        return []


_UPSERT_SQL = text(
    """
    INSERT INTO catalog_resources (
        resource_identity, portal, source_id,
        sheet_name, cuadro_numero, provincia,
        raw_title, canonical_title, display_name,
        title_source, title_confidence,
        resource_kind, materialization_status, materialized_table_name,
        parser_version, normalization_version,
        created_at, updated_at
    ) VALUES (
        :resource_identity, :portal, :source_id,
        :sheet_name, :cuadro_numero, :provincia,
        :raw_title, :canonical_title, :display_name,
        :title_source, :title_confidence,
        :resource_kind, :materialization_status, :materialized_table_name,
        :parser_version, :normalization_version,
        NOW(), NOW()
    )
    ON CONFLICT (resource_identity) DO UPDATE SET
        canonical_title = EXCLUDED.canonical_title,
        display_name = EXCLUDED.display_name,
        title_source = EXCLUDED.title_source,
        title_confidence = EXCLUDED.title_confidence,
        materialization_status = EXCLUDED.materialization_status,
        materialized_table_name = EXCLUDED.materialized_table_name,
        parser_version = EXCLUDED.parser_version,
        normalization_version = EXCLUDED.normalization_version,
        updated_at = NOW()
    """
)


@celery_app.task(
    name="openarg.ingest_censo2022",
    bind=True,
    soft_time_limit=600,
    time_limit=720,
)
def ingest_censo2022(self) -> dict:
    """Seed `catalog_resources` with Censo 2022 cuadros.

    Each entry in the JSON config has shape:
      {
        "cuadro": "1.7",
        "title": "Población por sexo y edad",
        "provincia": "Buenos Aires",
        "sheet_name": "1.7"
      }
    """
    cuadros = _load_cuadros()
    if not cuadros:
        return {
            "seeded": 0,
            "skipped": 0,
            "reason": f"missing_or_empty_config:{_config_path()}",
        }
    extractor = TitleExtractor()
    namer = PhysicalNamer()
    portal = "censo_2022"
    engine = get_sync_engine()
    seeded = 0
    skipped = 0
    for entry in cuadros:
        cuadro = str(entry.get("cuadro") or "").strip()
        if not cuadro:
            skipped += 1
            continue
        provincia = (entry.get("provincia") or "").strip()
        source_id = f"cuadro_{cuadro}_{provincia.lower().replace(' ', '_') or 'nacional'}"
        identity = f"{portal}::{source_id}"
        ctx = {
            "portal": portal,
            "index_title": entry.get("title"),
            "provincia": provincia,
            "cuadro_numero": cuadro,
        }
        extraction = extractor.extract(ctx)
        physical = namer.build(portal, source_id, slug_hint=entry.get("title") or source_id)
        params = {
            "resource_identity": identity,
            "portal": portal,
            "source_id": source_id,
            "sheet_name": entry.get("sheet_name"),
            "cuadro_numero": cuadro,
            "provincia": provincia,
            "raw_title": entry.get("title", "")[:1000],
            "canonical_title": extraction.canonical_title[:1000],
            "display_name": extraction.display_name[:1000],
            "title_source": extraction.title_source.value,
            "title_confidence": extraction.title_confidence,
            "resource_kind": RESOURCE_KIND_CUADRO,
            "materialization_status": MATERIALIZATION_PENDING,
            "materialized_table_name": physical.table_name,
            "parser_version": "1",
            "normalization_version": "1",
        }
        try:
            with engine.begin() as conn:
                conn.execute(_UPSERT_SQL, params)
            seeded += 1
        except Exception:
            logger.exception("Failed to upsert Censo 2022 entry %s", identity)
            skipped += 1
    return {"seeded": seeded, "skipped": skipped, "config": str(_config_path())}
