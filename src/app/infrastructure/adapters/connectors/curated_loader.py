"""Curated sources connector — loads `config/curated_sources.json` and upserts
each entry into `datasets` (with `portal='curated'`).

Used to fill gaps when:
  - the publisher uploaded a broken URL
  - the dataset exists on a `.gob.ar` site that has no CKAN/API
  - the dataset is too new and not yet in CKAN
  - we want to override a misconfigured CKAN URL (escape hatch)

CI validation reuses the WS0 detectors (HtmlAsDataDetector, HttpErrorDetector,
MissingDownloadUrlDetector, FileTooLargeDetector) so a URL that the runtime
guard would reject also fails CI before reaching prod.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


@dataclass
class CuratedSource:
    id: str
    title: str
    url: str
    format: str
    organization: str
    portal: str = "curated"
    refresh: str = "once"  # once | weekly | monthly
    added_at: str = ""
    added_by: str = "manual"
    notes: str = ""
    overrides_dataset: str | None = None


def default_config_path() -> Path:
    raw = os.getenv("OPENARG_CURATED_SOURCES_PATH", "config/curated_sources.json")
    return Path(raw)


def load_curated_sources(path: Path | None = None) -> list[CuratedSource]:
    target = path or default_config_path()
    if not target.exists():
        logger.info("Curated sources config not found at %s", target)
        return []
    try:
        raw = json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        logger.exception("Failed to parse curated sources at %s", target)
        return []
    sources_raw = raw.get("sources") or []
    out: list[CuratedSource] = []
    for entry in sources_raw:
        try:
            out.append(
                CuratedSource(
                    id=entry["id"],
                    title=entry["title"],
                    url=entry["url"],
                    format=entry.get("format", ""),
                    organization=entry.get("organization", ""),
                    portal=entry.get("portal", "curated"),
                    refresh=entry.get("refresh", "once"),
                    added_at=entry.get("added_at", ""),
                    added_by=entry.get("added_by", "manual"),
                    notes=entry.get("notes", ""),
                    overrides_dataset=entry.get("overrides_dataset"),
                )
            )
        except KeyError:
            logger.warning("Skipping curated source missing required fields: %s", entry)
    return out


_BROKEN_PATTERNS = (
    "content_type_mismatch",
    "source_url_returns_html",
    "ingestion_validation_failed",
    "no_download_url",
    "bad_zip_file",
)


def _existing_dataset_is_broken(engine: Engine, source_id: str) -> bool:
    """Per the plan, we only override an existing dataset when it's flagged
    broken — never silently replace a working URL.
    """
    sql = text(
        "SELECT cd.error_message "
        "FROM datasets d "
        "LEFT JOIN cached_datasets cd ON cd.dataset_id = d.id "
        "WHERE d.source_id = :sid "
        "ORDER BY cd.updated_at DESC NULLS LAST "
        "LIMIT 1"
    )
    with engine.connect() as conn:
        row = conn.execute(sql, {"sid": source_id}).fetchone()
    if not row:
        return False
    msg = (row.error_message or "").lower()
    return any(p in msg for p in _BROKEN_PATTERNS)


_UPSERT_DATASET_SQL = text(
    """
    INSERT INTO datasets (
        source_id, title, organization, portal, url, download_url, format,
        is_cached, created_at, updated_at
    ) VALUES (
        :source_id, :title, :organization, :portal, :url, :download_url, :format,
        false, NOW(), NOW()
    )
    ON CONFLICT (source_id, portal) DO UPDATE SET
        title = EXCLUDED.title,
        organization = EXCLUDED.organization,
        url = EXCLUDED.url,
        download_url = EXCLUDED.download_url,
        format = EXCLUDED.format,
        updated_at = NOW()
    """
)


def upsert_curated_sources(engine: Engine, sources: list[CuratedSource]) -> dict:
    """Apply the curated source list to `datasets`. Returns counts."""
    upserted = 0
    overridden = 0
    skipped = 0
    for src in sources:
        if src.overrides_dataset:
            if not _existing_dataset_is_broken(engine, src.overrides_dataset):
                logger.info(
                    "Skipping override of %s — existing dataset is not flagged broken",
                    src.overrides_dataset,
                )
                skipped += 1
                continue
            try:
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            "UPDATE datasets SET download_url = :url, format = :fmt, "
                            "       updated_at = NOW() "
                            "WHERE source_id = :sid"
                        ),
                        {"url": src.url, "fmt": src.format, "sid": src.overrides_dataset},
                    )
                overridden += 1
            except Exception:
                logger.exception("Failed to apply override for %s", src.overrides_dataset)
                skipped += 1
            continue
        try:
            with engine.begin() as conn:
                conn.execute(
                    _UPSERT_DATASET_SQL,
                    {
                        "source_id": src.id,
                        "title": src.title,
                        "organization": src.organization,
                        "portal": src.portal,
                        "url": src.url,
                        "download_url": src.url,
                        "format": src.format,
                    },
                )
            upserted += 1
        except Exception:
            logger.exception("Failed to upsert curated source %s", src.id)
            skipped += 1
    return {
        "upserted": upserted,
        "overridden": overridden,
        "skipped": skipped,
        "total": len(sources),
    }


def validate_curated_sources(sources: list[CuratedSource]) -> list[str]:
    """Static-validation pass for CI. Returns list of error messages.

    Reuses the WS0 detectors (`MissingDownloadUrlDetector`,
    `FileTooLargeDetector`) for any check that doesn't need network. Live
    HTTP probing is left to the CI script — see `scripts/ci/validate_curated_sources.py`.
    """
    from app.application.validation import IngestionValidator, Mode, ResourceContext
    from app.application.validation.detectors import (
        FileTooLargeDetector,
        MissingDownloadUrlDetector,
    )

    detectors = IngestionValidator(
        [MissingDownloadUrlDetector(), FileTooLargeDetector()]
    )
    errors: list[str] = []
    seen_ids: set[str] = set()
    for src in sources:
        if src.id in seen_ids:
            errors.append(f"duplicate id: {src.id}")
        seen_ids.add(src.id)
        ctx = ResourceContext(
            resource_id=src.id,
            portal=src.portal,
            source_id=src.id,
            download_url=src.url,
            declared_format=src.format,
        )
        for f in detectors.run(ctx, Mode.PRE_PARSE):
            errors.append(f"{src.id}: {f.detector_name}: {f.message}")
    return errors
