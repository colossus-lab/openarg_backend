"""Tests for curated_loader (config parsing + validation)."""

from __future__ import annotations

import json
from pathlib import Path

from app.infrastructure.adapters.connectors.curated_loader import (
    CuratedSource,
    load_curated_sources,
    validate_curated_sources,
)


def _write_config(tmp_path: Path, sources: list[dict]) -> Path:
    p = tmp_path / "curated.json"
    p.write_text(json.dumps({"version": "1.0", "sources": sources}), encoding="utf-8")
    return p


def test_load_curated_sources_parses_seed(tmp_path):
    p = _write_config(
        tmp_path,
        [
            {
                "id": "padron_2026",
                "title": "Padrón Oficial 2026",
                "url": "https://example.gob.ar/padron.xlsx",
                "format": "xlsx",
                "organization": "Min. Educación",
                "portal": "curated",
                "refresh": "weekly",
                "added_at": "2026-04-25",
                "added_by": "manual",
                "notes": "test",
            }
        ],
    )
    sources = load_curated_sources(p)
    assert len(sources) == 1
    assert sources[0].id == "padron_2026"
    assert sources[0].refresh == "weekly"


def test_load_skips_entries_missing_required_fields(tmp_path):
    p = _write_config(
        tmp_path,
        [
            {"id": "ok", "title": "T", "url": "https://x", "format": "csv"},
            {"id": "bad-no-title", "url": "https://y"},  # missing title → skipped
        ],
    )
    assert len(load_curated_sources(p)) == 1


def test_load_returns_empty_when_file_missing(tmp_path):
    assert load_curated_sources(tmp_path / "nope.json") == []


def test_validate_flags_missing_url():
    bad = CuratedSource(
        id="x", title="T", url="", format="csv", organization="O"
    )
    errors = validate_curated_sources([bad])
    assert any("missing_download_url" in e for e in errors)


def test_validate_flags_duplicate_id():
    a = CuratedSource(id="dup", title="T", url="https://x.gob.ar/a", format="csv", organization="O")
    b = CuratedSource(id="dup", title="T", url="https://x.gob.ar/b", format="csv", organization="O")
    errors = validate_curated_sources([a, b])
    assert any("duplicate id: dup" in e for e in errors)


def test_validate_passes_for_well_formed_source():
    src = CuratedSource(
        id="ok",
        title="OK",
        url="https://datos.example.gob.ar/file.csv",
        format="csv",
        organization="Org",
    )
    assert validate_curated_sources([src]) == []
