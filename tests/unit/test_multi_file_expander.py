"""Tests for the MultiFileExpander (WS5 subplan)."""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pytest

from app.application.expander import EntryDecision, MultiFileExpander, expand_zip


def _build_zip(tmp_path: Path, members: dict[str, bytes]) -> str:
    z = tmp_path / "x.zip"
    with zipfile.ZipFile(z, "w") as zf:
        for name, content in members.items():
            zf.writestr(name, content)
    return str(z)


def test_expands_csv_entries_to_resources(tmp_path):
    z = _build_zip(
        tmp_path,
        {
            "elecciones_2019.csv": b"id,nombre\n1,abc\n",
            "elecciones_2021.csv": b"id,nombre\n2,def\n",
        },
    )
    result = expand_zip(z)
    assert len(result.expanded) == 2
    assert {e.name for e in result.expanded} == {
        "elecciones_2019.csv",
        "elecciones_2021.csv",
    }
    assert all(e.decision == EntryDecision.EXPAND_TABULAR for e in result.expanded)
    assert result.document_bundle is False


def test_pdf_only_zip_marks_document_bundle(tmp_path):
    z = _build_zip(
        tmp_path,
        {
            "acta_001.pdf": b"%PDF-1.4 fake content",
            "acta_002.pdf": b"%PDF-1.4 fake content",
        },
    )
    result = expand_zip(z)
    assert result.document_bundle is True
    assert result.expanded == []
    assert len(result.skipped_non_tabular) == 2


def test_mixed_zip_keeps_csv_skips_pdf(tmp_path):
    z = _build_zip(
        tmp_path,
        {
            "data.csv": b"a,b\n1,2\n",
            "readme.pdf": b"%PDF-1.4 readme",
        },
    )
    result = expand_zip(z)
    assert len(result.expanded) == 1
    assert result.expanded[0].name == "data.csv"
    assert result.skipped_non_tabular == ["readme.pdf"]
    assert result.document_bundle is False


def test_oversized_inner_file_flagged():
    """Direct unit test on the classifier — avoids writing a 600 MB fixture."""
    from app.application.expander.multi_file_expander import _classify

    cls = _classify("big.csv", 600 * 1024 * 1024, b"a,b\n1,2\n")
    assert cls.decision == EntryDecision.OVERSIZED


def test_undersize_csv_passes():
    from app.application.expander.multi_file_expander import _classify

    cls = _classify("small.csv", 1024, b"a,b\n1,2\n")
    assert cls.decision == EntryDecision.EXPAND_TABULAR


def test_invalid_zip_returns_empty(tmp_path):
    bad = tmp_path / "bad.zip"
    bad.write_bytes(b"not a zip at all")
    result = expand_zip(str(bad))
    assert result.entries == []
    assert result.expanded == []


def test_magic_bytes_override_extension(tmp_path):
    """A file named .csv that's actually a PDF should be classified as document."""
    z = _build_zip(tmp_path, {"trick.csv": b"%PDF-1.4 secret"})
    result = expand_zip(z)
    # PDF magic wins → not in expanded list
    assert all(e.name != "trick.csv" for e in result.expanded)


def test_classification_ignores_directories(tmp_path):
    z = tmp_path / "with-dir.zip"
    with zipfile.ZipFile(z, "w") as zf:
        zf.writestr("subdir/", b"")
        zf.writestr("subdir/data.csv", b"id,x\n1,a\n")
    result = expand_zip(str(z))
    assert len(result.entries) == 1
    assert result.expanded[0].name == "subdir/data.csv"
