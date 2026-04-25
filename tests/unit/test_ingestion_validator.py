"""Tests for the ingestion validator + detectors (WS0).

Covers the canonical bug families confirmed in production (Apr 2026):
  - 38 HTML-as-data tables
  - 127 INDEC headers (col_*, .1/.2/.3 suffixes)
  - 553 size_bytes=0 / 12 row_count=0 corruption
  - 10 pg_typname collisions
  - 4 .rar disguised as .zip (ACUMAR)
  - GDrive virus-scan warning
  - 8 missing download URLs
  - 65+ HTTP error / dead-portal cases
"""

from __future__ import annotations

import pytest

from app.application.validation import (
    IngestionValidator,
    Mode,
    ResourceContext,
    Severity,
)
from app.application.validation.detectors import (
    EncodingMismatchDetector,
    FileTooLargeDetector,
    GDriveScanWarningDetector,
    HeaderFlattenDetector,
    HtmlAsDataDetector,
    HttpErrorDetector,
    MetadataIntegrityDetector,
    MissingDownloadUrlDetector,
    NonTabularZipDetector,
    RowCountDetector,
    SingleColumnDetector,
    TableNameCollisionDetector,
    UnsupportedArchiveDetector,
    build_default_detectors,
)


def _ctx(**kwargs) -> ResourceContext:
    return ResourceContext(resource_id=kwargs.pop("resource_id", "rid-1"), **kwargs)


def test_html_as_data_detects_doctype_prefix():
    det = HtmlAsDataDetector()
    ctx = _ctx(declared_format="csv", raw_byte_sample=b"<!DOCTYPE html>\n<html><body>...")
    finding = det.run(ctx, Mode.PRE_PARSE)
    assert finding is not None
    assert finding.severity == Severity.CRITICAL
    assert finding.should_redownload is True


def test_html_as_data_skips_when_format_is_html():
    det = HtmlAsDataDetector()
    ctx = _ctx(declared_format="html", raw_byte_sample=b"<!DOCTYPE html>")
    assert det.applicable_to(ctx) is False


def test_single_column_html_blob_detects_doctype_column_name():
    det = SingleColumnDetector()
    # Mirrors the 38 HTML-as-data tables: one column named like `<!DOCTYPE...`
    ctx = _ctx(materialized_columns=["<!DOCTYPE html PUBLIC ..."])
    finding = det.run(ctx, Mode.POST_PARSE)
    assert finding is not None
    assert finding.severity == Severity.CRITICAL


def test_single_column_html_blob_ignores_normal_single_column():
    det = SingleColumnDetector()
    ctx = _ctx(materialized_columns=["fecha"])
    assert det.run(ctx, Mode.POST_PARSE) is None


def test_header_flatten_detects_pandas_suffixes():
    det = HeaderFlattenDetector()
    # INDEC `1° trimestre`, `1° trimestre.1`, `.2`, `.3`
    ctx = _ctx(
        materialized_columns=[
            "1° trimestre",
            "1° trimestre.1",
            "1° trimestre.2",
            "1° trimestre.3",
        ]
    )
    finding = det.run(ctx, Mode.RETROSPECTIVE)
    assert finding is not None
    assert finding.payload["suffixed_count"] == 3


def test_header_flatten_detects_col_zero_pattern():
    det = HeaderFlattenDetector()
    # 87 INDEC tables in prod with col_0..N column names
    ctx = _ctx(materialized_columns=[f"col_{i}" for i in range(8)])
    finding = det.run(ctx, Mode.RETROSPECTIVE)
    assert finding is not None
    assert finding.payload["generic_count"] == 8


def test_gdrive_scan_warning_detects_marker():
    det = GDriveScanWarningDetector()
    ctx = _ctx(
        raw_byte_sample=b"<html>... Google Drive can't scan this file for viruses ...</html>"
    )
    finding = det.run(ctx, Mode.PRE_PARSE)
    assert finding is not None
    assert finding.should_redownload is True


def test_encoding_mismatch_detects_mojibake_in_columns():
    det = EncodingMismatchDetector()
    # latin-1 'Ñ' read as utf-8 yields 'Ã±' / 'Ñ' patterns
    ctx = _ctx(materialized_columns=["AÃ±o", "valor"])
    finding = det.run(ctx, Mode.POST_PARSE)
    assert finding is not None


def test_row_count_zero_with_zero_size_is_critical():
    det = RowCountDetector()
    ctx = _ctx(materialized_row_count=0, declared_size_bytes=0)
    finding = det.run(ctx, Mode.POST_PARSE)
    assert finding is not None
    assert finding.severity == Severity.CRITICAL


def test_row_count_zero_with_real_size_is_critical():
    det = RowCountDetector()
    ctx = _ctx(materialized_row_count=0, declared_size_bytes=12345)
    finding = det.run(ctx, Mode.POST_PARSE)
    assert finding is not None
    assert finding.severity == Severity.CRITICAL


def test_row_count_divergence_above_threshold():
    det = RowCountDetector()
    ctx = _ctx(materialized_row_count=10, declared_row_count=100)
    finding = det.run(ctx, Mode.POST_PARSE)
    assert finding is not None


def test_metadata_integrity_separates_ingester_bug_from_corruption():
    """553 ready with size_bytes=0 in prod — most are ingester bug, NOT corruption."""
    det = MetadataIntegrityDetector()
    # Bug case: size=0 but rows>0 — flagged as warn
    ctx = _ctx(materialized_row_count=1000, declared_size_bytes=0, materialized_columns=["a"])
    finding = det.run(ctx, Mode.RETROSPECTIVE)
    assert finding is not None
    assert finding.severity == Severity.WARN


def test_missing_download_url_detects_empty():
    det = MissingDownloadUrlDetector()
    ctx = _ctx(download_url=None)
    finding = det.run(ctx, Mode.PRE_PARSE)
    assert finding is not None


def test_missing_download_url_detects_non_http_scheme():
    det = MissingDownloadUrlDetector()
    ctx = _ctx(download_url="ftp://example.com/file.csv")
    finding = det.run(ctx, Mode.PRE_PARSE)
    assert finding is not None


def test_http_error_detects_dead_portal_host():
    det = HttpErrorDetector()
    ctx = _ctx(download_url="https://datos.santafe.gob.ar/dataset/foo.csv")
    finding = det.run(ctx, Mode.PRE_PARSE)
    assert finding is not None


def test_http_error_detects_5xx_status():
    det = HttpErrorDetector()
    ctx = _ctx(download_url="https://example.com/x", http_status=503)
    finding = det.run(ctx, Mode.PRE_PARSE)
    assert finding is not None
    assert finding.severity == Severity.CRITICAL


def test_file_too_large_per_inner_file():
    det = FileTooLargeDetector()
    ctx = _ctx(zip_member_sizes={"big.csv": 600 * 1024 * 1024, "small.csv": 1024})
    finding = det.run(ctx, Mode.PRE_PARSE)
    assert finding is not None
    assert "big.csv" in finding.payload["offenders"]


def test_file_too_large_accepts_many_small_zip_entries():
    """The ~50 zip_too_large cases (elections, bicycles) had many small files."""
    det = FileTooLargeDetector()
    sizes = {f"file_{i}.csv": 50 * 1024 * 1024 for i in range(20)}  # 1 GB total but each 50 MB
    ctx = _ctx(zip_member_sizes=sizes)
    assert det.run(ctx, Mode.PRE_PARSE) is None


def test_table_name_collision_detects_owner_mismatch():
    det = TableNameCollisionDetector()
    ctx = _ctx(
        table_name="cache_datos_gob_ar_indicadores_de_evoluci_n_del_sector",
        metadata={"existing_table_resource": "other-resource"},
    )
    finding = det.run(ctx, Mode.PRE_PARSE)
    assert finding is not None
    assert finding.severity == Severity.CRITICAL


def test_non_tabular_zip_detects_pdf_only():
    det = NonTabularZipDetector()
    ctx = _ctx(zip_member_names=["acta_001.pdf", "acta_002.pdf", "anexo.jpg"])
    finding = det.run(ctx, Mode.PRE_PARSE)
    assert finding is not None
    assert finding.severity == Severity.INFO


def test_non_tabular_zip_skips_when_csv_present():
    det = NonTabularZipDetector()
    ctx = _ctx(zip_member_names=["data.csv", "readme.pdf"])
    assert det.run(ctx, Mode.PRE_PARSE) is None


def test_unsupported_archive_detects_rar_disguised_as_zip():
    det = UnsupportedArchiveDetector()
    ctx = _ctx(declared_format="zip", raw_byte_sample=b"Rar!\x1a\x07\x00")
    finding = det.run(ctx, Mode.PRE_PARSE)
    assert finding is not None
    assert finding.payload["detected"] == "rar"


def test_unsupported_archive_passes_real_zip():
    det = UnsupportedArchiveDetector()
    ctx = _ctx(declared_format="zip", raw_byte_sample=b"PK\x03\x04...")
    assert det.run(ctx, Mode.PRE_PARSE) is None


def test_validator_runs_all_detectors_and_reports_findings():
    v = IngestionValidator(build_default_detectors())
    ctx = _ctx(
        declared_format="csv",
        raw_byte_sample=b"<!DOCTYPE html>...",
        materialized_columns=["<!DOCTYPE html..."],
        materialized_row_count=0,
        declared_size_bytes=0,
    )
    findings = v.run(ctx, Mode.POST_PARSE)
    # At least html_as_data + single_column + row_count critical
    detector_names = {f.detector_name for f in findings}
    assert "html_as_data" in detector_names
    assert "single_column_html_blob" in detector_names
    assert "row_count" in detector_names
    assert v.has_critical(findings)


def test_validator_first_critical_short_circuits_safely():
    v = IngestionValidator(build_default_detectors())
    ctx = _ctx(declared_format="csv", raw_byte_sample=b"<!DOCTYPE html>")
    findings = v.run(ctx, Mode.PRE_PARSE)
    crit = v.first_critical(findings)
    assert crit is not None
    assert crit.severity == Severity.CRITICAL


def test_input_hash_is_deterministic():
    ctx1 = _ctx(declared_format="csv", raw_byte_sample=b"abc", materialized_row_count=10)
    ctx2 = _ctx(declared_format="csv", raw_byte_sample=b"abc", materialized_row_count=10)
    assert IngestionValidator.input_hash(ctx1) == IngestionValidator.input_hash(ctx2)


def test_detector_crash_does_not_break_validator():
    class _Boom:
        name = "boom"
        version = "1"
        severity = Severity.CRITICAL

        def applicable_to(self, ctx):
            return True

        def run(self, ctx, mode):
            raise RuntimeError("kaboom")

    v = IngestionValidator([_Boom(), HtmlAsDataDetector()])
    ctx = _ctx(declared_format="csv", raw_byte_sample=b"<!DOCTYPE html>")
    findings = v.run(ctx, Mode.PRE_PARSE)
    # HtmlAsDataDetector still runs even though _Boom crashed
    assert any(f.detector_name == "html_as_data" for f in findings)


@pytest.mark.parametrize(
    "raw,expected",
    [
        (b"<html>", True),
        (b"<!DOCTYPE", True),
        (b"id,name,value\n1,a,2", False),
        (b"PK\x03\x04", False),
    ],
)
def test_html_as_data_table_driven(raw: bytes, expected: bool):
    det = HtmlAsDataDetector()
    ctx = _ctx(declared_format="csv", raw_byte_sample=raw)
    assert (det.run(ctx, Mode.PRE_PARSE) is not None) is expected
