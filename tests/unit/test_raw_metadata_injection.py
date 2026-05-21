"""Unit tests for the per-row lineage metadata injection.

Per MASTERPLAN Fase 1, every row that lands in `raw.*` carries:
  - `_source_url`            (where the file came from)
  - `_source_file_hash`      (content fingerprint)
  - `_parser_version`        (which parser handled it)
  - `_collector_version`     (which collector build emitted the row)

`_ingest_row_id` and `_ingested_at` come from the database (BIGSERIAL +
DEFAULT NOW()) so they are not asserted here.
"""

from __future__ import annotations

import pandas as pd

from app.infrastructure.celery.tasks.collector_tasks import _inject_raw_metadata_columns


def test_metadata_columns_added() -> None:
    df = pd.DataFrame({"col_a": [1, 2, 3], "col_b": ["x", "y", "z"]})
    out = _inject_raw_metadata_columns(
        df,
        source_url="https://example.com/data.csv",
        source_file_hash="abc123",
        parser_version="csv-v2",
        collector_version="2026.05.04",
    )
    for col in ("_source_url", "_source_file_hash", "_parser_version", "_collector_version"):
        assert col in out.columns
    assert (out["_source_url"] == "https://example.com/data.csv").all()
    assert (out["_source_file_hash"] == "abc123").all()
    assert (out["_parser_version"] == "csv-v2").all()
    assert (out["_collector_version"] == "2026.05.04").all()


def test_original_columns_preserved() -> None:
    df = pd.DataFrame({"col_a": [1, 2], "col_b": ["x", "y"]})
    out = _inject_raw_metadata_columns(df)
    assert list(out["col_a"]) == [1, 2]
    assert list(out["col_b"]) == ["x", "y"]


def test_input_df_not_mutated() -> None:
    df = pd.DataFrame({"col_a": [1]})
    original_columns = list(df.columns)
    _inject_raw_metadata_columns(df, source_url="x")
    # The original DataFrame must be unchanged (function copies internally).
    assert list(df.columns) == original_columns


def test_optional_metadata_can_be_none() -> None:
    df = pd.DataFrame({"col_a": [1, 2]})
    out = _inject_raw_metadata_columns(df)  # all kwargs default to None
    for col in ("_source_url", "_source_file_hash", "_parser_version", "_collector_version"):
        assert col in out.columns
        assert out[col].isna().all()


def test_resource_identity_pattern() -> None:
    """The naming helpers and `_resource_identity` must agree on key shape."""
    from app.infrastructure.celery.tasks.collector_tasks import _resource_identity

    base = _resource_identity("caba", "datasets-123")
    assert base == "caba::datasets-123"

    nested = _resource_identity("caba", "datasets-123", sub_path="sheet1")
    assert nested == "caba::datasets-123::sheet1"
