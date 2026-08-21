"""Tests for pre-drop schema snapshots (mig 0056).

The point of the feature is that a table's shape survives the table, so the
tests that matter are: the hash is stable and order-independent, the diff
recognises a rename from the value profile alone, and nothing in the capture
path can break the drop it precedes.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.application.catalog.schema_snapshot import (
    ColumnProfile,
    TableSnapshot,
    _parse_pg_array,
    capture_table_snapshot,
    diff_snapshots,
    profile_similarity,
    schema_hash_for,
    split_qualified,
)


def _col(name, ordinal=1, pg_type="text", *, mcv=None, hist=None, null_frac=0.0):
    return ColumnProfile(
        name=name,
        ordinal=ordinal,
        pg_type=pg_type,
        null_frac=null_frac,
        most_common_vals=mcv or [],
        histogram_sample=hist or [],
    )


def _snap(columns, table="t"):
    return TableSnapshot(
        schema_name="raw",
        table_name=table,
        columns=columns,
        row_count_estimate=100,
        stats_available=True,
    )


# ── schema hash ────────────────────────────────────────────────


def test_schema_hash_ignores_column_order():
    """Reordered columns are the same shape — a reorder is not drift."""
    assert schema_hash_for(["a", "b", "c"]) == schema_hash_for(["c", "a", "b"])


def test_schema_hash_ignores_collector_metadata_columns():
    """`_source_dataset_id` is ours, not the portal's. Including it would make
    the same upstream shape hash differently depending on the ingest path."""
    assert schema_hash_for(["a", "b"]) == schema_hash_for(["a", "b", "_source_dataset_id"])


def test_schema_hash_changes_when_a_column_changes():
    assert schema_hash_for(["a", "b"]) != schema_hash_for(["a", "b_renamed"])


def test_schema_hash_matches_collector_variant_suffix():
    """The hash must line up with the `_s<hash>` suffix the collector puts on
    schema-variant tables, so a snapshot can be matched against one."""
    from app.infrastructure.celery.tasks.collector_tasks import _schema_suffix

    columns = ["Provincia", "anio", "monto", "_source_dataset_id"]
    assert schema_hash_for(columns).startswith(_schema_suffix(columns))


# ── qualified names ────────────────────────────────────────────


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("raw.foo", ("raw", "foo")),
        ("cache_foo", ("public", "cache_foo")),
        ('"raw"."foo"', ("raw", "foo")),
        ("", ("public", "")),
    ],
)
def test_split_qualified_accepts_both_shapes(raw, expected):
    """`_record_cache_drop` is called with qualified names from the cleanup
    tasks and bare ones from the legacy collector path."""
    assert split_qualified(raw) == expected


# ── pg_stats array parsing ─────────────────────────────────────


def test_parse_pg_array_handles_list_form():
    assert _parse_pg_array(["a", "b"]) == ["a", "b"]


def test_parse_pg_array_handles_text_form():
    """psycopg surfaces `anyarray` as text for some element types."""
    assert _parse_pg_array('{"Buenos Aires",Córdoba}') == ["Buenos Aires", "Córdoba"]


def test_parse_pg_array_handles_null_and_empty():
    assert _parse_pg_array(None) == []
    assert _parse_pg_array("{}") == []


def test_parse_pg_array_is_bounded():
    """Default statistics target is 100 values per column; the payload would
    dwarf the row it describes."""
    from app.application.catalog.schema_snapshot import MAX_PROFILE_VALUES

    assert len(_parse_pg_array([str(i) for i in range(500)])) == MAX_PROFILE_VALUES


def test_parse_pg_array_truncates_long_values():
    from app.application.catalog.schema_snapshot import MAX_VALUE_CHARS

    assert len(_parse_pg_array(["x" * 5000])[0]) == MAX_VALUE_CHARS


# ── diff ───────────────────────────────────────────────────────


def test_diff_reports_no_change_for_identical_shapes():
    before = _snap([_col("a"), _col("b", 2)])
    after = _snap([_col("a"), _col("b", 2)])
    result = diff_snapshots(before, after)
    assert result["schema_changed"] is False
    assert result["added"] == [] and result["removed"] == []


def test_diff_reports_added_and_removed():
    before = _snap([_col("a"), _col("b", 2)])
    after = _snap([_col("a"), _col("c", 2)])
    result = diff_snapshots(before, after)
    assert result["schema_changed"] is True
    assert result["added"] == ["c"]
    assert result["removed"] == ["b"]


def test_diff_reports_type_change_for_a_surviving_column():
    before = _snap([_col("monto", pg_type="text")])
    after = _snap([_col("monto", pg_type="numeric")])
    result = diff_snapshots(before, after)
    assert result["type_changed"] == [{"column": "monto", "from": "text", "to": "numeric"}]


def test_diff_detects_a_rename_from_the_value_profile_alone():
    """The reason the profile is stored at all.

    A portal renames `provincia` to `jurisdiccion` and keeps the values. By
    name alone this is one column lost and one gained; by profile it is
    obviously the same column.
    """
    values = ["Buenos Aires", "Córdoba", "Santa Fe", "Mendoza", "Salta"]
    before = _snap([_col("provincia", mcv=values), _col("anio", 2, mcv=["2023", "2024"])])
    after = _snap([_col("jurisdiccion", mcv=values), _col("anio", 2, mcv=["2023", "2024"])])

    result = diff_snapshots(before, after)

    assert result["renamed_candidates"] == [
        {"from": "provincia", "to": "jurisdiccion", "similarity": pytest.approx(1.0, abs=0.01)}
    ]


def test_diff_does_not_invent_a_rename_for_unrelated_columns():
    before = _snap([_col("provincia", mcv=["Buenos Aires", "Córdoba"])])
    after = _snap([_col("importe", mcv=["1000", "2500"])])
    assert diff_snapshots(before, after)["renamed_candidates"] == []


# ── similarity ─────────────────────────────────────────────────


def test_similarity_is_zero_without_evidence():
    """No sampled values is 'cannot say', not 'not similar'. Returning a high
    score here would manufacture renames for every unanalysed table."""
    assert profile_similarity(_col("a"), _col("b")) == 0.0


def test_similarity_rewards_shared_values_over_shared_type():
    shared = ["x", "y", "z"]
    same_values = profile_similarity(
        _col("a", mcv=shared, pg_type="text"), _col("b", mcv=shared, pg_type="numeric")
    )
    same_type_only = profile_similarity(
        _col("a", mcv=["x"], pg_type="text"), _col("b", mcv=["q"], pg_type="text")
    )
    assert same_values > same_type_only


# ── the capture path must never break the drop ─────────────────


def test_capture_returns_none_when_the_table_is_already_gone():
    """Every caller sits in front of `DROP TABLE IF EXISTS`, so losing the
    race against another worker is expected, not an error."""
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchall.return_value = []

    assert capture_table_snapshot(engine, table_name="raw.gone", reason="r", actor="a") is None


def test_capture_swallows_database_errors():
    """A snapshot is a nice-to-have; the drop it precedes is not optional."""
    engine = MagicMock()
    engine.connect.side_effect = RuntimeError("connection pool exhausted")

    assert capture_table_snapshot(engine, table_name="raw.t", reason="r", actor="a") is None


def test_record_cache_drop_survives_a_snapshot_that_raises(monkeypatch):
    """The regression this guards against.

    The hook sits in front of a `DROP TABLE` that has to happen. An early
    version called it outside a try block, so a snapshot failure propagated
    and would have stopped the collector from recreating a table it had just
    decided to replace — turning a bookkeeping problem into a stuck ingest.
    """
    from app.infrastructure.celery.tasks import collector_tasks

    monkeypatch.setattr(
        collector_tasks,
        "_capture_schema_snapshot",
        MagicMock(side_effect=RuntimeError("boom")),
    )
    engine = MagicMock()

    collector_tasks._record_cache_drop(engine, table_name="raw.t", reason="r")

    # And the audit row still went in — the failure was contained to the hook.
    assert engine.begin.called


def test_snapshot_hook_is_skipped_when_the_flag_is_off(monkeypatch):
    """Rollback is an env var and a worker restart, not a code change."""
    from app.infrastructure.celery.tasks import collector_tasks

    monkeypatch.setenv("OPENARG_SCHEMA_SNAPSHOTS", "0")
    assert collector_tasks._schema_snapshots_enabled() is False
    assert (
        collector_tasks._capture_schema_snapshot(
            MagicMock(), table_name="raw.t", reason="r", actor="a"
        )
        is None
    )


def test_snapshot_hook_is_on_by_default(monkeypatch):
    from app.infrastructure.celery.tasks import collector_tasks

    monkeypatch.delenv("OPENARG_SCHEMA_SNAPSHOTS", raising=False)
    assert collector_tasks._schema_snapshots_enabled() is True
