"""Tests for the shadow-mode drift report.

Two things matter here beyond the happy path: the report must survive a
snapshot it cannot parse (one bad row must not cost the whole run), and it
must say something honest when there is nothing to compare — which is the
expected state for the first weeks after deploy.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

from app.application.catalog.schema_snapshot import Provenance, snapshot_from_row


class _Row:
    """A `raw_schema_snapshots` row, as SQLAlchemy would hand it over."""

    def __init__(self, **kw):
        defaults = {
            "schema_name": "raw",
            "table_name": "t",
            "resource_identity": "portal::x",
            "version": 1,
            "row_count_estimate": 100,
            "stats_available": True,
            "columns_profile": [],
            "parser_version": "phase4-v1",
            "normalization_version": "phase4-v1",
            "layout_profile": "simple_tabular",
            "header_quality": "good",
            "is_truncated": False,
        }
        defaults.update(kw)
        for k, v in defaults.items():
            setattr(self, k, v)


def _profile(name, *, mcv=None, pg_type="text"):
    return {
        "name": name,
        "ordinal": 1,
        "pg_type": pg_type,
        "null_frac": 0.0,
        "n_distinct": -0.5,
        "most_common_vals": mcv or [],
        "histogram_sample": [],
    }


# ── rehidratación ──────────────────────────────────────────────


def test_rehydration_round_trips_the_profile():
    row = _Row(columns_profile=[_profile("provincia", mcv=["Buenos Aires", "Córdoba"])])
    snap = snapshot_from_row(row)

    assert snap.table_name == "t"
    assert [c.name for c in snap.columns] == ["provincia"]
    assert snap.columns[0].most_common_vals == ["Buenos Aires", "Córdoba"]
    assert snap.provenance.parser_version == "phase4-v1"


def test_rehydration_accepts_a_json_string_profile():
    """psycopg returns jsonb as a Python object, but a driver or a fixture can
    hand it back as text. Both shapes have to work."""
    row = _Row(columns_profile=json.dumps([_profile("a")]))
    assert [c.name for c in snapshot_from_row(row).columns] == ["a"]


def test_rehydration_tolerates_a_pre_0057_row():
    """A snapshot captured between migrations 0056 and 0057 has no provenance
    columns at all. Reading it must degrade, not raise."""

    class _OldRow:
        schema_name = "raw"
        table_name = "t"
        resource_identity = None
        version = None
        row_count_estimate = None
        stats_available = False
        columns_profile = [_profile("a")]

    snap = snapshot_from_row(_OldRow())
    assert snap.provenance == Provenance()


# ── el adaptador de la mitad previa del par ────────────────────


def test_prev_row_adapter_exposes_the_prefixed_half():
    from app.infrastructure.celery.tasks.drift_report_tasks import _PrevRow

    class _Pair:
        p_schema_name = "raw"
        p_table_name = "vieja"
        p_resource_identity = "portal::x"
        p_version = 1
        p_row_count_estimate = 50
        p_stats_available = True
        p_columns_profile = [_profile("provincia")]
        p_parser_version = "phase3-v2"
        p_normalization_version = None
        p_layout_profile = None
        p_header_quality = None
        p_is_truncated = None

    prev = _PrevRow(_Pair())
    snap = snapshot_from_row(prev)

    assert snap.table_name == "vieja"
    assert snap.provenance.parser_version == "phase3-v2"


# ── el reporte ─────────────────────────────────────────────────


def _run_report(pair_rows, coverage=None):
    from app.infrastructure.celery.tasks import drift_report_tasks

    cov = coverage or MagicMock(
        snapshots=len(pair_rows) * 2,
        tables=len(pair_rows),
        with_stats=len(pair_rows),
        with_provenance=len(pair_rows),
        first_seen=None,
        last_seen=None,
    )
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.side_effect = [
        MagicMock(fetchone=lambda: cov),
        MagicMock(fetchall=lambda: pair_rows),
    ]
    drift_report_tasks.get_sync_engine = lambda: engine
    return drift_report_tasks.report_schema_drift.run()


def test_report_says_so_when_nothing_is_comparable():
    """The expected state for the first weeks: snapshots exist, but no table
    has been captured twice. Reporting zeros without that context reads like
    'nothing is wrong', which is a different claim."""
    result = _run_report([])

    assert result["pairs_found"] == 0
    assert result["evaluated"] == 0
    assert result["actionable"] == 0
    assert result["mode"] == "shadow"


def test_report_counts_an_exoneration_by_gate():
    """The per-gate breakdown is the deliverable. A parser-version change is
    ours, not the portal's, and must not land in the actionable pile."""
    pair = _Row(
        columns_profile=[_profile("provincia", mcv=["A", "B", "C", "D", "E"])],
        parser_version="phase4-v1",
        reason="schema_mismatch_recreate",
    )
    for k, v in {
        "p_schema_name": "raw",
        "p_table_name": "t",
        "p_resource_identity": "portal::x",
        "p_version": 1,
        "p_row_count_estimate": 90,
        "p_stats_available": True,
        "p_columns_profile": [_profile("col_1", mcv=["A", "B", "C", "D", "E"])],
        "p_parser_version": "phase3-v2",
        "p_normalization_version": None,
        "p_layout_profile": None,
        "p_header_quality": None,
        "p_is_truncated": None,
    }.items():
        setattr(pair, k, v)

    result = _run_report([pair])

    assert result["evaluated"] == 1
    assert result["actionable"] == 0
    assert result["exonerated_by_gate"] == {"G1_provenance": 1}


def test_one_unparseable_row_does_not_sink_the_report():
    """A snapshot written by an older version is a gap in coverage, not a
    reason to produce nothing."""
    broken = _Row(columns_profile="{not json")
    for k in (
        "p_schema_name",
        "p_table_name",
        "p_resource_identity",
        "p_version",
        "p_row_count_estimate",
        "p_stats_available",
        "p_columns_profile",
        "p_parser_version",
        "p_normalization_version",
        "p_layout_profile",
        "p_header_quality",
        "p_is_truncated",
    ):
        setattr(broken, k, None)

    result = _run_report([broken])

    assert result["pairs_found"] == 1
    assert result["evaluated"] == 0  # skipped, not crashed


def test_actionable_cases_are_included_inline():
    """P3 is a human reading twenty of these by hand. Making them write a
    query first is the friction that stops it happening."""
    pair = _Row(
        columns_profile=[
            _profile("a", mcv=["A", "B", "C", "D", "E"]),
            _profile("nueva", mcv=["1", "2", "3", "4", "5"]),
        ],
        reason="schema_mismatch_recreate",
    )
    for k, v in {
        "p_schema_name": "raw",
        "p_table_name": "t",
        "p_resource_identity": "portal::x",
        "p_version": 1,
        "p_row_count_estimate": 90,
        "p_stats_available": True,
        "p_columns_profile": [_profile("a", mcv=["A", "B", "C", "D", "E"])],
        "p_parser_version": "phase4-v1",
        "p_normalization_version": "phase4-v1",
        "p_layout_profile": "simple_tabular",
        "p_header_quality": "good",
        "p_is_truncated": False,
    }.items():
        setattr(pair, k, v)

    result = _run_report([pair])

    assert result["actionable"] == 1
    assert result["actionable_by_class"] == {"additive": 1}
    assert result["examples"][0]["added"] == ["nueva"]
    assert result["examples"][0]["reason_dropped"] == "schema_mismatch_recreate"
    # G0 and G2 abstain until something produces their input — the report has
    # to show that, or a reader would assume they passed.
    assert set(result["examples"][0]["gates_not_evaluated"]) == {"G0_identity", "G2_sibling"}
