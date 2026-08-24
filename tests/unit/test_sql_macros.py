from __future__ import annotations

import re

import pytest

from app.application.marts.sql_macros import (
    MacroResolutionError,
    _LiveRow,
    resolve_macros,
)


def test_resolve_macros_live_table_uses_targeted_lookup(monkeypatch) -> None:
    observed = {"targeted": False, "full_scan": False}

    def _fake_query_live_identities(_engine, identities):
        observed["targeted"] = True
        assert identities == ["bcra::cotizaciones"]
        return {
            "bcra::cotizaciones": _LiveRow(
                resource_identity="bcra::cotizaciones",
                schema_name="raw",
                table_name="bcra__cotizaciones__abcd1234__v1",
            )
        }

    def _fake_query_lives(_engine):
        observed["full_scan"] = True
        return []

    monkeypatch.setattr(
        "app.application.marts.sql_macros._query_live_identities",
        _fake_query_live_identities,
    )
    monkeypatch.setattr(
        "app.application.marts.sql_macros._query_lives",
        _fake_query_lives,
    )
    monkeypatch.setattr(
        "app.application.marts.sql_macros._query_live_by_portals",
        lambda _engine, _portals: [],
    )
    monkeypatch.setattr(
        "app.application.marts.sql_macros._query_live_by_identity_patterns",
        lambda _engine, _patterns: [],
    )
    monkeypatch.setattr(
        "app.application.marts.sql_macros._query_live_by_table_patterns",
        lambda _engine, _patterns: [],
    )

    sql = "SELECT * FROM {{ live_table('bcra::cotizaciones') }}"
    resolved = resolve_macros(sql, engine=object())

    assert observed["targeted"] is True
    assert observed["full_scan"] is False
    assert 'raw."bcra__cotizaciones__abcd1234__v1"' in resolved


def test_resolve_macros_pattern_uses_targeted_lookup(monkeypatch) -> None:
    observed = {"pattern": False, "full_scan": False}

    def _fake_query_live_identities(_engine, identities):
        return {}

    def _fake_query_lives(_engine):
        observed["full_scan"] = True
        return []

    def _fake_query_live_by_identity_patterns(_engine, patterns):
        observed["pattern"] = True
        assert patterns == ["bcra::*tasa*"]
        return [
            _LiveRow(
                resource_identity="bcra::tasa_activa",
                schema_name="raw",
                table_name="bcra__tasa_activa__abcd1234__v1",
            )
        ]

    monkeypatch.setattr(
        "app.application.marts.sql_macros._query_live_identities",
        _fake_query_live_identities,
    )
    monkeypatch.setattr(
        "app.application.marts.sql_macros._query_lives",
        _fake_query_lives,
    )
    monkeypatch.setattr(
        "app.application.marts.sql_macros._query_live_by_portals",
        lambda _engine, _portals: [],
    )
    monkeypatch.setattr(
        "app.application.marts.sql_macros._query_live_by_identity_patterns",
        _fake_query_live_by_identity_patterns,
    )
    monkeypatch.setattr(
        "app.application.marts.sql_macros._query_live_by_table_patterns",
        lambda _engine, _patterns: [],
    )

    sql = "SELECT * FROM {{ live_tables_by_pattern('bcra::*tasa*') }} src"
    resolved = resolve_macros(sql, engine=object())

    assert observed["pattern"] is True
    assert observed["full_scan"] is False
    assert 'raw."bcra__tasa_activa__abcd1234__v1"' in resolved


def test_resolve_macros_portal_uses_targeted_lookup(monkeypatch) -> None:
    observed = {"portal": False, "full_scan": False}

    monkeypatch.setattr(
        "app.application.marts.sql_macros._query_live_identities",
        lambda _engine, _identities: {},
    )
    monkeypatch.setattr(
        "app.application.marts.sql_macros._query_lives",
        lambda _engine: observed.__setitem__("full_scan", True) or [],
    )

    def _fake_query_live_by_portals(_engine, portals):
        observed["portal"] = True
        assert portals == ["bcra"]
        return [
            _LiveRow(
                resource_identity="bcra::cotizaciones",
                schema_name="raw",
                table_name="bcra__cotizaciones__abcd1234__v1",
            )
        ]

    monkeypatch.setattr(
        "app.application.marts.sql_macros._query_live_by_portals",
        _fake_query_live_by_portals,
    )
    monkeypatch.setattr(
        "app.application.marts.sql_macros._query_live_by_identity_patterns",
        lambda _engine, _patterns: [],
    )
    monkeypatch.setattr(
        "app.application.marts.sql_macros._query_live_by_table_patterns",
        lambda _engine, _patterns: [],
    )

    sql = "SELECT * FROM {{ live_tables_by_portal('bcra') }} src"
    resolved = resolve_macros(sql, engine=object())

    assert observed["portal"] is True
    assert observed["full_scan"] is False
    assert 'raw."bcra__cotizaciones__abcd1234__v1"' in resolved


# ── require_columns: filter set decoupled from projection set ─────────
#
# Measured on the presupuesto cluster (staging, 2026-07-26): requiring all
# 33 projected columns kept 36 of 560 live tables — 91k of 15.8M rows, i.e.
# 0.58 % of the domain — because 62 otherwise-complete tables lacked an
# optional column like `finalidad_funcion_id`. The mart looked healthy the
# whole time, which is how it ended up serving wrong rankings.

_EXPECTED = ["anio", "jurisdiccion", "monto", "finalidad"]
_CORE = ["anio", "jurisdiccion", "monto"]

_TABLES = {
    ("raw", "p_full"): {"anio", "jurisdiccion", "monto", "finalidad"},
    ("raw", "p_sin_finalidad"): {"anio", "jurisdiccion", "monto"},
    ("raw", "p_dimension"): {"finalidad", "descripcion"},
}


def _patch(monkeypatch, rows):
    monkeypatch.setattr(
        "app.application.marts.sql_macros._query_live_identities",
        lambda _e, _i: {},
    )
    monkeypatch.setattr(
        "app.application.marts.sql_macros._query_live_by_portals",
        lambda _e, _p: [],
    )
    monkeypatch.setattr(
        "app.application.marts.sql_macros._query_live_by_identity_patterns",
        lambda _e, _p: [],
    )
    monkeypatch.setattr("app.application.marts.sql_macros._query_lives", lambda _e: [])
    monkeypatch.setattr(
        "app.application.marts.sql_macros._query_live_by_table_patterns",
        lambda _e, _p: rows,
    )
    monkeypatch.setattr(
        "app.application.marts.sql_macros._query_columns",
        lambda _e, pairs: {p: _TABLES[p] for p in pairs},
    )


def _rows():
    return [
        _LiveRow(resource_identity=f"p::{n}", schema_name="raw", table_name=n)
        for n in ("p_full", "p_sin_finalidad", "p_dimension")
    ]


def test_require_all_columns_still_demands_every_expected_column(monkeypatch) -> None:
    """Regression guard: 26 shipped marts rely on this behaviour."""
    _patch(monkeypatch, _rows())
    sql = (
        "SELECT 1 FROM {{ live_tables_by_table_pattern('p*', "
        f"expected_columns={_EXPECTED!r}, require_all_columns=True) }}}} s"
    )
    resolved = resolve_macros(sql, engine=object())
    assert 'raw."p_full"' in resolved
    assert 'raw."p_sin_finalidad"' not in resolved
    assert 'raw."p_dimension"' not in resolved


def test_require_columns_keeps_tables_missing_an_optional_column(monkeypatch) -> None:
    _patch(monkeypatch, _rows())
    sql = (
        "SELECT 1 FROM {{ live_tables_by_table_pattern('p*', "
        f"expected_columns={_EXPECTED!r}, require_columns={_CORE!r}) }}}} s"
    )
    resolved = resolve_macros(sql, engine=object())
    assert 'raw."p_full"' in resolved
    assert 'raw."p_sin_finalidad"' in resolved, "an optional column must not cost the table"
    # The projection still resolves: the missing column comes through as NULL.
    assert 'NULL::text AS "finalidad"' in resolved
    # Real sub-shapes (dimension tables) are still excluded.
    assert 'raw."p_dimension"' not in resolved


def test_column_filter_records_its_coverage_in_the_sql(monkeypatch) -> None:
    """The kept/candidate ratio has to outlive the build that computed it.

    It used to exist only as a `logger.info`, so the one number that says
    "this mart answers about 6 % of its domain" was gone the moment the build
    finished — `mart_definitions` keeps a healthy-looking `last_row_count` and
    nothing else. The marker rides into `sql_definition`, where the quality
    auditor reads it.
    """
    _patch(monkeypatch, _rows())
    sql = (
        "SELECT 1 FROM {{ live_tables_by_table_pattern('p*', "
        f"expected_columns={_EXPECTED!r}, require_all_columns=True) }}}} s"
    )
    resolved = resolve_macros(sql, engine=object())
    assert "/* macro_coverage: kept 1 of 3 */" in resolved


def test_no_coverage_marker_when_nothing_was_filtered(monkeypatch) -> None:
    """Absent marker means "no filter ran", not "filter kept everything"."""
    _patch(monkeypatch, _rows())
    sql = "SELECT 1 FROM {{ live_tables_by_table_pattern('p*') }} s"
    resolved = resolve_macros(sql, engine=object())
    assert "macro_coverage" not in resolved


def test_source_marker_distinguishes_tables_sharing_a_dataset_id(monkeypatch) -> None:
    """The marker must differ per physical table, not per ingest column.

    Regression guard for the DDJJ bug: `ddjj_patrimonio_declarado` deduplicated
    on `_source_dataset_id`, three of its seven source tables carried the same
    value, and every row of the year they shared passed through twice. The
    build reported `success` with a plausible row count, so nothing caught it —
    only counting one declaration's rows against each source did.
    """
    _patch(monkeypatch, _rows())
    sql = (
        "SELECT 1 FROM {{ live_tables_by_table_pattern('p*', "
        f"expected_columns={_CORE!r}, source_marker='__src') }}}} s"
    )
    resolved = resolve_macros(sql, engine=object())

    markers = re.findall(r"'([^']+)'::text AS \"__src\"", resolved)
    assert markers == ["raw.p_full", "raw.p_sin_finalidad", "raw.p_dimension"]
    # One literal per branch, all distinct — the property the dedup rests on.
    assert len(set(markers)) == len(markers)


def test_source_marker_survives_zero_matches(monkeypatch) -> None:
    """An empty cluster must still expose the marker column.

    Without it the outer dedup references `__src` against a shape that lacks
    it and the mart fails to build instead of building empty — which is the
    whole point of the typed-empty fallback.
    """
    _patch(monkeypatch, [])
    sql = (
        "SELECT 1 FROM {{ live_tables_by_table_pattern('nomatch*', "
        f"expected_columns={_CORE!r}, source_marker='__src') }}}} s"
    )
    resolved = resolve_macros(sql, engine=object())
    assert 'NULL::text AS "__src"' in resolved
    assert "WHERE FALSE" in resolved


def test_source_marker_rejects_collision_with_an_expected_column(monkeypatch) -> None:
    _patch(monkeypatch, _rows())
    sql = (
        "SELECT 1 FROM {{ live_tables_by_table_pattern('p*', "
        f"expected_columns={_CORE!r}, source_marker={_CORE[0]!r}) }}}} s"
    )
    with pytest.raises(MacroResolutionError, match="collides"):
        resolve_macros(sql, engine=object())
