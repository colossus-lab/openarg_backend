from __future__ import annotations

from app.application.marts.sql_macros import _LiveRow, resolve_macros


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
