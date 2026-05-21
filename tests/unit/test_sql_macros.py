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
