"""Tests for WS3 hybrid discovery layer.

Pure-Python checks: feature flag behaviour and the row→DiscoveredResource
mapper. The DB-touching paths are exercised in integration suites.
"""

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

from app.application.discovery import (
    CatalogDiscovery,
    DiscoveredResource,
    catalog_only_mode,
    discovery_enabled,
)


def test_discovery_disabled_by_default(monkeypatch):
    monkeypatch.delenv("OPENARG_HYBRID_DISCOVERY", raising=False)
    monkeypatch.delenv("OPENARG_CATALOG_ONLY", raising=False)
    assert discovery_enabled() is False
    assert catalog_only_mode() is False


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on"])
def test_discovery_flag_truthy_values(monkeypatch, value):
    monkeypatch.delenv("OPENARG_CATALOG_ONLY", raising=False)
    monkeypatch.setenv("OPENARG_HYBRID_DISCOVERY", value)
    assert discovery_enabled() is True


@pytest.mark.parametrize("value", ["0", "false", "no", "off", ""])
def test_discovery_flag_falsy_values(monkeypatch, value):
    monkeypatch.delenv("OPENARG_CATALOG_ONLY", raising=False)
    monkeypatch.setenv("OPENARG_HYBRID_DISCOVERY", value)
    assert discovery_enabled() is False


def test_catalog_only_implies_discovery_enabled(monkeypatch):
    monkeypatch.delenv("OPENARG_HYBRID_DISCOVERY", raising=False)
    monkeypatch.setenv("OPENARG_CATALOG_ONLY", "1")
    assert catalog_only_mode() is True
    assert discovery_enabled() is True


@pytest.mark.parametrize("value", ["1", "true", "yes", "on"])
def test_catalog_only_truthy_values(monkeypatch, value):
    monkeypatch.setenv("OPENARG_CATALOG_ONLY", value)
    assert catalog_only_mode() is True


@pytest.mark.parametrize("value", ["0", "false", "", "off"])
def test_catalog_only_falsy_values(monkeypatch, value):
    monkeypatch.setenv("OPENARG_CATALOG_ONLY", value)
    assert catalog_only_mode() is False


def test_row_to_resource_handles_missing_fields():
    row = SimpleNamespace(
        resource_identity="x::1",
        portal="indec",
        source_id="1",
        canonical_title=None,
        display_name=None,
        materialization_status="pending",
        materialized_table_name=None,
        resource_kind="cuadro",
    )
    res = CatalogDiscovery._row_to_resource(row)
    assert isinstance(res, DiscoveredResource)
    assert res.canonical_title == ""
    assert res.display_name == ""
    assert res.materialization_status == "pending"


def test_find_by_text_returns_empty_for_blank_query(monkeypatch):
    d = CatalogDiscovery(engine=object())
    assert d.find_by_text("") == []
    assert d.find_by_text("   ") == []
