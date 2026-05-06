"""Unit tests for the marts module (MASTERPLAN Fase 3).

Covers YAML parsing, DDL generation (CREATE / COMMENT / REFRESH), and the
shipped config/marts/*.yaml files.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from app.application.marts import (
    build_comment_sql,
    build_create_view_sql,
    load_all_marts,
    load_mart,
)
from app.application.marts.builder import build_refresh_sql
from app.application.marts.mart import MartParseError


# ── YAML loading ──────────────────────────────────────────────────────────


def _minimal_yaml() -> str:
    return """
id: testmart
version: 1.0.0
description: A
domain: test
sources:
  portals: [test_portal]
canonical_columns:
  - name: foo
    type: int
    description: Foo
sql: |
  SELECT NULL::int AS foo WHERE FALSE
refresh:
  policy: manual
"""


def test_load_mart_minimal(tmp_path: Path) -> None:
    p = tmp_path / "m.yaml"
    p.write_text(_minimal_yaml())
    m = load_mart(p)
    assert m.id == "testmart"
    assert m.version == "1.0.0"
    assert m.domain == "test"
    assert m.source_portals == ["test_portal"]
    assert len(m.canonical_columns) == 1
    assert m.refresh.policy == "manual"
    assert m.refresh.unique_index == []
    assert m.qualified_name == 'mart."testmart"'


def test_load_mart_invalid_column_type(tmp_path: Path) -> None:
    p = tmp_path / "bad.yaml"
    p.write_text(_minimal_yaml().replace("type: int", "type: bogus"))
    with pytest.raises(MartParseError, match="invalid type"):
        load_mart(p)


def test_load_mart_invalid_refresh_policy(tmp_path: Path) -> None:
    p = tmp_path / "bad.yaml"
    p.write_text(_minimal_yaml().replace("policy: manual", "policy: weekly"))
    with pytest.raises(MartParseError, match="refresh.policy"):
        load_mart(p)


def test_load_mart_missing_sql(tmp_path: Path) -> None:
    p = tmp_path / "bad.yaml"
    yaml_text = """
id: x
version: 1.0.0
sources:
  portals: [test_portal]
canonical_columns:
  - name: y
    type: int
"""
    p.write_text(yaml_text)
    with pytest.raises(MartParseError, match="'sql'"):
        load_mart(p)


def test_load_all_marts_empty_dir(tmp_path: Path) -> None:
    assert load_all_marts(tmp_path) == []


# ── DDL generation ────────────────────────────────────────────────────────


def test_build_create_view_sql_basic(tmp_path: Path) -> None:
    p = tmp_path / "m.yaml"
    p.write_text(_minimal_yaml())
    m = load_mart(p)
    statements = build_create_view_sql(m)
    assert len(statements) == 2  # DROP + CREATE (no unique index)
    assert "DROP MATERIALIZED VIEW IF EXISTS" in statements[0]
    assert "CREATE MATERIALIZED VIEW" in statements[1]
    assert 'mart."testmart"' in statements[1]
    assert "SELECT NULL::int AS foo" in statements[1]


def test_build_create_view_sql_with_unique_index(tmp_path: Path) -> None:
    yaml_text = (
        _minimal_yaml().replace(
            "policy: manual\n",
            "policy: manual\n  unique_index: [foo]\n",
        )
    )
    p = tmp_path / "m.yaml"
    p.write_text(yaml_text)
    m = load_mart(p)
    statements = build_create_view_sql(m)
    assert len(statements) == 3  # DROP + CREATE + UNIQUE INDEX
    assert "CREATE UNIQUE INDEX" in statements[2]
    assert "(\"foo\")" in statements[2]


def test_build_comment_sql_skips_undocumented(tmp_path: Path) -> None:
    yaml_text = """
id: m
version: 1.0.0
sources:
  portals: [test_portal]
canonical_columns:
  - name: with_desc
    type: int
    description: Documented
  - name: no_desc
    type: int
sql: SELECT 1::int AS with_desc, 2::int AS no_desc
refresh:
  policy: manual
"""
    p = tmp_path / "m.yaml"
    p.write_text(yaml_text)
    m = load_mart(p)
    statements = build_comment_sql(m)
    assert len(statements) == 1  # only with_desc
    assert "with_desc" in statements[0]
    assert "Documented" in statements[0]


def test_build_comment_sql_escapes_quotes(tmp_path: Path) -> None:
    yaml_text = """
id: m
version: 1.0.0
sources:
  portals: [test_portal]
canonical_columns:
  - name: x
    type: int
    description: "It's tricky"
sql: SELECT 1::int AS x
refresh:
  policy: manual
"""
    p = tmp_path / "m.yaml"
    p.write_text(yaml_text)
    m = load_mart(p)
    statements = build_comment_sql(m)
    assert "It''s tricky" in statements[0]


def test_build_refresh_sql_concurrent(tmp_path: Path) -> None:
    yaml_text = (
        _minimal_yaml().replace(
            "policy: manual\n",
            "policy: manual\n  unique_index: [foo]\n",
        )
    )
    p = tmp_path / "m.yaml"
    p.write_text(yaml_text)
    m = load_mart(p)
    sql = build_refresh_sql(m)
    assert "REFRESH MATERIALIZED VIEW CONCURRENTLY" in sql
    assert 'mart."testmart"' in sql


def test_build_refresh_sql_non_concurrent(tmp_path: Path) -> None:
    p = tmp_path / "m.yaml"
    p.write_text(_minimal_yaml())  # no unique_index
    m = load_mart(p)
    sql = build_refresh_sql(m)
    assert "REFRESH MATERIALIZED VIEW " in sql
    assert "CONCURRENTLY" not in sql


# ── Real config/marts/ files ──────────────────────────────────────────────


def test_real_marts_load() -> None:
    """The shipped YAMLs in config/marts/ must parse without error."""
    project_root = Path(__file__).resolve().parents[2]
    marts = load_all_marts(project_root / "config" / "marts")
    ids = {m.id for m in marts}
    expected = {
        "presupuesto_consolidado",
        "series_economicas",
        "staff_estado",
        "escuelas_argentina",
        "legislatura_actividad",
    }
    assert expected <= ids


def test_real_marts_have_canonical_columns() -> None:
    """Every shipped mart must declare at least one canonical column."""
    project_root = Path(__file__).resolve().parents[2]
    for mart in load_all_marts(project_root / "config" / "marts"):
        assert len(mart.canonical_columns) > 0, f"{mart.id}: no canonical_columns"


def test_real_marts_ddl_round_trip() -> None:
    """Every shipped mart must produce valid-looking DDL."""
    project_root = Path(__file__).resolve().parents[2]
    for mart in load_all_marts(project_root / "config" / "marts"):
        statements = build_create_view_sql(mart)
        assert any("CREATE MATERIALIZED VIEW" in s for s in statements)
        assert build_refresh_sql(mart).startswith("REFRESH MATERIALIZED VIEW")
