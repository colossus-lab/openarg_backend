"""Unit tests for the dbt project structure (MASTERPLAN Fase 5).

These tests do NOT require dbt to be installed. They verify:
  - the project skeleton is in place
  - YAML files are parseable
  - models reference the staging schema correctly
  - the Celery wrapper handles the "dbt not installed" case cleanly
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_DIR = PROJECT_ROOT / "dbt"


# ── Project skeleton ─────────────────────────────────────────────────────


def test_dbt_project_yml_exists() -> None:
    assert (DBT_DIR / "dbt_project.yml").exists()


def test_dbt_profiles_example_exists() -> None:
    assert (DBT_DIR / "profiles.yml.example").exists()


def test_dbt_gitignore_excludes_target() -> None:
    gi = (DBT_DIR / ".gitignore").read_text()
    assert "target/" in gi
    assert "profiles.yml" in gi  # real profile must not be committed


def test_dbt_project_yml_parses() -> None:
    with (DBT_DIR / "dbt_project.yml").open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    assert data["name"] == "openarg"
    assert data["profile"] == "openarg"
    assert "models" in data
    assert data["models"]["openarg"]["staging"]["+schema"] == "staging"
    assert data["models"]["openarg"]["marts"]["+schema"] == "mart"


def test_models_dir_exists() -> None:
    assert (DBT_DIR / "models" / "staging").is_dir()
    assert (DBT_DIR / "models" / "marts").is_dir()


# ── Sources ──────────────────────────────────────────────────────────────


def test_sources_yml_parses() -> None:
    with (DBT_DIR / "models" / "staging" / "sources.yml").open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    assert data["version"] == 2
    assert any(s["name"] == "staging" for s in data["sources"])


def test_sources_align_with_staging_schema() -> None:
    """Sources must declare schema='staging' so dbt resolves to the
    medallion staging layer (not dbt's default schema).
    """
    with (DBT_DIR / "models" / "staging" / "sources.yml").open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    for source in data["sources"]:
        assert source["schema"] == "staging", f"{source['name']}: wrong schema"


# ── Marts ────────────────────────────────────────────────────────────────


def test_marts_have_schema_yml() -> None:
    """Every mart .sql should have an entry in schema.yml so dbt tests pick it up."""
    schema_yml = DBT_DIR / "models" / "marts" / "schema.yml"
    assert schema_yml.exists()
    with schema_yml.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    documented = {m["name"] for m in data.get("models", [])}

    sql_files = {p.stem for p in (DBT_DIR / "models" / "marts").glob("*.sql")}
    assert sql_files <= documented, f"Missing schema entries for: {sql_files - documented}"


def test_example_mart_references_source() -> None:
    sql = (DBT_DIR / "models" / "marts" / "series_economicas.sql").read_text()
    assert "{{ source(" in sql, "Example mart must use {{ source() }} for lineage"
    assert "staging" in sql


def test_mart_schema_yml_has_tests() -> None:
    """At least one column in schema.yml should have a test — that's the whole
    point of having dbt on top of the marts.
    """
    with (DBT_DIR / "models" / "marts" / "schema.yml").open(encoding="utf-8") as f:
        data = yaml.safe_load(f)
    has_tests = any(
        col.get("tests")
        for model in data.get("models", [])
        for col in model.get("columns", [])
    )
    assert has_tests, "No tests declared in marts/schema.yml — dbt isn't earning its keep"


# ── Celery wrapper ───────────────────────────────────────────────────────


def test_dbt_run_handles_missing_binary(monkeypatch: pytest.MonkeyPatch) -> None:
    """When dbt isn't installed, the wrapper returns status='skipped' with
    an actionable hint — it does NOT raise.
    """
    from app.infrastructure.celery.tasks import dbt_tasks

    monkeypatch.setattr(dbt_tasks, "_resolve_dbt_binary", lambda: None)
    result = dbt_tasks._run_dbt("run")
    assert result["status"] == "skipped"
    assert result["reason"] == "dbt_not_installed"
    assert "pip install" in result["hint"]


def test_dbt_run_handles_missing_project_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from app.infrastructure.celery.tasks import dbt_tasks

    monkeypatch.setattr(dbt_tasks, "_resolve_dbt_binary", lambda: "/usr/bin/dbt")
    fake_dir = tmp_path / "nonexistent"
    result = dbt_tasks._run_dbt("run", dbt_dir=str(fake_dir))
    assert result["status"] == "skipped"
    assert result["reason"] == "dbt_project_not_found"
