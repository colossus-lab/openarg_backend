"""Guard tests for the destructive staging reset.

Only the safety/CLI-validation paths are exercised; the actual destructive
SQL is integration-tested elsewhere.
"""

from __future__ import annotations

import importlib
from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mod(monkeypatch):
    monkeypatch.setenv("APP_ENV", "staging")
    return importlib.import_module("scripts.staging_reset")


def test_refuses_in_prod(monkeypatch):
    monkeypatch.setenv("APP_ENV", "prod")
    mod = importlib.import_module("scripts.staging_reset")
    with pytest.raises(SystemExit) as e:
        mod._refuse_in_prod()
    assert "REFUSED" in str(e.value)


def test_refuses_when_app_env_unset(monkeypatch):
    monkeypatch.delenv("APP_ENV", raising=False)
    mod = importlib.import_module("scripts.staging_reset")
    with pytest.raises(SystemExit) as e:
        mod._refuse_in_prod()
    assert "REFUSED" in str(e.value)


def test_allows_when_app_env_is_staging(monkeypatch):
    monkeypatch.setenv("APP_ENV", "staging")
    mod = importlib.import_module("scripts.staging_reset")
    mod._refuse_in_prod()  # should not raise


def test_allows_stage_alias(monkeypatch):
    monkeypatch.setenv("APP_ENV", "stage")
    mod = importlib.import_module("scripts.staging_reset")
    mod._refuse_in_prod()  # should not raise


def test_main_requires_explicit_flag_for_destructive(monkeypatch, mod):
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://x:y@localhost/x")
    with pytest.raises(SystemExit) as e:
        mod.main(["--no-repopulate"])
    assert "i-understand" in str(e.value)


def test_main_dry_run_does_not_require_flag(monkeypatch, mod):
    """Dry-run is harmless — should not require the destructive flag."""
    # Use a definitely-unreachable URL; the script bails inside reset() but
    # only after passing the guard, which is what we want to assert.
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg://nope:nope@127.0.0.1:1/nope")
    # We expect a connection error from reset(), not a SystemExit from the guard.
    with pytest.raises(Exception) as e:
        mod.main(["--dry-run", "--no-repopulate"])
    assert "REFUSED" not in str(e.value)


def test_toggle_cache_drop_trigger_is_best_effort(mod):
    mock_conn = MagicMock()
    mock_conn.execute.side_effect = [MagicMock(scalar=lambda: True), MagicMock()]
    mock_engine = MagicMock()
    mock_engine.begin.return_value.__enter__ = MagicMock(return_value=mock_conn)
    mock_engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    assert mod._toggle_cache_drop_trigger(mock_engine, enabled=False) is True
