from __future__ import annotations

from unittest.mock import MagicMock

from app.infrastructure.celery import app as celery_app_module


def test_startup_bootstrap_disabled_by_default(monkeypatch) -> None:
    monkeypatch.delenv("OPENARG_ENABLE_STARTUP_BOOTSTRAP", raising=False)

    transparency = MagicMock()
    bulk_collect = MagicMock()
    monkeypatch.setattr(celery_app_module, "_initial_transparency", transparency)
    monkeypatch.setattr(celery_app_module, "_initial_bulk_collect", bulk_collect)

    celery_app_module._initial_scrape(sender=None)

    transparency.assert_not_called()
    bulk_collect.assert_not_called()


def test_startup_bootstrap_enabled_truthy_values(monkeypatch) -> None:
    for value in ("1", "true", "yes", "on", "TRUE"):
        monkeypatch.setenv("OPENARG_ENABLE_STARTUP_BOOTSTRAP", value)
        assert celery_app_module._startup_bootstrap_enabled() is True


def test_startup_bootstrap_disabled_falsy_values(monkeypatch) -> None:
    for value in ("", "0", "false", "no", "off", "random"):
        monkeypatch.setenv("OPENARG_ENABLE_STARTUP_BOOTSTRAP", value)
        assert celery_app_module._startup_bootstrap_enabled() is False
