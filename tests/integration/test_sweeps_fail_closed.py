"""A deletion sweep must refuse to run when its registry is not trustworthy.

On 2026-08-03 `raw.cached_datasets` was dropped as an orphan. Every sweep in
`ops_fixes` decides what to delete by asking what that table does *not* claim,
and with the table gone the predicate is vacuously true for the entire
catalogue. They failed loudly only by accident — the SQL referenced a missing
relation. Had the table existed and been empty, they would have succeeded and
deleted everything.

These tests are that accident turned into a rule.
"""

from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, text


def _engine_or_skip():
    url = os.getenv("DATABASE_URL", "")
    if not url:
        pytest.skip("DATABASE_URL not set — fail-closed test needs a live DB")
    try:
        engine = create_engine(url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).scalar()
        return engine
    except Exception as exc:  # pragma: no cover — environmental
        pytest.skip(f"DB unreachable: {exc}")


@pytest.fixture
def registry(request):
    """A disposable `raw.cached_datasets`, restored however the test ends."""
    engine = _engine_or_skip()
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.execute(text("DROP TABLE IF EXISTS raw.cached_datasets CASCADE"))
        conn.execute(
            text("CREATE TABLE raw.cached_datasets (table_name text, status text)")
        )

    def _cleanup():
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS raw.cached_datasets CASCADE"))

    request.addfinalizer(_cleanup)
    return engine


def _fill(engine, n):
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO raw.cached_datasets "
                "SELECT 'cache_t'||i, 'ready' FROM generate_series(1, :n) i"
            ),
            {"n": n},
        )


def test_a_missing_registry_stops_the_sweep(registry):
    """The 2026-08-03 case exactly. Without the table every table in the
    catalogue looks unclaimed, so 'delete what nothing claims' means everything."""
    from app.infrastructure.celery.tasks.ops_fixes import (
        _RegistryUnavailable,
        _require_registry,
    )

    with registry.begin() as conn:
        conn.execute(text("DROP TABLE raw.cached_datasets"))

    with pytest.raises(_RegistryUnavailable, match="does not exist"):
        _require_registry(registry, task="test")


def test_a_truncated_registry_stops_the_sweep(registry):
    """The worse version of the same failure: the table exists and is nearly
    empty, so the sweep succeeds and deletes the catalogue quietly."""
    from app.infrastructure.celery.tasks.ops_fixes import (
        _RegistryUnavailable,
        _require_registry,
    )

    _fill(registry, 5)

    with pytest.raises(_RegistryUnavailable, match="below the"):
        _require_registry(registry, task="test")


def test_a_healthy_registry_lets_the_sweep_run(registry):
    from app.infrastructure.celery.tasks.ops_fixes import _require_registry

    _fill(registry, 2000)

    _require_registry(registry, task="test")  # must not raise


def test_every_sweep_that_deletes_by_absence_checks_first():
    """A sweep added later must not inherit the 2026-08-03 failure by omission."""
    import inspect

    from app.infrastructure.celery.tasks import ops_fixes

    for name in (
        "cleanup_raw_orphans",
        "cleanup_invariants",
        "cleanup_orphan_cache_tables",
    ):
        task = getattr(ops_fixes, name)
        source = inspect.getsource(task.__wrapped__)
        assert "_require_registry" in source, f"{name} deletes without checking the registry"
