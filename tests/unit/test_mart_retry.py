"""Retrying a degraded mart, and the ways that can go quietly wrong.

Both marts found degraded in production on 2026-08-23 were fixed by a plain
rebuild — the sources had been fine the whole time. What was missing was
anything that retried. These tests pin the two ways a retry sweep can look like
it is working while it is not: counting a rebuild-into-zero-rows as a recovery,
and grinding on the same broken mart every pass.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from app.infrastructure.celery.tasks.mart_tasks import retry_degraded_marts


class _Conn:
    def __init__(self, degraded, after):
        self.degraded = degraded
        self.after = after  # mart_id -> SimpleNamespace(st, rows)
        self.sql: list[str] = []

    def execute(self, stmt, params=None):
        s = str(stmt)
        self.sql.append(s)
        if "last_refresh_status = 'build_failed'" in s:
            return SimpleNamespace(fetchall=lambda: self.degraded)
        mid = (params or {}).get("m")
        return SimpleNamespace(fetchone=lambda: self.after.get(mid))

    def rollback(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Engine:
    def __init__(self, conn):
        self._conn = conn

    def connect(self):
        return self._conn


def _run(degraded, after, **kw):
    conn = _Conn(degraded, after)
    with patch(
        "app.infrastructure.celery.tasks.mart_tasks.get_sync_engine",
        return_value=_Engine(conn),
    ):
        return retry_degraded_marts(dry_run=False, **kw), conn


def test_a_rebuild_that_produces_rows_counts_as_recovered():
    degraded = [SimpleNamespace(mart_id="pobreza", last_refresh_status="built", rows=0)]
    after = {"pobreza": SimpleNamespace(st="built", rows=864)}
    with patch("app.infrastructure.celery.tasks.mart_tasks.build_mart") as bm:
        out, _ = _run(degraded, after)
    assert bm.called
    assert out["recovered"] == 1
    assert out["still_broken"] == 0


def test_a_rebuild_into_zero_rows_is_not_a_recovery():
    """The failure mode that makes a retry sweep report success while the mart
    stays invisible: the build succeeds, the mart holds nothing, and the serving
    filter keeps hiding it."""
    degraded = [SimpleNamespace(mart_id="vacio", last_refresh_status="built", rows=0)]
    after = {"vacio": SimpleNamespace(st="built", rows=0)}
    with patch("app.infrastructure.celery.tasks.mart_tasks.build_mart"):
        out, _ = _run(degraded, after)
    assert out["recovered"] == 0
    assert out["still_broken"] == 1


def test_one_mart_that_raises_does_not_cost_the_batch():
    degraded = [
        SimpleNamespace(mart_id="malo", last_refresh_status="build_failed", rows=0),
        SimpleNamespace(mart_id="bueno", last_refresh_status="build_failed", rows=0),
    ]
    after = {"bueno": SimpleNamespace(st="built", rows=10)}

    def _build(mid):
        if mid == "malo":
            raise RuntimeError("sigue rota")

    with patch("app.infrastructure.celery.tasks.mart_tasks.build_mart", side_effect=_build):
        out, _ = _run(degraded, after)
    assert out["recovered"] == 1
    assert out["still_broken"] == 1


def test_it_skips_marts_attempted_recently():
    """Without the age floor a fast-failing mart is rebuilt on every pass, and a
    broken mart costs more than a working one."""
    degraded = [SimpleNamespace(mart_id="x", last_refresh_status="build_failed", rows=0)]
    with patch("app.infrastructure.celery.tasks.mart_tasks.build_mart"):
        _, conn = _run(degraded, {"x": SimpleNamespace(st="built", rows=1)}, min_age_hours=6)
    q = next(s for s in conn.sql if "build_failed" in s)
    assert "min_age_hours" in q
    # And the oldest attempt goes first, so a permanently broken mart cannot
    # starve the others.
    assert "ORDER BY last_refreshed_at NULLS FIRST" in q


def test_dry_run_rebuilds_nothing():
    degraded = [SimpleNamespace(mart_id="x", last_refresh_status="build_failed", rows=0)]
    conn = _Conn(degraded, {})
    with (
        patch(
            "app.infrastructure.celery.tasks.mart_tasks.get_sync_engine",
            return_value=_Engine(conn),
        ),
        patch("app.infrastructure.celery.tasks.mart_tasks.build_mart") as bm,
    ):
        out = retry_degraded_marts(dry_run=True)
    assert not bm.called
    assert out["candidates"] == 1
    assert out["recovered"] == 0


def test_sample_queries_are_read_from_the_yaml():
    """Samples were populated once by a hand-run script, which is why marts added
    afterwards had none — nothing in the build path knew about them.

    Measured in production 2026-08-23: nine marts had zero samples, and they were
    the largest ones (5.9M, 4.3M, 2.8M rows). The biggest marts were the ones the
    router was least able to find, because a mart with no samples can never earn
    the +0.17 boost.
    """
    from pathlib import Path

    from app.application.marts.mart import load_all_marts

    marts = load_all_marts(Path("config/marts"))
    by_id = {m.id: m for m in marts}
    # The nine that had none now carry questions a person would actually ask.
    for mid in (
        "sociedades_registro_nacional",
        "inscripciones_iniciales_autos",
        "delitos_caba",
        "delitos_argentina_snic",
        "audiencias_gestion_intereses",
        "acumar_agentes_contaminantes",
    ):
        assert by_id[mid].sample_queries, f"{mid} sigue sin muestras"
        assert all(isinstance(q, str) and q.strip() for q in by_id[mid].sample_queries)


def test_a_mart_without_samples_still_loads():
    """Samples are optional: a mart that has none must not fail to parse."""
    from app.application.marts.mart import Mart

    m = Mart(
        id="x",
        version="1",
        description="d",
        domain=None,
        source_portals=[],
        source_resource_patterns=[],
        canonical_columns=[],
        sql="SELECT 1",
        refresh=None,  # type: ignore[arg-type]
    )
    assert m.sample_queries == []
