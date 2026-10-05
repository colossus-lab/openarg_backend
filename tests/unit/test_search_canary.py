"""Canario nocturno del recall de la búsqueda (índice HNSW contra la exacta)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from app.application.catalog.search_canary import (
    CANARY_QUERIES,
    CanaryReport,
    QueryRecall,
    tie_aware_recall,
)
from app.domain.ports.search.vector_search import SearchResult
from app.infrastructure.celery import app as celery_module
from app.infrastructure.celery.tasks import search_canary_tasks


def _r(i: int, score: float) -> SearchResult:
    return SearchResult(f"ds-{i}", f"T{i}", "", "p", "", "", score)


def test_recall_counts_hits_that_reach_the_tenth_exact_score() -> None:
    exact = [_r(i, 0.70 - i * 0.01) for i in range(10)]
    assert tie_aware_recall(exact, exact) == 1.0
    # El índice trae 5 de los 10 y 5 que puntúan menos que el décimo.
    ann = exact[:5] + [_r(100 + i, 0.50) for i in range(5)]
    assert tie_aware_recall(ann, exact) == 0.5


def test_recall_does_not_punish_a_different_pick_inside_a_tie() -> None:
    """30 "Listado de agentes" de Córdoba con el mismo vector: por id el
    recall daba 0,6 sin que faltara nada."""
    exact = [_r(i, 0.663) for i in range(10)]
    ann = [_r(50 + i, 0.663) for i in range(10)]
    assert tie_aware_recall(ann, exact) == 1.0


def test_recall_with_an_empty_exact_search_is_full() -> None:
    assert tie_aware_recall([], []) == 1.0


def _report(*recalls: float) -> CanaryReport:
    return CanaryReport(
        results=tuple(
            QueryRecall(f"q{i}", r, 0.5, 0.7, 700.0, 800.0) for i, r in enumerate(recalls)
        )
    )


def test_report_is_degraded_under_the_floor() -> None:
    assert not _report(1.0, 1.0, 0.9).degraded  # 0,967
    assert _report(1.0, 0.5, 0.9).degraded  # 0,8
    assert not CanaryReport(results=()).degraded


def test_detail_names_the_worst_queries() -> None:
    detail = _report(1.0, 0.0, 0.5).detail_es()
    assert "«q1» 0.0" in detail and "«q2» 0.5" in detail
    assert "«q0»" not in detail


def test_the_canary_queries_include_the_audits_fourteen() -> None:
    assert len(CANARY_QUERIES) >= 20
    assert "salario mínimo vital y móvil" in CANARY_QUERIES
    assert "cantidad de empleados públicos nacionales" in CANARY_QUERIES
    assert len(set(CANARY_QUERIES)) == len(CANARY_QUERIES)


def _run_task(report: CanaryReport):
    async def _fake_measure(queries):
        return report

    notify = MagicMock(return_value={"sent": 1})
    with (
        patch.object(search_canary_tasks, "_measure", _fake_measure),
        patch.object(search_canary_tasks, "get_sync_engine", return_value=MagicMock()),
        patch("app.application.quality.alerting.notify", notify),
    ):
        out = search_canary_tasks.search_recall_canary.run()
    return out, notify


def test_a_degraded_index_alerts_once_a_day() -> None:
    out, notify = _run_task(_report(1.0, 0.0, 0.5))

    assert out["degraded"] is True and out["mean_recall"] == 0.5
    [alerts] = [c.args[1] for c in notify.call_args_list]
    [alert] = alerts
    assert alert.kind == "search_recall"
    assert alert.key.startswith("recall:")
    assert "50%" in alert.title


def test_a_healthy_index_stays_quiet() -> None:
    out, notify = _run_task(_report(1.0, 1.0, 1.0))

    assert out["degraded"] is False
    notify.assert_not_called()


def test_the_canary_runs_nightly_on_a_queue_with_consumers() -> None:
    """`options` del beat PISA `task_routes`: las dos tienen que decir lo mismo,
    y la cola tiene que tener consumidor (lo verifica el test guardián)."""
    app = celery_module.celery_app
    entry = app.conf.beat_schedule["search-recall-canary"]
    assert entry["task"] == "openarg.search_recall_canary"
    assert entry["options"]["queue"] == "ingest"
    assert app.conf.task_routes["openarg.search_recall_canary"]["queue"] == "ingest"
    assert "openarg.search_recall_canary" in app.tasks


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("postgresql://u:p@h:5432/db", "postgresql+psycopg://u:p@h:5432/db"),
        ("postgresql+psycopg://u:p@h/db", "postgresql+psycopg://u:p@h/db"),
        ("postgresql+asyncpg://u:p@h/db", "postgresql+asyncpg://u:p@h/db"),
    ],
)
def test_async_url_keeps_an_async_driver(monkeypatch, url: str, expected: str) -> None:
    monkeypatch.setenv("DATABASE_URL", url)
    assert search_canary_tasks._async_url() == expected
