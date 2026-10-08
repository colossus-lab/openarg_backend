"""Canario nocturno del recall de la búsqueda (índice HNSW contra la exacta)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.application.catalog.search_canary import (
    CANARY_QUERIES,
    CanaryReport,
    QueryRecall,
    recall_band,
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


def test_detail_does_not_order_a_reindex() -> None:
    """Revisión del #176: el texto decía "REINDEX pendiente" y se leía como una
    orden (un REINDEX de ~1 GB en prod), cuando no está medido que arregle el
    0,915 de staging. Dice qué significa una alerta fija y qué hacer.

    Revisión del #183: el estado conocido no es un valor de una sola corrida.
    El recorrido de 400 solo daba 0,905 o 0,855 según el embedding (bandas 90
    y 85); lo que mide el canario ahora (con el recorrido ancho detrás) dio
    0,915 en cinco juegos de embeddings (08-oct)."""
    detail = _report(1.0, 0.0, 0.5).detail_es()
    assert "REINDEX pendiente" not in detail
    assert "REINDEX no está medido como arreglo" in detail
    assert "con OK" in detail and "primero en staging" in detail
    # Qué es una alerta que queda fija y qué una que empeora.
    assert "misma banda" in detail and "estado conocido" in detail
    assert "baja de banda" in detail
    assert "0,915 con cinco juegos de embeddings" in detail
    assert "0,905" not in detail


def test_detail_says_what_the_alert_identity_does_with_a_fixed_band() -> None:
    """El texto promete que una alerta fija no es una degradación nueva: eso es
    cierto sólo si la misma banda da la misma alerta y `notify` la reabre de a
    ratos, no todas las noches."""
    from app.application.quality.alerting import REOPEN_AT

    assert REOPEN_AT == (3, 10, 30, 100)  # lo que dice el docstring de detail_es
    assert recall_band(0.915) == recall_band(0.905) == recall_band(0.90) == 90
    assert recall_band(0.895) == 85
    # Lo que daba el recorrido de 400 solo (08-oct): dos bandas según el
    # embedding, o sea dos alertas que se alternan. Por eso no es lo que mide.
    assert {recall_band(0.905), recall_band(0.855)} == {90, 85}


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


def test_a_degraded_index_alerts_under_a_stable_identity() -> None:
    """La clave es la del problema (el índice y su banda de recall), no la de
    la noche: así `notify` deduplica y reabre a la 3ª/10ª/30ª vez."""
    out, notify = _run_task(_report(1.0, 0.0, 0.5))

    assert out["degraded"] is True and out["mean_recall"] == 0.5
    [alerts] = [c.args[1] for c in notify.call_args_list]
    [alert] = alerts
    assert alert.kind == "search_recall"
    assert alert.key == "hnsw:50pct"
    assert "50%" in alert.title

    # Otra noche con el mismo grado de degradación: la misma alerta.
    _, again = _run_task(_report(1.0, 0.1, 0.4))
    assert again.call_args.args[1][0].fingerprint() == alert.fingerprint()
    # Si empeora, es otra.
    _, worse = _run_task(_report(0.5, 0.0, 0.2))
    assert worse.call_args.args[1][0].key == "hnsw:20pct"


@pytest.mark.parametrize(
    ("recall", "band"),
    [(0.949, 90), (0.93, 90), (0.9, 90), (0.85, 85), (0.849, 80), (0.0, 0), (1.0, 100)],
)
def test_recall_band(recall: float, band: int) -> None:
    assert recall_band(recall) == band


def test_the_canary_measures_what_the_index_serves_before_the_exact_search(
    monkeypatch,
) -> None:
    """Revisión del #183: con 400 candidatos el recorrido solo daba 0,905 o
    0,855 según el embedding, y la alerta saltaba entre las bandas 90 y 85.
    El canario mide ``search_datasets_index`` (el recorrido y, si su top es
    débil, el ancho), que es lo que el buscador sirve antes de la exacta."""
    import asyncio

    from app.infrastructure.adapters.llm import bedrock_embedding_adapter
    from app.infrastructure.adapters.search.pgvector_search_adapter import (
        PgVectorSearchAdapter,
    )

    calls: list[tuple[str, int]] = []

    async def _index(self, vector, limit=10, *args, **kwargs):
        calls.append(("index", limit))
        return [_r(1, 0.70)]

    async def _hnsw(self, *args, **kwargs):
        raise AssertionError("el canario no mide el recorrido angosto solo")

    async def _exact(self, vector, limit=10, *args, **kwargs):
        calls.append(("exact", limit))
        return [_r(1, 0.70)]

    class _Embedder:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def embed(self, text_: str) -> list[float]:
            return [0.1]

    session = MagicMock()
    session.execute = AsyncMock()
    session.rollback = AsyncMock()

    class _Factory:
        def __call__(self):
            return self

        async def __aenter__(self):
            return session

        async def __aexit__(self, *exc) -> bool:
            return False

    engine = MagicMock()
    engine.dispose = AsyncMock()
    monkeypatch.setattr(PgVectorSearchAdapter, "search_datasets_index", _index)
    monkeypatch.setattr(PgVectorSearchAdapter, "search_datasets_hnsw", _hnsw)
    monkeypatch.setattr(PgVectorSearchAdapter, "search_datasets_exact", _exact)
    monkeypatch.setattr(bedrock_embedding_adapter, "BedrockEmbeddingAdapter", _Embedder)
    monkeypatch.setattr("sqlalchemy.ext.asyncio.create_async_engine", lambda *a, **k: engine)
    monkeypatch.setattr("sqlalchemy.ext.asyncio.async_sessionmaker", lambda *a, **k: _Factory())

    report = asyncio.run(search_canary_tasks._measure(("votaciones nominales",)))

    assert calls == [("index", 10), ("exact", 10)]
    assert [r.recall for r in report.results] == [1.0]


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
