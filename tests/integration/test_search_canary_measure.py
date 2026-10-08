"""El canario de recall corre contra pgvector de verdad, con el motor async del worker.

Lo único doble es Bedrock: el embedding de la consulta sale de una tabla fija.
Lo que se prueba es la parte que un doble en memoria no ve: el motor async
sobre la `DATABASE_URL` del worker (psycopg, sin statements preparados para
PgBouncer), las transacciones de sólo lectura y que la búsqueda por el índice
y la exacta del adaptador den el mismo top cuando el grafo está sano.
"""

from __future__ import annotations

import os
import re
import uuid
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text

from app.infrastructure.celery.tasks import search_canary_tasks

_N = 30


def _url_or_skip() -> str:
    url = os.getenv("DATABASE_URL", "")
    if not url:
        pytest.skip("DATABASE_URL not set — este test necesita una DB real")
    try:
        engine = create_engine(url, pool_pre_ping=True)
        with engine.connect() as conn:
            version = conn.execute(
                text("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
            ).scalar()
        engine.dispose()
    except Exception as exc:  # pragma: no cover — environmental
        pytest.skip(f"DB unreachable: {exc}")
    if version is None:
        pytest.skip("pgvector no está instalado")
    return url


@pytest.fixture
def catalog():
    url = _url_or_skip()
    engine = create_engine(url)
    portal = f"canary_{uuid.uuid4().hex[:8]}"
    with engine.begin() as conn:
        dims_type = conn.execute(
            text(
                "SELECT format_type(atttypid, atttypmod) FROM pg_attribute"
                " WHERE attrelid = 'dataset_chunks'::regclass AND attname = 'embedding'"
            )
        ).scalar()
        dims = int(re.search(r"\d+", dims_type).group())
        ids = []
        for i in range(_N):
            v = [0.0] * dims
            v[0], v[1], v[2] = 1.0, 0.3 + i * 0.01, 0.05 * (i % 3)
            ds = conn.execute(
                text(
                    "INSERT INTO datasets (source_id, title, portal)"
                    " VALUES (:s, :t, :p) RETURNING CAST(id AS text)"
                ),
                {"s": f"{portal}-{i}", "t": f"Canario {i}", "p": portal},
            ).scalar()
            conn.execute(
                text(
                    "INSERT INTO dataset_chunks (dataset_id, content, embedding)"
                    " VALUES (CAST(:d AS uuid), 'x', CAST(:e AS vector))"
                ),
                {"d": ds, "e": "[" + ",".join(str(x) for x in v) + "]"},
            )
            ids.append(ds)
    try:
        yield {"url": url, "dims": dims}
    finally:
        with engine.begin() as conn:
            conn.execute(
                text("DELETE FROM dataset_chunks WHERE CAST(dataset_id AS text) = ANY(:ids)"),
                {"ids": ids},
            )
            conn.execute(
                text("DELETE FROM datasets WHERE CAST(id AS text) = ANY(:ids)"), {"ids": ids}
            )
        engine.dispose()


def test_measure_compares_index_and_exact_with_the_worker_engine(catalog, monkeypatch) -> None:
    import asyncio

    monkeypatch.setenv("DATABASE_URL", catalog["url"])
    query = [0.0] * catalog["dims"]
    query[0] = 1.0

    async def _embed(self, text_: str) -> list[float]:
        return query

    with patch(
        "app.infrastructure.adapters.llm.bedrock_embedding_adapter.BedrockEmbeddingAdapter.embed",
        _embed,
    ):
        report = asyncio.run(search_canary_tasks._measure(("a", "b")))

    assert [r.query for r in report.results] == ["a", "b"]
    assert all(r.recall == 1.0 for r in report.results)
    assert all(r.hnsw_top == pytest.approx(r.exact_top) for r in report.results)
    assert not report.degraded
