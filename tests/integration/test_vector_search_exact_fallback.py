"""Cuando el índice HNSW no alcanza a los vecinos, la búsqueda exacta los trae.

El 04-oct, con ``hnsw.ef_search=200``, el índice de prod devolvía para "salario
mínimo vital y móvil" cinco municipios de Córdoba (0,517) mientras la búsqueda
exacta ponía el SMVM primero (0,746). El recorrido no vuelve vacío: vuelve
lleno de vecinos equivocados, o con menos de los que se le pidieron. Eso no se
reproduce con un doble en memoria, así que se arma contra pgvector de verdad un
grafo que el recorrido no puede cubrir: 300 puntos, cada uno sobre su propio
eje ortogonal y todos a la misma distancia entre sí. La poda de vecinos de HNSW
deja el grafo partido y sólo 33 de los 300 son alcanzables, con cualquier
``ef_search`` y con recorrido iterativo (medido con pgvector 0.8 sobre pg16).
"""

from __future__ import annotations

import math
import os
import re
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.infrastructure.adapters.search.pgvector_search_adapter import PgVectorSearchAdapter

_N = 300


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


def _weight(i: int) -> float:
    # Similitud con la consulta = 1/sqrt(1 + w²): de 0,707 (i=0) a ~0,59 (i=299).
    return 1.0 + i * 0.002


@pytest.fixture(scope="module")
def orthogonal():
    url = _url_or_skip()
    engine = create_engine(url)
    portal = f"orto_{uuid.uuid4().hex[:8]}"
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
            v[0] = 1.0
            v[4 + i] = _weight(i)
            ds_id = conn.execute(
                text(
                    "INSERT INTO datasets (source_id, title, portal)"
                    " VALUES (:s, :t, :p) RETURNING CAST(id AS text)"
                ),
                {"s": f"{portal}-{i}", "t": f"Ortogonal {i}", "p": portal},
            ).scalar()
            conn.execute(
                text(
                    "INSERT INTO dataset_chunks (dataset_id, content, embedding)"
                    " VALUES (CAST(:d AS uuid), 'x', CAST(:e AS vector))"
                ),
                {"d": ds_id, "e": "[" + ",".join(str(x) for x in v) + "]"},
            )
            ids.append(ds_id)
    try:
        yield {"url": url, "dims": dims, "ids": ids, "portal": portal}
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


@pytest.fixture
async def session(orthogonal):
    engine = create_async_engine(orthogonal["url"])
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as s:
        # Con 300 filas el planificador recorrería la tabla; en prod (112k
        # chunks) elige el índice solo.
        await s.execute(text("SELECT set_config('enable_seqscan', 'off', true)"))
        yield s
        await s.rollback()
    await engine.dispose()


def _query(dims: int) -> list[float]:
    q = [0.0] * dims
    q[0] = 1.0
    return q


async def test_the_index_alone_cannot_reach_them(orthogonal, session) -> None:
    """Control: si el índice llegara a los 60, el test siguiente no probaría el
    camino de la exacta (pasaría también sin ella)."""
    hits = await PgVectorSearchAdapter(session).search_datasets_hnsw(
        _query(orthogonal["dims"]), limit=60
    )
    if len(hits) >= 60:  # pragma: no cover — depende de cómo quedó armado el grafo
        pytest.skip("el grafo quedó conectado: el escenario no se armó en esta base")
    assert {h.dataset_id for h in hits} <= set(orthogonal["ids"])


async def test_the_exact_search_fills_what_the_index_could_not(orthogonal, session) -> None:
    results = await PgVectorSearchAdapter(session).search_datasets_ann(
        _query(orthogonal["dims"]), limit=60
    )

    # Los 60 más cercanos de verdad, en orden.
    assert [r.dataset_id for r in results] == orthogonal["ids"][:60]
    assert results[0].score == pytest.approx(1 / math.sqrt(1 + _weight(0) ** 2), abs=1e-4)


async def test_exact_search_matches_brute_force(orthogonal, session) -> None:
    results = await PgVectorSearchAdapter(session).search_datasets_exact(
        _query(orthogonal["dims"]), limit=_N, portal_filter=orthogonal["portal"]
    )

    assert [r.dataset_id for r in results] == orthogonal["ids"]
    for i, r in enumerate(results):
        assert r.score == pytest.approx(1 / math.sqrt(1 + _weight(i) ** 2), abs=1e-4)
