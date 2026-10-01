"""La búsqueda por el índice HNSW devuelve lo que pide, contra pgvector de verdad.

`search_datasets` filtraba por similitud en el WHERE: Postgres no puede usar
el índice HNSW para eso y recorría los 76k chunks (en staging, de 1 a más de
60 s). `search_datasets_ann` ordena por distancia con LIMIT. Dos ajustes de
pgvector deciden si el índice devuelve lo que la consulta pide, y ninguno se
ve con un doble en memoria:

- `hnsw.ef_search` (40 por defecto) es el techo de filas de un recorrido del
  índice: sin subirlo, `LIMIT 200` trae 40 chunks.
- Con filtro de portal, `hnsw.iterative_scan` sigue recorriendo hasta juntar
  chunks de ese portal. Filtrar después de traer N vecinos dejaba sin
  resultados a los portales chicos.

Para que el planificador use el índice con tablas chicas se apaga el
recorrido secuencial, como pasa solo con los 76k chunks de staging.
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

_NEAR = 300  # chunks del portal grande, todos cerca de la consulta
_FAR = 3  # chunks del portal chico, más lejos que cualquiera del grande


def _url_or_skip() -> str:
    url = os.getenv("DATABASE_URL", "")
    if not url:
        pytest.skip("DATABASE_URL not set — este test necesita una DB real")
    try:
        engine = create_engine(url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).scalar()
            version = conn.execute(
                text("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
            ).scalar()
        engine.dispose()
    except Exception as exc:  # pragma: no cover — environmental
        pytest.skip(f"DB unreachable: {exc}")
    if version is None:
        pytest.skip("pgvector no está instalado")
    return url


def _vec(dims: int, i: int, weight: float) -> str:
    """The query (unit axis 0) plus ``weight`` in a direction of the e1-e2 plane.

    Cosine similarity to the query is 1/sqrt(1 + weight²) whatever the
    direction. The points live in a low-dimensional subspace on purpose: put
    each one on its own orthogonal axis instead and they are all equidistant,
    HNSW's neighbour pruning leaves the graph disconnected, and 33 of 300 rows
    are reachable at any ef_search — a property of that geometry, not of
    pgvector or of real embeddings.
    """
    v = [0.0] * dims
    theta = i * 2.399963  # golden angle: no two points share a direction
    v[0] = 1.0
    v[1] = weight * math.cos(theta)
    v[2] = weight * math.sin(theta)
    v[3] = 0.05 * math.sin(i * 0.7)
    return "[" + ",".join(str(x) for x in v) + "]"


@pytest.fixture(scope="module")
def catalog():
    url = _url_or_skip()
    engine = create_engine(url)
    tag = uuid.uuid4().hex[:8]
    big, small, other = f"ann_big_{tag}", f"ann_small_{tag}", f"ann_other_{tag}"
    with engine.begin() as conn:
        dims_type = conn.execute(
            text(
                "SELECT format_type(atttypid, atttypmod) FROM pg_attribute"
                " WHERE attrelid = 'dataset_chunks'::regclass AND attname = 'embedding'"
            )
        ).scalar()
        dims = int(re.search(r"\d+", dims_type).group())

        def add(portal: str, title: str, specs: list[tuple[int, float]]) -> list[str]:
            """One dataset per (point, weight), with one chunk each."""
            ids = []
            for i, (point, weight) in enumerate(specs):
                ds_id = conn.execute(
                    text(
                        "INSERT INTO datasets (source_id, title, portal)"
                        " VALUES (:s, :t, :p) RETURNING CAST(id AS text)"
                    ),
                    {"s": f"{portal}-{i}", "t": f"{title} {i}", "p": portal},
                ).scalar()
                conn.execute(
                    text(
                        "INSERT INTO dataset_chunks (dataset_id, content, embedding)"
                        " VALUES (CAST(:d AS uuid), :c, CAST(:e AS vector))"
                    ),
                    {"d": ds_id, "c": title, "e": _vec(dims, point, weight)},
                )
                ids.append(ds_id)
            return ids

        # Cosine ~0.93 down to ~0.92: the lower i, the closer.
        big_ids = add(big, "Cercano", [(i, 0.40 + i * 0.001) for i in range(_NEAR)])
        # Cosine ~0.61: farther than every chunk of the big portal.
        small_ids = add(small, "Lejano", [(_NEAR + i, 1.30) for i in range(_FAR)])
        # Cosine ~0.20: under the 0.40 threshold.
        other_ids = add(other, "Ajeno", [(_NEAR + _FAR, 5.0)])
        # A second, closer chunk for the first small dataset: grouped, it
        # scores by its best chunk.
        conn.execute(
            text(
                "INSERT INTO dataset_chunks (dataset_id, content, embedding)"
                " VALUES (CAST(:d AS uuid), 'Lejano bis', CAST(:e AS vector))"
            ),
            {"d": small_ids[0], "e": _vec(dims, _NEAR + _FAR + 1, 1.0)},  # cosine ~0.71
        )
    try:
        yield {
            "url": url,
            "dims": dims,
            "big": big,
            "small": small,
            "big_ids": big_ids,
            "small_ids": small_ids,
            "other_ids": other_ids,
        }
    finally:
        with engine.begin() as conn:
            ids = big_ids + small_ids + other_ids
            conn.execute(
                text("DELETE FROM dataset_chunks WHERE CAST(dataset_id AS text) = ANY(:ids)"),
                {"ids": ids},
            )
            conn.execute(
                text("DELETE FROM datasets WHERE CAST(id AS text) = ANY(:ids)"), {"ids": ids}
            )
        engine.dispose()


@pytest.fixture
async def session(catalog):
    engine = create_async_engine(catalog["url"])
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as s:
        # 300 rows is a table the planner would scan; on staging's 76k chunks
        # it picks the index on its own.
        await s.execute(text("SELECT set_config('enable_seqscan', 'off', true)"))
        yield s
        await s.rollback()
    await engine.dispose()


def _query(dims: int) -> list[float]:
    q = [0.0] * dims
    q[0] = 1.0
    return q


async def test_query_plan_uses_the_hnsw_index(catalog, session) -> None:
    """The shape the adapter sends is one the index answers."""
    plan = "\n".join(
        r[0]
        for r in await session.execute(
            text(
                "EXPLAIN SELECT dataset_id FROM dataset_chunks"
                " ORDER BY embedding <=> CAST(:e AS vector) LIMIT 200"
            ),
            {"e": "[" + ",".join(str(x) for x in _query(catalog["dims"])) + "]"},
        )
    )
    assert "ix_dataset_chunks_embedding" in plan


async def test_returns_more_than_the_default_ef_search(catalog, session) -> None:
    """At ef_search=40 this would cap at 40 datasets."""
    # Control: the same index scan at the default ef_search stops at 40, so
    # what follows only passes because the adapter raises it.
    q = "[" + ",".join(str(x) for x in _query(catalog["dims"])) + "]"
    capped = (
        await session.execute(
            text(
                "SELECT count(*) FROM (SELECT 1 FROM dataset_chunks"
                " ORDER BY embedding <=> CAST(:e AS vector) LIMIT 600) s"
            ),
            {"e": q},
        )
    ).scalar()
    assert capped <= 40

    results = await PgVectorSearchAdapter(session).search_datasets_ann(
        _query(catalog["dims"]), limit=60, min_similarity=0.40
    )

    assert len(results) == 60
    assert {r.dataset_id for r in results} <= set(catalog["big_ids"])
    scores = [r.score for r in results]
    assert scores == sorted(scores, reverse=True)
    assert len({r.dataset_id for r in results}) == len(results)


async def test_small_portal_is_found_behind_closer_chunks_of_other_portals(
    catalog, session
) -> None:
    """300 closer chunks belong to another portal: filtering after fetching
    200 neighbours would return nothing."""
    results = await PgVectorSearchAdapter(session).search_datasets_ann(
        _query(catalog["dims"]), limit=10, portal_filter=catalog["small"], min_similarity=0.40
    )

    assert [r.dataset_id for r in results][:1] == [catalog["small_ids"][0]]
    assert {r.dataset_id for r in results} == set(catalog["small_ids"])
    assert all(r.portal == catalog["small"] for r in results)
    # Scored by its best chunk (~0.71), not its worst (~0.61).
    assert results[0].score == pytest.approx(1 / (1 + 1.0**2) ** 0.5, abs=0.01)


async def test_threshold_drops_unrelated_datasets(catalog, session) -> None:
    results = await PgVectorSearchAdapter(session).search_datasets_ann(
        _query(catalog["dims"]), limit=500, min_similarity=0.40
    )

    found = {r.dataset_id for r in results}
    assert not found & set(catalog["other_ids"])
    assert all(r.score >= 0.40 for r in results)
