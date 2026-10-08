"""La búsqueda por el índice HNSW devuelve lo que pide, contra pgvector de verdad.

`search_datasets` filtraba por similitud en el WHERE: Postgres no puede usar
el índice HNSW para eso y recorría los 76k chunks (en staging, de 1 a más de
60 s). `search_datasets_ann` ordena por distancia con LIMIT. Dos ajustes de
pgvector deciden si el índice devuelve lo que la consulta pide, y ninguno se
ve con un doble en memoria:

- `hnsw.ef_search` (40 por defecto) es el techo de filas de un recorrido del
  índice: sin subirlo, `LIMIT 200` trae 40 chunks.
- `hnsw.iterative_scan` sigue recorriendo cuando la primera pasada no
  alcanza: trae los candidatos que pasan de `ef_search` y, con filtro de
  portal, los chunks de ese portal. Filtrar después de traer N vecinos
  dejaba sin resultados a los portales chicos.

Y un tercero que decide si hay recorrido: con `ef_search=1000` el planificador
cambiaba el índice por un seq scan de toda la tabla en staging y en prod
(revisión del 05-oct, H091). El adaptador apaga el seq scan sólo para su
recorrido; acá se prueba con el plan de su propia consulta, sobre una tabla
chica que el planificador, solo, recorrería entera.
"""

from __future__ import annotations

import math
import os
import re
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError
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
        # 300 rows is a table the planner would scan. The adapter turns the
        # scan off for its own walk; this is for the queries the tests write.
        await s.execute(text("SELECT set_config('enable_seqscan', 'off', true)"))
        yield s
        await s.rollback()
    await engine.dispose()


@pytest.fixture
async def natural_session(catalog):
    """A session with the planner as it comes: on 300 rows it scans the table."""
    engine = create_async_engine(catalog["url"])
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with factory() as s:
        yield s
        await s.rollback()
    await engine.dispose()


class _ExplainTheWalk:
    """Session stand-in: EXPLAINs the index walk right before running it, in
    the same transaction and with the settings the adapter left for it."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self.walks: list[tuple[str, dict]] = []
        self.plans: list[str] = []

    async def execute(self, statement, params=None):
        sql = str(statement)
        if "LIMIT :candidates" in sql:
            self.walks.append((sql, dict(params or {})))
            self.plans.append(await _explain(self._session, sql, params))
        return await self._session.execute(statement, params)

    def begin_nested(self):
        return self._session.begin_nested()


async def _explain(session: AsyncSession, sql: str, params: dict | None) -> str:
    rows = await session.execute(text("EXPLAIN " + sql), params)
    return "\n".join(r[0] for r in rows)


async def _seqscan(session: AsyncSession) -> str:
    return (await session.execute(text("SELECT current_setting('enable_seqscan')"))).scalar()


def _query(dims: int) -> list[float]:
    q = [0.0] * dims
    q[0] = 1.0
    return q


def _weak_query(dims: int) -> list[float]:
    """The query tilted off the catalogue's subspace (axis 5 is unused).

    Every chunk's cosine is the axis-0 one times 0.54: the big portal's land
    at ~0.44-0.50, over the 0.40 threshold and under _ANN_WEAK_TOP_SCORE, and
    the small portal's under the threshold. A full answer with a weak top.
    """
    q = [0.0] * dims
    q[0] = 0.54
    q[5] = math.sqrt(1 - 0.54**2)
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


async def test_the_adapters_walk_uses_the_index_where_the_planner_would_scan(
    catalog, natural_session
) -> None:
    """The adapter's own statement, with its own ef_search and iterative scan,
    planned in its own transaction: the HNSW index, not a scan of every chunk.

    On 2026-10-05 the unit tests checked the parameters and this file checked
    the plan of another query at ef_search=40, while the real one, at 1000,
    was a parallel seq scan on staging and prod (H091)."""
    proxy = _ExplainTheWalk(natural_session)
    hits = await PgVectorSearchAdapter(proxy).search_datasets_hnsw(
        _query(catalog["dims"]), limit=60
    )

    [plan] = proxy.plans
    assert "ix_dataset_chunks_embedding" in plan, plan
    assert "Seq Scan on dataset_chunks" not in plan, plan
    assert len(hits) == 60
    assert proxy.walks[0][1]["candidates"] == PgVectorSearchAdapter._ANN_CANDIDATES


async def test_control_left_alone_the_planner_scans_this_table(catalog, natural_session) -> None:
    """Control for the test above: the same statement and settings, without
    the adapter turning the scan off, is a seq scan on these 305 chunks. If
    it were not, the test above would pass without the adapter's help."""
    proxy = _ExplainTheWalk(natural_session)
    await PgVectorSearchAdapter(proxy).search_datasets_hnsw(_query(catalog["dims"]), limit=60)
    [(sql, params)] = proxy.walks

    # ef_search and the iterative scan are still set (they last for the
    # transaction); enable_seqscan is back on.
    assert await _seqscan(natural_session) == "on"
    natural = await _explain(natural_session, sql, params)
    assert "Seq Scan on dataset_chunks" in natural, natural


async def test_with_a_portal_the_walk_does_not_scan_every_chunk_either(
    catalog, natural_session
) -> None:
    """With a portal the planner still chooses between the index and the btree
    on dataset_id; what it does not get is the scan of the whole table."""
    proxy = _ExplainTheWalk(natural_session)
    hits = await PgVectorSearchAdapter(proxy).search_datasets_hnsw(
        _query(catalog["dims"]), limit=10, portal_filter=catalog["small"]
    )

    [plan] = proxy.plans
    assert "Seq Scan on dataset_chunks" not in plan, plan
    assert {h.dataset_id for h in hits} == set(catalog["small_ids"])


async def test_the_walk_leaves_seqscan_as_it_found_it(catalog, session, natural_session) -> None:
    """Back to what it was, not to "on": the agent shares one transaction
    across a whole turn, and a caller that turned it off keeps it off."""
    await PgVectorSearchAdapter(session).search_datasets_hnsw(_query(catalog["dims"]), limit=10)
    assert await _seqscan(session) == "off"

    await PgVectorSearchAdapter(natural_session).search_datasets_hnsw(
        _query(catalog["dims"]), limit=10
    )
    assert await _seqscan(natural_session) == "on"


async def test_candidates_past_ef_search_come_from_the_iterative_scan(
    catalog, natural_session
) -> None:
    """ef_search is 300 and the walk asks for 400 chunks: past the frontier
    the iterative scan keeps walking. Without it one scan stops at 300 chunks,
    all of the big portal, and the small portal's 3 datasets never show up."""
    version = (
        await natural_session.execute(
            text("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
        )
    ).scalar()
    if tuple(int(p) for p in re.findall(r"\d+", str(version))) < (0, 8):
        pytest.skip(f"pgvector {version}: sin recorrido iterativo")
    wanted = set(catalog["big_ids"]) | set(catalog["small_ids"])
    # Precondition: more datasets than one frontier holds.
    assert len(wanted) > PgVectorSearchAdapter._ANN_EF_SEARCH

    hits = await PgVectorSearchAdapter(natural_session).search_datasets_hnsw(
        _query(catalog["dims"]), limit=400, min_similarity=0.40
    )

    assert wanted <= {h.dataset_id for h in hits}


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


async def _statement_timeout(session: AsyncSession) -> str:
    return (await session.execute(text("SELECT current_setting('statement_timeout')"))).scalar()


async def test_the_exact_search_leaves_statement_timeout_as_it_found_it(
    catalog, natural_session
) -> None:
    """Under the ceiling the exact search is served and its savepoint
    released, with statement_timeout back to what it was: a released
    savepoint keeps its SET LOCAL until the transaction ends, and the agent's
    turn goes on in this transaction."""
    before = await _statement_timeout(natural_session)

    # 303 datasets over the threshold, 500 asked for: "pocos", the exact
    # search runs (on 305 chunks, far under the ceiling).
    hits = await PgVectorSearchAdapter(natural_session).search_datasets_ann(
        _query(catalog["dims"]), limit=500, min_similarity=0.40
    )

    assert {h.dataset_id for h in hits} == set(catalog["big_ids"]) | set(catalog["small_ids"])
    assert await _statement_timeout(natural_session) == before


async def test_past_the_ceiling_the_walk_is_served_and_the_transaction_goes_on(
    catalog, natural_session, monkeypatch
) -> None:
    """What the unit tests' double imitates, on Postgres: statement_timeout
    fires inside the savepoint, the rollback to it leaves the transaction
    usable and takes the local timeout with it, and the walk's own settings
    (set before the savepoint) stay."""
    adapter = PgVectorSearchAdapter(natural_session)
    walk = await adapter.search_datasets_hnsw(
        _query(catalog["dims"]), limit=500, min_similarity=0.40
    )
    before = await _statement_timeout(natural_session)
    monkeypatch.setattr(PgVectorSearchAdapter, "_EXACT_FALLBACK_TIMEOUT_MS", 50)
    finished: list[bool] = []

    async def _slow_exact(*args: object, **kwargs: object) -> list:
        await natural_session.execute(text("SELECT pg_sleep(2)"))
        finished.append(True)
        return []

    monkeypatch.setattr(adapter, "search_datasets_exact", _slow_exact)

    hits = await adapter.search_datasets_ann(
        _query(catalog["dims"]), limit=500, min_similarity=0.40
    )

    assert finished == []  # cancelled at 50 ms, not slept through
    assert [h.dataset_id for h in hits] == [h.dataset_id for h in walk]
    assert await _statement_timeout(natural_session) == before
    ef = await natural_session.execute(text("SELECT current_setting('hnsw.ef_search')"))
    assert ef.scalar() == str(PgVectorSearchAdapter._ANN_EF_SEARCH)


async def test_control_without_a_savepoint_the_timeout_aborts_the_transaction(
    catalog, natural_session
) -> None:
    """Control for the test above: the same timeout outside a savepoint leaves
    the transaction accepting nothing but a rollback."""
    await natural_session.execute(text("SELECT set_config('statement_timeout', '50', true)"))
    with pytest.raises(DBAPIError, match="statement timeout"):
        await natural_session.execute(text("SELECT pg_sleep(2)"))
    with pytest.raises(DBAPIError, match="current transaction is aborted"):
        await natural_session.execute(text("SELECT 1"))


async def test_a_weak_top_walks_again_wider_on_the_index_before_the_exact_search(
    catalog, natural_session
) -> None:
    """Review of #183: a weak top walks again with _ANN_WIDE_CANDIDATES before
    the exact search. Both walks are planned on the index (no scan of every
    chunk), the planner's setting comes back after each, and with the top
    still weak the exact search (in its savepoint) answers."""
    proxy = _ExplainTheWalk(natural_session)
    hits = await PgVectorSearchAdapter(proxy).search_datasets_ann(
        _weak_query(catalog["dims"]), limit=10, min_similarity=0.40
    )

    assert [p["candidates"] for _, p in proxy.walks] == [
        PgVectorSearchAdapter._ANN_CANDIDATES,
        PgVectorSearchAdapter._ANN_WIDE_CANDIDATES,
    ]
    for plan in proxy.plans:
        assert "ix_dataset_chunks_embedding" in plan, plan
        assert "Seq Scan on dataset_chunks" not in plan, plan
    assert await _seqscan(natural_session) == "on"
    assert len(hits) == 10
    assert {h.dataset_id for h in hits} <= set(catalog["big_ids"])
    assert hits[0].score < PgVectorSearchAdapter._ANN_WEAK_TOP_SCORE


async def test_the_wide_walk_brings_what_the_first_one_did_and_more(
    catalog, natural_session, monkeypatch
) -> None:
    """``search_datasets_index`` serves the wide walk's answer instead of the
    first one's: on the same graph it starts where the first walk did and
    goes further, so rank by rank it scores at least as high. 305 chunks fit
    in 400 candidates, so the first walk is cut at 50 here to have a tail."""
    monkeypatch.setattr(PgVectorSearchAdapter, "_ANN_CANDIDATES", 50)
    adapter = PgVectorSearchAdapter(natural_session)
    query = _weak_query(catalog["dims"])
    narrow = await adapter.search_datasets_hnsw(query, limit=100, min_similarity=0.40)
    wide = await adapter.search_datasets_hnsw(
        query,
        limit=100,
        min_similarity=0.40,
        candidates=PgVectorSearchAdapter._ANN_WIDE_CANDIDATES,
    )

    assert 0 < len(narrow) <= 50 and len(wide) == 100
    assert all(w.score >= n.score - 1e-9 for w, n in zip(wide, narrow, strict=False))
