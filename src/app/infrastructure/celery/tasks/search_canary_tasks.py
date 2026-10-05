"""Nightly recall canary for the catalog search (HNSW index vs exact search).

See `app.application.catalog.search_canary` for why it exists and what it
measures. This module does the I/O: ~20 query embeddings through Bedrock
(Cohere, `search_query`, under US$0.0001 a night), and for each one the
adapter's own index search and exact search, each in a READ ONLY transaction.
It alerts through the existing channel (`notify`, Telegram) and, being in
the beat schedule, leaves its heartbeat in `ingest_heartbeat` like every
other scheduled task.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any

from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)


def _async_url() -> str:
    """The worker's DATABASE_URL, on an async-capable driver.

    Workers get `postgresql+psycopg://` (psycopg 3 does async too); the API
    container gets `+asyncpg`. A bare `postgresql://` is pinned to psycopg.
    """
    url = os.getenv(
        "DATABASE_URL", "postgresql+psycopg://postgres:postgres@localhost:5432/openarg_db"
    )
    if url.startswith("postgresql://"):
        url = "postgresql+psycopg://" + url[len("postgresql://") :]
    return url


async def _measure(queries: tuple[str, ...]) -> Any:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

    from app.application.catalog.search_canary import (
        CanaryReport,
        K,
        QueryRecall,
        tie_aware_recall,
    )
    from app.infrastructure.adapters.llm.bedrock_embedding_adapter import BedrockEmbeddingAdapter
    from app.infrastructure.adapters.search.pgvector_search_adapter import PgVectorSearchAdapter

    embedder = BedrockEmbeddingAdapter(
        region=os.getenv("AWS_REGION", "us-east-1"),
        model=os.getenv("BEDROCK_EMBEDDING_MODEL", "cohere.embed-multilingual-v3"),
    )
    url = _async_url()
    # Through PgBouncer psycopg must not prepare statements on its own.
    connect_args = {"prepare_threshold": None} if "+psycopg" in url else {}
    engine = create_async_engine(url, pool_size=1, max_overflow=0, connect_args=connect_args)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def _read_only(search):
        async with factory() as session:
            await session.execute(text("SET TRANSACTION READ ONLY"))
            t0 = time.perf_counter()
            try:
                return await search(PgVectorSearchAdapter(session)), time.perf_counter() - t0
            finally:
                await session.rollback()

    results = []
    try:
        for q in queries:
            vector = await embedder.embed(q)
            hnsw, hnsw_s = await _read_only(lambda a, v=vector: a.search_datasets_hnsw(v, K))
            exact, exact_s = await _read_only(lambda a, v=vector: a.search_datasets_exact(v, K))
            results.append(
                QueryRecall(
                    query=q,
                    recall=tie_aware_recall(hnsw, exact),
                    hnsw_top=hnsw[0].score if hnsw else 0.0,
                    exact_top=exact[0].score if exact else 0.0,
                    hnsw_ms=hnsw_s * 1000,
                    exact_ms=exact_s * 1000,
                )
            )
    finally:
        await engine.dispose()
    return CanaryReport(results=tuple(results))


@celery_app.task(
    name="openarg.search_recall_canary",
    bind=True,
    soft_time_limit=900,
    time_limit=1200,
)
def search_recall_canary(self, limit: int | None = None) -> dict[str, Any]:
    """Compare the index's top 10 with the exact search's, and alert if it drifts."""
    from app.application.catalog.search_canary import CANARY_QUERIES, recall_band
    from app.application.quality.alerting import Alert, notify

    queries = CANARY_QUERIES[:limit] if limit else CANARY_QUERIES
    report = asyncio.run(_measure(queries))
    for r in report.results:
        logger.info(
            "search canary: %r recall@10=%.2f top hnsw=%.3f exacta=%.3f ms hnsw=%.0f exacta=%.0f",
            r.query,
            r.recall,
            r.hnsw_top,
            r.exact_top,
            r.hnsw_ms,
            r.exact_ms,
        )
    summary: dict[str, Any] = {
        "queries": len(report.results),
        "mean_recall": round(report.mean_recall, 3),
        "below_floor": [r.query for r in report.results if r.recall < report.floor],
        "degraded": report.degraded,
    }
    logger.info("search canary: %s", summary)
    if report.degraded:
        summary["alert"] = notify(
            get_sync_engine(),
            [
                Alert(
                    kind="search_recall",
                    # Identity of the problem, not of the night: the index,
                    # plus the 5-point band it fell to. A degradation that
                    # stays put is reported once and re-opened by `notify` on
                    # its 3rd/10th/30th sighting; one that gets worse lands in
                    # a new band and is news again (same idea as the Redis
                    # alert's `redis:{pct}pct`).
                    key=f"hnsw:{recall_band(report.mean_recall)}pct",
                    title=(
                        f"Búsqueda: el índice HNSW trae {report.mean_recall:.0%} del top 10 "
                        f"de la exacta (piso {report.floor:.0%})"
                    ),
                    detail=report.detail_es(),
                )
            ],
            heading="OpenArg · búsqueda",
        )
    return summary
