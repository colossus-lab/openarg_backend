"""Hybrid discovery over `catalog_resources` (WS3).

Why not just join `table_catalog`?
  - `table_catalog` only knows about *materialized* tables. The whole point
    of WS2 is to expose a logical catalog that includes:
      - resources without a physical table yet (planner can request lazy mat)
      - connector endpoints (BCRA / Series Tiempo / argentina_datos / etc.)
      - non-tabular bundles (ACUMAR PDFs)
  - `cache_*` keeps serving the queries that already work; discovery just
    layers on top.

Activation:
  - Off by default (feature-flag `OPENARG_HYBRID_DISCOVERY=1`).
  - When off, callers should not touch this module — `dataset_index` and the
    sandbox keep behaving exactly as before.

Two access shapes:
  - `find_by_text(query)` — keyword + ILIKE search over canonical_title /
    raw_title / display_name. Cheap, no embeddings.
  - `find_by_embedding(vector, k)` — semantic search via the HNSW index on
    `catalog_resources.embedding`. Caller supplies the embedding (we don't
    embed inside this module to keep the dependency surface small).
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterable
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


_TRUTHY = {"1", "true", "yes", "on"}


def discovery_enabled() -> bool:
    """Feature flag — set OPENARG_HYBRID_DISCOVERY=1 to opt in.

    Either flag implies "the planner should see catalog_resources":
      * `OPENARG_HYBRID_DISCOVERY=1` — append catalog_resources hits to the
        existing table_catalog hits (transition mode).
      * `OPENARG_CATALOG_ONLY=1`     — replace table_catalog entirely (final
        cutover; staging uses this).
    """
    raw = os.getenv("OPENARG_HYBRID_DISCOVERY", "").strip().lower()
    if raw in _TRUTHY:
        return True
    return catalog_only_mode()


def catalog_only_mode() -> bool:
    """Cutover mode — `catalog_resources` is the *only* discovery source.

    When enabled:
      - the table_catalog SQL path is bypassed in the planner
      - the sandbox resolves table names via `catalog_resources`
      - existing `cache_*` tables can still be queried but are not surfaced
        unless they're attached to a catalog_resources row

    Use only when the catalog has been backfilled (call
    `openarg.catalog_backfill` first) and any legacy state has been wiped
    (see `scripts/staging_reset.py`).
    """
    raw = os.getenv("OPENARG_CATALOG_ONLY", "").strip().lower()
    return raw in _TRUTHY


@dataclass(frozen=True)
class DiscoveredResource:
    resource_identity: str
    portal: str
    source_id: str
    canonical_title: str
    display_name: str
    materialization_status: str  # 'pending'|'ready'|'live_api'|'non_tabular'|'materialization_corrupted'|'failed'
    materialized_table_name: str | None
    resource_kind: str
    score: float = 0.0


_KEYWORD_SQL = text(
    """
    SELECT resource_identity, portal, source_id, canonical_title, display_name,
           materialization_status, materialized_table_name, resource_kind
    FROM catalog_resources
    WHERE materialization_status NOT IN ('materialization_corrupted', 'failed')
      AND (
        canonical_title ILIKE :pat
        OR display_name ILIKE :pat
        OR raw_title ILIKE :pat
      )
    ORDER BY title_confidence DESC, materialization_status, updated_at DESC
    LIMIT :k
    """
)

_EMBEDDING_SQL = text(
    """
    SELECT resource_identity, portal, source_id, canonical_title, display_name,
           materialization_status, materialized_table_name, resource_kind,
           1 - (embedding <=> CAST(:vec AS vector)) AS similarity
    FROM catalog_resources
    WHERE embedding IS NOT NULL
      AND materialization_status NOT IN ('materialization_corrupted', 'failed')
    ORDER BY embedding <=> CAST(:vec AS vector)
    LIMIT :k
    """
)


def _vector_literal(vec: Iterable[float]) -> str:
    return "[" + ",".join(f"{float(v):.6f}" for v in vec) + "]"


class CatalogDiscovery:
    """Read-only access to `catalog_resources` for WS3 hybrid serving."""

    def __init__(self, engine: Engine | None = None) -> None:
        self._engine = engine

    def _eng(self) -> Engine:
        if self._engine is not None:
            return self._engine
        # Late import to avoid pulling celery DB into web-only contexts.
        from app.infrastructure.celery.tasks._db import get_sync_engine

        return get_sync_engine()

    def find_by_text(self, query: str, *, k: int = 10) -> list[DiscoveredResource]:
        if not query.strip():
            return []
        eng = self._eng()
        try:
            with eng.connect() as conn:
                rows = conn.execute(
                    _KEYWORD_SQL, {"pat": f"%{query.strip()}%", "k": k}
                ).fetchall()
        except Exception:
            logger.exception("catalog_discovery.find_by_text failed")
            return []
        return [self._row_to_resource(r) for r in rows]

    def find_by_embedding(self, vec: Iterable[float], *, k: int = 10) -> list[DiscoveredResource]:
        eng = self._eng()
        try:
            with eng.connect() as conn:
                rows = conn.execute(
                    _EMBEDDING_SQL, {"vec": _vector_literal(vec), "k": k}
                ).fetchall()
        except Exception:
            logger.exception("catalog_discovery.find_by_embedding failed")
            return []
        results: list[DiscoveredResource] = []
        for r in rows:
            res = self._row_to_resource(r)
            sim = float(getattr(r, "similarity", 0.0) or 0.0)
            results.append(
                DiscoveredResource(
                    resource_identity=res.resource_identity,
                    portal=res.portal,
                    source_id=res.source_id,
                    canonical_title=res.canonical_title,
                    display_name=res.display_name,
                    materialization_status=res.materialization_status,
                    materialized_table_name=res.materialized_table_name,
                    resource_kind=res.resource_kind,
                    score=sim,
                )
            )
        return results

    def resolve_table(self, resource_identity: str) -> str | None:
        """Return the materialized table name, if any.

        Used by the sandbox: when a query targets a logical resource that
        has a backing table, we can route the SQL straight to it.
        """
        eng = self._eng()
        try:
            with eng.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT materialized_table_name "
                        "FROM catalog_resources WHERE resource_identity = :rid "
                        "  AND materialization_status = 'ready'"
                    ),
                    {"rid": resource_identity},
                ).fetchone()
            return row.materialized_table_name if row else None
        except Exception:
            logger.exception("catalog_discovery.resolve_table failed")
            return None

    @staticmethod
    def _row_to_resource(r) -> DiscoveredResource:
        return DiscoveredResource(
            resource_identity=r.resource_identity,
            portal=r.portal,
            source_id=r.source_id,
            canonical_title=r.canonical_title or "",
            display_name=r.display_name or "",
            materialization_status=r.materialization_status,
            materialized_table_name=r.materialized_table_name,
            resource_kind=r.resource_kind,
        )


_singleton: CatalogDiscovery | None = None


def catalog_discovery() -> CatalogDiscovery:
    """Process-wide singleton."""
    global _singleton  # noqa: PLW0603
    if _singleton is None:
        _singleton = CatalogDiscovery()
    return _singleton
