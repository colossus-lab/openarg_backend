"""Serving Port resolver — the single bridge between LangGraph nodes and
the data layers.

LangGraph nodes don't import Dishka or ports directly; they call helpers
from this module, which carries the `IServingPort` instance forward through
the runtime state. This keeps node code small and testable.

Usage from a LangGraph node:

    from app.application.pipeline.connectors.serving_resolver import (
        ServingResolver,
    )

    async def my_node(state: State) -> State:
        resolver: ServingResolver = state["serving_resolver"]
        hits = await resolver.discover(state["query"])
        if hits:
            schema = await resolver.get_schema(hits[0].resource_id)
            ...

`ServingResolver` is a thin wrapper that:
  - Caches `get_schema` results within a single request to avoid repeating
    `information_schema` queries inside one pipeline run.
  - Surfaces `ServingLayer` in the result so metrics can track mart-coverage
    over time (cheap signal that the medallion migration is paying off).
  - Honors `OPENARG_PIPELINE_USE_SERVING_PORT` — set to 0 to bypass the port
    in nodes that have a fallback path (rollback escape hatch during the
    Fase 4 cutover).
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass

from app.domain.entities.serving import (
    CatalogEntry,
    Resource,
    Rows,
    Schema,
    ServingLayer,
)
from app.domain.ports.serving.serving_port import (
    IServingPort,
    ResourceNotFoundError,
)

logger = logging.getLogger(__name__)


def serving_port_enabled() -> bool:
    """Default ON. Operators can flip to 0 to make resolver methods raise
    `RuntimeError`, forcing call sites to use their legacy fallback path
    while debugging.
    """
    return os.getenv("OPENARG_PIPELINE_USE_SERVING_PORT", "1").strip().lower() in (
        "1",
        "true",
        "yes",
    )


@dataclass
class _ResolverCache:
    schemas: dict[str, Schema]


class ServingResolver:
    """Per-request facade around `IServingPort`. Caches schema lookups so
    a single pipeline run that touches the same resource three times pays
    one round-trip instead of three.

    Errors propagate unchanged; callers decide whether to fall back to a
    legacy path.
    """

    def __init__(self, port: IServingPort) -> None:
        self._port = port
        self._cache = _ResolverCache(schemas={})

    async def discover(
        self,
        query_text: str,
        *,
        limit: int = 10,
        portal: str | None = None,
        domain: str | None = None,
    ) -> list[Resource]:
        if not serving_port_enabled():
            raise RuntimeError("OPENARG_PIPELINE_USE_SERVING_PORT=0; resolver bypassed")
        return await self._port.discover(
            query_text, limit=limit, portal=portal, domain=domain
        )

    async def get_schema(self, resource_id: str) -> Schema:
        if not serving_port_enabled():
            raise RuntimeError("OPENARG_PIPELINE_USE_SERVING_PORT=0; resolver bypassed")
        cached = self._cache.schemas.get(resource_id)
        if cached is not None:
            return cached
        schema = await self._port.get_schema(resource_id)
        self._cache.schemas[resource_id] = schema
        return schema

    async def query(
        self,
        resource_id: str,
        sql: str,
        *,
        max_rows: int = 1000,
        timeout_seconds: int = 30,
    ) -> Rows:
        if not serving_port_enabled():
            raise RuntimeError("OPENARG_PIPELINE_USE_SERVING_PORT=0; resolver bypassed")
        return await self._port.query(
            resource_id, sql, max_rows=max_rows, timeout_seconds=timeout_seconds
        )

    async def explain(self, resource_id: str) -> CatalogEntry:
        if not serving_port_enabled():
            raise RuntimeError("OPENARG_PIPELINE_USE_SERVING_PORT=0; resolver bypassed")
        return await self._port.explain(resource_id)

    async def discover_for_planner(
        self,
        query_text: str,
        *,
        limit: int = 10,
        domain: str | None = None,
    ) -> tuple[list[Resource], dict[str, int]]:
        """Discovery + tracking. Returns `(resources, layer_counts)` so the
        planner / metrics can report how many hits came from each layer.
        Useful for measuring mart coverage without scraping logs.
        """
        resources = await self.discover(query_text, limit=limit, domain=domain)
        layer_counts: dict[str, int] = {}
        for r in resources:
            key = r.layer.value if isinstance(r.layer, ServingLayer) else str(r.layer)
            layer_counts[key] = layer_counts.get(key, 0) + 1
        return resources, layer_counts


__all__ = ["ResourceNotFoundError", "ServingResolver", "serving_port_enabled"]
