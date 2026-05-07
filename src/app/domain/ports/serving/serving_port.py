"""Serving Port — the single interface the LangGraph pipeline uses to reach data.

Stability contract:
- The signatures below MUST stay backward-compatible. The pipeline depends on
  them.
- New methods may be added; existing methods may not change shape.
- DTOs (`Resource`, `Schema`, `Rows`, `CatalogEntry`) live in
  `app.domain.entities.serving` and follow the same stability contract.

Migration contract:
- The adapter behind this port is allowed to change which physical layer it
  reads from (cache_* legacy → staging_* → mart_*). The pipeline does not see
  the change; only the `layer` field of the DTOs reports it for metrics.
"""

from __future__ import annotations

from abc import ABC, abstractmethod

from app.domain.entities.serving import CatalogEntry, Resource, Rows, Schema


class IServingPort(ABC):
    """The pipeline's only entry point to the data layer."""

    @abstractmethod
    async def discover(
        self,
        query_text: str,
        *,
        limit: int = 10,
        portal: str | None = None,
        domain: str | None = None,
    ) -> list[Resource]:
        """Find resources relevant to a free-text query.

        Implementations should combine vector search over `catalog_resources`
        with optional lexical fallback. `portal` and `domain` are filters; if
        omitted, search is unrestricted.

        Returns up to `limit` resources sorted by relevance.
        """

    @abstractmethod
    async def get_schema(self, resource_id: str) -> Schema:
        """Return the queryable shape of a resource.

        For mart-backed resources, semantics come from `COMMENT ON COLUMN`.
        For staging, from the contract metadata. For raw/cache_legacy, an
        empty `semantics` map is acceptable.

        Raises `ResourceNotFoundError` if `resource_id` is unknown.
        """

    @abstractmethod
    async def query(
        self,
        resource_id: str,
        sql: str,
        *,
        max_rows: int = 1000,
        timeout_seconds: int = 30,
    ) -> Rows:
        """Execute a read-only SQL statement against a resource.

        Implementations MUST enforce read-only semantics — no INSERT, UPDATE,
        DELETE, DDL. The SQL is expected to reference the resource by its
        canonical name (resolved from `resource_id` by the adapter), not by
        the underlying physical table name.

        Raises `ResourceNotFoundError`, `QueryTimeoutError`,
        `WriteAttemptedError` as appropriate.
        """

    @abstractmethod
    async def explain(self, resource_id: str) -> CatalogEntry:
        """Return rich catalog metadata for the planner.

        Combines schema + sample queries + tags + freshness. The planner uses
        this to decide whether a resource is worth including in its plan.
        """


class ResourceNotFoundError(Exception):
    """Raised when a resource_id does not resolve to anything in the catalog."""


class WriteAttemptedError(Exception):
    """Raised when a query contained a write statement (INSERT, UPDATE, etc.).

    Read-only enforcement is the adapter's responsibility.
    """


class QueryTimeoutError(Exception):
    """Raised when a query exceeded its timeout budget."""


class QueryResourceMismatchError(Exception):
    """Raised when a query does not reference the resource bound by resource_id."""
