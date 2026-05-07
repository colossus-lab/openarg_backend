"""Legacy Serving Port adapter — Phase 0.

Reads the current infrastructure (`catalog_resources` for discovery,
`table_catalog` for hints, `cache_*` for query execution). Behaves like the
existing direct-access pattern, just behind the stable `IServingPort`
interface so the pipeline can migrate without coordination.

Replaced incrementally:
- Phase 2: `discover()` and `get_schema()` learn to prefer `staging.*`.
- Phase 3: same methods learn to prefer `mart.*`.
- Phase 4: `cache_*` access removed entirely.

Until then this adapter is an honest representation of "what the pipeline does
today, just typed".
"""

from __future__ import annotations

import logging
import re
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.ext.asyncio import AsyncEngine

from app.domain.entities.serving import (
    CatalogEntry,
    Resource,
    Rows,
    Schema,
    ServingLayer,
)
from app.domain.ports.serving.serving_port import (
    IServingPort,
    QueryResourceMismatchError,
    QueryTimeoutError,
    ResourceNotFoundError,
    WriteAttemptedError,
)

logger = logging.getLogger(__name__)

_WRITE_KEYWORDS = re.compile(
    r"\b(insert|update|delete|drop|truncate|alter|create|grant|revoke|copy)\b",
    re.IGNORECASE,
)
_RELATION_REF_RE = re.compile(
    r'\b(?:from|join)\s+(?:"?(\w+)"?\.)?"?(\w+)"?',
    re.IGNORECASE,
)


def _parse_qualified_name(value: str) -> tuple[str, str]:
    """Split `materialized_table_name` into `(schema, bare_name)`.

    Accepts four shapes that appear in `catalog_resources`:
      - `staging."<bare>"`  — Phase 2 staging layer (preferred when present).
      - `raw."<bare>"`      — Phase 1.5 raw layer.
      - `<schema>.<bare>`   — generic qualified form.
      - `<bare>`            — legacy unqualified, defaults to schema `public`.
    """
    if not value:
        return "public", value
    s = value.strip()
    if "." in s:
        schema, _, rest = s.partition(".")
        return schema.strip('"'), rest.strip('"')
    return "public", s.strip('"')


def _layer_for_schema(schema_name: str) -> ServingLayer:
    """Map a Postgres schema to the Serving Port layer enum."""
    return {
        "staging": ServingLayer.STAGING,
        "raw": ServingLayer.RAW,
        "mart": ServingLayer.MART,
        "public": ServingLayer.CACHE_LEGACY,
    }.get(schema_name, ServingLayer.CACHE_LEGACY)


def _sql_references_relation(sql: str, *, schema_name: str, bare_name: str) -> bool:
    """Return True when the SQL references the expected relation.

    Legacy `public` resources may appear unqualified (`FROM cache_x`) or
    qualified (`FROM public.cache_x`). Medallion layers must be schema-
    qualified (`raw.foo`, `mart.bar`, `staging.baz`) to count as a match.
    """
    expected_schema = schema_name.lower()
    expected_table = bare_name.lower()
    for match in _RELATION_REF_RE.finditer(sql):
        found_schema = (match.group(1) or "").strip('"').lower()
        found_table = match.group(2).strip('"').lower()
        if found_table != expected_table:
            continue
        if expected_schema == "public":
            if found_schema in ("", "public"):
                return True
        elif found_schema == expected_schema:
            return True
    return False


def _discover_marts_enabled() -> bool:
    """Default ON — marts are the preferred surface once they exist.
    Operators can disable with OPENARG_DISCOVER_MARTS=0 for debugging.
    """
    import os

    return os.getenv("OPENARG_DISCOVER_MARTS", "1").strip().lower() in (
        "1",
        "true",
        "yes",
    )


class LegacyServingAdapter(IServingPort):
    """Phase 0 adapter — reads from current `cache_*` + `catalog_resources`."""

    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def _expected_relation_for_resource(
        self, resource_id: str
    ) -> tuple[str, str]:
        if resource_id.startswith("mart::"):
            mart_id = resource_id[len("mart::") :]
            async with self._engine.connect() as conn:
                row = (
                    await conn.execute(
                        text(
                            "SELECT mart_schema, mart_view_name "
                            "FROM mart_definitions WHERE mart_id = :id"
                        ),
                        {"id": mart_id},
                    )
                ).fetchone()
            if row is None:
                raise ResourceNotFoundError(resource_id)
            return str(row.mart_schema), str(row.mart_view_name)

        if resource_id.startswith("raw::"):
            return "raw", resource_id[len("raw::") :]

        async with self._engine.connect() as conn:
            row = (
                await conn.execute(
                    text(
                        "SELECT materialized_table_name "
                        "FROM catalog_resources WHERE resource_identity = :rid"
                    ),
                    {"rid": resource_id},
                )
            ).fetchone()
        if row is None or not row.materialized_table_name:
            raise ResourceNotFoundError(resource_id)
        return _parse_qualified_name(str(row.materialized_table_name))

    async def discover(
        self,
        query_text: str,
        *,
        limit: int = 10,
        portal: str | None = None,
        domain: str | None = None,
    ) -> list[Resource]:
        # Phase 0: lexical-only discovery against catalog_resources.title /
        # raw_title. Vector path will be wired in Phase 2 when embeddings are
        # populated; until then a simple ILIKE keeps the adapter honest about
        # current capabilities rather than claiming functionality it does not
        # yet have.
        #
        # Phase 3: marts join the discovery sweep when
        # OPENARG_DISCOVER_MARTS=1 (default ON). They appear *first* in the
        # result list — the planner should prefer a curated mart over a raw
        # resource whenever both match.
        results: list[Resource] = []

        if _discover_marts_enabled():
            mart_results = await self._discover_marts(
                query_text, limit=limit, domain=domain, portal=portal
            )
            results.extend(mart_results)
            if len(results) >= limit:
                return results[:limit]

        params: dict[str, Any] = {"q": f"%{query_text}%", "lim": limit - len(results)}
        sql = (
            "SELECT resource_identity, COALESCE(canonical_title, raw_title) AS title, "
            "       domain, subdomain, portal, materialized_table_name "
            "FROM catalog_resources "
            "WHERE COALESCE(canonical_title, raw_title) ILIKE :q "
        )
        if portal:
            sql += "AND portal = :portal "
            params["portal"] = portal
        if domain:
            sql += "AND domain = :domain "
            params["domain"] = domain
        sql += "ORDER BY title LIMIT :lim"

        async with self._engine.connect() as conn:
            result = await conn.execute(text(sql), params)
            rows = result.fetchall()

        for row in rows:
            schema_name, _ = _parse_qualified_name(row.materialized_table_name or "")
            results.append(
                Resource(
                    resource_id=str(row.resource_identity),
                    title=str(row.title or ""),
                    domain=row.domain,
                    subdomain=row.subdomain,
                    portal=row.portal,
                    layer=_layer_for_schema(schema_name),
                )
            )
        return results

    async def _discover_marts(
        self,
        query_text: str,
        *,
        limit: int,
        domain: str | None,
        portal: str | None = None,
    ) -> list[Resource]:
        """Return marts whose `mart_id`, description, or canonical column
        names contain ANY of the query words. Lexical only — vector
        wiring is deferred until `mart_definitions.embedding` is populated
        (DEBT-019-001).

        Word-level matching (vs substring of the whole query) is what
        makes a long natural-language question like
        "produccion de petroleo argentina" match a mart whose description
        contains "petroleo" but not the whole phrase.
        """
        # Tokenize the query; drop very short noise words. Each token >=3
        # chars becomes its own ILIKE clause OR-joined. 3 (not 4) so
        # short domain words like "gas", "ipc", "pbi" still match.
        # Common stopwords are filtered to keep the LIKE list small.
        _STOP = {"que", "los", "las", "del", "para", "como", "una", "uno",
                 "este", "esta", "esto", "con", "por", "sin", "sus", "fue"}
        words = [
            w.lower()
            for w in re.findall(r"[\wáéíóúñÁÉÍÓÚÑ]+", query_text)
            if len(w) >= 3 and w.lower() not in _STOP
        ]
        # Filter out empty marts: `last_row_count > 0` ensures the planner
        # never sees a mart that would return zero rows on query. Better
        # to surface raw / cache_legacy when no curated mart has data.
        params: dict[str, Any] = {"lim": limit}
        if not words:
            # No useful tokens → fall back to original substring match
            params["q"] = f"%{query_text}%"
            search_clause = (
                "(mart_id ILIKE :q OR description ILIKE :q "
                "OR canonical_columns_json::text ILIKE :q)"
            )
        else:
            # Build OR clause of N ILIKE checks, one per token.
            ors: list[str] = []
            for idx, w in enumerate(words):
                key = f"w{idx}"
                params[key] = f"%{w}%"
                ors.append(
                    f"mart_id ILIKE :{key} OR description ILIKE :{key} "
                    f"OR canonical_columns_json::text ILIKE :{key}"
                )
            search_clause = "(" + " OR ".join(ors) + ")"

        sql = (
            "SELECT mart_id, description, domain, mart_schema, mart_view_name "
            "FROM mart_definitions "
            f"WHERE COALESCE(last_row_count, 0) > 0 AND {search_clause} "
        )
        if domain:
            sql += "AND domain = :domain "
            params["domain"] = domain
        # Portal scoping: when the caller asks for a specific portal,
        # only marts that consume that portal in `source_portals` are
        # eligible. `:portal IS NULL` short-circuits when the caller
        # leaves portal unset (common for general LLM queries).
        if portal:
            sql += "AND :portal = ANY(source_portals) "
            params["portal"] = portal
        sql += "ORDER BY mart_id LIMIT :lim"

        try:
            async with self._engine.connect() as conn:
                rs = await conn.execute(text(sql), params)
                rows = rs.fetchall()
        except (ProgrammingError, OperationalError) as exc:
            # ProgrammingError covers `mart_definitions` not yet existing
            # (migration 0041 not applied) and column-name regressions.
            # OperationalError covers transient DB connection drops. We
            # degrade to "no marts" so the rest of the discovery flow can
            # still serve from `catalog_resources`. Anything else (bug in
            # this module's SQL building, type errors, etc.) MUST surface.
            logger.warning(
                "_discover_marts degraded to empty: %s", exc, exc_info=True
            )
            return []

        return [
            Resource(
                resource_id=f"mart::{row.mart_id}",
                title=str(row.mart_id or ""),
                domain=row.domain,
                subdomain=None,
                portal=None,
                layer=ServingLayer.MART,
            )
            for row in rows
        ]

    async def get_schema(self, resource_id: str) -> Schema:
        # Mart resources are addressed as `mart::<mart_id>` from
        # `_discover_marts`. Resolve to `mart.<view_name>` directly.
        if resource_id.startswith("mart::"):
            return await self._get_mart_schema(resource_id[len("mart::"):])
        # Raw resources coming from the NL2SQL serving path are currently
        # addressed as `raw::<bare_table_name>`. Resolve them directly from
        # `information_schema` so execution can stay on the serving path
        # without requiring a prior catalog identity lookup.
        if resource_id.startswith("raw::"):
            return await self._get_raw_schema(resource_id[len("raw::"):])

        async with self._engine.connect() as conn:
            cat = await conn.execute(
                text(
                    "SELECT materialized_table_name, parser_version "
                    "FROM catalog_resources WHERE resource_identity = :rid"
                ),
                {"rid": resource_id},
            )
            cat_row = cat.fetchone()
            if cat_row is None or not cat_row.materialized_table_name:
                raise ResourceNotFoundError(resource_id)

            schema_name, bare_name = _parse_qualified_name(cat_row.materialized_table_name)
            cols = await conn.execute(
                text(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_schema = :sch AND table_name = :tn "
                    "ORDER BY ordinal_position"
                ),
                {"sch": schema_name, "tn": bare_name},
            )
            col_rows = cols.fetchall()
            if not col_rows:
                raise ResourceNotFoundError(
                    f"{resource_id} (table {schema_name}.{bare_name} missing)"
                )

        return Schema(
            columns=[r.column_name for r in col_rows],
            column_types={r.column_name: r.data_type for r in col_rows},
            layer=_layer_for_schema(schema_name),
        )

    async def _get_mart_schema(self, mart_id: str) -> Schema:
        """Resolve a mart resource to its column schema via
        `mart_definitions` + `information_schema`. Pulls the per-column
        descriptions from `pg_description` so the planner sees the canonical
        semantics.
        """
        async with self._engine.connect() as conn:
            mdef = await conn.execute(
                text(
                    "SELECT mart_schema, mart_view_name, canonical_columns_json "
                    "FROM mart_definitions WHERE mart_id = :id"
                ),
                {"id": mart_id},
            )
            mart_row = mdef.fetchone()
            if mart_row is None:
                raise ResourceNotFoundError(f"mart::{mart_id}")

            cols = await conn.execute(
                text(
                    "SELECT c.column_name, c.data_type, "
                    "       pg_catalog.col_description(pgc.oid, c.ordinal_position) AS comment "
                    "FROM information_schema.columns c "
                    "JOIN pg_catalog.pg_class pgc "
                    "  ON pgc.relname = c.table_name "
                    "JOIN pg_catalog.pg_namespace pgn "
                    "  ON pgn.oid = pgc.relnamespace AND pgn.nspname = c.table_schema "
                    "WHERE c.table_schema = :sch AND c.table_name = :tn "
                    "ORDER BY c.ordinal_position"
                ),
                {"sch": mart_row.mart_schema, "tn": mart_row.mart_view_name},
            )
            col_rows = cols.fetchall()
            if not col_rows:
                raise ResourceNotFoundError(
                    f"mart::{mart_id} (view {mart_row.mart_schema}.{mart_row.mart_view_name} missing)"
                )

        semantics = {
            r.column_name: str(r.comment) for r in col_rows if getattr(r, "comment", None)
        }
        return Schema(
            columns=[r.column_name for r in col_rows],
            column_types={r.column_name: r.data_type for r in col_rows},
            semantics=semantics,
            layer=ServingLayer.MART,
        )

    async def _get_raw_schema(self, bare_name: str) -> Schema:
        """Resolve a raw-layer table addressed as `raw::<table_name>`."""
        async with self._engine.connect() as conn:
            cols = await conn.execute(
                text(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_schema = 'raw' AND table_name = :tn "
                    "ORDER BY ordinal_position"
                ),
                {"tn": bare_name},
            )
            col_rows = cols.fetchall()
            if not col_rows:
                raise ResourceNotFoundError(f"raw::{bare_name}")

        return Schema(
            columns=[r.column_name for r in col_rows],
            column_types={r.column_name: r.data_type for r in col_rows},
            layer=ServingLayer.RAW,
        )

    async def query(
        self,
        resource_id: str,
        sql: str,
        *,
        max_rows: int = 1000,
        timeout_seconds: int = 30,
    ) -> Rows:
        if _WRITE_KEYWORDS.search(sql):
            raise WriteAttemptedError(sql[:200])
        expected_schema, expected_table = await self._expected_relation_for_resource(
            resource_id
        )
        if not _sql_references_relation(
            sql, schema_name=expected_schema, bare_name=expected_table
        ):
            raise QueryResourceMismatchError(
                f"{resource_id} expects {expected_schema}.{expected_table}"
            )
        schema = await self.get_schema(resource_id)

        async with self._engine.connect() as conn:
            try:
                await conn.execute(
                    text(f"SET LOCAL statement_timeout = {int(timeout_seconds) * 1000}")
                )
                result = await conn.execute(text(sql))
                rows = result.fetchmany(max_rows + 1)
            except Exception as exc:
                if "statement timeout" in str(exc).lower():
                    raise QueryTimeoutError(sql[:200]) from exc
                raise

        truncated = len(rows) > max_rows
        rows = rows[:max_rows]
        columns = list(result.keys())
        data = [list(r) for r in rows]
        return Rows(
            columns=columns,
            data=data,
            truncated=truncated,
            layer=schema.layer,
        )

    async def explain(self, resource_id: str) -> CatalogEntry:
        if resource_id.startswith("mart::"):
            mart_id = resource_id[len("mart::") :]
            schema = await self.get_schema(resource_id)
            async with self._engine.connect() as conn:
                row = (
                    await conn.execute(
                        text(
                            "SELECT mart_id, description, domain, yaml_version, updated_at "
                            "FROM mart_definitions WHERE mart_id = :id"
                        ),
                        {"id": mart_id},
                    )
                ).fetchone()
            if row is None:
                raise ResourceNotFoundError(resource_id)
            return CatalogEntry(
                resource=Resource(
                    resource_id=f"mart::{row.mart_id}",
                    title=str(row.mart_id or ""),
                    domain=row.domain,
                    description=row.description,
                    layer=schema.layer,
                ),
                schema=schema,
                parser_version=str(row.yaml_version) if row.yaml_version else None,
                last_refreshed_at=str(row.updated_at) if row.updated_at else None,
            )

        schema = await self.get_schema(resource_id)
        async with self._engine.connect() as conn:
            row = (
                await conn.execute(
                    text(
                        "SELECT resource_identity, COALESCE(canonical_title, raw_title) AS title, "
                        "       domain, subdomain, portal, parser_version, updated_at "
                        "FROM catalog_resources WHERE resource_identity = :rid"
                    ),
                    {"rid": resource_id},
                )
            ).fetchone()

        if row is None:
            raise ResourceNotFoundError(resource_id)

        resource = Resource(
            resource_id=str(row.resource_identity),
            title=str(row.title or ""),
            domain=row.domain,
            subdomain=row.subdomain,
            portal=row.portal,
            layer=schema.layer,
        )
        return CatalogEntry(
            resource=resource,
            schema=schema,
            parser_version=row.parser_version,
            last_refreshed_at=str(row.updated_at) if row.updated_at else None,
        )
