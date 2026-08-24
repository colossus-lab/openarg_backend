"""SQL macros for mart definitions.

Marts in `config/marts/*.yaml` may reference raw layer tables via macros
that this module resolves at build time. Supported macros:

  {{ live_table('portal::source_id') }}
      → `raw."<portal>__<slug>__<discrim>__v<N>"`
        where N is the live (max) version for that resource_identity.

  {{ live_table('portal::source_id', expected_columns=['c1','c2']) }}
      → same as above, but when the resource is MISSING returns
        `(SELECT NULL::text AS c1, NULL::text AS c2 WHERE FALSE)` so
        the consuming mart still references a known schema and builds
        with 0 rows. Without `expected_columns`, missing resources
        emit `SELECT NULL AS dummy WHERE FALSE` which crashes any
        outer SELECT that references specific column names.

  {{ live_tables_by_portal('bcra') }}
      → `(<UNION ALL of every live raw table whose resource_identity
        starts with "bcra::">)`
        Suitable as a subquery: `FROM {{ live_tables_by_portal('bcra') }} sub`.

  {{ live_tables_by_pattern('bcra::*tasa*') }}
      → `(<UNION ALL of live raw tables whose resource_identity
        matches the SQL LIKE pattern, with `%` substituted for `*`>)`

  {{ live_tables_by_table_pattern('caba__*establecimientos*',
                                   expected_columns=['cue','nombre']) }}
      → matches against table_name (slug-stable) instead of identity
        (often a volatile UUID). With `expected_columns` produces a
        schema-intersection projection: only columns common to ALL
        matched tables AND in the expected list are SELECTed.
        With 0 matches, falls back to `SELECT NULL::text AS c1, ...
        WHERE FALSE`.

  {{ live_tables_by_table_pattern('ddjj__*', expected_columns=[...],
                                   source_marker='__src') }}
      → each union branch also projects `'<schema>.<table>'::text AS __src`,
        so a mart can deduplicate by which physical table a row came from.
        The ingest columns cannot answer that: `_source_dataset_id` names
        the CKAN dataset, and one dataset routinely lands in several tables.

When a macro resolves to ZERO live tables, the result is a deterministic
empty-shape subquery (`SELECT WHERE FALSE`), so the mart still builds
with a known shape and stays empty until upstream lands.

The resolver is pure-Python (no Postgres function side effects). It
reads `public.raw_table_versions` once per `resolve_macros` call. When a
caller passes `expected_columns`, an additional cheap query inspects
`information_schema.columns` for the matched table_names so the
intersection can be computed.

Macro syntax: literal Python call expression inside `{{ ... }}`.
Args parsed via `ast.parse` + `ast.literal_eval` so only literals
are allowed. If we need more, dbt is the upgrade path.
"""

from __future__ import annotations

import ast
import logging
import os
import re
from collections.abc import Iterable
from dataclasses import dataclass

from sqlalchemy import text

logger = logging.getLogger(__name__)

# Match the entire `{{ ... }}` payload — content parsed afterward by `ast`.
_MACRO_RE = re.compile(r"\{\{\s*(?P<call>.+?)\s*\}\}", re.DOTALL)
_VALID_MACRO_NAMES = {
    "live_table",
    "live_tables_by_portal",
    "live_tables_by_pattern",
    "live_tables_by_table_pattern",
}

_RESOURCE_IDENTITY_RE = re.compile(r"^[A-Za-z0-9_.\-:* ]+$")

# Cap on the number of tables a `live_tables_by_*` macro can expand to.
# Without this, a permissive pattern like `*` would generate one
# `SELECT * FROM raw."<table>"` per live resource — potentially thousands.
# The resulting MATERIALIZED VIEW would compile but be operationally awful.
# 200 is the safety default; specific marts that legitimately need more
# (e.g. presupuesto_nacional_ejecutado spans 32 fact-shape tables out of
# 533 in the cluster — pattern matches the whole cluster, post-filter is
# fine) can override via `OPENARG_MART_MAX_UNION_TABLES`.
_MAX_UNION_TABLES = int(os.getenv("OPENARG_MART_MAX_UNION_TABLES", "200"))


class MacroResolutionError(ValueError):
    """Raised when a macro cannot be resolved (bad syntax, bad arg)."""


class MacroExpansionTooLarge(MacroResolutionError):
    """Raised when a `live_tables_by_*` macro would expand to more than
    `_MAX_UNION_TABLES` tables. Operators should narrow the pattern.
    """


@dataclass(frozen=True)
class _LiveRow:
    resource_identity: str
    schema_name: str
    table_name: str


@dataclass(frozen=True)
class _MacroCall:
    name: str
    arg: str
    kwargs: dict


def _query_lives(engine) -> list[_LiveRow]:
    """Read every (resource_identity, schema, table) where superseded_at IS NULL."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT resource_identity, schema_name, table_name "
                # Qualified, and this is load-bearing. Production carries
                # raw_table_versions in both schemas — `public` with the live
                # 27,855 rows and `raw` with a stale 166 — and the connection
                # goes through PGBouncer in transaction pooling, where a
                # session-level search_path does not stick. Measured
                # 2026-08-22: `live_table('senado::decretos_presidenciales')`
                # resolved to the empty placeholder 9 times out of 10, which is
                # why three marts had been failing on a column that exists.
                "FROM public.raw_table_versions "
                "WHERE superseded_at IS NULL"
            )
        ).fetchall()
    return [
        _LiveRow(
            resource_identity=str(r.resource_identity),
            schema_name=str(r.schema_name),
            table_name=str(r.table_name),
        )
        for r in rows
    ]


def _query_live_identities(engine, identities: list[str]) -> dict[str, _LiveRow]:
    """Load only the requested live identities.

    This is the fast path for SQLs that use only `live_table(...)`, where
    a full scan of every live raw table is unnecessary.
    """
    if not identities:
        return {}
    unique_identities = list(dict.fromkeys(identities))
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT resource_identity, schema_name, table_name "
                "FROM public.raw_table_versions "
                "WHERE superseded_at IS NULL AND resource_identity = ANY(:ids)"
            ),
            {"ids": unique_identities},
        ).fetchall()
    return {
        str(r.resource_identity): _LiveRow(
            resource_identity=str(r.resource_identity),
            schema_name=str(r.schema_name),
            table_name=str(r.table_name),
        )
        for r in rows
    }


def _query_live_by_portals(engine, portals: list[str]) -> list[_LiveRow]:
    """Load live raws matching any `portal::` prefix."""
    if not portals:
        return []
    unique_portals = list(dict.fromkeys(portals))
    clauses = []
    params: dict[str, str] = {}
    for idx, portal in enumerate(unique_portals):
        key = f"p{idx}"
        clauses.append(f"resource_identity LIKE :{key}")
        params[key] = f"{portal}::%"
    sql = (
        "SELECT resource_identity, schema_name, table_name "
        "FROM public.raw_table_versions "
        "WHERE superseded_at IS NULL AND (" + " OR ".join(clauses) + ")"
    )
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).fetchall()
    return [
        _LiveRow(
            resource_identity=str(r.resource_identity),
            schema_name=str(r.schema_name),
            table_name=str(r.table_name),
        )
        for r in rows
    ]


def _query_live_by_identity_patterns(engine, patterns: list[str]) -> list[_LiveRow]:
    """Load live raws whose resource_identity matches any glob pattern."""
    if not patterns:
        return []
    unique_patterns = list(dict.fromkeys(patterns))
    clauses = []
    params: dict[str, str] = {}
    for idx, pattern in enumerate(unique_patterns):
        key = f"ip{idx}"
        clauses.append(f"resource_identity LIKE :{key} ESCAPE '\\'")
        params[key] = _glob_to_like(pattern)
    sql = (
        "SELECT resource_identity, schema_name, table_name "
        "FROM public.raw_table_versions "
        "WHERE superseded_at IS NULL AND (" + " OR ".join(clauses) + ")"
    )
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).fetchall()
    return [
        _LiveRow(
            resource_identity=str(r.resource_identity),
            schema_name=str(r.schema_name),
            table_name=str(r.table_name),
        )
        for r in rows
    ]


def _query_live_by_table_patterns(engine, patterns: list[str]) -> list[_LiveRow]:
    """Load live raws whose table_name matches any glob pattern."""
    if not patterns:
        return []
    unique_patterns = list(dict.fromkeys(patterns))
    clauses = []
    params: dict[str, str] = {}
    for idx, pattern in enumerate(unique_patterns):
        key = f"tp{idx}"
        clauses.append(f"table_name LIKE :{key} ESCAPE '\\'")
        params[key] = _glob_to_like(pattern)
    sql = (
        "SELECT resource_identity, schema_name, table_name "
        "FROM public.raw_table_versions "
        "WHERE superseded_at IS NULL AND (" + " OR ".join(clauses) + ")"
    )
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).fetchall()
    return [
        _LiveRow(
            resource_identity=str(r.resource_identity),
            schema_name=str(r.schema_name),
            table_name=str(r.table_name),
        )
        for r in rows
    ]


def _qualified(row: _LiveRow) -> str:
    """Build `<schema>."<table>"` with the table name double-quoted."""
    return f'{row.schema_name}."{row.table_name}"'


def _build_union(
    lives: Iterable[_LiveRow],
    *,
    macro_name: str = "",
    expected_columns: list[str] | None = None,
    require_all_columns: bool = False,
    require_columns: list[str] | None = None,
    source_marker: str | None = None,
    engine=None,
) -> str:
    """Build a UNION ALL subquery from N live rows.

    With `expected_columns`:
      - Empty list → fallback `(SELECT NULL::text AS c1, ..., WHERE FALSE)`
        so any outer SELECT referencing those columns still parses.
      - N matches → schema-intersection: each SELECT projects only the
        columns common to ALL matched tables AND in `expected_columns`.
        Drops the rest so UNION ALL never trips on heterogeneous schemas.

    With `require_all_columns=True` (only valid alongside expected_columns):
      Tables missing ANY of the expected columns are filtered out BEFORE
      the cap check. Useful when the pattern matches many sub-shapes
      and only the FULL shape is desired (e.g. fact tables vs. dimension
      tables in the same cluster).

    With `require_columns=[...]`:
      Same filtering, but on an explicit SUBSET instead of the whole
      projection list. This decouples "what identifies a fact table" from
      "what the mart projects", which matters because an optional column
      should not cost you the table. Measured on the presupuesto cluster:
      requiring all 33 projected columns kept 36 of 560 tables — 91k of
      15.8M rows, 0.58 % of the data — while 62 further tables were
      complete apart from `finalidad_funcion_*` or
      `impacto_presupuestario_mes`. Columns in `expected_columns` but
      absent from a matched table are still projected as NULL, so the
      outer SELECT is unaffected.
      Takes precedence over `require_all_columns` when both are given.

    With `source_marker='<col>'`:
      Each branch of the union also projects a literal naming the physical
      table it came from. Needed whenever a mart must deduplicate by
      provenance: the ingest columns cannot do it, because
      `_source_dataset_id` identifies the CKAN *dataset* and one dataset
      routinely publishes several resources into several tables. Measured on
      the DDJJ cluster: three physically distinct tables share a single
      `_source_dataset_id`, so grouping by it merged them into one group and
      a dedup built on it passed every row through — the mart double-counted
      2017 while reporting a healthy row count.

    Without `expected_columns`:
      - Empty list → `SELECT NULL::text AS dummy WHERE FALSE` (legacy).
      - N matches → `SELECT * FROM <each>` UNION ALL (legacy).
    """
    lives_list = list(lives)

    filter_set: set[str] | None = None
    if require_columns:
        filter_set = set(require_columns)
    elif require_all_columns and expected_columns:
        filter_set = set(expected_columns)

    # Coverage marker, emitted into the generated SQL below. The kept/candidate
    # ratio used to exist only as the log line further down, which meant the one
    # number that says "this mart answers about 6 % of its domain" vanished the
    # moment the build finished — `mart_definitions` stores a healthy-looking
    # `last_row_count` and nothing else. A block comment rides along into
    # `sql_definition`, where the quality auditor can read it.
    coverage_note = ""

    if filter_set:
        if engine is None:
            raise MacroResolutionError(
                "require_all_columns/require_columns need an engine for schema introspection"
            )
        actual_cols = _query_columns(
            engine,
            [(r.schema_name, r.table_name) for r in lives_list],
        )
        before = len(lives_list)
        lives_list = [
            r
            for r in lives_list
            if filter_set.issubset(actual_cols.get((r.schema_name, r.table_name), set()))
        ]
        # Dropping source tables silently is how a mart ends up serving a
        # fraction of its domain while looking healthy. Say it out loud.
        logger.info(
            "%s: column filter kept %d of %d live tables (required: %s)",
            macro_name or "live_tables_by_*",
            len(lives_list),
            before,
            ", ".join(sorted(filter_set)),
        )
        coverage_note = f"/* macro_coverage: kept {len(lives_list)} of {before} */ "

    if len(lives_list) > _MAX_UNION_TABLES:
        raise MacroExpansionTooLarge(
            f"Macro {macro_name or 'live_tables_by_*'} would expand to "
            f"{len(lives_list)} tables (cap is {_MAX_UNION_TABLES}). "
            f"Narrow the pattern or pre-aggregate upstream."
        )

    if expected_columns:
        if not lives_list:
            cols_for_empty = (
                [*expected_columns, source_marker] if source_marker else expected_columns
            )
            return _typed_empty_select(cols_for_empty, coverage_note=coverage_note)
        # Inspect the actual schema of each matched table and project the
        # intersection that's also in `expected_columns`. Tables missing
        # a particular column emit `NULL::text AS col` to keep the union
        # row-shape consistent.
        if engine is None:
            raise MacroResolutionError("expected_columns requires engine for schema introspection")
        actual_cols_by_table = _query_columns(
            engine,
            [(r.schema_name, r.table_name) for r in lives_list],
        )
        selects = []
        for r in lives_list:
            cols_present = actual_cols_by_table.get((r.schema_name, r.table_name), set())
            # CAST to ::text on every column (present or NULL) so that
            # the UNION ALL never trips on type mismatches across
            # heterogeneous source tables (e.g. one table has `cue` as
            # bigint, another as text). The outer mart SELECT applies
            # its own casts (`cue::int`, `lat::numeric`, etc.) — text
            # is castable from anything that started as NULL or string.
            projected = [
                (f'"{c}"::text AS "{c}"' if c in cols_present else f'NULL::text AS "{c}"')
                for c in expected_columns
            ]
            if source_marker:
                # A literal, not a column: it must differ per physical table
                # even when every ingest column is identical across mirrors.
                literal = f"{r.schema_name}.{r.table_name}".replace("'", "''")
                projected.append(f"'{literal}'::text AS \"{source_marker}\"")
            selects.append(f"SELECT {', '.join(projected)} FROM {_qualified(r)}")
        return "(" + coverage_note + " UNION ALL ".join(selects) + ")"

    # Legacy path (no expected_columns).
    if source_marker:
        selects = [
            f"""SELECT *, '{f"{r.schema_name}.{r.table_name}".replace("'", "''")}'::text """
            f'AS "{source_marker}" FROM {_qualified(r)}'
            for r in lives_list
        ]
    else:
        selects = [f"SELECT * FROM {_qualified(r)}" for r in lives_list]
    if not selects:
        return f"({coverage_note}SELECT NULL::text AS dummy WHERE FALSE)"
    return "(" + coverage_note + " UNION ALL ".join(selects) + ")"


def _typed_empty_select(expected_columns: list[str], *, coverage_note: str = "") -> str:
    """Emit a schema-shaped empty subquery for the 0-match case.

    All columns typed as `text` because we don't know the real types
    upfront — the outer SELECT in the mart YAML must apply its own
    casts (`col::numeric`, `col::int`, etc.). `text` is castable to
    every type when the value is NULL, so this is safe.
    """
    cols = ", ".join(f'NULL::text AS "{c}"' for c in expected_columns)
    return f"({coverage_note}SELECT {cols} WHERE FALSE)"


def _query_columns(
    engine, schema_table_pairs: list[tuple[str, str]]
) -> dict[tuple[str, str], set[str]]:
    """Bulk-load column lists for the given (schema, table) pairs.

    Used by the schema-intersection projection in `_build_union`.
    Single round-trip; the result is keyed by (schema, table_name).
    """
    if not schema_table_pairs:
        return {}
    schemas = list({s for s, _ in schema_table_pairs})
    tables = list({t for _, t in schema_table_pairs})
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT table_schema, table_name, column_name "
                "FROM information_schema.columns "
                "WHERE table_schema = ANY(:schemas) "
                "  AND table_name = ANY(:tables)"
            ),
            {"schemas": schemas, "tables": tables},
        ).fetchall()
    out: dict[tuple[str, str], set[str]] = {}
    target = set(schema_table_pairs)
    for r in rows:
        key = (str(r.table_schema), str(r.table_name))
        if key not in target:
            continue
        out.setdefault(key, set()).add(str(r.column_name))
    return out


def _glob_to_like(pattern: str) -> str:
    """Convert a `*`-style glob to a SQL LIKE pattern.

    `bcra::*tasa*` → `bcra::%tasa%`. Other `_` and `%` chars are escaped
    so they don't accidentally match.
    """
    escaped = pattern.replace("%", r"\%").replace("_", r"\_")
    return escaped.replace("*", "%")


def _parse_macro_call(call_text: str) -> tuple[str, str, dict]:
    """Parse `<name>('<arg>', expected_columns=[...])` via `ast`.

    Returns `(name, positional_arg, kwargs)`. Rejects anything that's not
    a single Call with literal-only arguments (uses `ast.literal_eval` on
    each kwarg value). Defensive: avoids arbitrary code paths from YAML
    inputs.
    """
    try:
        node = ast.parse(call_text.strip(), mode="eval").body
    except SyntaxError as exc:
        raise MacroResolutionError(f"Macro syntax error: {exc}") from exc
    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
        raise MacroResolutionError(f"Macro must be a function call, got {ast.dump(node)}")
    name = node.func.id
    if name not in _VALID_MACRO_NAMES:
        raise MacroResolutionError(f"Unknown macro: {name}")
    if len(node.args) != 1:
        raise MacroResolutionError(f"Macro {name}() must take exactly one positional arg")
    try:
        positional = ast.literal_eval(node.args[0])
    except (ValueError, SyntaxError) as exc:
        raise MacroResolutionError(f"Macro {name}() positional arg must be a literal") from exc
    if not isinstance(positional, str):
        raise MacroResolutionError(f"Macro {name}() positional arg must be a string")
    kwargs: dict = {}
    for kw in node.keywords:
        if kw.arg is None:
            raise MacroResolutionError(f"Macro {name}() rejects **kwargs")
        try:
            kwargs[kw.arg] = ast.literal_eval(kw.value)
        except (ValueError, SyntaxError) as exc:
            raise MacroResolutionError(
                f"Macro {name}() kwarg {kw.arg!r} must be a literal"
            ) from exc
    return name, positional, kwargs


def _collect_macro_calls(sql: str) -> list[_MacroCall]:
    """Parse all macro calls in `sql` once so resolution can choose
    a cheaper loading strategy before replacement begins.
    """
    calls: list[_MacroCall] = []
    for match in _MACRO_RE.finditer(sql):
        name, arg, kwargs = _parse_macro_call(match.group("call"))
        calls.append(_MacroCall(name=name, arg=arg, kwargs=kwargs))
    return calls


def resolved_source_tables(sql: str, engine) -> list[tuple[str, str]]:
    """The `(schema, table)` pairs the macros in `sql` resolve to right now.

    `resolve_macros` already computes this set and then throws it away, keeping
    only the SQL it produced. The build is the one moment the system knows
    exactly which tables a mart reads — afterwards the mart is a matview and the
    link is gone — so exposing the set lets `build_mart` record how old its
    sources were, which is what a reader actually needs to know and what
    `last_refreshed_at` cannot tell them.

    Reuses the same queries as the resolution rather than re-deriving them, so
    the two can never disagree about what a mart reads.
    """
    macro_calls = _collect_macro_calls(sql)
    if not macro_calls:
        return []
    rows: list[_LiveRow] = [
        *_query_live_identities(
            engine, [c.arg for c in macro_calls if c.name == "live_table"]
        ).values(),
        *_query_live_by_portals(
            engine, [c.arg for c in macro_calls if c.name == "live_tables_by_portal"]
        ),
        *_query_live_by_identity_patterns(
            engine, [c.arg for c in macro_calls if c.name == "live_tables_by_pattern"]
        ),
        *_query_live_by_table_patterns(
            engine,
            [c.arg for c in macro_calls if c.name == "live_tables_by_table_pattern"],
        ),
    ]
    return sorted({(r.schema_name, r.table_name) for r in rows})


def resolve_macros(sql: str, engine) -> str:
    """Replace every `{{ macro(...) }}` in `sql` with its concrete SQL.

    Reads `public.raw_table_versions` ONCE for the lifetime of this call.
    Unknown macros or bad args raise `MacroResolutionError` so build_mart
    can record the failure in `mart_definitions.last_refresh_error`.
    """
    macro_calls = _collect_macro_calls(sql)
    direct_identity_rows = _query_live_identities(
        engine,
        [call.arg for call in macro_calls if call.name == "live_table"],
    )
    portal_rows = _query_live_by_portals(
        engine,
        [call.arg for call in macro_calls if call.name == "live_tables_by_portal"],
    )
    identity_pattern_rows = _query_live_by_identity_patterns(
        engine,
        [call.arg for call in macro_calls if call.name == "live_tables_by_pattern"],
    )
    table_pattern_rows = _query_live_by_table_patterns(
        engine,
        [call.arg for call in macro_calls if call.name == "live_tables_by_table_pattern"],
    )

    all_rows: dict[tuple[str, str, str], _LiveRow] = {}
    for row in [
        *direct_identity_rows.values(),
        *portal_rows,
        *identity_pattern_rows,
        *table_pattern_rows,
    ]:
        all_rows[(row.resource_identity, row.schema_name, row.table_name)] = row

    lives: list[_LiveRow] = list(all_rows.values())
    by_identity: dict[str, _LiveRow] = {row.resource_identity: row for row in lives}
    columns_cache: dict[tuple[str, str], set[str]] = {}

    def _replace(match: re.Match[str]) -> str:
        call_text = match.group("call")
        name, arg, kwargs = _parse_macro_call(call_text)
        expected_columns = kwargs.get("expected_columns")
        if expected_columns is not None:
            if not isinstance(expected_columns, list) or not all(
                isinstance(c, str) for c in expected_columns
            ):
                raise MacroResolutionError(
                    f"Macro {name}(): expected_columns must be a list of strings"
                )
        require_all_columns = kwargs.get("require_all_columns", False)
        if not isinstance(require_all_columns, bool):
            raise MacroResolutionError(f"Macro {name}(): require_all_columns must be a bool")
        require_columns = kwargs.get("require_columns")
        if require_columns is not None:
            if not isinstance(require_columns, list) or not all(
                isinstance(x, str) for x in require_columns
            ):
                raise MacroResolutionError(
                    f"Macro {name}(): require_columns must be a list of strings"
                )
            if not expected_columns:
                raise MacroResolutionError(
                    f"Macro {name}(): require_columns requires expected_columns"
                )
            missing = set(require_columns) - set(expected_columns)
            if missing:
                raise MacroResolutionError(
                    f"Macro {name}(): require_columns not in expected_columns: {sorted(missing)}"
                )
        source_marker = kwargs.get("source_marker")
        if source_marker is not None:
            if not isinstance(source_marker, str) or not source_marker:
                raise MacroResolutionError(
                    f"Macro {name}(): source_marker must be a non-empty string"
                )
            if expected_columns and source_marker in expected_columns:
                raise MacroResolutionError(
                    f"Macro {name}(): source_marker {source_marker!r} collides with "
                    f"an expected column"
                )
        if require_all_columns and not expected_columns:
            raise MacroResolutionError(
                f"Macro {name}(): require_all_columns=True requires expected_columns"
            )

        if not _RESOURCE_IDENTITY_RE.match(arg):
            raise MacroResolutionError(f"Macro {name}(): invalid arg {arg!r} (charset)")

        if name == "live_table":
            row = by_identity.get(arg)
            if row is None:
                # Missing → typed-empty if expected_columns given, else
                # legacy untyped dummy (caller's outer SELECT may break).
                if expected_columns:
                    return _typed_empty_select(expected_columns)
                return f"(SELECT NULL::text AS dummy WHERE FALSE) /* live_table({arg!r}) absent */"
            if expected_columns:
                # Project only the requested columns (NULL-fallback for
                # ones that don't exist in this specific table).
                cache_key = (row.schema_name, row.table_name)
                actual_cols = columns_cache.get(cache_key)
                if actual_cols is None:
                    actual_cols = _query_columns(engine, [cache_key]).get(cache_key, set())
                    columns_cache[cache_key] = actual_cols
                if require_all_columns and not set(expected_columns).issubset(actual_cols):
                    return _typed_empty_select(expected_columns)
                projected = [
                    (f'"{c}"::text AS "{c}"' if c in actual_cols else f'NULL::text AS "{c}"')
                    for c in expected_columns
                ]
                return f"(SELECT {', '.join(projected)} FROM {_qualified(row)})"
            return _qualified(row)

        if name == "live_tables_by_portal":
            prefix = arg + "::"
            matched = [r for r in lives if r.resource_identity.startswith(prefix)]
            return _build_union(
                matched,
                macro_name=f"live_tables_by_portal({arg!r})",
                expected_columns=expected_columns,
                require_all_columns=require_all_columns,
                require_columns=require_columns,
                source_marker=source_marker,
                engine=engine,
            )

        if name == "live_tables_by_pattern":
            parts = arg.split("*")
            escaped = ".*".join(re.escape(p) for p in parts)
            py_regex = re.compile("^" + escaped + "$")
            matched = [r for r in lives if py_regex.match(r.resource_identity)]
            return _build_union(
                matched,
                macro_name=f"live_tables_by_pattern({arg!r})",
                expected_columns=expected_columns,
                require_all_columns=require_all_columns,
                require_columns=require_columns,
                source_marker=source_marker,
                engine=engine,
            )

        if name == "live_tables_by_table_pattern":
            parts = arg.split("*")
            escaped = ".*".join(re.escape(p) for p in parts)
            py_regex = re.compile("^" + escaped + "$")
            matched = [r for r in lives if py_regex.match(r.table_name)]
            return _build_union(
                matched,
                macro_name=f"live_tables_by_table_pattern({arg!r})",
                expected_columns=expected_columns,
                require_all_columns=require_all_columns,
                require_columns=require_columns,
                source_marker=source_marker,
                engine=engine,
            )

        raise MacroResolutionError(f"Unknown macro: {name}")

    try:
        return _MACRO_RE.sub(_replace, sql)
    except MacroResolutionError:
        raise
    except re.error as exc:
        raise MacroResolutionError(f"Macro regex error: {exc}") from exc
