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

When a macro resolves to ZERO live tables, the result is a deterministic
empty-shape subquery (`SELECT WHERE FALSE`), so the mart still builds
with a known shape and stays empty until upstream lands.

The resolver is pure-Python (no Postgres function side effects). It
reads `raw_table_versions` once per `resolve_macros` call. When a
caller passes `expected_columns`, an additional cheap query inspects
`information_schema.columns` for the matched table_names so the
intersection can be computed.

Macro syntax: literal Python call expression inside `{{ ... }}`.
Args parsed via `ast.parse` + `ast.literal_eval` so only literals
are allowed. If we need more, dbt is the upgrade path.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass
from typing import Iterable

from sqlalchemy import text

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
# 200 is a safety upper bound; marts that legitimately need more should
# pre-aggregate upstream or use `live_tables_by_portal('specific_portal')`.
_MAX_UNION_TABLES = 200


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


def _query_lives(engine) -> list[_LiveRow]:
    """Read every (resource_identity, schema, table) where superseded_at IS NULL."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT resource_identity, schema_name, table_name "
                "FROM raw_table_versions "
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


def _qualified(row: _LiveRow) -> str:
    """Build `<schema>."<table>"` with the table name double-quoted."""
    return f'{row.schema_name}."{row.table_name}"'


def _build_union(
    lives: Iterable[_LiveRow],
    *,
    macro_name: str = "",
    expected_columns: list[str] | None = None,
    engine=None,
) -> str:
    """Build a UNION ALL subquery from N live rows.

    With `expected_columns`:
      - Empty list → fallback `(SELECT NULL::text AS c1, ..., WHERE FALSE)`
        so any outer SELECT referencing those columns still parses.
      - N matches → schema-intersection: each SELECT projects only the
        columns common to ALL matched tables AND in `expected_columns`.
        Drops the rest so UNION ALL never trips on heterogeneous schemas.

    Without `expected_columns`:
      - Empty list → `SELECT NULL::text AS dummy WHERE FALSE` (legacy).
      - N matches → `SELECT * FROM <each>` UNION ALL (legacy).
    """
    lives_list = list(lives)
    if len(lives_list) > _MAX_UNION_TABLES:
        raise MacroExpansionTooLarge(
            f"Macro {macro_name or 'live_tables_by_*'} would expand to "
            f"{len(lives_list)} tables (cap is {_MAX_UNION_TABLES}). "
            f"Narrow the pattern or pre-aggregate upstream."
        )

    if expected_columns:
        if not lives_list:
            return _typed_empty_select(expected_columns)
        # Inspect the actual schema of each matched table and project the
        # intersection that's also in `expected_columns`. Tables missing
        # a particular column emit `NULL::text AS col` to keep the union
        # row-shape consistent.
        if engine is None:
            raise MacroResolutionError(
                "expected_columns requires engine for schema introspection"
            )
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
                (
                    f'"{c}"::text AS "{c}"'
                    if c in cols_present
                    else f'NULL::text AS "{c}"'
                )
                for c in expected_columns
            ]
            selects.append(
                f"SELECT {', '.join(projected)} FROM {_qualified(r)}"
            )
        return "(" + " UNION ALL ".join(selects) + ")"

    # Legacy path (no expected_columns).
    selects = [f"SELECT * FROM {_qualified(r)}" for r in lives_list]
    if not selects:
        return "(SELECT NULL::text AS dummy WHERE FALSE)"
    return "(" + " UNION ALL ".join(selects) + ")"


def _typed_empty_select(expected_columns: list[str]) -> str:
    """Emit a schema-shaped empty subquery for the 0-match case.

    All columns typed as `text` because we don't know the real types
    upfront — the outer SELECT in the mart YAML must apply its own
    casts (`col::numeric`, `col::int`, etc.). `text` is castable to
    every type when the value is NULL, so this is safe.
    """
    cols = ", ".join(
        f'NULL::text AS "{c}"' for c in expected_columns
    )
    return f"(SELECT {cols} WHERE FALSE)"


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
        raise MacroResolutionError(
            f"Macro must be a function call, got {ast.dump(node)}"
        )
    name = node.func.id
    if name not in _VALID_MACRO_NAMES:
        raise MacroResolutionError(f"Unknown macro: {name}")
    if len(node.args) != 1:
        raise MacroResolutionError(
            f"Macro {name}() must take exactly one positional arg"
        )
    try:
        positional = ast.literal_eval(node.args[0])
    except (ValueError, SyntaxError) as exc:
        raise MacroResolutionError(
            f"Macro {name}() positional arg must be a literal"
        ) from exc
    if not isinstance(positional, str):
        raise MacroResolutionError(
            f"Macro {name}() positional arg must be a string"
        )
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


def resolve_macros(sql: str, engine) -> str:
    """Replace every `{{ macro(...) }}` in `sql` with its concrete SQL.

    Reads `raw_table_versions` ONCE for the lifetime of this call.
    Unknown macros or bad args raise `MacroResolutionError` so build_mart
    can record the failure in `mart_definitions.last_refresh_error`.
    """
    lives = _query_lives(engine)
    by_identity: dict[str, _LiveRow] = {row.resource_identity: row for row in lives}

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

        if not _RESOURCE_IDENTITY_RE.match(arg):
            raise MacroResolutionError(
                f"Macro {name}(): invalid arg {arg!r} (charset)"
            )

        if name == "live_table":
            row = by_identity.get(arg)
            if row is None:
                # Missing → typed-empty if expected_columns given, else
                # legacy untyped dummy (caller's outer SELECT may break).
                if expected_columns:
                    return _typed_empty_select(expected_columns)
                return (
                    f"(SELECT NULL::text AS dummy WHERE FALSE) "
                    f"/* live_table({arg!r}) absent */"
                )
            if expected_columns:
                # Project only the requested columns (NULL-fallback for
                # ones that don't exist in this specific table).
                actual_cols = _query_columns(
                    engine, [(row.schema_name, row.table_name)]
                ).get((row.schema_name, row.table_name), set())
                projected = [
                    (
                        f'"{c}"::text AS "{c}"'
                        if c in actual_cols
                        else f'NULL::text AS "{c}"'
                    )
                    for c in expected_columns
                ]
                return (
                    f"(SELECT {', '.join(projected)} FROM {_qualified(row)})"
                )
            return _qualified(row)

        if name == "live_tables_by_portal":
            prefix = arg + "::"
            matched = [r for r in lives if r.resource_identity.startswith(prefix)]
            return _build_union(
                matched,
                macro_name=f"live_tables_by_portal({arg!r})",
                expected_columns=expected_columns,
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
                engine=engine,
            )

        raise MacroResolutionError(f"Unknown macro: {name}")

    try:
        return _MACRO_RE.sub(_replace, sql)
    except MacroResolutionError:
        raise
    except re.error as exc:
        raise MacroResolutionError(f"Macro regex error: {exc}") from exc
