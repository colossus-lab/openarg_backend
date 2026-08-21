"""Remember what a table looked like, just before it stops existing.

The collector's answer to an incompatible re-ingest is DROP + CREATE in
place (`schema_mismatch_recreate`, 19,293 times in production). Three
cleanup tasks drop tables for their own reasons. In every case the old
shape is gone the moment the drop commits, which is why the question
"did this resource change format, and how" has no answer today: of 644
consecutive version pairs in production, 642 have no `v1` left.

`capture_table_snapshot` runs immediately before those drops and writes
one row describing the table that is about to disappear.

Two design choices worth stating:

**The profile comes from `pg_stats`, not from the table.** PostgreSQL has
already computed null fractions, distinct estimates, most-common values
and histogram bounds during autovacuum's ANALYZE. Reading them is an
index scan on the catalog — it never touches the data. That matters here
more than usual: this code runs on the path of a table that is being
dropped, often because something already went wrong, and it must not add
a scan to that path. When the table was never analysed we record the
shape anyway and set `stats_available=False`; the columns and their types
are the part that answers "did the format change", and those always come
from the catalog.

**Nothing here is allowed to fail loudly.** A snapshot is a nice-to-have
at the moment it is taken; the drop it precedes is not optional. Every
entry point swallows its exceptions and returns None.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# How many entries of `most_common_vals` / `histogram_bounds` to keep per
# column. PostgreSQL's default statistics target is 100, which is far more
# than a shape comparison needs and would make the JSON payload the largest
# part of the row. Twenty is enough to recognise a column across a rename.
MAX_PROFILE_VALUES = 20

# Each stored value is truncated to this many characters. Some columns hold
# whole paragraphs; the prefix is what identifies the column, and the rest
# is weight.
MAX_VALUE_CHARS = 120

# Above this width we record names and types but skip the value profile.
# A 1,400-column table (the string-NaN bug produced several) would otherwise
# generate a payload orders of magnitude larger than the row it describes.
MAX_PROFILED_COLUMNS = 300

# Metadata columns the collector adds. They say nothing about the upstream
# format, and including them would make two otherwise-identical shapes hash
# differently depending on when they were ingested.
_INTERNAL_COLUMNS = frozenset({"_source_dataset_id", "_ingested_at", "_source_url"})


@dataclass
class ColumnProfile:
    """One column, as PostgreSQL last understood it."""

    name: str
    ordinal: int
    pg_type: str
    null_frac: float | None = None
    # Negative values are PostgreSQL's way of expressing a ratio of the row
    # count rather than an absolute count; kept verbatim so a consumer can
    # tell the two apart.
    n_distinct: float | None = None
    most_common_vals: list[str] = field(default_factory=list)
    histogram_sample: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "ordinal": self.ordinal,
            "pg_type": self.pg_type,
            "null_frac": self.null_frac,
            "n_distinct": self.n_distinct,
            "most_common_vals": self.most_common_vals,
            "histogram_sample": self.histogram_sample,
        }


@dataclass(frozen=True)
class Provenance:
    """Which of *our* versions produced this shape.

    Without it, a snapshot diff cannot tell an upstream format change from one
    of our own parser improvements — and we ship those constantly (~9.500
    `parse_repair` operations, five schema-rewriting transforms in the
    collector). A diff across differing provenance is our change, not theirs,
    and that is a proof rather than a heuristic.
    """

    parser_version: str | None = None
    normalization_version: str | None = None
    layout_profile: str | None = None
    header_quality: str | None = None
    is_truncated: bool | None = None

    def differs_from(self, other: Provenance) -> list[str]:
        """Which provenance fields disagree. Empty means same pipeline.

        A field that is unknown on either side is NOT counted as a difference:
        absence of evidence would otherwise exonerate every diff involving a
        legacy table, which is most of production.
        """
        changed = []
        for field_name in (
            "parser_version",
            "normalization_version",
            "layout_profile",
            "header_quality",
            "is_truncated",
        ):
            mine = getattr(self, field_name)
            theirs = getattr(other, field_name)
            if mine is None or theirs is None:
                continue
            if mine != theirs:
                changed.append(field_name)
        return changed


@dataclass
class TableSnapshot:
    """Everything worth keeping about a table that is about to be dropped."""

    schema_name: str
    table_name: str
    columns: list[ColumnProfile]
    row_count_estimate: int | None
    stats_available: bool
    resource_identity: str | None = None
    version: int | None = None
    provenance: Provenance = field(default_factory=Provenance)

    @property
    def schema_hash(self) -> str:
        return schema_hash_for(c.name for c in self.columns)

    @property
    def column_count(self) -> int:
        return len(self.columns)


def schema_hash_for(column_names) -> str:
    """Stable hash of a column set, ignoring order and internal columns.

    Same construction as `collector_tasks._schema_suffix` so a snapshot hash
    can be compared directly against the `_s<hash>` suffix the collector puts
    on schema-variant tables. Kept as its own function rather than imported
    to avoid a dependency from the application layer onto a Celery module.
    """
    signature = "|".join(sorted(str(c) for c in column_names if str(c) not in _INTERNAL_COLUMNS))
    return hashlib.sha1(signature.encode("utf-8"), usedforsecurity=False).hexdigest()


def _clip(value: Any) -> str:
    return str(value)[:MAX_VALUE_CHARS]


def _parse_pg_array(raw: Any) -> list[str]:
    """Turn a `pg_stats` anyarray into a bounded list of strings.

    `most_common_vals` and `histogram_bounds` come back as `anyarray`, which
    psycopg surfaces either as a Python list or as the literal text form
    `{a,b,c}` depending on the element type. Both shapes appear in practice
    against the same table, so handle both rather than guessing.
    """
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        return [_clip(v) for v in raw[:MAX_PROFILE_VALUES]]
    text_form = str(raw).strip()
    if text_form.startswith("{") and text_form.endswith("}"):
        text_form = text_form[1:-1]
    if not text_form:
        return []
    # Naive split on commas. Values containing commas end up split, which is
    # acceptable: this is a fingerprint for comparison, not a faithful copy.
    parts = [p.strip().strip('"') for p in text_form.split(",")]
    return [_clip(p) for p in parts[:MAX_PROFILE_VALUES] if p]


_COLUMNS_SQL = text(
    "SELECT a.attname AS name, a.attnum AS ordinal, "
    "       format_type(a.atttypid, a.atttypmod) AS pg_type "
    "FROM pg_class c "
    "JOIN pg_namespace n ON n.oid = c.relnamespace "
    "JOIN pg_attribute a ON a.attrelid = c.oid "
    "WHERE n.nspname = :sch AND c.relname = :tbl "
    "  AND a.attnum > 0 AND NOT a.attisdropped "
    "ORDER BY a.attnum"
)

_STATS_SQL = text(
    "SELECT attname, null_frac, n_distinct, most_common_vals, histogram_bounds "
    "FROM pg_stats WHERE schemaname = :sch AND tablename = :tbl"
)

_RELTUPLES_SQL = text(
    "SELECT c.reltuples::bigint AS approx_rows FROM pg_class c "
    "JOIN pg_namespace n ON n.oid = c.relnamespace "
    "WHERE n.nspname = :sch AND c.relname = :tbl"
)

_IDENTITY_SQL = text(
    "SELECT resource_identity, version, is_truncated FROM public.raw_table_versions "
    "WHERE table_name = :tbl AND schema_name = :sch "
    "ORDER BY version DESC LIMIT 1"
)

# Provenance lives in two places and neither is guaranteed to have a row:
# `catalog_resources` carries the parser versions, `cached_datasets` the parse
# path. Both are keyed by the physical table name. LEFT JOIN so a missing
# catalog row still yields whatever `cached_datasets` knows.
# `public.raw_table_versions` first, because it is the only per-version record.
# `catalog_resources` holds one row per resource — 32,564 rows for 32,564
# resources, with no version column — so both sides of a historical pair read
# back the same value and G1 is structurally blind. That is why it fired zero
# times against five findings that were all our parser's doing.
#
# The catalogue stays as a fallback for layout and header quality, which it is
# the only source of, and for provenance on rows the registry never recorded.
_PROVENANCE_SQL = text(
    "SELECT COALESCE(rtv.parser_version, cr.parser_version) AS parser_version, "
    "       COALESCE(rtv.normalization_version, cr.normalization_version) "
    "           AS normalization_version, "
    "       COALESCE(cr.layout_profile, cd.layout_profile) AS layout_profile, "
    "       COALESCE(cr.header_quality, cd.header_quality) AS header_quality "
    "FROM (SELECT :tbl AS tn, :sch AS sn) k "
    "LEFT JOIN public.raw_table_versions rtv "
    "       ON rtv.table_name = k.tn AND rtv.schema_name = k.sn "
    "LEFT JOIN catalog_resources cr ON cr.materialized_table_name = k.tn "
    "LEFT JOIN cached_datasets cd ON cd.table_name = k.tn "
    "LIMIT 1"
)

_INSERT_SQL = text(
    "INSERT INTO raw.raw_schema_snapshots ("
    "  schema_name, table_name, resource_identity, version, reason, actor, "
    "  column_count, row_count_estimate, schema_hash, columns_profile, "
    "  stats_available, extra, "
    "  parser_version, normalization_version, layout_profile, header_quality, "
    "  is_truncated"
    ") VALUES ("
    "  :sch, :tbl, :rid, :ver, :reason, :actor, "
    "  :ncols, :rows, :hash, CAST(:profile AS jsonb), "
    "  :stats, CAST(:extra AS jsonb), "
    "  :pver, :nver, :layout, :hquality, :trunc"
    ") RETURNING id"
)


def split_qualified(name: str, *, default_schema: str = "public") -> tuple[str, str]:
    """Split `schema.table` into its parts, tolerating an unqualified name.

    `_record_cache_drop` is called with both shapes — `raw.foo` from the
    cleanup tasks, bare `cache_foo` from the legacy collector path — so the
    reader has to accept either.
    """
    value = (name or "").strip().strip('"')
    if "." not in value:
        return default_schema, value
    schema, _, table = value.partition(".")
    return schema.strip('"') or default_schema, table.strip('"')


def collect_snapshot(
    engine: Engine,
    schema_name: str,
    table_name: str,
) -> TableSnapshot | None:
    """Read the shape and (if available) the value profile of one table.

    Returns None when the table does not exist — which is a normal outcome:
    every caller sits in front of a `DROP TABLE IF EXISTS`, so racing with
    another worker that already dropped it is expected, not an error.
    """
    if not table_name:
        return None
    params = {"sch": schema_name, "tbl": table_name}
    with engine.connect() as conn:
        column_rows = conn.execute(_COLUMNS_SQL, params).fetchall()
        if not column_rows:
            conn.rollback()
            return None

        wide = len(column_rows) > MAX_PROFILED_COLUMNS
        stats_by_column: dict[str, Any] = {}
        if not wide:
            for row in conn.execute(_STATS_SQL, params).fetchall():
                stats_by_column[row.attname] = row

        approx = conn.execute(_RELTUPLES_SQL, params).scalar()

        # Best-effort. The registry lives in `public` on some deployments and
        # in `raw` on others, and on one of them it exists in both — so a
        # failure here is informational, never fatal.
        resource_identity: str | None = None
        version: int | None = None
        is_truncated: bool | None = None
        try:
            identity_row = conn.execute(_IDENTITY_SQL, params).fetchone()
            if identity_row is not None:
                resource_identity = identity_row.resource_identity
                version = int(identity_row.version)
                is_truncated = identity_row.is_truncated
        except Exception:
            logger.debug(
                "schema snapshot: could not resolve identity for %s.%s",
                schema_name,
                table_name,
                exc_info=True,
            )

        # Provenance is what lets a later diff tell "they changed the file"
        # from "we changed the parser". Its absence degrades the verdict to
        # "cannot exonerate", never to a wrong one.
        provenance = Provenance(is_truncated=is_truncated)
        try:
            prov_row = conn.execute(_PROVENANCE_SQL, params).fetchone()
            if prov_row is not None:
                provenance = Provenance(
                    parser_version=prov_row.parser_version,
                    normalization_version=prov_row.normalization_version,
                    layout_profile=prov_row.layout_profile,
                    header_quality=prov_row.header_quality,
                    is_truncated=is_truncated,
                )
        except Exception:
            logger.debug(
                "schema snapshot: could not resolve provenance for %s.%s",
                schema_name,
                table_name,
                exc_info=True,
            )
        conn.rollback()

    columns = []
    for row in column_rows:
        stat = stats_by_column.get(row.name)
        columns.append(
            ColumnProfile(
                name=row.name,
                ordinal=int(row.ordinal),
                pg_type=row.pg_type,
                null_frac=float(stat.null_frac) if stat and stat.null_frac is not None else None,
                n_distinct=float(stat.n_distinct) if stat and stat.n_distinct is not None else None,
                most_common_vals=_parse_pg_array(stat.most_common_vals) if stat else [],
                histogram_sample=_parse_pg_array(stat.histogram_bounds) if stat else [],
            )
        )

    return TableSnapshot(
        schema_name=schema_name,
        table_name=table_name,
        columns=columns,
        # `reltuples` is -1 for a relation that was never analysed. Mapping
        # that to None keeps "unknown" distinguishable from "empty", which is
        # exactly the distinction a drift consumer needs.
        row_count_estimate=int(approx) if approx is not None and approx >= 0 else None,
        stats_available=bool(stats_by_column),
        resource_identity=resource_identity,
        version=version,
        provenance=provenance,
    )


def snapshot_from_row(row: Any) -> TableSnapshot:
    """Rebuild a `TableSnapshot` from a `raw_schema_snapshots` row.

    The inverse of the capture. It exists so a consumer can compare two
    snapshots long after both tables were dropped — which is the whole reason
    the rows are stored, and the reason `diff_snapshots` takes objects rather
    than reading the database itself.

    Tolerant by design: a row written before migration 0057 has no provenance
    columns, and `getattr` with a default keeps it readable instead of raising.
    """
    raw_profile = row.columns_profile or []
    if isinstance(raw_profile, str):
        raw_profile = json.loads(raw_profile)

    columns = [
        ColumnProfile(
            name=str(c.get("name", "")),
            ordinal=int(c.get("ordinal") or 0),
            pg_type=str(c.get("pg_type", "")),
            null_frac=c.get("null_frac"),
            n_distinct=c.get("n_distinct"),
            most_common_vals=list(c.get("most_common_vals") or []),
            histogram_sample=list(c.get("histogram_sample") or []),
        )
        for c in raw_profile
    ]
    return TableSnapshot(
        schema_name=row.schema_name,
        table_name=row.table_name,
        columns=columns,
        row_count_estimate=row.row_count_estimate,
        stats_available=bool(row.stats_available),
        resource_identity=row.resource_identity,
        version=row.version,
        provenance=Provenance(
            parser_version=getattr(row, "parser_version", None),
            normalization_version=getattr(row, "normalization_version", None),
            layout_profile=getattr(row, "layout_profile", None),
            header_quality=getattr(row, "header_quality", None),
            is_truncated=getattr(row, "is_truncated", None),
        ),
    )


def capture_table_snapshot(
    engine: Engine,
    *,
    table_name: str,
    reason: str,
    actor: str,
    schema_name: str | None = None,
    extra: dict[str, Any] | None = None,
) -> str | None:
    """Record what `table_name` looks like right now. Returns the snapshot id.

    Best-effort by contract: returns None on any failure, including a table
    that no longer exists. The caller is about to drop the table and must not
    be blocked by anything that happens here.

    `table_name` may be qualified (`raw.foo`) or bare; `schema_name` overrides
    the parsed value when the caller knows better.
    """
    try:
        parsed_schema, bare_name = split_qualified(table_name)
        schema = schema_name or parsed_schema
        snapshot = collect_snapshot(engine, schema, bare_name)
        if snapshot is None:
            return None

        payload = json.dumps([c.as_dict() for c in snapshot.columns], ensure_ascii=False)
        if snapshot.columns and len(snapshot.columns) > MAX_PROFILED_COLUMNS:
            extra = {**(extra or {}), "profile_skipped": "too_many_columns"}

        with engine.begin() as conn:
            snapshot_id = conn.execute(
                _INSERT_SQL,
                {
                    "sch": snapshot.schema_name,
                    "tbl": snapshot.table_name,
                    "rid": snapshot.resource_identity,
                    "ver": snapshot.version,
                    "reason": reason[:128],
                    "actor": actor[:128],
                    "ncols": snapshot.column_count,
                    "rows": snapshot.row_count_estimate,
                    "hash": snapshot.schema_hash,
                    "profile": payload,
                    "stats": snapshot.stats_available,
                    "extra": json.dumps(extra, ensure_ascii=False) if extra else None,
                    "pver": snapshot.provenance.parser_version,
                    "nver": snapshot.provenance.normalization_version,
                    "layout": snapshot.provenance.layout_profile,
                    "hquality": snapshot.provenance.header_quality,
                    "trunc": snapshot.provenance.is_truncated,
                },
            ).scalar()
        return str(snapshot_id) if snapshot_id else None
    except Exception:
        logger.warning(
            "schema snapshot failed for %s (reason=%s); the drop proceeds unrecorded",
            table_name,
            reason[:60],
            exc_info=True,
        )
        return None


def diff_snapshots(before: TableSnapshot, after: TableSnapshot) -> dict[str, Any]:
    """Compare two snapshots of the same table.

    Pure — no database access — so a consumer can call it over rows read from
    `raw_schema_snapshots` long after both tables are gone. That is the whole
    point of storing the snapshots in the first place.

    `renamed_candidates` is the interesting output: a column that disappeared
    and one that appeared whose value profiles are close are almost certainly
    the same column under a new name. Detecting that needs no model, only the
    profile — which is why the profile is worth storing.
    """
    before_names = {c.name for c in before.columns}
    after_names = {c.name for c in after.columns}
    added = sorted(after_names - before_names)
    removed = sorted(before_names - after_names)

    type_changed: list[dict[str, str]] = []
    before_by_name = {c.name: c for c in before.columns}
    for column in after.columns:
        previous = before_by_name.get(column.name)
        if previous is not None and previous.pg_type != column.pg_type:
            type_changed.append(
                {"column": column.name, "from": previous.pg_type, "to": column.pg_type}
            )

    renamed: list[dict[str, Any]] = []
    ambiguous: list[dict[str, Any]] = []
    if added and removed:
        after_by_name = {c.name: c for c in after.columns}
        for gone in removed:
            old = before_by_name[gone]
            if not is_identifiable(old):
                # Gate 5 — sufficiency. A column of two values looks like every
                # other column of two values. Argentine public data is full of
                # them, and of columns whose sampled values are all `""`. Left
                # in the matching they manufacture a rename for every pair.
                continue
            scored = [
                (name, profile_similarity(old, after_by_name[name]))
                for name in added
                if is_identifiable(after_by_name[name])
            ]
            over_threshold = [(n, s) for n, s in scored if s >= RENAME_THRESHOLD]
            if not over_threshold:
                continue
            over_threshold.sort(key=lambda pair: pair[1], reverse=True)
            if len(over_threshold) > 1:
                # Gate 6 — uniqueness. `provincia_origen` and `provincia_destino`
                # share the same 24 values, so both score ~1.0 against a removed
                # `provincia`. A high score is not evidence when several
                # candidates share it; requiring a single match is what makes
                # this usable. Reported separately so the ambiguity is visible
                # rather than silently dropped.
                ambiguous.append(
                    {
                        "from": gone,
                        "candidates": [
                            {"to": n, "similarity": round(s, 3)} for n, s in over_threshold
                        ],
                    }
                )
                continue
            best_name, best_score = over_threshold[0]
            renamed.append({"from": gone, "to": best_name, "similarity": round(best_score, 3)})

    return {
        "schema_changed": before.schema_hash != after.schema_hash,
        "added": added,
        "removed": removed,
        "type_changed": type_changed,
        "renamed_candidates": renamed,
        "ambiguous_renames": ambiguous,
        "provenance_changed": after.provenance.differs_from(before.provenance),
    }


# A column has to carry enough signal to be recognised across a rename.
# Below these it is indistinguishable from any other column of the same
# trivial shape — `sexo` (M/F), `activo` (S/N), or one whose sampled values
# are all empty, which is extremely common in Argentine public data:
# `most_common_vals` starting with `""` and `-` shows up throughout the
# production `pg_stats`.
MIN_IDENTIFIABLE_VALUES = 4
MAX_IDENTIFIABLE_NULL_FRAC = 0.95

# A single candidate must clear this to be reported as a rename. Not
# calibrated against real diffs yet — see spec 023 CL-023-001. Chosen so
# identical value sets clear it comfortably.
RENAME_THRESHOLD = 0.6

# Values that carry no identity: they appear in every mostly-empty column.
_EMPTY_MARKERS = frozenset({"", "-", "s/d", "sd", "n/a", "na", "null", "none", "."})


def is_identifiable(column: ColumnProfile) -> bool:
    """Gate 5 — can this column be recognised from its values at all?

    Excludes a column from rename matching rather than from the diff: its
    disappearance is still reported under `removed`, we just refuse to guess
    what it became.
    """
    if column.null_frac is not None and column.null_frac > MAX_IDENTIFIABLE_NULL_FRAC:
        return False
    meaningful = {
        v
        for v in set(column.most_common_vals) | set(column.histogram_sample)
        if v.strip().lower() not in _EMPTY_MARKERS
    }
    return len(meaningful) >= MIN_IDENTIFIABLE_VALUES


def profile_similarity(left: ColumnProfile, right: ColumnProfile) -> float:
    """How alike two columns look, in [0, 1], from their profiles alone.

    Jaccard overlap of the sampled values, nudged by agreement on type and
    null fraction. Deliberately crude: the output feeds a *candidate* list
    that a human or a stricter check confirms, so a rough score that is cheap
    and explainable beats a precise one that is neither.

    Returns 0.0 when neither column has any sampled values — no evidence is
    not the same as no similarity, and the caller's threshold treats it as
    "cannot say".
    """
    left_values = set(left.most_common_vals) | set(left.histogram_sample)
    right_values = set(right.most_common_vals) | set(right.histogram_sample)
    if not left_values or not right_values:
        return 0.0

    overlap = len(left_values & right_values) / len(left_values | right_values)

    same_type = 1.0 if left.pg_type == right.pg_type else 0.0
    if left.null_frac is None or right.null_frac is None:
        null_agreement = 0.5
    else:
        null_agreement = 1.0 - min(abs(left.null_frac - right.null_frac), 1.0)

    return 0.7 * overlap + 0.2 * same_type + 0.1 * null_agreement
