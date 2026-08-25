# Plan 023 — Pre-drop Schema Snapshots

**Spec**: [./spec.md](./spec.md)
**Last synced with code**: 2026-08-21

## File map

```
src/app/application/catalog/schema_snapshot.py     capture + diff        (~340 LOC)
src/app/infrastructure/persistence_sqla/alembic/versions/
    2026_08_21_0056_raw_schema_snapshots.py        the table
src/app/infrastructure/celery/tasks/
    collector_tasks.py                             the hook + the flag
    ops_fixes.py                                   the missing audit call
tests/unit/test_schema_snapshot.py                 25 tests
tests/unit/test_ops_cleanup_invariants.py          +1 test for FR-012
```

## The measurement that motivated it

```sql
-- 644 consecutive (v1, v2) pairs in raw_table_versions.
-- 642 have no physical v1 left. Of the 2 that do, 0 changed schema.
WITH pares AS (
  SELECT v1.table_name t1, v1.schema_name s1, v2.table_name t2, v2.schema_name s2
  FROM raw_table_versions v1
  JOIN raw_table_versions v2
    ON v1.resource_identity = v2.resource_identity AND v2.version = v1.version + 1
)
SELECT count(*) FILTER (WHERE NOT EXISTS (
         SELECT 1 FROM pg_tables t WHERE t.schemaname=p.s1 AND t.tablename=p.t1))
FROM pares p;   -- 642
```

And who removed them, from `raw.cache_drop_audit`:

| reason | actor | count |
|---|---|---|
| `retain_raw_versions` | `ops_fixes.retain_raw_versions` | 19.906 |
| **`schema_mismatch_recreate`** | **`collector._to_sql_safe`** | **19.293** |
| `raw_orphan_cleanup` | `ops_fixes.cleanup_raw_orphans` | 17.756 |
| `orphan_cleanup` | `ops_fixes.cleanup_orphan_cache_tables` | 1.200 |

`retain_raw_versions` keeps the last 2 versions (`rn > keep_last`), so it is not
what removed the 642 — a resource with exactly two versions never becomes a
candidate. `schema_mismatch_recreate` is: it is the collector dropping and
recreating in place when a re-ingest does not fit.

## Where the hook goes, and why there

```
collector_tasks._record_cache_drop(engine, table_name=…, reason=…, actor=…)
  │
  ├─ 1. _capture_schema_snapshot(...)      ← NEW, wrapped in try/except
  │       └─ application/catalog/schema_snapshot.capture_table_snapshot
  │             ├─ pg_class + pg_attribute   → columns, types, ordinals
  │             ├─ pg_stats                  → null_frac, n_distinct, MCV, histogram
  │             ├─ pg_class.reltuples        → row estimate (-1 → NULL)
  │             ├─ raw_table_versions        → resource_identity, version (best effort)
  │             └─ INSERT raw.raw_schema_snapshots
  │
  ├─ 2. INSERT raw.cache_drop_audit
  └─ 3. MetricsCollector().record_cache_drop(reason)

…then the caller executes its DROP TABLE.
```

Four call sites reach `_record_cache_drop` and therefore get snapshots for free:

| Call site | Reason |
|---|---|
| `collector_tasks._to_sql_safe` | `schema_mismatch_recreate` |
| `collector_tasks._drop_table_if_exists` | `schema_refresh` |
| `ops_fixes.retain_raw_versions` | `retain_raw_versions` |
| `ops_fixes.cleanup_raw_orphans` | `raw_orphan_cleanup` |
| `ops_fixes.cleanup_empty_raw_tables` | `empty_raw_bloat` |
| `ops_fixes.cleanup_orphan_cache_tables` | `orphan_cleanup` |
| **`ops_fixes.cleanup_invariants`** | **`empty_orphan_invariant` — added here** |

The last one did not call `_record_cache_drop` at all. It was the single
unaudited `DROP TABLE` in the codebase, which is why "no rows in
`cache_drop_audit` since 20 May" had two readings and no way to tell them apart.

## Why guarded twice

`_capture_schema_snapshot` catches its own exceptions **and** the call site wraps
it again. That is not belt-and-braces for its own sake: the helper's guard cannot
cover a failure in its own import, and this code sits in front of a `DROP TABLE`
that has to happen — the collector has already decided to replace the table.
Turning a bookkeeping problem into a stuck ingest would be a strictly worse
outcome than losing a snapshot.

`test_record_cache_drop_survives_a_snapshot_that_raises` pins this. An earlier
draft called the hook outside the try block and that test failed, which is how
the flaw was found.

## The table

```sql
raw.raw_schema_snapshots
  id                 uuid pk default gen_random_uuid()
  schema_name        varchar(63)   not null
  table_name         varchar(63)   not null
  resource_identity  varchar(512)  null      -- best effort, no FK (see spec §4.3)
  version            integer       null
  reason             varchar(128)  not null  -- mirrors cache_drop_audit.reason
  actor              varchar(128)  not null
  column_count       integer       not null
  row_count_estimate bigint        null      -- reltuples, -1 normalised to NULL
  schema_hash        varchar(40)   not null  -- sha1 of sorted names, sans metadata
  columns_profile    jsonb         not null  -- [{name, ordinal, pg_type, null_frac,
                                             --   n_distinct, most_common_vals[],
                                             --   histogram_sample[]}]
  stats_available    boolean       not null
  extra              jsonb         null
  captured_at        timestamptz   not null default NOW()

indexes
  (schema_name, table_name, captured_at DESC)          "what did this table look like"
  (resource_identity, captured_at DESC) WHERE NOT NULL "how did this resource evolve"
  (schema_hash)                                        "show me every shape change"
```

## Bounds

| Limit | Value | Why |
|---|---|---|
| `MAX_PROFILE_VALUES` | 20 | PostgreSQL's default statistics target is 100; a shape comparison needs far fewer, and the payload would otherwise dwarf the row |
| `MAX_VALUE_CHARS` | 120 | Some columns hold paragraphs. The prefix identifies the column; the rest is weight |
| `MAX_PROFILED_COLUMNS` | 300 | The string-NaN bug produced tables with 1.400 ghost columns. Above the cap the shape is still recorded, the profile is skipped, and `extra.profile_skipped` says so |

Roughly 4 KB per snapshot → order-of-100 MB for the whole corpus, against the
terabytes retaining the tables would cost.

## The diff, and what it is for

`diff_snapshots(before, after)` is pure — no engine — so it runs over stored rows
long after both tables are gone. It returns:

```python
{
  "schema_changed": bool,          # schema_hash differs
  "added": [...], "removed": [...],
  "type_changed": [{"column", "from", "to"}],
  "renamed_candidates": [{"from", "to", "similarity"}],
}
```

`profile_similarity` scores two columns as
`0.7 · jaccard(sampled values) + 0.2 · same type + 0.1 · null-fraction agreement`,
and returns `0.0` when neither column has sampled values — *no evidence* is not
*no similarity*, and scoring it high would invent a rename for every unanalysed
table. The threshold for reporting a candidate is `0.6`; see CL-023-001, it is
not yet calibrated against real data.

Deliberately crude. The output is a **candidate list** for a human or a stricter
check to confirm, so cheap and explainable beats precise and opaque.

## Rollback

`OPENARG_SCHEMA_SNAPSHOTS=0` and restart the workers. No code change, no
migration reversal. The table can stay; it simply stops growing.

## Verification performed

- 25 new unit tests, plus 1 added to `test_ops_cleanup_invariants.py` for FR-012.
- Full unit suite: 1.724 passed. The single failure
  (`test_collect_dataset_reroutes_transient_heavy_failure_to_retry_queue`) was
  confirmed pre-existing by stashing these changes and re-running it.
- `ruff check` clean, `ruff format` applied.
- `alembic heads` → `0056 (head)`, single head, chain closed.
- `pg_stats` availability and cost confirmed against production: the profile
  query plans as an index scan on `pg_statistic_relid_att_inh_index`.

## What has to happen next for this to be worth anything

Spec [DEBT-023-001]: nothing reads these rows. The first consumer should be a
periodic job that, per `resource_identity`, diffs consecutive snapshots and
reports the **rate and the classes of change**. That number is the one the
adaptive-repair decision is waiting on — and today nobody can produce it.
