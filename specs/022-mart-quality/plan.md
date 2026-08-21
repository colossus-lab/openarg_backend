# Plan 022 — Mart Quality Audit

**Spec**: [./spec.md](./spec.md)
**Last synced with code**: 2026-08-19

## File map

```
src/app/application/marts/quality/
├── __init__.py                     # public surface (audit_all, summarize, …)
├── check.py                        # MartCheck ABC + _finding() helper          (62 LOC)
├── context.py                      # MartAuditContext, MartColumn, SourceTable  (96 LOC)
├── auditor.py                      # catalog gathering + orchestration         (232 LOC)
└── checks/
    ├── __init__.py                 # build_default_mart_checks() — flat list
    ├── row_count_drift.py          # mart_hidden_despite_rows                   (95 LOC)
    ├── source_coverage.py          # mart_source_coverage                       (66 LOC)
    ├── row_filter.py               # mart_amount_filter_before_aggregation      (92 LOC)
    └── numeric_typing.py           # mart_amount_column_is_text                 (97 LOC)

src/app/infrastructure/celery/tasks/mart_audit_tasks.py   # openarg.audit_marts
```

810 LOC total in the application layer.

## Why it does not reuse `validation.Detector`

The spec's sibling module (013) has an ABC that caps a detector at **one**
finding. A mart legitimately yields several problems at once — three columns
typed TEXT is three findings, not one — so `MartCheck.run()` returns a
**list**. Everything downstream of the finding (`Finding`, `Severity`, `Mode`,
`persist_findings`, `resolve_missing`) is reused unchanged.

`MartAuditContext` is likewise deliberately **not** `ResourceContext`: that one
describes a downloaded file (`raw_bytes`, `zip_member_names`, `http_status`) and
roughly none of it applies to a materialized view.

## How the context is gathered

`collect_contexts(engine)` runs **five catalog queries once**, then fans the
result out to every mart — so a sweep over 71 marts costs one pass, not one per
mart per check.

| Query | Source | Feeds |
|---|---|---|
| `_MART_ROWS_SQL` | `mart_definitions` | registration, resolved SQL, `serving_blocked` |
| `_MART_STATS_SQL` | `pg_class.reltuples` for `mart.*` | `approx_row_count` (`-1` → `None`) |
| `_COLUMNS_SQL` | `pg_attribute` + `format_type()` | real Postgres types per column |
| `_SOURCE_STATS_SQL` | `pg_class.reltuples` for `raw.*`/`public.*` | source row estimates |
| `_TRAFFIC_SQL` | `query_analytics`, 30 d, `served_table LIKE 'mart.%'` | `hits_30d`, `success_rate_30d` |

Two derived values are parsed out of the resolved SQL with regexes rather than
re-resolving macros (which would need the engine, the live-version tables and a
column introspection pass just to learn what a mart was built from):

- `_SOURCE_REF_RE` → `raw."name"` / `public."name"` references → `source_tables`
- `_COVERAGE_RE` → the `/* macro_coverage: kept N of M */` marker that
  `sql_macros._build_union` writes at line 328 → `candidate_table_count` /
  `kept_table_count`

## The idempotency tuple, and how it bit

`persist_findings` upserts on
`(resource_id, detector_name, detector_version, mode, input_hash)`. The task
builds `input_hash` as `"<detector>:<discriminator>"`, where
`finding_discriminator()` picks, in order:

1. `payload["finding_key"]` — set by a check that emits several findings,
2. `payload["column"]` — the per-column checks,
3. the mart id — the floor for checks emitting exactly one.

**This is not cosmetic.** On the auditor's first real run,
`mart_hidden_despite_rows` emitted both its signals for the same mart without
distinct keys: the WARN about a failed refresh overwrote the CRITICAL about
52 million unreachable rows, while the summary went on counting both. That is
why `_finding()` takes a `key=` and why FR-008/FR-009 exist.

## Wire-in points

| Where | What |
|---|---|
| `celery/app.py:168` | task route → `ingest` queue |
| `celery/app.py:392` | beat: `crontab(hour=3, minute=45)` — after `refresh-via-b-marts-daily` at 03:00, so it audits the state users will be served |
| `marts/mart.py:231-256` | YAML loader reads `serving.blocked` / `serving.blocked_reason`; raises `MartParseError` when blocked without a reason |
| `mart_tasks.py::_upsert_mart_definition` | persists both fields on every build — that is what makes the block survive `DROP + CREATE` |
| `pg_sandbox_adapter.py:212` | `_blocked_mart_error()` — execution-time enforcement (FR-016) |
| 8 discovery sites | `NOT COALESCE(serving_blocked, FALSE)` — see spec [DEBT-022-003] for the 2 that lack it |

## Migrations

| Rev | What | Why |
|---|---|---|
| **0054** | `mart_definitions.serving_blocked BOOLEAN NOT NULL DEFAULT FALSE` + `serving_blocked_reason TEXT` | The only gate before it was `last_row_count > 0`, which covers *empty*. There was no way to say "has rows, but they are wrong". |
| **0055** | Extends the `ck_ingestion_findings_mode` CHECK with `'mart_audit'` | The mode vocabulary from 0033 enumerates the four ingestion phases. Folding mart audits into `state_invariant` would work at the cost of making the two indistinguishable in every ops query. |

## Task contract

```
openarg.audit_marts(persist: bool = True) -> dict
  queue: ingest · soft_time_limit 600 · time_limit 900

  returns {
    marts_audited, marts_with_findings, findings,
    by_severity: {critical: N, warn: N, …},
    affected_marts: [mart_id, …],
    persisted, resolved, persist
  }
```

Any critical finding logs at `WARNING` on purpose (SC-006).

## Tests

| File | Covers |
|---|---|
| `test_mart_quality_checks.py` | the four checks in isolation |
| `test_mart_rowcount_drift.py` | hidden-despite-rows, both signals |
| `test_mart_row_filters.py` | WHERE-before-GROUP-BY detection |
| `test_mart_audit_finding_keys.py` | FR-008/FR-009 — the collision that ate a CRITICAL |
| `test_mart_serving_block.py` | YAML load, persistence across rebuild, discovery filter |
| `test_sandbox_blocked_mart_execution.py` | FR-016 — execution-time enforcement |

## Adding a check

1. Subclass `MartCheck` in `checks/`, set `name` / `version` / `severity`.
2. Narrow with `applicable_to()` — a check that cannot say anything about a
   mart must skip it, not emit a null finding.
3. Return a **list**. Set `key=` on `_finding()` if more than one is possible.
4. Write the `remediation` in the payload as an instruction, not a diagnosis.
5. Instantiate in `build_default_mart_checks()`, in reading order.
6. Bump `version` when the logic changes: it is part of the idempotency tuple,
   so a bumped version re-opens findings under the new rule and
   `resolve_missing` closes the old ones.

## Known gaps

Carried from spec §9 — the two worth acting on first:

- **[DEBT-022-001]** nothing reads the findings. The sweep has run nightly
  since 2026-07-27 and the four `build_failed` marts in production on
  2026-08-19 were found by a manual audit.
- **[DEBT-022-002]** a `built` mart that resolves to **zero** rows produces no
  finding at all, which is the state of `presupuesto_consolidado` and
  `pobreza_indec_aglomerados` in production today.
