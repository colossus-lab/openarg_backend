# Plan 024 — Drift Classification

**Spec**: [./spec.md](./spec.md)
**Last synced with code**: 2026-08-21

## File map

```
src/app/application/drift/
├── __init__.py            public surface
└── classifier.py          the cascade                              (~240 LOC)

src/app/application/catalog/schema_snapshot.py
    + Provenance           the G1/G3 evidence, with differs_from()
    + is_identifiable      G5
    + ambiguous_renames    G6
    + RENAME_THRESHOLD     named, was a literal

src/app/infrastructure/persistence_sqla/alembic/versions/
    2026_08_21_0057_snapshot_provenance.py

tests/unit/test_drift_classifier.py                                 22 tests
```

## Order of the gates, and why that order

```
G0 identity      ─┐  before anything: comparing two different resources
G2 sibling       ─┘  makes every downstream answer meaningless

no-change check      only meaningful once the two are comparable

G1 provenance        cheapest proof, largest expected yield
G3 pipeline          same parser, different path

fall through → classify → UNEXPLAINED
```

G0/G2 run first because "no change" between two things that are not the same
thing is not an answer. G1 before G3 because a parser-version change subsumes a
path change — if the parser moved, the path moving too says nothing extra.

## The evidence, and where each field comes from

| Field | Source table | Join key |
|---|---|---|
| `parser_version` | `catalog_resources` | `materialized_table_name` |
| `normalization_version` | `catalog_resources` | `materialized_table_name` |
| `layout_profile` | `catalog_resources`, falling back to `cached_datasets` | table name |
| `header_quality` | idem | table name |
| `is_truncated` | `raw_table_versions` | `(schema_name, table_name)` |

All copied onto the snapshot row rather than joined at read time — the same
reason 0056 avoided a foreign key: three of the dropping paths delete those
rows in the same transaction as the drop.

## `Provenance.differs_from` — the one subtle rule

A field that is `None` on either side is **not** a difference. That is
deliberate and load-bearing: most of production predates provenance tracking,
and treating unknown as "same parser" would exonerate nearly every diff and
silently switch the cascade off. FR-004 and
`test_g1_does_not_exonerate_on_unknown_provenance` pin it.

## G5 and G6, concretely

**G5 — sufficiency.** A column enters rename matching only if it has at least
`MIN_IDENTIFIABLE_VALUES = 4` sampled values that are not empty markers
(`""`, `-`, `s/d`, `n/a`, `null`, …) and `null_frac ≤ 0.95`. The production
`pg_stats` is full of columns whose top values are `""` and `-`; without this
they match each other perfectly and manufacture a rename for every pair.

Excluded from **matching**, not from the diff: the column still appears under
`removed`. Refusing to guess is not the same as hiding the change.

**G6 — uniqueness.** All candidates above `RENAME_THRESHOLD` are collected. One
survivor → a rename. More than one → `ambiguous_renames`, reported rather than
dropped, so the ambiguity is visible.

This is what handles `provincia` against `provincia_origen` +
`provincia_destino`: identical domains, both score ~1.0, and a high score is
not evidence when several candidates share it.

## Verdict shape

```python
DriftVerdict(
  verdict:              Verdict.NO_CHANGE | EXONERATED | UNEXPLAINED
  change_class:         ChangeClass | None      # only when unexplained
  exonerated_by:        "G1_provenance" | ...   # only when exonerated
  reason:               str                     # checkable by a person
  diff:                 dict                    # what it was derived from
  gates_not_evaluated:  ["G0_identity", ...]    # abstentions, never passes
)
verdict.is_actionable  →  verdict is UNEXPLAINED
```

## `summarize` is the tuning surface

It breaks a batch down **per gate**, not only per verdict. If G1 clears 90 % of
diffs, that is not a statistic — it is the discovery that the noise was ours,
and it changes what to fix. Without the per-gate count the cascade is a black
box whose thresholds cannot be justified.

## Verification performed

- 22 new tests. Full unit suite: **1.746 passing**. The single failure
  (`test_collect_dataset_reroutes_transient_heavy_failure_to_retry_queue`) was
  confirmed pre-existing by stashing and re-running.
- `ruff check` clean, format applied.
- `alembic heads` → `0057 (head)`, single head.

## What is missing, in order

1. **The consumer.** Pair consecutive snapshots per `resource_identity`,
   classify, aggregate with `summarize`, report. Without it none of this
   produces anything — see spec DEBT-024-003.
2. **G0 / G2 producers.** They abstain on every call today and would eliminate
   the largest measured sources of noise. See DEBT-024-001.
3. **G4 freshness** (`pg_stat_user_tables.last_analyze` at capture time) and
   **G7 persistence** (K consecutive observations). Designed, not built.
4. **A richer profile** for distribution drift — the only route to detecting
   semantic change, which the shape cannot see. That is the whylogs decision,
   and it is a dependency call, not a code call.
