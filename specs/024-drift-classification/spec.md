# Spec 024 — Drift Classification (the exoneration cascade)

**Type**: Forward-engineered
**Status**: Implemented 2026-08-21 (migration 0057)
**Last synced with code**: 2026-08-21
**Hexagonal scope**: Application (pure) + one migration
**Related plan**: [./plan.md](./plan.md)
**Sister specs**: [023-schema-snapshots](../023-schema-snapshots/spec.md) (the evidence this reads),
[013-ingestion-validation](../013-ingestion-validation/spec.md) (where verdicts will land)

---

## 1. Context & Purpose

Spec 023 made shape changes visible. This module decides what one *was*.

The obvious design — a classifier that answers "is this upstream drift?" —
cannot be made safe. Every increase in its sensitivity buys false positives,
and a false positive here eventually triggers a repair on data that was fine.

So it is built inverted. **Each gate can only prove that a diff is *not*
upstream drift, or say it cannot tell.** None can assert that a diff *is*
drift. A component that can only prove the negative can only produce false
negatives — and a false negative here means "we did not flag something we could
have", which is exactly the status quo. The asymmetry is structural, not a
precaution.

What survives every gate is not a conclusion. It is the small set of changes
nothing could explain away, which is the only set worth a human's or a model's
attention.

**Why this matters more than accuracy.** Measured on production: of 78
multi-variant table families, 29 were born the same day (siblings, not drift);
5.485 URLs are registered under more than one `source_id`; ~9.500
`parse_repair` operations have renamed columns; and five collector transforms
rewrite schemas with no upstream involvement. A detector that ignores all that
would report mostly noise, and a repair layer fed by it would act on mostly
noise.

---

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Gate** | One check that can exonerate a diff or abstain. Never accuses. |
| **Exonerate** | Prove, from evidence, that a diff is not an upstream format change. |
| **Verdict** | `no_change`, `exonerated` (with the gate that fired) or `unexplained`. |
| **Actionable** | `unexplained` — the only verdict that should ever reach a person or a model. |
| **Provenance** | Which of *our* versions produced a shape: parser, normalization, parse path, truncation. |
| **Change class** | The shape of an unexplained change: additive, removal, type change, rename, reshape, semantic. |
| **Abstain** | A gate whose input the caller did not supply. Recorded, never treated as a pass. |

---

## 3. The gates

| Gate | Exonerates when | Evidence |
|---|---|---|
| **G0** identity | The two snapshots are not the same logical resource | Caller-supplied (`DriftContext`) |
| **G2** sibling | They are different files of one bundle, not one file over time | Caller-supplied |
| **G1** provenance | Our parser or normalization version changed between captures | On the snapshot (mig 0057) |
| **G3** pipeline | Same parser, different path: layout profile, header quality or truncation moved | On the snapshot (mig 0057) |
| **G5** sufficiency | A column has too little signal to be matched at all | In `diff_snapshots` |
| **G6** uniqueness | A rename match is ambiguous across several candidates | In `diff_snapshots` |

G0 and G2 need data no snapshot holds — the resource's canonical URL and
content hash. They are modelled as optional context so the verdict can record
that they **abstained**, rather than silently assuming they passed.

---

## 4. Functional Requirements

- **FR-001**: No gate may return a verdict that asserts a diff *is* upstream
  drift. The only outcomes are exonerate, abstain, or fall through.
- **FR-002**: `classify_change` MUST be pure — no database, no clock, no
  network — so it can run over stored snapshots long after both tables are gone.
- **FR-003**: A gate whose input is `None` MUST be recorded in
  `gates_not_evaluated` and MUST NOT exonerate. Absence of evidence is not
  evidence of absence.
- **FR-004**: G1 MUST NOT treat an unknown provenance field as agreement. Most
  of production predates provenance tracking; treating `None` as "same parser"
  would exonerate nearly everything and quietly disable the cascade.
- **FR-005**: G0 and G2 MUST run before the no-change check. "No change"
  between two things that are not comparable is not a meaningful answer.
- **FR-006**: G5 MUST exclude a column from **rename matching only** — its
  disappearance still appears under `removed`. Refusing to guess is not the
  same as hiding the change.
- **FR-007**: G6 MUST require a **unique** match above the threshold. Several
  candidates sharing a high score is ambiguity, not evidence.
- **FR-008**: An ambiguous match MUST be reported under `ambiguous_renames`,
  not dropped. A silently discarded candidate is indistinguishable from one that
  was never found.
- **FR-009**: Every verdict MUST carry a `reason` a person can check, and the
  `diff` it was derived from.
- **FR-010**: `summarize` MUST break results down **per gate**, not only by
  verdict. The per-gate count is what makes the cascade tunable.
- **FR-011**: Snapshots MUST record `parser_version`, `normalization_version`,
  `layout_profile`, `header_quality` and `is_truncated`, copied onto the row so
  the record stays self-contained after its sources are deleted.

---

## 5. Success Criteria

- **SC-001**: A diff across differing parser versions is exonerated by G1.
- **SC-002**: A wide→long change caused by `unpivot_if_time_pivoted` crossing
  its threshold is exonerated by G3.
- **SC-003**: A removed `provincia` against both `provincia_origen` and
  `provincia_destino` yields zero rename candidates and one ambiguity.
- **SC-004**: A two-valued column (`sexo`) never produces a rename candidate.
- **SC-005**: With no context supplied, `gates_not_evaluated` names G0 and G2.
- **SC-006**: `summarize` over a batch reports how many diffs each gate cleared.

---

## 6. Assumptions & Out of Scope

**Assumptions**

- Provenance is populated going forward. Snapshots captured before migration
  0057 have `None` and, by FR-004, cannot be exonerated by G1 — they will read
  as unexplained more often than they should. That is the safe direction.

**Out of scope**

- **Deciding what to do.** The cascade classifies; policy per change class is
  a separate decision and belongs in configuration, not here.
- **Calling an LLM.** Nothing in this module talks to a model. What survives is
  handed on; who handles it is not decided here.
- **G4 (freshness) and G7 (persistence).** G4 needs `pg_stat_user_tables.
  last_analyze` at capture time; G7 needs accumulated history. Both are
  designed and neither is implemented.

---

## 7. Open Questions

- **[NEEDS CLARIFICATION CL-024-001]** — `RENAME_THRESHOLD = 0.6`,
  `MIN_IDENTIFIABLE_VALUES = 4` and `MAX_IDENTIFIABLE_NULL_FRAC = 0.95` are
  chosen so that identical value sets clear them and trivial columns do not.
  None is calibrated against real diffs, because there are none yet. First
  recalibration should come from the shadow run, not from reasoning.
- **[NEEDS CLARIFICATION CL-024-002]** — G3 exonerates on any change to
  `layout_profile`. A portal change could plausibly *cause* the parse path to
  change, in which case G3 would clear a real drift. The conservative reading
  was chosen deliberately; whether it is too conservative is an empirical
  question.

---

## 7bis. First measurement (2026-08-21, staging)

The point of the module is to produce a number nobody could produce before.
Here is the first one, and it does not say what the project assumed.

| | |
|---|---|
| Pairs evaluated | 203 (91 same-table, 112 version) |
| `no_change` | 198 |
| Actionable (`UNEXPLAINED`) | 5 — 1 reshape, 4 renames |
| Exonerated by a gate | **0** |

All five actionable findings were then read by hand. **All five are our own
parser, not upstream drift.**

- `datos_gob_ar::energia_ab782c06` v2→v3 — v2's "columns" included
  `2026-05-04`, `col_0`, a sentence of release notes and a URL. Those are a
  title block parsed as a header. v3 dropped them and gained the `_source_*`
  metadata columns. The reshape is our parse getting *better*.
- Four PAMI `compras_y_contrataciones` resources, v1→v2 — v1 had
  `destino`, `estado`, `expediente`, `fecha_de_apertura`; v2 has
  `LISTADO DE LLAMADOS DE LICITACIONES PUBLICAS - AÑO 2018 / G_10`. That is
  the `title_as_columns` bug [021-parser-hardening](../021-parser-hardening/spec.md)
  already has a repair for. The change is a regression of ours.

Precision for the question the module exists to answer — *did the upstream
format change?* — is **0 of 5** in this sample. That is the finding, and it is
why shadow mode was the right default. Had a notification been wired on day
one, its first five alerts would all have been noise for that question.

It is a small sample and it is one environment. It is also the first evidence
this project has ever had on the subject, and it points at a specific defect
rather than a vague "needs tuning": see DEBT-024-005.

---

## 8. Tech Debt Discovered

- **[DEBT-024-001] — G0 and G2 have no producer.** The two gates that would
  eliminate the largest measured sources of noise (5.485 duplicate identities,
  366 sibling variants) abstain on every call, because nothing computes a
  content-derived identity or resolves a snapshot back to its source URL. The
  cascade works without them and is materially weaker.

- **[DEBT-024-002] — Semantic change is undetectable here.** Same columns,
  same types, values that moved (pesos → thousands of pesos). The shape is
  identical, so `ChangeClass.SEMANTIC` can only be reached as a fallthrough and
  in practice never is. It is the change that produces confident wrong answers,
  and detecting it needs distribution comparison — which needs a richer profile
  than `pg_stats` gives.

- **[DEBT-024-003] — RESOLVED 2026-08-21.** The consumer is
  `openarg.report_schema_drift` (weekly, Mondays 06:15 ART), which pairs
  consecutive snapshots, calls `classify_change` on each pair and reports
  `summarize()` broken down per gate. It ran for the first time on staging the
  same day and returned honestly: two snapshots, no consecutive pair, nothing
  comparable yet.

- **[DEBT-024-004] — The gate that would matter most is the one we cannot
  build yet.** G1 (provenance) is the only gate with a real producer today, and
  it can only exonerate a change when the parser version moved. The upstream
  case this project actually cares about — a portal regenerating its
  `source_id` and re-publishing the same data under a new identity — is exactly
  what G0 would catch, and G0 abstains on every call. Measured on staging on
  2026-08-21 while restoring collection: 652 live tables holding 99.2M rows had
  been orphaned by precisely that re-keying, and nothing in this module or
  anywhere else in the system recognised them as the same resource. Until an
  identity-resolution producer exists, this classifier's verdicts on a re-keyed
  resource will read UNEXPLAINED when the honest verdict is "we never knew it
  was the same thing".

- **[DEBT-024-005] — G1 cannot speak about a historical pair, and that is why
  all five findings were noise.** The gate exonerates when *our* parser moved,
  which is exactly what happened in all five cases — yet it fired zero times.
  Two reasons, both structural:

  1. Provenance is read from `catalog_resources` **at snapshot time**, not
     recorded at ingest time. For a pair captured today, both sides carry
     today's value.
  2. Where a value does exist for the older side it is `legacy:unknown`, a
     placeholder that names no parser. Measured: of 165 version pairs, 163
     have "differing" provenance, and the difference is almost always
     `None` vs `legacy:unknown`. `Provenance.differs_from` deliberately treats
     a `None` as *not* a difference — the right rule, since the alternative is
     inventing exonerations — so the gate abstains.

  The fix is to stamp the parser and normalization versions onto
  `raw_table_versions` at write time, so a version carries the provenance of
  the run that produced it rather than of whenever someone looked. Until then
  G1 is decorative on anything but a pair captured across a deploy, and the
  cascade will keep returning UNEXPLAINED for changes that are ours.