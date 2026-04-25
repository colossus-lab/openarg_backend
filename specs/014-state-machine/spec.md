# Spec 014 — `cached_datasets` State Machine (WS0.5)

**Type**: Forward-engineered (collector_plan.md WS0.5)
**Status**: Implemented (code only — pending operator review and rollout)
**Hexagonal scope**: Application (state machine + enforcer) + Infrastructure (sweep + DB trigger)
**Related plan**: [./plan.md](./plan.md)
**Implements**: WS0.5 of [collector_plan.md](../../../collector_plan.md)
**Sister spec**: [013-ingestion-validation](../../013-ingestion-validation/spec.md) (shares `ingestion_findings` audit trail under `mode='state_invariant'`)

---

## 1. Context & Purpose

The April 2026 production radiografía showed that **the largest fault family is the orchestrator, not the files**: 305 cases = 30% of all failures. Concretely:

- 79 `schema_mismatch` rows in a retry loop with `retry_count=4` (post-March fix only covered `SoftTimeLimitExceeded`).
- 134 `Exhausted retries while stuck in downloading`.
- 144 `Table missing: marked for re-download` (root cause unknown).
- 43 stuck in `pending` for 1–3 weeks.

This spec defines the explicit transition table, the invariants to enforce, and the closed `error_category` taxonomy that replaces free-text `LIKE` queries.

## 2. State Machine

### States
`pending → downloading → parsing → materialized → ready`
plus terminal `permanently_failed` and exception lanes `error`, `schema_mismatch`.

### Allowed transitions (frozen set)
- `pending → downloading | permanently_failed`
- `downloading → parsing | error | schema_mismatch | permanently_failed | ready` (legacy fast-path)
- `parsing → materialized | error`
- `materialized → ready | error`
- `ready → error | downloading` (re-materialize)
- `error → pending | downloading | permanently_failed`
- `schema_mismatch → error | permanently_failed`
- **`permanently_failed`** is **terminal** — no outgoing transitions.

### Invariants (sweep-enforced + DB trigger)
- **I1** — `retry_count >= MAX_TOTAL_ATTEMPTS ⇒ status='permanently_failed'`. Enforced by both `enforce_cached_datasets_retry_invariant` trigger AND `StateMachineEnforcer`. Trigger short-circuits to terminal, except for an explicit manual reset escape hatch (`status='pending' AND retry_count=0`).
- **I2** — `status='ready' ⇒ table referenced exists`. Sweep registers violations as `invariant_ready_missing_table` (auto_fixable=False; root cause uninvestigated, so we don't auto-flip).
- **I3** — `status='pending' AND updated_at < now() - 7 days ⇒ permanently_failed` (timeout).
- **I4** — `status='schema_mismatch' AND retry_count >= MAX_TOTAL_ATTEMPTS ⇒ permanently_failed`.

## 3. `error_category` Taxonomy (closed)

Replaces 50+ unique free-text `error_message` strings with one of:

`download_network`, `download_http_error`, `download_timeout`, `parse_format`, `parse_encoding`, `parse_schema_mismatch`, `materialize_table_collision`, `materialize_disk_full`, `validation_failed`, `policy_too_large`, `policy_non_tabular`, `metadata_no_url`, `orchestration_recovery_loop`, `orchestration_table_missing`, `unknown`.

Backfill in migration 0034 reclassifies the ~1034 existing messages using a first-match-wins rule table.

## 4. Functional Requirements

- **FR-001**: `cached_datasets` MUST carry `error_category` enum (CHECK-constrained).
- **FR-002**: A BEFORE INSERT/UPDATE trigger MUST enforce I1.
- **FR-003**: `permanently_failed` rows MUST be immutable to the trigger, except for the explicit manual reset transition `pending + retry_count=0`.
- **FR-004**: `StateMachineEnforcer.scan()` MUST be read-only — never mutates state.
- **FR-005**: `StateMachineEnforcer.enforce(dry_run)` MUST default to `dry_run=True` (registers findings only). Activation by env `OPENARG_WS0_5_AUTO_ENFORCE=1`.
- **FR-006**: Auto-corrections MUST apply only to deterministic violations (I1, I3, I4). I2 (`ready_missing_table`) MUST NEVER be auto-flipped — root cause is uninvestigated.
- **FR-007**: Every violation MUST also be persisted as a finding under `mode='state_invariant'` so dashboards line up with WS0.
- **FR-008**: Migration 0034 historical healing MUST run in bounded batches rather than one unbounded `UPDATE`, to keep deploy-time lock duration bounded if the violation population grows beyond the current ~250 rows.

## 5. Success Criteria

- **SC-001**: After migration 0034 + first auto-enforce sweep, the 250 prod rows with `retry_count>=4 AND status='error'` are flipped to `permanently_failed`.
- **SC-002**: After first sweep, the 79 `schema_mismatch` loops, 134 stuck-downloading and 43 stuck-pending have all moved out of their stuck states.
- **SC-003**: Operational queries that grouped by `LIKE '%schema_mismatch%'` are replaced by `WHERE error_category='parse_schema_mismatch'`.
- **SC-004**: A second sweep against unchanged data flips ZERO additional rows (idempotency).

## 6. Out of Scope

- Investigating the root cause of the 144 `Table missing` cases (separate WS — `pg_event_trigger` audit in `cache_drop_audit` is a starting point, see migration 0036).
- Per-portal retry policy customization.

## 7. Tech Debt

- **DEBT-014-001**: `MAX_TOTAL_ATTEMPTS=5` is duplicated in `collector_tasks.py`, `cached_dataset_enforcer.py` and migration 0034 trigger. A change requires touching all three.
- **DEBT-014-002**: The trigger encodes `retry_count >= 5` as a magic number — no `pg_settings` lookup.
