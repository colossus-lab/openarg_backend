# Spec: Ingest Mapa del Estado

**Type**: Reverse-engineered
**Status**: Draft
**Hexagonal scope**: Infrastructure (task only)
**Extends**: [`../spec.md`](../spec.md)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

Monthly ETL that downloads the roster of **national Executive Branch authorities** from `mapadelestado.jefatura.gob.ar` (President, Vice President, Ministers, Secretaries, etc.). It is the complementary source to `002f-staff/` (legislative) and `002l-gobernadores/` (provincial).

## 2. User Stories

- **US-001 (P1)**: As a user, I want to ask who the minister of X is and get the current name.
- **US-002 (P2)**: As a system, I want to refresh monthly after cabinet changes.

## 3. Functional Requirements

- **FR-001**: MUST download CSV from mapadelestado.jefatura.gob.ar.
- **FR-002**: MUST persist to the `cache_mapa_estado` table.
- **FR-003**: MUST register in `datasets` + `cached_datasets`.

## 4. Tech Debt Discovered

- **[DEBT-001]** — **Hardcoded URL** in the task.
- **[DEBT-002]** — **No change detection** between snapshots (unlike `staff_changes`, which does detect additions/removals).
- **[DEBT-003]** — **No domain port**.

---

**End of spec.md**
