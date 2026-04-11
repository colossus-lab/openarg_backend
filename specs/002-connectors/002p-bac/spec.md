# Spec: Ingest BAC (Boletín Oficial / Autoridades)

**Type**: Reverse-engineered
**Status**: Draft
**Hexagonal scope**: Infrastructure (task only)
**Extends**: [`../spec.md`](../spec.md)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

Weekly ETL that ingests data from the **BAC** (Boletín Oficial / Compras Argentinas) system. It persists the data in `cache_bac_*` tables for querying via the sandbox.

## 2. User Stories

- **US-001 (P1)**: As an analyst, I want to query recent public procurements and tenders.

## 3. Functional Requirements

- **FR-001**: MUST download data from BAC weekly.
- **FR-002**: MUST persist to `cache_bac_*` tables.
- **FR-003**: MUST register in `datasets` + `cached_datasets`.

## 4. Open Questions

- **[RESOLVED CL-001]** — **BAC = Buenos Aires Compras** (procurement system of the City of Buenos Aires government). It is NOT Boletín Oficial nor national COMPR.AR. Source: `https://cdn.buenosaires.gob.ar/datosabiertos/datasets/ministerio-de-economia-y-finanzas/buenos-aires-compras`. Format: **OCDS** (Open Contracting Data Standard). See `bac_tasks.py:23`.
- **[RESOLVED CL-002]** — Downloads **4 OCDS CSVs**: `tender.csv`, `parties.csv`, `contracts.csv`, `award.csv`. Each is persisted in its own table: `cache_bac_tenders`, `cache_bac_parties`, `cache_bac_contracts`, `cache_bac_awards`. See `bac_tasks.py:25-30` (`BAC_FILES` constant).
- **[PARTIAL CL-003]** — Volume is not measured in the code. Being Buenos Aires Compras OCDS data, the volume depends on the pace of tenders/contracts in the City of Buenos Aires government. No historical metric. To estimate empirically: `SELECT COUNT(*) FROM cache_bac_tenders` in production.

## 5. Tech Debt Discovered

- **[DEBT-001]** — **Ambiguity of the name "BAC"** — no clear docstring about the source.
- **[DEBT-002]** — **No domain port**.
- **[DEBT-003]** — **No specific metrics**.

---

**End of spec.md**
