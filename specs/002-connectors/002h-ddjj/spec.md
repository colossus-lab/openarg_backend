# Spec: Connector DDJJ (Declaraciones Juradas Patrimoniales)

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-11
**Hexagonal scope**: Application + Infrastructure
**Extends**: [`../spec.md`](../spec.md)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

In-memory connector over **195 patrimonial asset declarations (DDJJ) of National Diputados** (year 2024). Supports search by name/CUIT, ranking by patrimonio/income/assets, aggregate statistics, and details per person. The data lives in a **static, preloaded JSON file**; there is no automated ingestion.

It is the only connector in the system with an **explicitly incomplete dataset** — only 195 diputados, it does not include senadores, executive, or judges. It includes defensive logic to prevent the LLM from hallucinating answers about people outside the dataset.

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **DDJJ** | Declaración Jurada Patrimonial — mandatory financial disclosure for Argentine public officials. |
| **Patrimonio** | Total sum of assets (at the start or close of the year). |
| **Variación patrimonial** | `patrimonio_cierre - patrimonio_inicio`. |
| **Bienes** | Assets (real estate, movable goods, cash, debts). |
| **Ingresos del trabajo neto** | Reported annual income. |

## 3. User Stories

### US-001 (P1) — Ranking of diputados by patrimonio
**As** a journalist, **I want** to see the 10 diputados with the highest patrimonio at year-end.

### US-002 (P1) — Detail for a legislator
**As** a researcher, **I want** the full profile of a specific diputado.

### US-003 (P2) — Search by CUIT
**As** an analyst, **I want** to find an official by their CUIT.

### US-004 (P2) — Aggregate statistics
**As** a user, **I want** to know the median patrimonio of diputados.

### US-005 (P1) — Fail-safe for people outside the dataset
**As** the system, **I need** to respond "NOT IN DATASET" in an informative way when someone asks about a senador or minister, **to** prevent the LLM from hallucinating.

## 4. Functional Requirements

- **FR-001**: MUST load 195 records from `src/app/infrastructure/data/ddjj_dataset.json` lazily on first call.
- **FR-002**: MUST implement exponential backoff on load errors (60s → 120s → 240s → cap 3600s).
- **FR-003**: MUST support search by name (normalized: accent-stripped) or CUIT (with `-` stripping).
- **FR-004**: MUST support ranking by `patrimonioCierre`, `ingresosTrabajoNeto`, `bienesCierre` with asc/desc order.
- **FR-004a**: Ranking records MUST be **compact** — only top-level numeric and identity fields (name, cargo, organismo, patrimonio_cierre, bienes_cierre, deudas_cierre, variacion_patrimonial, cantidad_bienes). The full `bienes_detalle` asset list MUST be omitted from ranking rows so the analyst can fit all N requested records in its output budget. (FIX-007, 2026-04-11: the verbose shape bloated each record to 5–9KB and caused the LLM to truncate the answer after 4 rows, fabricating a "records 5–10 incompletos" disclaimer that the prompt explicitly forbids.)
- **FR-005**: MUST return an informative "NOT FOUND" with an explicit message about dataset scope when there is no match.
- **FR-006**: MUST pre-compute aggregate stats (min, max, mean, median patrimonio) in a single pass.
- **FR-007**: MUST group bienes by type when formatting the `DataResult`. (Non-ranking paths only: search, get_by_name, stats. Ranking uses the compact shape from FR-004a.)

## 5. Success Criteria

- **SC-001**: Search responds in **<100ms** (data in memory).
- **SC-002**: Initial load completes in **<500ms**.
- **SC-003**: **Zero false positives** in answers about people outside the dataset (the LLM must not invent).
- **SC-004**: Exponential backoff prevents hot-loop errors if the file does not exist.

## 6. Assumptions & Out of Scope

### Assumptions
- The JSON file is kept up to date manually (for now).
- 195 records fit comfortably in memory.
- Users understand the dataset is partial.

### Out of scope (this iteration)
- **Senadores**, **executive**, **judges** — not in the dataset.
- Declarations from years other than 2024.
- Auditing or cross-validation against other sources.
- Detection of suspicious patrimonial changes.
- DB persistence (everything is in-memory).

## 7. Open Questions

- **[RESOLVED CL-001]** — Expansion of the DDJJ dataset to other branches (senadores, executive, judges) is **accepted as out of scope for now**. The current dataset (195 diputados) is sufficient for current use cases.
- **[NEEDS CLARIFICATION CL-002]** — How is the JSON refreshed when new declarations are available? Manual process or script?
- **[RESOLVED CL-003]** — **No abstract port exists.** `src/app/infrastructure/adapters/connectors/ddjj_adapter.py` defines `class DDJJAdapter:` with no inheritance, and there is no file under `src/app/domain/ports/` with `IDDJJ`, `DDJJPort`, `DdjjPort`, `IDdjj` or any similar interface. Same hexagonal violation as BCRA — already tracked as `DEBT-001`. (resolved 2026-04-11 via code inspection)
- **[NEEDS CLARIFICATION CL-004]** — The regex for stripping "EN EL PAÍS/EXTERIOR" from asset types is fragile. Is the upstream format consistent?

## 8. Tech Debt Discovered

- **[DEBT-001]** — **No port in `domain/ports/connectors/ddjj.py`** — same problem as BCRA. Possible violation of the hexagonal pattern.
- **[DEBT-002]** — **Static JSON file** with no automated refresh mechanism — stale data risk.
- **[DEBT-003]** — **Hardcoded exponential backoff** (60s base, 3600s cap). Not configurable.
- **[DEBT-004]** — **Naive accent stripping** for name matching — does not handle typos ("Peron" vs "Perón" works; "Beron" does not).
- **[DEBT-005]** — **Fragile bienes type regex** (`EN EL PAÍS/EXTERIOR`). Depends on exact upstream format.
- **[DEBT-006]** — **Stats calculation mixes min/max/sum/sort** in a single pass — hard to read and maintain.
- **[DEBT-007]** — **No audit trail** or versioning — impossible to track historical patrimonio changes.
- **[DEBT-008]** — **Explicitly partial dataset** (195 diputados) with no visible banner/header warning the user about scope.

---

**End of spec.md**
