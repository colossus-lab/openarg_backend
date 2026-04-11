# OpenArg Specs

Project specifications following **Spec-Driven Design** adapted to hexagonal architecture.

## Objective

This directory contains the structural documentation of OpenArg **reverse-engineered** from the code: it describes what each component does, how it is built today, and what tech debt exists. It does not modify code; it only documents reality.

Going forward it serves as the basis for **forward SDD**: new features are specified before being coded, reusing the conventions established here.

## Structure

```
specs/
├── README.md                   # this file
├── constitution.md             # non-negotiable project principles (TBD)
├── 000-architecture/           # macro view of the system (TBD)
├── 001-query-pipeline/         # LangGraph pipeline (TBD)
├── 002-connectors/             # generic connector pattern + each specific one
│   ├── spec.md                 # abstract connector pattern (TBD — extract when there are ≥2)
│   ├── 002a-bcra/              # pilot — first specified connector
│   │   ├── spec.md             # WHAT / WHY (tech-agnostic)
│   │   └── plan.md             # HOW (as-built)
│   ├── 002b-ckan/              # (future)
│   ├── 002c-series-tiempo/     # (future)
│   └── ...
├── 003-auth/                   # NextAuth + JWT + API keys (TBD)
├── 004-semantic-cache/         # (TBD)
└── ...
```

## Spec format

Each feature has **two files**: `spec.md` (the what) and `plan.md` (the how). The separation is the same one used by GitHub Spec Kit and respects the hexagonal boundary: `spec.md` lives in the domain language, `plan.md` speaks of infrastructure.

### `spec.md` — structure

```markdown
# Spec: <Name>

**Type**: Reverse-engineered | Forward
**Status**: Draft | Reviewed | Canonical
**Last synced with code**: YYYY-MM-DD
**Hexagonal scope**: Domain | Domain+App | Full-stack | Adapter-only

## 1. Context & Purpose
One paragraph. What this feature does for the user/system. Domain level, no technology.

## 2. Ubiquitous Language
Terms with their definitions. Domain only.

## 3. User Stories
Prioritized stories (P1/P2/P3). Each independently testable.
- **US-NNN (Pn)**: As a <role>, I want <action>, so that <benefit>.
  - Trigger, actors, preconditions, happy path (Given/When/Then), edge cases

## 4. Functional Requirements
Numbered list. Each FR is testable.
- **FR-NNN**: The system MUST ...

## 5. Success Criteria
Observable, tech-agnostic metrics.
- **SC-NNN**: ...

## 6. Assumptions & Out of Scope
What we take for granted and what doesn't belong.

## 7. Open Questions
- **[NEEDS CLARIFICATION]** questions that remain open

## 8. Tech Debt Discovered
- **[DEBT-NNN]** findings from reverse engineering (only in reverse-eng specs)
```

### `plan.md` — structure

```markdown
# Plan: <Name>

**Related spec**: ./spec.md
**Type**: Reverse-engineered | Forward
**Status**: Draft | Reviewed | Canonical
**Last synced with code**: YYYY-MM-DD

## 1. Hexagonal Mapping
What lives in what layer.
- **Domain**: entities, ports (or [MISSING])
- **Application**: use cases, orchestration
- **Infrastructure**: adapters, Celery tasks, persistence
- **Presentation**: HTTP routes (if applicable)

## 2. As-Built Flow
ASCII diagram or sequence describing the real flow.

## 3. Ports & Contracts
Interfaces consumed/exposed.

## 4. External Dependencies
URLs, auth, rate limits, timeouts, retry policies.

## 5. Persistence
Tables read/written, key columns, referenced migrations.

## 6. Async / Scheduled Work
Celery tasks (name, schedule, queue, retry), LangGraph nodes.

## 7. Observability
Metrics, logs, audit trail.

## 8. Source Files
Map to real files (with paths relative to `src/`).

## 9. Deviations from Constitution
Known violations of `constitution.md` with justification or debt ticket.
```

## Format rules

1. **`spec.md` does not mention technology**. If "HTTP", "SQL", "Celery", "BCRA API" appears — it goes in `plan.md`.
2. **`plan.md` does not invent requirements**. It only describes what exists (reverse) or what was decided (forward).
3. **Every tech debt finding is explicitly marked** with `[DEBT-NNN]` and documented in `spec.md` section 8.
4. **Ambiguities are marked with `[NEEDS CLARIFICATION]`** — they are not resolved by inventing, they are opened as questions.
5. **Child specs inherit from parent specs** with an `## Extends` section at the top — they don't duplicate content.
6. **Traceability always present**: section 8 of `plan.md` points to real files with paths. If a spec cannot point to the code, it is outdated.

## Current state

| Spec | Status | Notes |
|---|---|---|
| `constitution.md` | TBD | Extract from `CLAUDE.md` + `MEMORY.md` + code |
| `000-architecture/` | TBD | Macro as-built |
| `002-connectors/002a-bcra/` | **Pilot** | First validation of the format |
| Rest | TBD | Scaled up after validating the pilot |
