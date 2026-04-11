# Spec: Dataset Catalog

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10
**Hexagonal scope**: Domain + Infrastructure + Presentation
**Parent module**: [../spec.md](../spec.md)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

Canonical catalog of datasets indexed by OpenArg. Exposes the `Dataset` domain entity, the `IDatasetRepository` port, and the HTTP endpoints that list, stat, and download datasets. This sub-module owns **metadata only** — the ingestion pipeline (scrape → collect → embed) lives in `006b-ingestion/`.

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Dataset** | Canonical metadata unit: title, description, portal, format, columns, etc. |
| **Source ID** | Upstream portal identifier used together with `portal` as the natural key. |
| **Portal** | Source portal name (datos.gob.ar, CABA, etc.). |
| **Catalog** | The collection of all `Dataset` rows across portals. |

## 3. User Stories

### US-001 (P1) — Listable catalog
**As** a user, **I want** to list the indexed datasets filtered by portal.

### US-002 (P1) — Catalog stats
**As** an operator, **I want** to see how many datasets there are per portal.

### US-006 (P2) — Download a dataset file
**As** a user, **I want** to download a cached dataset file directly (redirect to S3 or upstream).

## 4. Functional Requirements

- **FR-001**: MUST expose `IDatasetRepository` with: `save`, `get_by_id`, `get_by_source_id`, `list_by_portal`, `upsert`.
- **FR-002**: `upsert` MUST be idempotent by `(source_id, portal)` UNIQUE.
- **FR-008**: Endpoints MUST expose listing, stats, and a scrape trigger.
  - `GET /api/v1/datasets/` — list with pagination and `portal` filter.
  - `GET /api/v1/datasets/stats` — count per portal.
  - `GET /api/v1/datasets/{id}/download` — redirect to S3 or upstream.
  - (`POST /api/v1/datasets/scrape/{portal}` lives here at the HTTP level but dispatches a worker owned by `006b-ingestion`.)

## 5. Success Criteria

- **SC-001**: Dataset listing responds in **<500ms (p95)**.

## 6. Assumptions & Out of Scope

### Assumptions
- CKAN portals return metadata in the standard format.
- The `(source_id, portal)` pair is stable enough to serve as a natural key.

### Out of scope
- **Dataset versioning** — updates overwrite, no change audit.
- **Dataset diffing** between snapshots.
- **Document-level search** (chunk-level only — lives in vector search spec).
- **Write / edit** endpoints — catalog is read-only from the HTTP side; writes come from workers.

## 7. Open Questions

- **[NEEDS CLARIFICATION CL-004]** — What happens if a CKAN portal changes the `source_id` of a dataset? Duplicate or orphaned?

## 8. Tech Debt Discovered

- **[DEBT-003]** — **No dataset versioning** — updates overwrite without audit.

---

**End of spec.md**
