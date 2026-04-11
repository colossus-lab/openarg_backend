# Spec: Semantic Cache

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10
**Hexagonal scope**: Infrastructure (transparent to pipeline)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

**Vector** cache that stores pipeline responses so that semantically similar queries return pre-computed results. It implements a dual strategy: **exact hash match** (fast, SHA-256) followed by **similarity match** (pgvector HNSW). Variable TTLs based on the detected intent type (real-time / daily / static). It works transparently to the pipeline: the `cache_check` node queries it and `finalize` writes to it.

It is one of the modules with a **critical fix applied in Mar 2026**: the SQL `::type` casts were replaced with `CAST(:param AS type)` to fix a write bug (see `MEMORY.md`).

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Exact match** | Hit by exact SHA-256 hash of the normalized query. |
| **Similarity match** | Hit by cosine distance over the embedding, with a configurable threshold (default 0.92). |
| **Normalization** | Lowercase + strip + NFC Unicode normalization before hashing. |
| **Intent** | Tag derived from the query (`dolar` → real-time, `inflacion` → daily, `ddjj` → static). |
| **TTL by intent** | 300s (real-time) / 1800s (daily) / 7200s (static). |

## 3. User Stories

### US-001 (P1) — Transparent cache hit
**As** the system, **I want** a query "cuánto esta el dólar" to be answered from cache if an identical query was answered less than 5 minutes ago.

### US-002 (P1) — Similarity hit
**As** the system, **I want** "cuánto cuesta el dólar hoy" to return the cache of "cuánto está el dólar" if they are semantically similar (threshold 0.92).

### US-003 (P2) — Differentiated TTLs
**As** the system, **I need** a short TTL (300s) for volatile data (exchange rates) and a long TTL (7200s) for stable data (DDJJ).

### US-004 (P2) — Periodic cleanup
**As** an operator, **I need** the cache to periodically clean up expired entries so it doesn't grow indefinitely.

## 4. Functional Requirements

- **FR-001**: The system MUST normalize the query before hashing: lowercase, strip, NFC unicode.
- **FR-002**: The system MUST try exact hash match first, similarity match second.
- **FR-003**: Similarity match MUST use `cosine distance` over `embedding vector(1024)` with a configurable threshold (default `CACHE_SIMILARITY_THRESHOLD=0.92`).
- **FR-004**: Both matches MUST honor `expires_at` in the DB.
- **FR-005**: The set MUST use UPSERT with `ON CONFLICT` over `question_hash`.
- **FR-006**: The set MUST validate the embedding against NaN/Inf before persisting.
- **FR-007**: TTL MUST be derived from the `plan_intent` via `ttl_for_intent()`:
  - Real-time (dolar, cotizacion, riesgo_pais): 300s
  - Daily (inflacion, series, emae, tipo_cambio, reservas, desempleo): 1800s
  - Static (ddjj, sesiones, ckan, catalogo, legisladores, georef): 7200s
  - Default: 1800s
- **FR-008**: `cleanup()` MUST delete expired entries and return the count.

## 5. Success Criteria

- **SC-001**: Cache hit responds in **<200ms** (exact) / **<500ms** (similarity).
- **SC-002**: Target hit rate: **≥30%** (not currently measured).
- **SC-003**: Zero writes with invalid embeddings (NaN/Inf).
- **SC-004**: Periodic cleanup runs daily (TBD — no visible task).

## 6. Assumptions & Out of Scope

### Assumptions
- Cohere Embed v3 embeddings are deterministic (same input → same output).
- A threshold of 0.92 is appropriate for the Spanish/Argentine domain.
- The cache TTL is sufficient for most queries.

### Out of scope
- **Cache invalidation by events** (e.g. "invalidate when the BCRA snapshot is updated"). Only time-based TTL.
- Proactive **cache warming** (no pre-population).
- **Compression** of cached responses.
- **Multi-tenant isolation** (none).

## 7. Open Questions

- **[RESOLVED CL-001]** — **No cleanup task exists**. Confirmed: greps for `query_cache` in `celery/tasks/` and for `DELETE.*query_cache` across the entire codebase return zero matches. Expired entries **accumulate indefinitely**; they are only filtered on reads via `expires_at > now()`. The HNSW index degrades with dead entries. Concrete debt — see `FIX_BACKLOG.md#FIX-007`.
- **[NEEDS CLARIFICATION CL-002]** — Was the 0.92 threshold validated empirically? Against which dataset?
- **[NEEDS CLARIFICATION CL-003]** — `ttl_for_intent` mapping — where is the mapping decided? Hardcoded.
- **[RESOLVED CL-004]** — **Direct verification in the production DB (2026-04-10)**: `query_cache` has **just 1 entry total** (created on 2026-04-06, expired), table size 1248 kB (mostly HNSW index overhead). **No hit-rate metrics are exported** — `MetricsCollector` has `record_cache_event()` but it has not been verified whether it is wired into all paths. With current traffic volume (~28 historical queries) the hit rate is trivially 0%. **Prerequisites for a useful metric**: (1) volume ≥100 queries/day; (2) consistent wiring of `record_cache_event` in the `cache.py` node; (3) exposure in `/api/v1/metrics`. Without those, measuring hit rate is not actionable.
- **[NEEDS CLARIFICATION CL-005]** — What happens to the cached response if the underlying model changes (e.g. an upgrade of Claude Haiku)?

## 8. Tech Debt Discovered

- **[DEBT-001]** — **Hardcoded similarity threshold** (0.92) — no per-intent tuning.
- **[DEBT-002]** — **Manual pgvector string construction** (`[v1,v2,...]`) instead of robust casting.
- **[DEBT-003]** — **No metrics** for hit rate.
- **[DEBT-004]** — **No scheduled cleanup task** — expired entries are not deleted automatically.
- **[DEBT-005]** — **Hardcoded TTL mapping** in code, not in config.
- **[DEBT-006]** — **No abstract port** — used directly by the pipeline (similar to BCRA/DDJJ).

---

**End of spec.md**
