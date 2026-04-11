# Spec: Connector Sesiones (HCDN Legislative Transcripts)

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10
**Hexagonal scope**: Domain + Application + Infrastructure
**Extends**: [`../spec.md`](../spec.md)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

Search connector over **parliamentary session transcripts from the Chamber of Diputados (HCDN)**. Implements a hybrid retrieval model: **semantic pgvector (primary)** via Bedrock Cohere embeddings + **local keyword search (fallback)** over chunks pre-loaded from JSON files. Supports filtering by legislative period and orador.

It is the only connector in the system with **dual-mode retrieval** (vector + keyword) and silent fallback between the two.

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Sesión** | Parliamentary meeting with a textual transcript. |
| **Legislative period** | Calendar year of the Argentine congress. |
| **Orador** | Diputado or official speaking in a session. |
| **Chunk** | Fragment of text from a session (typically one speaking turn) with embedding and metadata. |
| **Reunión** | Meeting number within the period. |

## 3. User Stories

### US-001 (P1) — Semantic search over sessions
**As** a researcher, **I want** to search for "discussions about the 2025 budget" and obtain the most relevant fragments even if they don't literally mention those terms.

### US-002 (P1) — Search filtered by orador
**As** a user, **I want** to see all interventions by a specific diputado on a topic.

### US-003 (P2) — Search filtered by period
**As** a researcher, **I want** to scope my search to legislative period 2025.

### US-004 (P2) — Graceful fallback when vector search fails
**As** the system, **I need** that if Bedrock Cohere fails, search continues to work using local keyword search.

## 4. Functional Requirements

- **FR-001**: MUST expose port `ISesionesConnector` with method `search(query, periodo, orador, limit)`.
- **FR-002**: MUST implement primary retrieval via pgvector cosine distance over the `sesion_chunks` table.
- **FR-003**: MUST implement fallback keyword search over local JSON files at `src/app/infrastructure/data/chunks/*.json`.
- **FR-004**: MUST truncate the query to 2000 chars before the embedding call.
- **FR-005**: MUST use `input_type="search_query"` in Cohere Embed.
- **FR-006**: MUST apply filters by periodo and orador (post-filter for vector, pre-filter for keyword).
- **FR-007**: MUST support bonuses in keyword scoring: +10 if the term appears in the speaker name, +20 if it is an exact match.
- **FR-008**: MUST load local chunks only once (lazy init) and keep them in memory.
- **FR-009**: MUST normalize output to `DataResult` with fields: periodo, reunion, fecha, tipo_sesion, pdf_url, total_pages, speaker, text.

## 5. Success Criteria

- **SC-001**: Semantic search responds in **<3 seconds (p95)**.
- **SC-002**: Keyword fallback responds in **<500ms** (data in memory).
- **SC-003**: **100% availability** — if Bedrock fails, keyword search responds.
- **SC-004**: Local chunks are kept in sync with the DB (at minimum via manual review).

## 6. Assumptions & Out of Scope

### Assumptions
- Chunks are pre-populated in the DB (`sesion_chunks`) and in local JSON files.
- Bedrock Cohere Embed is available most of the time.
- Users accept embedding generation latency on cold cache.

### Out of scope
- Automatic ingestion of new sessions (currently manual/offline).
- Search over Senado sessions (HCDN only for now).
- Automatic translation or summarization of chunks.
- Named entity recognition in transcripts.

## 7. Open Questions

- **[RESOLVED CL-001]** — **There is a Celery task**: `index_sesiones_chunks()` in `embedding_tasks.py:70-200`. Flow: loads JSON files from disk → generates Bedrock Cohere embeddings in batches → bulk `INSERT INTO sesion_chunks` (lines 170-181). It is not manual; it runs on-demand via the admin task registry (`index_sesiones_chunks` listed in `tasks_router.py`).
- **[RESOLVED CL-002]** — **JSON files are the source of truth, the DB is derived**. Sync flow: the `index_sesiones_chunks` task in `embedding_tasks.py:70-201` reads `src/app/infrastructure/data/chunks/*.json`, generates embeddings via Bedrock Cohere, and performs `INSERT INTO sesion_chunks`. Skips already-indexed chunks in subsequent runs. `sesiones_adapter.py` **never writes** to the JSON files — it only reads them as fallback if pgvector fails. **Operational implication**: to add new sessions → (1) place JSON files on disk, (2) run `index_sesiones_chunks` via the admin orchestrator.
- **[RESOLVED CL-003]** — **Verified in prod DB (2026-04-10)**: only **1 legislative period indexed — period 143 (year 2025)** with 8240 chunks. **Nothing from prior periods** (142=2024, 141=2023, 140=2022). Also no chunks from period 144 (2026 in progress). **Coverage debt**: if the user asks about pre-2025 sessions or the current period, the system returns empty or irrelevant results without warning.
- **[RESOLVED CL-004]** — **Logs only, no structured metric**. When pgvector fails: `logger.error("pgvector sesiones search failed — falling back to local keyword search", exc_info=True)` (`sesiones_adapter.py:154-157`). When pgvector returns empty: `logger.info("pgvector returned no results, falling back to local keyword search")` (line 308). **There is no counter, no incremental metric, no alert**. Persistent Bedrock Cohere problems would be invisible without manually grepping logs.

## 8. Tech Debt Discovered

- **[DEBT-001]** — **Chunk files must be pre-populated manually** in `/src/app/infrastructure/data/chunks/`. There is no automated scraper. High maintenance burden.
- **[DEBT-002]** — **Fallback to keyword search is silent** — no structured logging of the mode used. Hard to debug which results are vector vs keyword in production.
- **[DEBT-003]** — **Keyword search uses naive substring matching** — does not handle synonyms, stemming, or fuzzy matching.
- **[DEBT-004]** — **Inconsistent periodo/orador filters across modes**: post-filter for vector, pre-filter for keyword. Different semantics can yield inconsistent results.
- **[DEBT-005]** — **Term extraction filters terms <3 chars** — loses short keywords like "ley", "pan", "IVA".
- **[DEBT-006]** — **Embedding generation is a blocking boto3 call** wrapped in `asyncio.to_thread()` — resource-intensive under high load.
- **[DEBT-007]** — **No pagination** in keyword search; it scores all candidates and dumps them into a heap.
- **[DEBT-008]** — **No circuit breaker** for Bedrock Cohere — the fallback always runs even when the service is degraded.

---

**End of spec.md**
