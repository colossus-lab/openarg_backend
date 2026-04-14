# Spec: Vector Search (Dataset Discovery)

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-12
**Hexagonal scope**: Domain + Infrastructure
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

**Semantic search over indexed datasets** module. Given a query embedding, it returns the most relevant datasets ordered by cosine similarity over `dataset_chunks`. Supports **BM25 + vector** hybrid search via **Reciprocal Rank Fusion (RRF)**. Feeds the pipeline's `search_datasets` step and the discovery of relevant sources before calling specific connectors.

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Chunk** | Text fragment of a dataset (main / columns / contextual) with a 1024-dim embedding. |
| **HNSW** | Hierarchical Navigable Small World — approximate index algorithm for vector search. |
| **RRF** | Reciprocal Rank Fusion — combination of rankings from two retrievers. |
| **BM25** | Classic full-text (lexical) ranking algorithm. |
| **min_similarity** | Cosine similarity threshold below which a result is discarded (default 0.55). |

## 3. User Stories

### US-001 (P1) — Search datasets by meaning
**As** the pipeline, **I want** to search datasets relevant to "health spending" and get the best ones even if they don't mention those exact words.

### US-002 (P2) — Hybrid search for lexical queries
**As** the pipeline, **I want** to use BM25 + vector when the query has specific keywords (proper names, numbers).

### US-003 (P2) — Filter by portal
**As** a user, **I want** to restrict the search to datasets of a specific portal.

### US-004 (P2) — Index a dataset
**As** the worker (`index_dataset_embedding`), **I want** to persist chunks with embeddings so they become searchable.

## 4. Functional Requirements

- **FR-001**: The system MUST expose the `IVectorSearch` port with methods `search_datasets`, `search_datasets_hybrid`, `index_dataset`, `delete_dataset_chunks`.
- **FR-002**: `search_datasets` MUST use cosine distance with `1 - (embedding <=> query) >= min_similarity`.
- **FR-003**: MUST support an optional `portal_filter`.
- **FR-004**: MUST partition by `dataset_id` to return the **best chunk** per dataset (no duplicate datasets).
- **FR-005**: MUST return `SearchResult(dataset_id, title, description, portal, download_url, columns, score)`.
- **FR-006**: `search_datasets_hybrid` MUST combine BM25 (PG full-text) + vector with RRF (rrf_k=60 default).
- **FR-007**: `index_dataset` MUST insert chunks into `dataset_chunks` with the persisted embedding.
- **FR-008**: `delete_dataset_chunks` MUST delete all chunks for a given `dataset_id` (for reindex).
- **FR-009**: The HNSW index on `dataset_chunks.embedding` MUST use `vector_cosine_ops`.
- **FR-010**: Any query with lexical signal (at least 2 meaningful terms) MUST execute the `HYBRID_FULL` retrieval path directly, skipping the standalone vector fast-path precheck and preserving portal filters and RRF fusion.
- **FR-011**: The retrieval strategy MUST expose only two effective modes at runtime: `VECTOR_ONLY` for low-signal queries and `HYBRID_FULL` for lexical queries. `HYBRID_LIGHT` is out of the active routing path.

## 5. Success Criteria

- **SC-001**: Vector search responds in **<300ms (p95)** with a warm HNSW.
- **SC-002**: Hybrid search responds in **<500ms (p95)**.
- **SC-003**: Top-10 precision ≥70% on common queries (not measured).
- **SC-004**: Indexing of 1 dataset (3 chunks) in **<2 seconds** (including Cohere call).

## 6. Assumptions & Out of Scope

### Assumptions
- 1024-dim embeddings are sufficient for the Spanish-language domain.
- The HNSW index is configured with reasonable parameters (m, ef_construction).
- The 3-chunk strategy (main, columns, contextual) captures dataset content well.

### Out of scope
- **Cross-encoder reranking** — only cosine + RRF.
- **Per-user personalization** — search is stateless.
- **Popularity boosting** — not implemented.
- **Multi-lingual reranking** — Spanish only.
- **Dedup** across sources when multiple datasets have the same content.

## 7. Open Questions

- **[NEEDS CLARIFICATION CL-001]** — Is the `min_similarity=0.55` threshold empirical? Against which test dataset?
- **[NEEDS CLARIFICATION CL-002]** — Is `rrf_k=60` tuned or is it the standard default?
- **[RESOLVED CL-003]** — A dataset **cannot** have more than 3 chunks. `index_dataset_embedding` in `src/app/infrastructure/celery/tasks/scraper_tasks.py:595-674` builds a fixed pipeline: chunk 1 (main metadata), chunk 2 (columns, only if `cols` non-empty), chunk 3 (context/use-case). There is no loop over a variable number of chunks — datasets without columns end up with 2 chunks, all others with 3. The "3" is a generation cap, not a retrieval cap. (resolved 2026-04-11 via code inspection)
- **[RESOLVED CL-004]** — The heuristic at `src/app/infrastructure/adapters/search/pgvector_search_adapter.py` tokenises the query on `\w+`, keeps tokens with length ≥ 3, and treats queries with at least **2 meaningful tokens** as lexical-signal queries that go straight to `HYBRID_FULL`. Single-word queries like "inflación" still fall back to `VECTOR_ONLY`. Queries with lots of stopwords (`de`, `la`, `el`) can still collapse to few meaningful tokens. Tracked separately as `DEBT-002`. (updated 2026-04-12 via code inspection)
- **[NEEDS CLARIFICATION CL-005]** — Known debt from the initial reverse-engineering pass: "Vector search lacks threshold, reranking, dedup". Plan to close it?

## 8. Tech Debt Discovered

- **[DEBT-001]** — **RRF uses fixed k=60** — not adaptive to result quality.
- **[DEBT-002]** — **Simple heuristic for lexical signal detection** (token length ≥3) — no TF-IDF nor term weighting.
- **[DEBT-003]** — **No cross-encoder reranking** — results can be noisy.
- **[DEBT-004]** — **No dedup** across similar chunks of the same dataset.
- **[DEBT-005]** — **Hardcoded 5-chunk strategy** (`scraper_tasks.py:592-695`) — no configurable chunking.
- **[DEBT-006]** — **No metrics** for search quality (precision/recall).
- **[DEBT-007]** — ~~**`HYBRID_FULL` no longer pays the standalone vector precheck roundtrip, but `HYBRID_LIGHT` still does.**~~ **FIXED 2026-04-12**: lexical queries now route directly to `HYBRID_FULL`, so the extra vector-precheck roundtrip is gone from the active lexical path altogether.

---

**End of spec.md**
