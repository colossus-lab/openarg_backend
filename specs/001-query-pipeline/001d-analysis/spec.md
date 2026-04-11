# Spec: Query Pipeline — Phase D: Analysis

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10
**Hexagonal scope**: Application (analyst + policy nodes + chart builder) + Infrastructure (Bedrock streaming LLM)
**Parent**: [../spec.md](../spec.md)
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

The **Analysis phase** turns raw `data_results` into a human-readable narrative. It is the pipeline's second LLM-heavy phase and the only one that uses **streaming**: chunks are buffered, tags are stripped, and the cleaned prose is pushed to the WebSocket client as it is generated. It also owns deterministic chart building, GeoJSON map extraction, and `<!--META:-->` tag parsing for confidence and citations.

Nodes covered:

1. `analyst` — the main LLM synthesis step (streaming) using `analyst.md` / `analyst_no_data.md`.
2. `policy` (optional) — additional LLM enrichment when `policy_mode=True`, delegating to `policy_agent.py`.

This phase also owns the chart builder helpers and the META-tag extraction logic invoked by the analyst node.

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Analyst** | The `analyst` node — the LLM synthesis step of the pipeline. |
| **META tag** | `<!--META:confidence=X, citations=[...]-->` comment the analyst emits for post-processing. |
| **CHART tag** | `<!--CHART:{...}-->` comment the analyst emits to request a chart when deterministic building fails. |
| **`clean_answer`** | The analyst response with all internal tags stripped. |
| **Deterministic chart** | A chart built directly from columnar data (`chart_builder.py`), no LLM involved. |
| **Policy mode** | Opt-in flag that chains the `policy` node after the analyst for public-policy framing. |
| **`map_data`** | Deterministic GeoJSON payload built when records contain `_geometry_geojson` fields. |
| **Chunk buffering** | Holding partial streaming output until a tag boundary is safe to emit. |

## 3. User Stories

### US-002 (P1) — Real-time progress streaming (analysis portion)
**As** a user, **I want** to see the response text appear token by token as the LLM writes it. **Trigger**: the `analyst` node calls `chat_stream()` and pushes each chunk through `get_stream_writer()`.

### US-009 (P2) — Policy mode (public policy analysis)
**As** an advanced user, **I can** enable the `policy_mode` flag so the response includes a public policy analysis section. **Trigger**: `policy_mode=True` → after the analyst, it goes to the `policy` node → enriches `clean_answer` before `finalize`.

### US-011 (P2) — Deterministic and LLM-generated chart building
**As** a user, **I want** to see charts when the data allows. Priority: deterministic charts (`build_deterministic_charts()`) → fallback to LLM-generated charts via `<!--CHART:{}-->` tags in the analyst's response.

### US-012 (P2) — GeoJSON maps when the data is geographic
**As** a user, **I want** to see maps when the data includes `_geometry_geojson` fields. **Trigger**: `analyst` detects geo fields and builds deterministic `map_data`.

## 4. Functional Requirements

### Analysis
- **FR-018**: The analyst MUST use streaming LLM (`chat_stream()`) with fallback to non-streaming.
- **FR-019**: The analyst MUST build the prompt contextualizing data, memory, errors, and skills.
- **FR-020**: The analyst MUST use the `analyst.md` prompt in the normal flow and `analyst_no_data.md` in the fallback (no data).
- **FR-021**: The analyst MUST extract `confidence` and `citations` from meta tags (`<!--META:confidence=X, citations=[...]-->`).
- **FR-022**: The analyst MUST build charts deterministically when there are temporal/categorical + numeric columns, with fallback to LLM-generated charts.
- **FR-023**: The analyst MUST build GeoJSON `map_data` when the records contain `_geometry_geojson`.
- **FR-024**: The analyst MUST buffer streaming chunks to avoid emitting incomplete tags.
- **FR-025**: The analyst MUST strip internal tags (`<!--CHART:-->`, `<!--META:-->`) from `clean_answer` before returning it.

### Security (cross-reference)
- **FR-042**: The pipeline MUST NOT leak `analysis_prompt`, tracebacks, or internal prompts to the client. *(This phase is the primary producer of `analysis_prompt` — the streaming whitelist defined in the HTTP router enforces the outbound contract. See also Phase E / top-level spec.)*

## 5. Success Criteria

- **SC-001** *(partial)*: P1 response (normal path, cache miss) in **<15 seconds (p95)**. The streaming analyst dominates perceived latency.
- **SC-005**: **Zero internal prompts leaked** to the client via streaming. *(Enforced by the whitelist, but this phase is the main source of sensitive strings.)*
- **SC-009**: Policy mode responds in **<25 seconds** (overhead ≤40% over the normal path).

## 6. Assumptions & Out of Scope

### Assumptions
- Bedrock Claude Haiku 4.5 supports streaming with usage metadata in the final `message_stop` event (see CL-001 / DEBT-001 FIX).
- The prompts `analyst.txt` and `analyst_no_data.txt` live under `prompts/` (note: `.txt` extension, not `.md`).
- The frontend parses `<!--CHART:-->` / `<!--META:-->` tags only AFTER the backend has stripped them (i.e., they should never reach the client).

### Out of scope (this sub-module)
- **Policy agent internals** — delegates to `policy_agent.py`, not part of the main graph.
- **Chart rendering** — frontend responsibility.
- **Map rendering** — frontend responsibility (Leaflet / MapLibre).
- **Cache write, audit log, memory update** — see `../001e-finalization/`.
- **Prompt authoring and evaluation** — see `012-prompts/` (if present).

## 7. Open Questions

- **[RESOLVED CL-001]** — Broken token counting (`tokens_used=0` always in streaming mode, `analyst.py:239`). **Decision**: use stream metadata — the final Bedrock Streaming chunk has a `usage` field with the real count (`input_tokens` + `output_tokens` in the `message_stop` event). Precise, free, requires parsing the end of the stream. It is the industry standard. See [`FIX_BACKLOG.md#fix-006-token-counting-via-bedrock-stream-metadata`](../../FIX_BACKLOG.md).
- **[RESOLVED CL-008]** — **Deliberate anti-hallucination decision**, documented in code. `analyst.py:108-117` explicitly excludes `memory_ctx_analyst=""` on the no-data path with the comment: *"Don't pass memory context in no-data mode — it causes the LLM to hallucinate 'we already discussed this' when no data was ever shown."* The `analyst_no_data.txt:16-19` prompt reinforces with an explicit ANTI-HALLUCINATION rule: *"NUNCA digas que 'ya exploramos'"*. **Impact on continuity**: mitigated because `load_memory` (memory.py) restores the full context for the NEXT query. It is a single-query safety valve, not conversational amnesia. **Good design, not a bug**.
- **[RESOLVED CL-010]** — **Design decision documented in the code**, not a workaround. Explicit comment in `analyst.py:222-226`: *"When map data is present, suppress charts (geo data doesn't chart well)"*. When `_build_map_data(results)` detects geo features, the analyst sets `charts = None`. Rationale: geographic distributions are better visualized on maps than on traditional charts (bar/line/pie).

## 8. Tech Debt Discovered

- **[DEBT-001]** — ~~**Token count always 0 in streaming mode**~~ **FIXED 2026-04-10**: `analyst.py` now passes a `usage_out` dict to `chat_stream()` and the Bedrock / fallback adapters populate it from the stream metadata event. `finalize.py` forwards the real count to `MetricsCollector.record_tokens_used()`. See `FIX_BACKLOG.md#fix-006`.
- **[DEBT-007]** — **Fragile chart building heuristics** (`chart_builder.py:15-100`). Hardcoded column-name patterns (`fecha`, `nombre`, `patrimonio`) don't scale to new schemas.
- **[DEBT-008]** — ~~**Token counting absent in the `policy` node**~~ **FIXED 2026-04-10** as part of FIX-006: `usage_out` is threaded through the shared LLM port so any node that streams (analyst, policy) gets correct token counts.
- **[DEBT-011]** — **Analyst prompt building is string concat** — it doesn't truncate the context to the token budget limit. Prompts can blow up.

---

**End of spec.md** — See [./plan.md](./plan.md) for the as-built topology of this phase.
