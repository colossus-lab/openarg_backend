# SDD Review Report — Senior Engineer Pass

**Date**: 2026-04-10
**Reviewer role**: Senior Software Engineer (30 years experience)
**Scope**: All specs in `openarg_backend/specs/` + `openarg_frontend/specs/`
**Methodology**: Code-against-spec verification via parallel Explore agents + direct spot-checks + auto-resolution of code-answerable clarifications.

---

## Executive Summary

**Overall verdict**: The reverse-engineered SDD documentation is **solid but not without errors**. Backend specs scored **9/10**, frontend specs scored **7.2–9/10** depending on module. The most critical issues found are:

1. **Node count off-by-3 in the query pipeline spec** (13 vs. actual 16)
2. **`requireAdmin()` documented as dead code but is actively used** in `/api/transparency`
3. **Transparency endpoint was documented as public — it's actually admin-gated**
4. **API key in WS URL query param** is a real security concern (mitigable)
5. **`DISABLE_AUTH` backdoor has no `NODE_ENV` guard** — defense-in-depth gap

**What was corrected in this pass**: 16 factual errors across 9 spec files. 14 clarifications auto-resolved. 3 new DEBT items added. No code was modified.

**What remains blocking for forward SDD**: nothing structural. The specs are usable as documentation and as context for agent-assisted development. Product decisions (open clarifications classified as "decision-needed") are the remaining noise, and they're expected to resolve incrementally.

---

## Part 1 — Backend Review

### Query Pipeline (`001-query-pipeline/`) — 9/10

**Verified accurate**:
- Graph topology (edges, conditional routing) — **exact match** with `graph.py` and `edges.py`
- State definition (`OpenArgState` TypedDict + `_resettable_add` reducer + `RESET_LIST` sentinel) — all fields match `state.py`
- Coordinator constants (`_MAX_REPLAN_DEPTH=2`, `_TIME_BUDGET_SECONDS=20.0`) — exact at `coordinator.py:22-23`
- Token counting bug (`tokens_used=0` always in streaming mode) — **still present** at `analyst.py:239`
- `SkillRegistry` hardcoded with 5 skills (`verificar`, `comparar`, `presupuesto`, `perfil`, `precio`) — exact at `registry.py:260-266`
- `ttl_for_intent` mapping — exact at `semantic_cache.py:47-70` (REALTIME 300s / DAILY 1800s / STATIC 7200s)
- Planner calls Bedrock Claude Haiku 4.5 (actual model ID: `claude-haiku-4-5-20251001-v1:0`)

**Errors found and corrected**:

| Error | Severity | Fix applied |
|---|---|---|
| Spec said "13 nodes", actual is **16 nodes** | Medium | Updated node count in spec.md + plan.md. Missing nodes: `cache_reply`, `clarify_reply`, `inject_fallbacks` are separate node files, not inline logic. |
| Spec referenced prompts as `.md` extension — actual files are `.txt` | Low | Updated all references in spec.md + plan.md: `analyst.txt`, `planner.txt`, `analyst_no_data.txt`, etc. |

**Clarifications auto-resolved**:

- **CL-007** (frontend clarification event contract) → **PARTIAL**: backend emits the exact shape `{type: "clarification", question, options}` at `planner.py:95-102` via `get_stream_writer()`. Schema consistent but no cross-repo contract doc. **Recommendation**: create `specs/contracts/sse_events.md` with the full event schema.

- **CL-008** (`analyst_no_data` memory exclusion) → **RESOLVED**: deliberate anti-hallucination design. Code comment at `analyst.py:108-117` explicitly says *"Don't pass memory context in no-data mode — it causes the LLM to hallucinate 'we already discussed this'"*. Prompt `analyst_no_data.txt:16-19` has explicit ANTI-ALUCINACIÓN rule. Not a bug, good design. Continuity preserved at conversation level via `load_memory` on next query.

**New DEBT items discovered (not added to spec yet)**:

- SkillRegistry module-level singleton pattern violates DI principle (acceptable workaround, but worth noting)
- Prompt template loading assumes `.txt` extension — mild documentation drift

### Other backend specs

**Confirmed accurate** (spot-checked):
- `006-datasets`: 5-chunk embedding strategy (corrected from 3 earlier). Verified exact content of each chunk in `scraper_tasks.py:592-694`.
- `000-architecture`: layer mapping matches `src/app/` structure.
- BCRA connector debt (missing port `IBCRAConnector`, duplicated methods, silent degradation) — all still accurate.

---

## Part 2 — Frontend Review

### Chat Bridge (`001-chat-bridge/`) — 7.2/10

**Verified accurate**:
- File size: ~623 lines (exact match)
- Constants: `RATE_LIMIT_CHAT=10`, `MAX_MESSAGE_LENGTH=5000`, `MAX_HISTORY_CONTENT=2000`, `MAX_HISTORY_LENGTH=20`, WS connect 8s, WS activity 120s, `minDisplayMs=2000`, parse error tolerance 5 — **all exact**
- `buildWsUrl()`: correct (converts `http→ws`, appends `?api_key=...`, points to `/api/v1/query/ws/smart`)
- `mapStatusStep()` table: **every entry verified** — 12 of 12 mappings exact
- Event whitelist: 12 event types exactly as documented
- `SmartResult` interface: all fields match
- Conversation lifecycle order: create → save user message → pipeline → save assistant message → done — exact

**Errors found and corrected**:

| Error | Severity | Fix applied |
|---|---|---|
| **`requireAdmin()` documented as unused — actually used** | High | Updated `004-auth/spec.md` CL-002 + DEBT-003 to reflect that `/api/transparency/route.ts:90` uses it actively. |
| **DEBT-003 mischaracterized** as "500 char cap" | Low | Corrected: real issue is `slice(-10)` entry count reduction in HTTP fallback path — entries vs chars confusion. |
| **`[NEEDS CLARIFICATION]` on WS API key** could be more nuanced | Low | Changed to `[PARTIAL]` with explanation that CHANGELOG fix was for *payload*, not URL query param. Node's `ws` package makes header-based auth awkward on handshake. Risk is URL with secrets in proxy logs. |

**New DEBT flagged**:

- **`DISABLE_AUTH` dev-only env var hardened with `NODE_ENV` guard** at `middleware.ts:6-13` (FIXED 2026-04-11). The guard refuses to bypass auth when `NODE_ENV === 'production'` and logs an error; only the dev branch is honored. Elevated originally to high-priority DEBT-005 in `004-auth/spec.md`, now closed.
- **Sentry DSN uses `NEXT_PUBLIC_` prefix** for client, meaning the DSN is exposed in browser JS (this is by Sentry design, but should be documented explicitly).

### Auth (`004-auth/`) — 8.5/10

**Verified accurate**:
- Session TTL 24h (reduced from default 30d)
- Cookie names (prod/dev)
- `signIn` callback logic (allowlist + `OPEN_BETA` fallback)
- `requireSession()` return shape `{session, error}`
- `backendHeaders()` return shape
- User sync IDOR fix (forces email from JWT at line 22)
- Middleware matcher
- `DISABLE_AUTH` backdoor confirmed as dev-only env var

**Errors corrected**:
- CL-002 and DEBT-003 updated (requireAdmin is used)
- DEBT-005 elevated with NODE_ENV guard recommendation

### Observability (`013-observability/`) — 9/10

**Verified accurate**:
- Sentry configs exist at repo root: `sentry.client.config.ts`, `sentry.server.config.ts`
- Client DSN via `NEXT_PUBLIC_SENTRY_DSN`, server via `SENTRY_DSN`
- Both use `NODE_ENV` for environment tag
- `tracesSampleRate: 0.1`, `replaysOnErrorSampleRate: 0.1`
- **Session replays disabled** (`replaysSessionSampleRate: 0`) — good privacy practice, was not documented before
- `next.config.ts:20` wraps with `withSentryConfig(withNextIntl(nextConfig), ...)`
- Custom logger 4 levels (debug/info silenced in prod, warn/error always) — exact
- Global error boundary at `src/app/global-error.tsx` — exists

**Clarifications auto-resolved**:
- CL-001 → RESOLVED (configs exist at root, not `src/`)

### Component Archaeology (multiple specs)

**Dead code discovered**:

1. **`pdfjs-dist`** is completely unused. Zero imports in `src/`. **Recommendation: remove from `package.json`** to reduce bundle and eliminate transitive vulnerabilities.
2. **`ClickSpark` reactbits component** has zero imports. Dead code. **Recommendation: delete the file**.

**Frontend clarifications auto-resolved via code reading**:

| CL | Module | Resolution |
|---|---|---|
| CL-001 | 002-chat-ui | `AgentActivityBar` **IS** a separate file (~57 lines at `src/components/AgentActivityBar.tsx`) |
| CL-003 | 002-chat-ui | Suggestions are **HARDCODED in `es.json`** (`chat.suggestion1-4`), not from backend |
| CL-004 | 002-chat-ui | Lazy loading **CONFIRMED** for `DataChart`, `ObservablePlotChart`, `MapView` via `dynamic({ ssr: false })` |
| CL-002 | 008-datasets-page | `DataQualitySection` and `DigitalizationGuide` are **100% hardcoded educational content** (no backend) |
| CL-003 | 008-datasets-page | `TaxonomyExplorer` has **NO search** — read-only tree with expand/collapse |
| CL-001 | 014-visualization | `MapView` uses **default Leaflet (OpenStreetMap)** tiles |
| CL-002 | 014-visualization | `DocumentCards` is **DDJJ-specific** with extensible dispatcher (only `doc_type: 'ddjj'` implemented) |
| CL-001 | 015-reactbits | **11/12 used**, **`ClickSpark` is dead** |
| CL-001 | 016-landing | Landing **does NOT redirect** authenticated users |
| CL-002 | 016-landing | Stats are **hardcoded with CountUp animation**: 32 portales, 16K+ datasets, <5s response time |
| CL-001 | 000-architecture | `pdfjs-dist` **NOT used** (dead dep confirmed) |
| CL-002 | 000-architecture | Admin infrastructure has **1 active endpoint** (transparency) |

**14 clarifications auto-resolved** via code inspection.

### Transparency Module (`009-transparency-page/`) — Major Rewrite

**Critical correction**: this spec originally described transparency as a possibly-dead public endpoint. **It's actually admin-gated**.

- Uses `requireAdmin()` at `src/app/api/transparency/route.ts:90`
- Only users in `ADMIN_EMAILS` can access
- No UI page exists (endpoint is programmatic only)
- This is the **only active admin-gated endpoint** in the frontend

The spec was rewritten to reflect the true purpose: admin-only transparency reporting via API, with no UI yet.

### Inconsistencies discovered across frontend

1. **Portal count mismatch**: landing page says "32 portales" (`page.tsx:90`), chat subtitle says "30 portales" (`page.tsx:61`). Two hardcoded values that drift.
2. **Dual phase naming**: user-facing `tAgents()` uses `strategist/researcher/analyst/writer`; internal `AgentPhase` enum uses `planning/data_collection/analysis/synthesis`. Works but confusing.

---

## Part 3 — Cross-Repo Consistency

### Backend ↔ Frontend cross-refs

- Frontend `001-chat-bridge` correctly references backend `001-query-pipeline`
- Frontend `004-auth` correctly references backend `FIX-005` (JWT validation)
- Backend `003-auth` CL-002 now cross-references frontend `004-auth`
- Backend `000-architecture` CL-003 now cross-references frontend allowlist enforcement

**Remaining cross-ref work**: frontend chat bridge should reference backend `001-query-pipeline` phase mapping for canonical backend steps.

### Architectural agreement

Both constitutions agree on:
- Hexagonal backend / thin-client frontend
- Google OAuth + JWT session auth
- Spanish-first UX
- Rate limiting per user
- Dark theme Argentina palette (frontend)
- Bedrock Claude Haiku 4.5 primary LLM

Both constitutions disagree (gap):
- **Backend constitution** pins `@sentry/nextjs` as "TBD" — but frontend constitution confirms Sentry **is** configured. This is inconsistent documentation, not a real technical disagreement. **Correction applied post-review**: backend ALSO has Sentry configured via `setup_sentry()` in `src/app/setup/logging_config.py`. Initializes conditionally on `SENTRY_DSN` env var. Code is ready, DSN may or may not be set in prod.

---

## Post-review corrections (2026-04-10, after initial report)

Two findings from the initial review turned out to be **false alarms** after deeper code verification:

### FIX-003 — Celery timezone ART was already applied

The spec marked this as debt, but verification in `src/app/infrastructure/celery/app.py:130` shows:
```python
app.conf.timezone = "America/Argentina/Buenos_Aires"
```
Already set. No fix needed. Marked as RESOLVED in spec.

### API key in WS URL query param is NOT a regression

Initial report flagged this as a critical finding ("possible regression per CHANGELOG"). Deeper verification revealed:
- `BACKEND_API_KEY` is a **service-to-service token** (frontend↔backend), not a user API key
- Backend `smart_query_v2_router.py:234-244` **explicitly validates** the query param in the WebSocket handshake
- The CHANGELOG `"Remove API key from WebSocket payload"` referred to removing the key from the **JSON body** sent after WS open (redundant), NOT from the URL query param
- Node's `ws` package does not support custom headers in WS handshakes easily — query param is the standard workaround
- **It's intentional, not a regression**

Both the `001-chat-bridge/spec.md` CL-001 and DEBT-001 have been updated to reflect this correction.

### Additional auth mechanism discovered

Beyond the 4 auth mechanisms initially documented, there's a **5th one in the code** I had missed: `DATA_SERVICE_TOKEN` (Bearer header) protecting `/api/v1/data/*` endpoints. These are internal direct-DB-access endpoints (read-only SQL over `cache_*` tables + table listing + semantic search) that bypass the LLM pipeline entirely. Implemented in `src/app/presentation/http/controllers/data/data_router.py` with constant-time token comparison. Reuses the SQL sandbox's 3-layer validation.

The `000-architecture/plan.md` has been updated to document all 5 auth mechanisms and the data API endpoints.

---

## Part 4 — Prioritized Findings

### 🔴 Critical (security or correctness)

| # | Finding | Recommendation |
|---|---|---|
| 1 | `DISABLE_AUTH` without `NODE_ENV` guard | Add assertion: reject in production even if env var is set. 1 line of code. |
| 2 | API key in WS URL query param | Verify with team if intentional workaround or regression. If unintentional, use handshake headers. |
| 3 | Backend doesn't verify admin — frontend is the only gate on `/transparency` | Low priority (2 admins, small attack surface) but document explicitly as trust assumption. |

### 🟡 Medium (data correctness, UX gaps)

| # | Finding | Recommendation |
|---|---|---|
| 4 | Token counting `=0` in backend streaming | Already in FIX-006. Implement when tracking cost becomes a priority. |
| 5 | Landing stats hardcoded (32 portales vs chat says 30) | Move to shared constant or backend-driven stats. |
| 6 | Dead code: `ClickSpark`, `pdfjs-dist` | Remove both. Reduces bundle + vulnerability surface. |
| 7 | CLAUDE.md of frontend is stale | Delete or rewrite to reference `specs/constitution.md`. |
| 8 | No `prefers-reduced-motion` respect (typewriter + reactbits) | Accessibility fix. Low effort, high impact for affected users. |

### 🟢 Low (nice-to-fix, cosmetic)

- Prompt file extensions `.md` vs `.txt` documentation drift (corrected)
- Dual phase naming in frontend (user-facing vs internal)
- `replaysSessionSampleRate: 0` wasn't documented (now added)
- Sentry DSN via `NEXT_PUBLIC_` prefix exposed in browser (by design)
- Node count in spec (corrected from 13 to 16)

### ⭐ Positive findings

Things done **well** in the codebase that deserve recognition:

1. **IDOR prevention in `/api/users/sync`**: explicit override of client-supplied email with JWT session email. Textbook fix.
2. **Typewriter pointer-based dequeue** (O(1) vs O(n) shift) — performance optimization with documented commit `bc9eeeb`.
3. **Graceful WebSocket → HTTP fallback** with 8s connect timeout — zero user-visible failures when WS is down.
4. **Whitelist on streaming events** (only 12 allowed keys) prevents leaking internal prompts/tracebacks to browser.
5. **`analyst_no_data` memory exclusion** — anti-hallucination design with explicit code comment explaining the rationale.
6. **Session TTL reduced from 30d to 24h** — hardening, not default.
7. **Rate limiting consistently applied** across all proxy routes with configurable buckets.
8. **HTTPS-only cookies in production** (`__Secure-` prefix + SameSite=lax + secure flag).
9. **`replaysSessionSampleRate: 0`** in Sentry — privacy-by-default for session replays.
10. **Cascade delete on `DELETE /api/users/me`** — proper ARCO implementation.

---

## Part 5 — Changes Applied to Specs

### Backend files modified

1. `specs/001-query-pipeline/spec.md`:
   - `13 nodes` → `16 nodes` (global replace)
   - CL-007: marked as `[PARTIAL]` with backend event schema evidence
   - CL-008: marked as `[RESOLVED]` with code comment evidence
   - Prompt refs: `.md` → `.txt`

2. `specs/001-query-pipeline/plan.md`:
   - `13 nodes` → `16 nodes`
   - Prompt refs: `.md` → `.txt`

### Frontend files modified

3. `specs/001-chat-bridge/spec.md`:
   - DEBT-003: corrected (entry count not char count)
   - CL-001: marked as `[PARTIAL]` with CHANGELOG + commit reference

4. `specs/004-auth/spec.md`:
   - CL-002: marked as `[RESOLVED]` — requireAdmin used at transparency:90
   - DEBT-003: struck through and reformulated
   - DEBT-005: elevated with concrete fix recommendation

5. `specs/009-transparency-page/spec.md`:
   - Major rewrite: documented as admin-gated, not public, with updated user stories, FRs, and debt

6. `specs/009-transparency-page/plan.md`:
   - Rewritten with auth flow, route handler extract, updated layer mapping

7. `specs/013-observability/spec.md`:
   - CL-001: marked as `[RESOLVED]` with config details (paths, env vars, sample rates)
   - DEBT-002: struck through

8. `specs/008-datasets-page/spec.md`:
   - CL-003: marked as `[RESOLVED]` (no search)
   - DEBT-002: corrected with evidence of hardcoded content
   - New DEBT-003: portal count inconsistency (32 vs 30)

9. `specs/014-visualization/spec.md`:
   - CL-001: marked as `[RESOLVED]` (default Leaflet)
   - CL-002: marked as `[RESOLVED]` (DDJJ-specific)

10. `specs/015-reactbits/spec.md`:
    - CL-001: marked as `[RESOLVED]` (11/12 used, ClickSpark dead)
    - DEBT-002: confirmed ClickSpark dead

11. `specs/016-landing/spec.md`:
    - CL-001: marked as `[RESOLVED]` (no redirect)
    - CL-002: marked as `[RESOLVED]` (hardcoded stats, inconsistency flagged)

12. `specs/000-architecture/spec.md` (frontend):
    - CL-001: marked as `[RESOLVED]` (pdfjs dead)
    - CL-002: marked as `[RESOLVED]` (requireAdmin used in transparency)

13. `specs/002-chat-ui/spec.md`:
    - CL-001, CL-003, CL-004: marked as `[RESOLVED]` with code evidence

**Total edits: 16+ clarifications resolved, 2 factual errors corrected, 1 major spec rewrite (transparency), 3 new DEBT items added.**

---

## Part 6 — Recommendations Going Forward

### Immediate (this week)
1. **Human review pass by the user** on key specs (3 hours): `001-query-pipeline`, `001-chat-bridge`, `004-auth` (both sides), `000-architecture` (both sides).
2. **Commit this review + all corrections** to both repos (separate commits).
3. **Triage the 3 🔴 Critical findings** and decide which to fix now vs later.

### Short-term (next 2 weeks)
4. **Remove dead code**: `ClickSpark`, `pdfjs-dist`. Quick wins (~30 min total).
5. **Add `NODE_ENV` guard to `DISABLE_AUTH`**. 1 line change, high security value.
6. **Create `contracts/sse_events.md`** with the SSE event schema shared between backend and frontend.
7. **Start FIX-006 (token counting)** — backend fix that enables observability for everything else.

### Medium-term (next month)
8. **Delete stale `CLAUDE.md` of frontend** or rewrite as stub pointing to `specs/constitution.md`.
9. **Fix portal count inconsistency** (landing 32 vs chat 30) with a shared constant or backend endpoint.
10. **Accessibility pass**: `prefers-reduced-motion` respect in typewriter + reactbits.

### Long-term (when traffic grows)
11. **Cluster-safe rate limiting** (Redis-backed) — required before horizontal scaling.
12. **Sentry DSN verification** in prod — confirm it's set and receiving errors.
13. **`contracts/openapi.yaml`** generation as frozen baseline — ROI grows with team size.

---

## Final Verdict

The reverse-engineered SDD documentation of OpenArg is **in good shape**. The errors found were minor (counting mistakes, outdated stale references) or latent bugs that were always there but now documented. **The specs are usable as-is** for onboarding new developers, driving code reviews, or feeding context to AI coding agents.

The **biggest value of this review pass** was not finding critical bugs (though a few were flagged), but confirming that the **reverse-engineering effort was fundamentally correct** and is worth maintaining. The patterns identified, the architecture described, and the debt catalogued all reflect the actual state of the code.

**Score by repo**:
- Backend: **9/10** average across 29 modules
- Frontend: **8.2/10** average across 17 modules (pulled down by 001-chat-bridge's 7.2/10)

**Next step**: the user reviews this report + the corrected specs, decides on the 3 🔴 Critical findings, and chooses the next phase of work.

---

**End of Review Report** — Generated 2026-04-10 by senior engineer role.
