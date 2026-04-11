# Spec: Public API (/ask endpoint)

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-11
**Hexagonal scope**: Presentation + Application
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

OpenArg's public API for **external integrators**. A single endpoint: `POST /api/v1/ask` with **Bearer token** authentication (API key). Rate limited by plan (free / basic / pro). Does not persist conversations (stateless). It is the channel for third-party applications that want to query Argentine public data via LLM without implementing their own pipeline.

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **API key** | Bearer token with format `oarg_sk_<random>`, hashed with SHA-256 in DB. |
| **Plan** | Subscription tier (free / basic / pro) with different rate limits. |
| **Global free cap** | Aggregated limit of 5000 requests/day across ALL free users, to prevent massive abuse. |
| **IP cap** | Limit of 20 requests/day per IP (across all plans). |

## 3. User Stories

### US-001 (P1) — Query OpenArg from an external app
**As** a developer, **I want** to send a question to OpenArg and receive a JSON response with answer + sources, to integrate it into my app.

### US-002 (P1) — Transparent rate limiting
**As** an API consumer, **I want** rate limit headers (`X-RateLimit-Remaining`, `X-RateLimit-Reset`) to know how much I have left.

### US-003 (P2) — Abuse protection
**As** an operator, **I need** aggregated limits (global free cap, IP cap) to prevent massive abuse without affecting legitimate users.

## 4. Functional Requirements

- **FR-001**: The endpoint MUST accept `Authorization: Bearer oarg_sk_<token>`.
- **FR-002**: The endpoint MUST validate the token by hashing with SHA-256 and comparing against `api_keys.key_hash`.
- **FR-003**: The endpoint MUST validate with `secrets.compare_digest()` (constant-time comparison).
- **FR-004**: The endpoint MUST reject with 401 without distinguishing failure type (prevent enumeration).
- **FR-005**: The endpoint MUST apply rate limiting at 3 levels: per-key per-minute, per-key per-day, per-IP per-day.
- **FR-006**: The endpoint MUST enforce a `GLOBAL_FREE_DAILY_CAP=5000` (all free users together).
- **FR-007**: The endpoint MUST **fail-open** if Redis is down (allow the request, log a warning).
- **FR-008**: The endpoint MUST invoke the query pipeline (`001-query-pipeline`) but **without persisting the conversation** (stateless).
- **FR-009**: The endpoint MUST record usage in `api_usage` (append-only: endpoint, tokens, duration, status).
- **FR-010**: The endpoint MUST return `{answer, sources, chart_data?, map_data?, confidence, citations, warnings}`.

## 5. Success Criteria

- **SC-001**: Response time with cache hit: **<1 second (p95)**.
- **SC-002**: Normal response time: **<15 seconds (p95)**.
- **SC-003**: Rate limiting is **exact** (zero over-limit requests under normal load).
- **SC-004**: Fail-open active when Redis is down (requests keep working).
- **SC-005**: Availability ≥99% monthly.

## 6. Assumptions & Out of Scope

### Assumptions
- Generated API keys are impossible to guess (random 32 bytes).
- SHA-256 is sufficient for key hashing (does not need bcrypt because they are not human passwords).
- Redis is available most of the time.

### Out of scope
- **WebSocket streaming** — HTTP sync only in the public API.
- **Conversational memory** — stateless.
- **OAuth flow** — see `003-auth/`.
- **Sandbox SQL endpoint** — not exposed in the public API.
- **Billing / Payments** — see MEMORY.md, not implemented.

## 7. Open Questions

- **[RESOLVED CL-001]** — ~~`expires_at` is dead code~~ **FIXED 2026-04-11 via deletion** (Alembic 0030). The column, the `ApiKey.expires_at` entity field, the `api_key_mappings` binding, and the `api_key_service.verify_api_key` check are all gone. API keys now live until explicitly revoked — that is the actual contract, and the spec + code now say the same thing. See `008-developers-keys` DEBT-003 for the full rationale.
- **[RESOLVED CL-002]** — `GLOBAL_FREE_DAILY_CAP=5000` **is fine for the current alpha**. It is adjusted by editing the constant in code + deploy. **Revisit when**: there are signs of demand (many 429s for legitimate free-tier users) or Bedrock economics change.
- **[RESOLVED CL-003]** — **NO WebSocket in the public API**. If streaming is needed, **SSE (Server-Sent Events)** will be used because: (1) it is plain HTTP, compatible with Bearer auth + SlowAPI rate limit + existing proxies with no refactor; (2) it is the industry standard — OpenAI, Anthropic and Google Gemini use SSE, not WS; (3) it is unidirectional (server→client), which is exactly what LLM streaming needs. **Prerequisites before implementing SSE**: (1) FIX-006 (real token counting via Bedrock stream metadata) — without this, billing breaks more than it already does. **Timing**: when there is real demand from integrators building chatbots on top of OpenArg. There is none today — HTTP sync is enough. For now the public API is sync-only.
- **[RESOLVED CL-004]** — `X-RateLimit-*` headers are **NOT** returned on successful responses — they only appear on `429` errors (`X-RateLimit-Limit-Minute`, `X-RateLimit-Remaining-Minute`, `Retry-After` at `api_key_service.py:140-155`). On the happy path `/ask` returns quota info inside the JSON body under `usage.requests_remaining_today` / `usage.requests_remaining_minute` (`ask_router.py:113-119`), not as headers. (resolved 2026-04-11 via code inspection)

## 8. Tech Debt Discovered

- **[DEBT-001]** — **Fail-open on Redis down** — allows over-limit during outages. Accepted trade-off but not measured.
- **[DEBT-002]** — **No billing** — there is no integration with a payment system for plan upgrades.
- **[DEBT-003]** — **Key rotation not automated** — the user must create a new key and update their app manually.
- **[DEBT-004]** — No structured audit trail of requests (only `api_usage` append-only).

---

**End of spec.md**
