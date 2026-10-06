# Spec: Public API (/ask endpoint)

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-09-29
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
| **Global free cap** | Aggregated limit of requests/day across ALL free users (`PUBLIC_API_GLOBAL_DAILY_CAP`, default 300). It is the Bedrock spending ceiling of the public API. |
| **IP cap** | Limit of requests/day per client IP (`PUBLIC_API_IP_DAILY_LIMIT`, default 30, across all plans). |
| **Unbilled runs cap** | Limit per person and UTC day of model runs that are not charged — timeout, error, clarification, empty answer (`PUBLIC_API_USER_UNBILLED_DAILY_LIMIT`, default 20, across all plans). Reserved on entry before the IP and global caps; given back when the answer is charged, when the turn did not use the model, or when a later check rejects the request. Over the cap: 429 with `Retry-After` until 00:00 UTC. Without it, one free key rotating ~10 IPs drained the global free cap for everyone (H108, 2026-10-05). |
| **UTC day** | Daily counters are keyed by the UTC date (`…:day:YYYY-MM-DD`) and reset at 00:00 UTC (21:00 in Argentina). |

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
- **FR-005**: The endpoint MUST apply rate limiting at 3 levels: per-key per-minute, per-key per-day, per-IP per-day. Free plan: 2/min, 10/day.
- **FR-005a**: Counters MUST be atomic (`ICacheService.increment_with_ttl`), checked in this order: minute → day → IP → global. A request rejected at one level MUST NOT consume the levels after it.
- **FR-005b**: The client IP MUST be the real one: uvicorn runs with `--proxy-headers --forwarded-allow-ips='*'` (safe because the backend publishes no port; Caddy rewrites `X-Forwarded-For`).
- **FR-006**: The endpoint MUST enforce the global free cap (all free users together) and answer **503** when it is reached.
- **FR-007**: Per-key and per-IP counters MUST **fail-open** if Redis is down. The global free cap MUST **fail-closed** (503): it is the spending ceiling, and without Redis there is no way to know what was spent today.
- **FR-007a**: The pipeline MUST be invoked with an ephemeral `configurable.thread_id` (`efimero-<uuid>`) when the checkpointer is active. Without it LangGraph raises and every request was a 500 (found 2026-09-29; no router test existed).
- **FR-007b**: A prompt flagged as injection MUST return 400, as `/smart` does.
- **FR-007c**: Pipeline timeout is `PUBLIC_API_TIMEOUT_SECONDS` (default 30).
- **FR-008**: The endpoint MUST invoke the query pipeline (`001-query-pipeline`) but **without persisting the conversation** (stateless).
- **FR-009**: The endpoint MUST record usage in `api_usage` (append-only: endpoint, tokens, duration, status).
- **FR-010**: The endpoint MUST return `{answer, sources, chart_data?, map_data?, citations, warnings}`.
- **FR-010a**: `sources` are dataset-level provenance entries derived from executed results.
- **FR-010b**: `citations` may include grounding metadata (`verified`, `grounding`, `unsupported_numbers`) for claim-level verification. Clients MUST treat unverified citations as best-effort, not as hard evidence.
- **FR-010c**: When numeric claims in the answer cannot be grounded against executed results, the endpoint MUST surface that via `warnings`. (Historical `confidence` field was removed from the public response in commit `acc884a` — the score was unreliable and the frontend chip built on it was misleading; the pipeline still computes it internally for logging.)

## 5. Success Criteria

- **SC-001**: Response time with cache hit: **<1 second (p95)**.
- **SC-002**: Normal response time: **<15 seconds (p95)**.
- **SC-003**: Rate limiting is **exact** (zero over-limit requests under normal load).
- **SC-004**: With Redis down, paid plans keep working and the free plan answers 503.
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
- **Billing / Payments** — not implemented; rate limiting is the only free-tier guardrail today.

## 7. Open Questions

- **[RESOLVED CL-001]** — ~~`expires_at` is dead code~~ **FIXED 2026-04-11 via deletion** (Alembic 0030). The column, the `ApiKey.expires_at` entity field, the `api_key_mappings` binding, and the `api_key_service.verify_api_key` check are all gone. API keys now live until explicitly revoked — that is the actual contract, and the spec + code now say the same thing. See `008-developers-keys` DEBT-003 for the full rationale.
- **[RESOLVED CL-002]** — ~~`GLOBAL_FREE_DAILY_CAP=5000` hard-coded~~ **Superseded 2026-09-29** for the public MCP launch: the cap comes from `PUBLIC_API_GLOBAL_DAILY_CAP` and is sized as USD 10/day (USD 300/month budget) ÷ measured cost per question. 5000/day had no relation to any budget.
- **[RESOLVED CL-003]** — **NO WebSocket in the public API**. If streaming is needed, **SSE (Server-Sent Events)** will be used because: (1) it is plain HTTP, compatible with Bearer auth + SlowAPI rate limit + existing proxies with no refactor; (2) it is the industry standard — OpenAI, Anthropic and Google Gemini use SSE, not WS; (3) it is unidirectional (server→client), which is exactly what LLM streaming needs. **Prerequisites before implementing SSE**: (1) FIX-006 (real token counting via Bedrock stream metadata) — without this, billing breaks more than it already does. **Timing**: when there is real demand from integrators building chatbots on top of OpenArg. There is none today — HTTP sync is enough. For now the public API is sync-only.
- **[RESOLVED CL-004]** — `X-RateLimit-*` headers are **NOT** returned on successful responses — they only appear on `429` errors (`X-RateLimit-Limit-Minute`, `X-RateLimit-Remaining-Minute`, `Retry-After` at `api_key_service.py:140-155`). On the happy path `/ask` returns quota info inside the JSON body under `usage.requests_remaining_today` / `usage.requests_remaining_minute` (`ask_router.py:113-119`), not as headers. (resolved 2026-04-11 via code inspection)

## 8. Tech Debt Discovered

- **[DEBT-001]** — ~~Fail-open on Redis down~~ **Partially fixed 2026-09-29**: the global free cap fails closed. Per-key/per-IP counters still fail open (accepted).
- **[DEBT-002]** — **No billing** — there is no integration with a payment system for plan upgrades.
- **[DEBT-003]** — **Key rotation not automated** — the user must create a new key and update their app manually.
- **[DEBT-004]** — No structured audit trail of requests (only `api_usage` append-only).
- **[DEBT-005]** — `usage.tokens` / `api_usage.tokens_used` only count the analyst (`analyst.py`); planner, NL2SQL, classifier and embeddings are not counted. Cost per question must be measured from Bedrock CloudWatch metrics, not from this field.
- **[DEBT-006]** — A request rejected by the IP limit or the global cap has already consumed one of the caller's daily questions (the port has no atomic decrement).

---

**End of spec.md**
