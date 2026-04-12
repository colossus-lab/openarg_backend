# Spec: Developers Portal (API Key Management)

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-11
**Hexagonal scope**: Presentation + Application + Infrastructure
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

Endpoints for **authenticated users** to manage their own **API keys**: create, list, revoke, and view usage statistics. It is OpenArg's developer portal. The keys are used in the public API (`007-public-api/`). One key per user by default; creating a new one automatically revokes the previous one.

## 2. User Stories

### US-001 (P1) — Create my API key
**As** a developer, **I want** to create an API key from the portal and see the plaintext token **only once** when I create it (I cannot recover it later).

### US-002 (P1) — List my keys
**As** a developer, **I want** to see my existing keys (masked, with `key_prefix` only).

### US-003 (P1) — Revoke a key
**As** a developer, **I want** to disable a compromised or old key.

### US-004 (P2) — View my usage
**As** a developer, **I want** to see how many requests I made, how many tokens I consumed, and what my current limit is.

## 3. Functional Requirements

- **FR-001**: The system MUST generate tokens with format `oarg_sk_<random>` (random enough to be unguessable).
- **FR-002**: The system MUST persist only the SHA-256 hash, **never** the plaintext.
- **FR-003**: The system MUST return the plaintext **only in the POST /keys response** (once).
- **FR-004**: The system MUST store the `key_prefix` (first ~8 chars) for display.
- **FR-005**: POST MUST revoke (set `is_active=false`) the user's existing key before creating the new one (**1 key per user**).
- **FR-006**: DELETE MUST set `is_active=false` (soft delete), not delete the record.
- **FR-007**: GET /keys MUST return masked keys (only prefix + metadata).
- **FR-008**: GET /usage MUST aggregate statistics from `api_usage`.
- **FR-009**: All endpoints MUST authenticate with **`Authorization: Bearer <google_id_token>`** (validated by `GoogleJwtAuthMiddleware` against Google's JWKS per FIX-005) plus the shared service token `X-API-Key: <BACKEND_API_KEY>`. The user identity is read from the verified Google JWT `email` claim, never from a header the frontend can set directly.

## 4. Success Criteria

- **SC-001**: Create key responds in **<500ms**.
- **SC-002**: List keys responds in **<300ms**.
- **SC-003**: Usage summary responds in **<1s**.
- **SC-004**: Zero key plaintexts persisted in logs or DB.

## 5. Assumptions & Out of Scope

### Assumptions
- Users understand that the key is shown once and they must save it.
- `BACKEND_API_KEY` is shared between frontend and backend as a secret.

### Out of scope
- **Multiple keys per user** — only one for now.
- **Key expiration** — keys do not expire. If expiration is ever needed, it becomes a new feature with its own FR + migration + UI (see DEBT-003 below, which tracks the former dead-code state and the 2026-04-11 cleanup).
- **Billing** / plan upgrade.
- **Key scopes / permissions** — all keys have the same permissions.
- **Webhook notifications** for anomalous usage.

## 6. Open Questions

- **[RESOLVED CL-001]** — ~~`expires_at` is dead code never written by any endpoint~~ **FIXED 2026-04-11 via deletion**. The column + entity field + mapping + validation check have been removed (Alembic migration 0030). See DEBT-003 below for the rationale — §0 "delete before you add" + "abstractions pay rent only when a second caller exists". Same change closed the sibling `007-public-api/CL-001`.
- **[NEEDS CLARIFICATION CL-002]** — Why 1 key per user? Plan to allow more?
- **[RESOLVED CL-003]** — `BACKEND_API_KEY`: **currently NOT rotated**. Manual rotation on demand (requires coordinated backend + frontend restart). Accepted debt, not a priority. If compromised, it is rotated manually by editing env vars and restarting services.

## 7. Tech Debt Discovered

- **[DEBT-001]** — **Key prefix stored for display** — no rotation mechanism.
- **[DEBT-002]** — **Global cap checked only against Redis** — fail-open allows over-limit.
- **[DEBT-003]** — ~~`expires_at` is dead code~~ **FIXED 2026-04-11 via deletion**: the column is gone (Alembic 0030), the `ApiKey.expires_at` entity field is removed, the `api_key_service.verify_api_key` check that compared it to `now()` is removed, and the `api_key_mappings` Column is removed. Rationale: the field was being **read but never written**, so every key was persisted with `expires_at = NULL` and the check at `api_key_service.py:69` always short-circuited on the `api_key.expires_at and ...` guard — pure dead validation. If expiration ever becomes a real feature, it should be re-introduced with a UI flow that actually sets it (see the Out of Scope section above).
- **[DEBT-004]** — **No soft delete audit** — revoking a key leaves no trace of "who revoked it when".

---

**End of spec.md**
