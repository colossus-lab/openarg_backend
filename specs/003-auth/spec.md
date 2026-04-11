# Spec: Authentication & User Management

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10
**Hexagonal scope**: Domain + Application + Infrastructure + Presentation
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

**User authentication** module for the OpenArg web chat. It uses an **OAuth-first** model: the frontend (Next.js) handles authentication with Google via NextAuth, and then syncs the user's email to the backend. The backend does not handle passwords directly. It also implements the **ARCO rights** (access, rectification, cancellation, opposition) via export and deletion endpoints.

The **API keys** module for programmatic access is documented in `008-developers-keys/`; this spec covers only chat user auth.

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **User** | Person who used Google OAuth to authenticate in the chat. |
| **Sync** | Idempotent action of upserting a user when the frontend confirms OAuth authentication. |
| **Save history** | Toggle that decides whether the user's conversations are persisted (default: true). |
| **Privacy accepted** | Timestamp of when the user accepted the privacy policy. |
| **ARCO** | Rights of Access, Rectification, Cancellation, Opposition (Argentine law). |
| **X-User-Email** | Custom header used to identify the authenticated user in a request. |

## 3. User Stories

### US-001 (P1) — Login with Google OAuth
**As** a new visitor, **I want** to authenticate with my Google account without creating a new password. **Trigger**: frontend completes the NextAuth flow → POST `/users/sync` to the backend → upsert user by email.

### US-002 (P1) — Get my profile
**As** an authenticated user, **I want** to see my name, email, avatar, and current settings.

### US-003 (P1) — Toggle save_history
**As** a user, **I want** to choose whether OpenArg saves my conversation history. **Effect**: if I disable it, my existing conversations are deleted (cascade).

### US-004 (P1) — Delete my account (ARCO erasure)
**As** a user, **I want** to delete my account and all associated data when I request it, without manual operator intervention.

### US-005 (P1) — Export my data (ARCO access)
**As** a user, **I want** to download all my conversations and queries in JSON format.

## 4. Functional Requirements

- **FR-001**: The system MUST expose the `IUserRepository` port with methods: `upsert_by_email`, `get_by_email`, `export_user_data`, `delete_user_and_data`.
- **FR-002**: The system MUST persist users in the `users` table with fields: `id` (UUID), `email` (unique), `name`, `image_url`, `created_at`, `privacy_accepted_at`, `save_history`.
- **FR-003**: The upsert MUST be idempotent: multiple syncs of the same email do not create duplicates.
- **FR-004**: Deletion (DELETE /me) MUST cascade to `conversations`, `messages`, `user_queries`.
- **FR-005**: Export (GET /me/data) MUST return a JSON with all the user's entities.
- **FR-006**: The `save_history=false` toggle MUST delete existing conversations in the same operation.
- **FR-007**: Authenticated endpoints MUST identify the user by the `X-User-Email` header.
- **FR-008**: User creation (sync) MUST record `privacy_accepted_at` when the frontend reports it.

## 5. Success Criteria

- **SC-001**: Sync responds in **<300ms** (DB upsert).
- **SC-002**: Export user data responds in **<2s** for typical users.
- **SC-003**: Delete user data is **atomic** (transaction with cascade).
- **SC-004**: Zero cross-user leaks (a user never accesses another user's data).

## 6. Assumptions & Out of Scope

### Assumptions
- The frontend (NextAuth) correctly validates the Google OAuth token before sending the email.
- The `X-User-Email` header is provisioned by the trusted reverse proxy, not by external clients. A future migration to server-side Google JWT validation (planned as FIX-005) will remove this assumption.
- Emails are unique (DB constraint).

### Out of scope
- **Passwords** — OAuth only, never password hashing.
- **MFA / 2FA** — not implemented.
- **Role-based access control (RBAC)** — no roles; the only level is authenticated "user" + admin (via `ADMIN_API_KEY`).
- **Backend-side session tokens** — the frontend manages sessions via NextAuth.
- **API keys** — separate spec (`008-developers-keys/`).
- **Backend-side Google OAuth flow** — lives in the frontend.

## 7. Open Questions

- **[RESOLVED CL-001]** — The `X-User-Email` header without server-side validation is an acceptable vulnerability only temporarily. **Decision**: implement server-side JWT validation (Google OAuth ID token). The frontend sends `Authorization: Bearer <jwt>`, the backend validates the signature via Google's JWKS, verifies `aud` + `iss` + `exp`, and extracts the email from the verified claim. See [`FIX_BACKLOG.md#fix-005-x-user-email--jwt-server-side-validation`](../FIX_BACKLOG.md).
- **[RESOLVED CL-002]** — The allowlist lives in the **frontend** (`openarg_frontend/src/lib/authOptions.ts`, NextAuth signIn callback), not in the backend. Policy per environment:
  - **Staging**: allowlist **ACTIVE** via `ALLOWED_EMAILS` with `OPEN_BETA=false`. Only the listed operators can authenticate — private alpha.
  - **Production**: allowlist **BYPASSED** (`OPEN_BETA=true` or empty allowlist + open domains). Public access.
  - **Admins**: operators listed in `ADMIN_EMAILS` (the `requireAdmin()` helper exists and is used by the transparency endpoint).

  The backend trusts the `X-User-Email` it receives from the frontend (already validated by NextAuth), without its own cryptographic enforcement. See `../../openarg_frontend/specs/004-auth/` for details and FIX-005 for the planned migration to server-side JWT validation.
- **[RESOLVED CL-003]** — Backend-own JWT session tokens: **future work, not prioritized for now**. FIX-005 implements validation of the Google OAuth JWT (the token NextAuth already emits), which closes the immediate security gap. Emitting backend-own session tokens is evaluated later if the trust model changes.
- **[RESOLVED CL-004]** — `delete_user_and_data` **does NOT need to cascade to S3 or the semantic cache**. Decision: no user-identifiable data is stored in S3 (it is storage for public datasets, not per-user content) or in the semantic cache (it caches queries + responses with no user_id associated in the hash). The current cascade over `conversations`, `messages`, `user_queries` is sufficient to fulfill the right to cancellation.

## 8. Tech Debt Discovered

- **[DEBT-001]** — **No password hashing** — acceptable for the OAuth-only model, but it should be documented as an explicit decision.
- **[DEBT-002]** — **`X-User-Email` header trust model** — the backend accepts the caller identity from a header set by the trusted proxy, without its own cryptographic validation. Planned improvement: migrate to server-side Google OAuth JWT validation using the JWKS endpoint, so the identity is verified at the API layer. Tracked as `FIX_BACKLOG.md#fix-005`.
- **[DEBT-003]** — **No audit trail** of login/logout events.
- **[DEBT-004]** — **No rate limiting** on `/users/sync` — an attacker could spam emails.

---

**End of spec.md**
