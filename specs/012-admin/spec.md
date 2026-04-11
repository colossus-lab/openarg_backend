# Spec: Admin Tasks Orchestration

**Type**: Reverse-engineered
**Status**: Draft
**Last synced with code**: 2026-04-10
**Hexagonal scope**: Presentation + Infrastructure
**Related plan**: [./plan.md](./plan.md)

---

## 1. Context & Purpose

**Administration** endpoint that allows manually triggering any registered background task (portal scrape, dataset collect, connector snapshot, etc.) from an admin panel. Protected by the `X-Admin-Key` header. Supports querying the status of enqueued tasks. Complements the beat schedule for cases where a trigger is needed outside the scheduled time.

## 2. Ubiquitous Language

| Term | Definition |
|---|---|
| **Task registry** | Hardcoded dict that maps friendly names ("scrape_catalog") to Celery entries (`openarg.scrape_catalog` + queue + params). |
| **Admin key** | Shared secret via env `ADMIN_API_KEY` (fallback: `BACKEND_API_KEY`). |
| **AsyncResult** | Celery handler that allows querying the state of an enqueued task by ID. |

## 3. User Stories

### US-001 (P1) — Trigger a task manually
**As** an admin, **I want** to trigger `snapshot_bcra` outside the schedule to force an update.

### US-002 (P1) — Check a task's status
**As** an admin, **I want** to check whether an enqueued task has finished, and see the result.

### US-003 (P2) — List available tasks
**As** an admin, **I want** to see the full registry with descriptions and params.

## 4. Functional Requirements

- **FR-001**: MUST validate the `X-Admin-Key` header on all admin endpoints.
- **FR-002**: MUST return 401 if the key does not match, 503 if it is not configured.
- **FR-003**: MUST maintain a `TASK_REGISTRY` with mapping: `task_name → {celery_name, description, params_schema, queue}`.
- **FR-004**: POST MUST enqueue the task with the provided params on the correct queue.
- **FR-005**: GET MUST return the state of a task by ID (PENDING|STARTED|SUCCESS|FAILURE|RETRY).
- **FR-006**: MUST support at least: `scrape_catalog`, `collect_dataset`, `index_dataset_embedding`, `bulk_collect_all`, `score_portal_health`, `snapshot_staff`, `snapshot_bcra`, and the rest of the registry tasks.

## 5. Success Criteria

- **SC-001**: Task enqueueing responds in **<200ms**.
- **SC-002**: Status poll responds in **<100ms**.
- **SC-003**: **Zero tasks triggered without valid auth**.

## 6. Assumptions & Out of Scope

### Assumptions
- The admin key is rotated manually.
- The registry is kept in sync with the task code.
- Only a handful of admins have access.

### Out of scope
- **Audit log** of admin actions.
- **Rate limiting** of the admin endpoint.
- **UI / visual panel** — API only, the UI lives elsewhere.
- **Task rollback** — once enqueued, it can only be cancelled with native Celery tools.
- Granular **RBAC** (all admins can trigger everything).

## 7. Open Questions

- **[NEEDS CLARIFICATION CL-001]** — Is there an admin UI (part of the frontend)? What tasks does it actually expose?
- **[RESOLVED CL-002]** — `ADMIN_API_KEY`: **currently NOT rotated**. Manual rotation on demand by editing env vars + restart. Accepted debt, not a priority.
- **[RESOLVED CL-003]** — **Single request, client retries**. `GET /admin/tasks/status/{celery_task_id}` calls `AsyncResult(id).state` only once and returns the current state (PENDING, STARTED, SUCCESS, FAILURE, RETRY, REVOKED) without blocking. There is no long-polling. See `tasks_router.py:288-327`.

## 8. Tech Debt Discovered

- **[DEBT-001]** — **No rate limiting** — a compromised admin can spam tasks.
- **[DEBT-002]** — **Hardcoded task registry** — adding a new task requires code edit + deploy.
- **[DEBT-003]** — **No audit log** of admin actions.
- **[DEBT-004]** — **No RBAC** — all admins have the same permissions.
- **[DEBT-005]** — **`verify_admin_key` can collapse to `BACKEND_API_KEY` fallback** — reduces secret separation.

---

**End of spec.md**
