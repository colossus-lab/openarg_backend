# OpenArg Fix Backlog

**Purpose**: Ordered list of pending fixes derived from the April 2026 reverse-SDD. Each entry has priority, affected module, problem description, chosen resolution, acceptance criteria, and affected files.

**Convention**:
- `FIX-NNN` IDs are **global to the project** (not per module).
- Priority: **High** (security or user-visible correctness) / **Medium** (structural debt) / **Low** (nice-to-fix).
- Status: `Pending` | `In progress` | `Completed`.

**Overall status**: Updated 2026-04-11 after a third fix pass under strict §0.5 spec-first discipline. **7 out of 8** fixes applied (FIX-001, FIX-002, FIX-003, FIX-004, FIX-006, FIX-007, FIX-008). The only remaining item is FIX-005 (X-User-Email → JWT server-side validation), which is deliberately deferred because it requires coordinated front+back rollout that cannot be done autonomously.

**Relation to specs**: Each fix is referenced from its corresponding `spec.md` in the "Open Questions" section (now with marker `[RESOLVED CL-XXX]` → `specs/FIX_BACKLOG.md#FIX-NNN`).

---

## Summary

| ID | Priority | Module | Title | Status |
|---|---|---|---|---|
| FIX-001 | **High** | `002a-bcra` | BCRA silent degradation → surface errors | ✅ Completed 2026-04-10 |
| FIX-002 | Medium | `002a-bcra` | BCRA deduplicate broken methods | ✅ Completed 2026-04-10 |
| FIX-003 | Low | `002a-bcra` + Celery config | BCRA schedule timezone → ART | ✅ Completed (already applied) |
| FIX-004 | Medium | `001-query-pipeline` + `010-sandbox-sql` | NL2SQL subgraph integration | ✅ Completed 2026-04-11 |
| FIX-005 | **High (security)** | `003-auth` | X-User-Email → JWT server-side validation | Pending (front+back coord.) |
| FIX-006 | Medium | `001-query-pipeline` | Token counting via Bedrock stream metadata | ✅ Completed 2026-04-10 |
| FIX-007 | Medium | `004-semantic-cache` | Semantic cache cleanup task (expired entries never deleted) | ✅ Completed 2026-04-10 |
| FIX-008 | Low-Medium | `002f-staff` | Staff diff misses field changes (transfers between offices invisible) | ✅ Completed 2026-04-11 |

---

## FIX-001: BCRA silent degradation → surface errors

**Priority**: High
**Module**: `002-connectors/002a-bcra/`
**Type**: Correctness + UX + Observability
**Spec**: `specs/002-connectors/002a-bcra/spec.md` CL-005
**Related debt**: `002a-bcra/DEBT-007`, `DEBT-008`, `DEBT-009`

### Problem
`execute_bcra_step` in `application/pipeline/connectors/bcra.py:48-50` catches any adapter exception and silently returns `[]`:

```python
except Exception:
    logger.warning("BCRA step %s failed", step.id, exc_info=True)
    return []
```

Symptoms:
- The user receives a pipeline response without knowing that BCRA data is missing
- `MetricsCollector` does not record the failure (there is no entry for BCRA in metrics)
- No integration with the global circuit breaker
- Impossible to debug in production without searching specific logs
- The analyst can generate made-up answers about quotes it does not have

### Chosen resolution
When the BCRA adapter fails, the system MUST:
1. Catch the exception but **NOT silently return `[]`**
2. Add an entry to `step_warnings` with error code and message
3. Increment the metric `MetricsCollector.record_connector_call("bcra", success=False, ...)`
4. Let the analyst see the warning in its context and mention it in the response to the user
5. Integrate with the circuit breaker at `infrastructure/resilience/circuit_breaker.py`

### Acceptance criteria
- [ ] When BCRA is down, `step_warnings` contains `"BCRA unavailable: <reason>"` with the `error_code` from `ConnectorError`
- [ ] The response to the user (via analyst) explicitly mentions that BCRA data could not be obtained (e.g.: "No se pudieron obtener datos de BCRA en este momento. La respuesta se generó con las demás fuentes disponibles.")
- [ ] `MetricsCollector.get_metrics()["connectors"]["bcra"]["errors"]` increments on each failure
- [ ] The circuit breaker opens after 5 consecutive failures (existing config: `failure_threshold=5`, `recovery_timeout=60s`)
- [ ] When the circuit is open, `execute_bcra_step` fails fast without making HTTP calls
- [ ] Unit test: mock adapter that raises → `step_warnings` populated + empty `data_results` + incremented metric
- [ ] Integration test with mocked HTTP simulating 500 → warning visible in final response

### Affected files
- `application/pipeline/connectors/bcra.py` — remove the generic `try/except Exception`, replace with explicit handling of `ConnectorError`
- `infrastructure/adapters/connectors/bcra_adapter.py` — integrate `@circuit_breaker` alongside `@with_retry`
- `application/pipeline/step_executor.py` — verify that warnings propagate correctly to the state
- New tests in `tests/unit/application/pipeline/connectors/test_bcra_step.py`

---

## FIX-002: BCRA deduplicate broken methods

**Priority**: Medium
**Module**: `002-connectors/002a-bcra/`
**Type**: Structural debt, hygiene
**Spec**: `specs/002-connectors/002a-bcra/spec.md` CL-001
**Related debt**: `002a-bcra/DEBT-002`

### Problem
The `BCRAAdapter` has 4 methods, but **3 of them end up calling the same endpoint** `/estadisticascambiarias/v1.0/Cotizaciones` because the endpoint `v2/PrincipalesVariables` was deprecated by BCRA:

- `get_cotizaciones()` — **works correctly**
- `get_principales_variables()` — alias that calls `/Cotizaciones` (misleading name)
- `get_variable_historica(id_variable, ...)` — ignores `id_variable`, calls `/Cotizaciones` (misleading name)
- `search(query)` — ignores `query`, delegates to `get_cotizaciones()` (shim)

The methods with misleading names are **bugs at rest**: anyone who uses them downstream thinks they are asking for monetary variables or specific historical series and actually receives quotes.

### Chosen resolution
**Deduplicate**: remove `get_principales_variables`, `get_variable_historica` and `search` from the adapter. Leave only `get_cotizaciones` as a public method.

Update all callers:
- `application/pipeline/connectors/bcra.py` — remove the `tipo == "variables"` and `tipo == "historica"` branches from the dispatch; keep only `"cotizaciones"` as a valid type
- Verify whether there are external callers (orchestrator, admin tasks) that depend on the removed methods

### Acceptance criteria
- [ ] `BCRAAdapter` exposes only `get_cotizaciones(moneda?, fecha_desde?, fecha_hasta?)`
- [ ] No adapter method makes HTTP calls to deprecated endpoints
- [ ] `execute_bcra_step` only recognizes `tipo = "cotizaciones"` in `step.params`
- [ ] Planner prompt updated so it does not generate steps with `tipo=variables|historica`
- [ ] Existing tests still pass
- [ ] Grep for `get_principales_variables` and `get_variable_historica` returns 0 matches in `src/`

### Affected files
- `infrastructure/adapters/connectors/bcra_adapter.py` — remove 3 methods, leave only `get_cotizaciones`
- `application/pipeline/connectors/bcra.py` — simplify dispatch to just `cotizaciones`
- `prompts/query_planner.md` (or similar) — update PlanStep examples for BCRA
- Global grep to verify that no external caller uses the removed methods

---

## FIX-003: BCRA schedule timezone → ART

**Priority**: Low
**Module**: `002-connectors/002a-bcra/` + global Celery config
**Type**: Operational configuration
**Spec**: `specs/002-connectors/002a-bcra/spec.md` CL-003

### Problem
`celery/app.py:237-241` defines:

```python
"snapshot-bcra": {
    "task": "openarg.snapshot_bcra",
    "schedule": crontab(hour=4, minute=0),  # Daily 4:00 AM ART  ← comment
    "options": {"queue": "ingest"},
},
```

The comment says "4:00 AM ART", but **Celery interprets cron in the worker's timezone** (default UTC if not configured explicitly). If the worker is in UTC, the snapshot runs at 01:00 ART, not 04:00 ART. This affects all the system's beat schedules, not just BCRA.

### Chosen resolution
Configure `celery_app.conf.timezone = "America/Argentina/Buenos_Aires"` in `infrastructure/celery/app.py`. Verify that all existing cron schedules remain coherent with the new TZ (times commented as "ART" now apply literally).

### Acceptance criteria
- [ ] `celery_app.conf.timezone = "America/Argentina/Buenos_Aires"` explicit in `celery/app.py`
- [ ] `celery_app.conf.enable_utc = False` (necessary complement)
- [ ] Beat schedule comments match the real behavior
- [ ] Smoke test: beat + worker in staging environment, verify that `snapshot-bcra` runs at 04:00 ART and not another time
- [ ] Document in README or deploy docs that schedules are ART, not UTC

### Affected files
- `infrastructure/celery/app.py` — add 2 configuration lines
- Optional: review `docker-compose.prod.yml` in case there's a `TZ` env var affecting it

### Note
This fix affects **all** beat schedules, not just BCRA. Verify `ingest-bac`, `ingest-indec`, `ingest-presupuesto-dimensiones`, `scrape-dkan-*`, `snapshot-staff`, `snapshot-senado-staff`, etc. None should break (the comments already say "ART"), but it's worth confirming.

---

## FIX-004: NL2SQL subgraph integration

**Priority**: Medium
**Module**: `001-query-pipeline/` + `010-sandbox-sql/`
**Type**: Dead code → resurrection
**Spec**: `specs/001-query-pipeline/spec.md` DEBT-016 + sandbox SQL

### Problem
`application/pipeline/subgraphs/nl2sql.py` exists as a complete LangGraph subgraph with `generate_sql_node`, `execute_sql_node`, `fix_sql_node` nodes and conditional edges for a retry loop. But **it is not integrated into the main graph**. The current NL2SQL logic lives inlined in `application/pipeline/connectors/sandbox.py` (with its own ad-hoc retry loop).

Result:
- Duplicated code: two implementations of the same loop (subgraph + inline in connector)
- The subgraph is dead code — nobody executes it
- Hard to test: the inlined logic in `connectors/sandbox.py` is not reusable
- Architectural inconsistency with the rest of the pipeline (which is LangGraph-native)

### Chosen resolution
Integrate the `nl2sql.py` subgraph into the real flow:
1. `connectors/sandbox.py` stops having the inlined retry loop
2. The sandbox step invokes the `nl2sql` subgraph as a subcall from the `execute_steps` node of the main graph, or as a compiled subgraph invoked directly
3. The subgraph state (`NL2SQLState`) encapsulates: `nl_query`, `tables_context`, `few_shot_block`, `generated_sql`, `attempt`, `max_attempts`, `last_error`, `data_results`
4. The subgraph returns `DataResult` to the main graph like any other connector

### Acceptance criteria
- [ ] `application/pipeline/subgraphs/nl2sql.py` is invoked from `application/pipeline/connectors/sandbox.py` or from a node in the main graph
- [ ] The inlined retry loop in `connectors/sandbox.py` is removed
- [ ] External behavior preserved:
  - [ ] Only SELECT/WITH allowed (3-layer validation intact)
  - [ ] Max 2 self-correction retries
  - [ ] Same response to the user under the same conditions
- [ ] Integration tests for the `/sandbox/ask` endpoint still pass
- [ ] A new unit test for the isolated subgraph (mock LLM and sandbox)
- [ ] The query pipeline `[DEBT-016]` debt is resolved

### Affected files
- `application/pipeline/subgraphs/nl2sql.py` — possible refactor to expose a cleaner interface
- `application/pipeline/connectors/sandbox.py` — remove inlined retry loop, delegate to subgraph
- Possibly `application/pipeline/graph.py` if it is decided to expose it as a subgraph to the main graph
- New tests in `tests/unit/application/pipeline/subgraphs/test_nl2sql.py`

### Additional consideration
Before starting: **verify that the subgraph is not out of date** compared to the inlined logic. If the inline has improvements that the subgraph lacks (e.g., better timeout handling, additional validation), first port those improvements to the subgraph, and then migrate.

---

## FIX-005: X-User-Email → JWT server-side validation

**Priority**: **High (security)**
**Module**: `003-auth/`
**Type**: Vulnerability
**Spec**: `specs/003-auth/spec.md` CL-001
**Related debt**: `003-auth/DEBT-002`

### Problem
The backend currently derives the caller identity from the `X-User-Email` header, which is provisioned by the trusted reverse proxy after the frontend completes the Google OAuth flow. The backend itself does not cryptographically verify the claim — it trusts the upstream chain (NextAuth + reverse proxy) to set the header correctly.

The improvement is to move identity verification into the API layer itself, so the backend no longer depends on the infrastructure chain being configured correctly to know who the caller is. Server-side validation of Google's OAuth ID token (via JWKS) provides that independent check.

### Chosen resolution
Replace the trust model with **server-side JWT validation**:

1. **Frontend (NextAuth)**: after successful OAuth with Google, obtains a Google JWT ID token. Sends it to the backend as `Authorization: Bearer <jwt>`.
2. **Backend**: a new middleware validates the JWT on every authenticated request:
   - Verifies signature with Google's public keys (JWKS endpoint: `https://www.googleapis.com/oauth2/v3/certs`)
   - Verifies `iss` (issuer = `accounts.google.com` or `https://accounts.google.com`)
   - Verifies `aud` (audience = the configured OAuth client_id)
   - Verifies `exp` (not expired) and `iat` (reasonable)
   - Extracts `email` from the verified claim
3. **Compatibility**: keep the `X-User-Email` header as a deprecated fallback during the transition (with warning log), then remove completely.

### Acceptance criteria
- [ ] New middleware `JwtAuthMiddleware` in `presentation/http/middleware/`
- [ ] The middleware validates Google JWTs with JWKS cache (refresh every ~24h)
- [ ] Endpoints `/users/*`, `/developers/keys/*`, `/query/smart` reject requests without a valid JWT (401)
- [ ] The email is extracted only from verified JWT claims, not from the header
- [ ] The `X-User-Email` header is ignored (or just logged as a warning if it appears)
- [ ] Frontend `openarg_frontend` updated to send `Authorization: Bearer <jwt>`
- [ ] Security tests:
  - [ ] Request with valid JWT → OK
  - [ ] Request with JWT of another `aud` → 401
  - [ ] Request with expired JWT → 401
  - [ ] Request with JWT signed with another key → 401
  - [ ] Request without Authorization header → 401
- [ ] Documented in `003-auth/spec.md` as an applied architectural decision

### Affected files (backend)
- `presentation/http/middleware/jwt_auth.py` (new)
- `presentation/http/middleware/__init__.py`
- `setup/ioc/provider_registry.py` — register middleware
- `presentation/http/controllers/users/users_router.py` — remove direct reading of `X-User-Email`
- `presentation/http/controllers/developers/developers_router.py` — same
- `setup/config/settings.py` — add `GOOGLE_OAUTH_CLIENT_ID` config
- New tests

### Affected files (frontend, out of scope of this repo)
- `openarg_frontend` — NextAuth config to expose the ID token to API calls
- API client wrapper that injects the `Authorization: Bearer` header

### Rollout note
This fix requires backend + frontend coordination. Suggested rollout:
1. Backend accepts BOTH: valid JWT OR `X-User-Email` (with warning log when it uses the header)
2. Frontend is updated to use JWT
3. After verifying 0 requests with the header in logs for ~1 week, the backend definitively rejects the header

---

## FIX-006: Token counting via Bedrock stream metadata

**Priority**: Medium
**Module**: `001-query-pipeline/`
**Type**: Observability + Correctness
**Spec**: `specs/001-query-pipeline/spec.md` CL-001
**Related debt**: `001-query-pipeline/DEBT-001`, `DEBT-008`

### Problem
The `analyst` node in `application/pipeline/nodes/analyst.py:239` sets `tokens_used = 0` unconditionally when in streaming mode:

```python
tokens_used = 0  # TODO: tracking en streaming mode
```

Consequences:
- `api_usage.tokens_used` is always 0 for streamed queries
- `MetricsCollector.record_tokens()` never receives real values from the analyst
- Impossible to track cost per user, per query, per model
- Cannot detect prompts that are being inflated
- Cannot optimize based on real data
- Budget alerting impossible

The same problem (possibly) exists in the `policy` node (not verified).

### Chosen resolution
**Option B: Stream metadata**. Bedrock Claude Haiku 4.5 exposes `usage.input_tokens` and `usage.output_tokens` in the stream's `message_stop` event. It is industry standard, accurate, adds no extra cost, and requires parsing the end of the stream.

Implementation:
1. In `infrastructure/adapters/llm/bedrock_llm_adapter.py`, the `chat_stream()` method must capture the `usage` from the last chunk and expose it as part of the return (e.g., last yield with a special type, or a property accessible on the async iterator).
2. In `application/pipeline/nodes/analyst.py`, read that `usage` at the end of the stream and assign it to `state["tokens_used"]`.
3. In `finalize.py`, `MetricsCollector.record_tokens(count, model)` receives the real value.
4. Do the same for `gemini_adapter.py` (the fallback must also report tokens).
5. Update the `ILLMProvider` port if necessary so that both adapters expose usage.

### Acceptance criteria
- [ ] `BedrockLLMAdapter.chat_stream()` exposes `usage` (input_tokens + output_tokens) to the consumer
- [ ] `GeminiAdapter.chat_stream()` equivalent (Gemini also exposes usage in the final response)
- [ ] `analyst.py` assigns `state["tokens_used"] = usage.input_tokens + usage.output_tokens`
- [ ] `finalize.py` calls `metrics.record_tokens(tokens_used, model="claude-haiku-4.5")` with the real value
- [ ] `api_usage.tokens_used` in DB reflects the real count for queries via `/api/v1/ask`
- [ ] `/api/v1/metrics` shows `tokens.total > 0` in production
- [ ] Unit test: mock Bedrock stream with `message_stop` that has `usage` → analyst returns correct tokens_used
- [ ] Integration test with real LLM (optional, staging): a real query reports tokens > 0
- [ ] The query pipeline debts `[DEBT-001]` and `[DEBT-008]` are resolved

### Affected files
- `domain/ports/llm/llm_provider.py` — possible update of the `chat_stream()` contract
- `infrastructure/adapters/llm/bedrock_llm_adapter.py` — parse `message_stop` event
- `infrastructure/adapters/llm/gemini_adapter.py` — equivalent for Gemini
- `application/pipeline/nodes/analyst.py` — consume usage from the stream
- `application/pipeline/nodes/policy.py` — same (if it also uses LLM)
- `application/pipeline/nodes/finalize.py` — verify that it passes tokens_used to metrics
- Tests

---

## FIX-007: Semantic cache cleanup task

**Priority**: Medium
**Module**: `004-semantic-cache/`
**Type**: Structural debt + performance
**Spec**: `specs/004-semantic-cache/spec.md` CL-001
**Related debt**: `004-semantic-cache/DEBT-004`

### Problem
The `query_cache` table has an `expires_at` field and is filtered on read (`WHERE expires_at > now()`), but **no scheduled task exists to delete expired entries**. Confirmed via grep: there is no `DELETE FROM query_cache` nor `cleanup_query_cache` in `celery/tasks/*.py`.

Consequences:
- Expired entries accumulate indefinitely
- The HNSW index `query_cache(embedding)` includes dead embeddings in its graph → progressive degradation of vector search performance
- The table size grows monotonically without limit
- Over the long term, storage and memory footprint increase with no benefit

### Chosen resolution
Create a scheduled Celery task that deletes expired entries periodically:

```python
@celery_app.task(name="openarg.cleanup_semantic_cache")
def cleanup_semantic_cache():
    with engine.begin() as conn:
        result = conn.execute(text(
            "DELETE FROM query_cache WHERE expires_at < now() - INTERVAL '1 hour'"
        ))
        deleted = result.rowcount
    logger.info("Semantic cache cleanup: %d entries deleted", deleted)
    # Opcional: REINDEX CONCURRENTLY cada N ejecuciones
    return {"deleted": deleted}
```

Add it to the beat schedule, running every 6 hours (reasonable given that the minimum TTL is 300s = 5 min). Suggested queue: `ingest` (low priority, does not block user queries).

### Acceptance criteria
- [ ] New task `cleanup_semantic_cache` registered in `celery/tasks/` (new file or inside an existing generic one)
- [ ] Beat schedule entry added: every 6h in the `ingest` queue
- [ ] The task deletes entries with `expires_at < now() - 1 hour` (buffer to avoid race condition with simultaneous reads)
- [ ] Logging of the deleted count
- [ ] New metric: `semantic_cache.entries_deleted_per_cleanup`
- [ ] Unit tests with test DB verifying the deletion
- [ ] Consider periodic `REINDEX CONCURRENTLY query_cache` (e.g., weekly) to keep the HNSW index efficient — optional, evaluate performance first
- [ ] Entry in `specs/004-semantic-cache/plan.md` documenting the new task
- [ ] The semantic cache `[DEBT-004]` debt is resolved

### Affected files
- `infrastructure/celery/tasks/` — new `cache_cleanup_tasks.py` file (or add to an existing one)
- `infrastructure/celery/app.py` — beat schedule entry + task routing
- `specs/004-semantic-cache/plan.md` — document the task
- New tests

### Consideration
Evaluate whether the cleanup should also do `VACUUM ANALYZE query_cache` occasionally, or whether Postgres autovacuum is sufficient (probably yes for the current volume).

---

## FIX-008: Staff diff misses field changes (invisible transfers)

**Priority**: Low-Medium
**Module**: `002-connectors/002f-staff/`
**Type**: Data correctness / feature gap
**Spec**: `specs/002-connectors/002f-staff/spec.md` CL-003
**Related debt**: `002f-staff/DEBT-002` (snapshot model rigidity)

### Problem
The diff logic in `staff_tasks.py:136-158` (task `snapshot_staff`) **only detects two types of changes**:
- **additions**: new `legajo` not present in prev snapshot
- **removals**: `legajo` present in prev but absent in current

**Field changes** in existing employees are **completely ignored**. Specifically:
- If an advisor changes their `area_desempeno` (transfer between diputados' offices)
- If they change their `escalafon` (promotion/category change)
- If they change their `convenio` (from temporary to permanent, etc.)

None of these changes generate an event in `staff_changes`. The table only supports `tipo IN ('alta', 'baja')`, never `update`.

### Impact
- Queries like "how many advisors were transferred between offices last month?" are **impossible**
- Organizational shuffling within Congreso (staff reassignment to new blocs post-elections) is **invisible to the system**
- The "changes" reports are **incomplete** — they only capture hirings and firings
- There is no way to detect promotions/demotions (scale change)

### Chosen resolution
Enrich the diff logic to detect field changes in records that persist across snapshots. When `area_desempeno`, `escalafon` or `convenio` change between two consecutive snapshots, generate an event in `staff_changes` with a new type (e.g., `"update"` or `"transfer"`).

Pseudo-code:

```python
# staff_tasks.py:136-158 (enhanced)
prev_by_legajo = {r.legajo: r for r in prev_snapshot}
curr_by_legajo = {r.legajo: r for r in current_snapshot}

altas = [curr for l, curr in curr_by_legajo.items() if l not in prev_by_legajo]
bajas = [prev for l, prev in prev_by_legajo.items() if l not in curr_by_legajo]

# NEW: detect field changes for existing legajos
updates = []
for legajo, curr in curr_by_legajo.items():
    prev = prev_by_legajo.get(legajo)
    if prev is None:
        continue
    changes = {}
    for field in ("area_desempeno", "escalafon", "convenio"):
        if getattr(prev, field) != getattr(curr, field):
            changes[field] = {"from": getattr(prev, field), "to": getattr(curr, field)}
    if changes:
        updates.append({"legajo": legajo, "changes": changes, "current": curr})

# Persist: altas, bajas, AND updates into staff_changes
```

### Acceptance criteria
- [ ] Diff logic detects changes in `area_desempeno`, `escalafon`, `convenio` between consecutive snapshots
- [ ] `staff_changes.tipo` accepts a new value: `"update"` (or alternatively 2 events: `"baja_area"` + `"alta_area"` if you prefer to keep the binary enum)
- [ ] Alembic migration if `tipo` is a CHECK constraint or enum
- [ ] New optional field `staff_changes.changes_json` (jsonb) to capture the exact diff (which field, from, to)
- [ ] Unit test: prev_snapshot with employee in area A + current_snapshot with the same legajo in area B → generates 1 update event in staff_changes
- [ ] Query example enabled: `SELECT COUNT(*) FROM staff_changes WHERE tipo = 'update' AND detected_at > now() - INTERVAL '30 days'`
- [ ] The staff `[DEBT-002]` debt is partially resolved

### Affected files
- `infrastructure/celery/tasks/staff_tasks.py` — enhance diff logic (lines 136-158)
- Possible Alembic migration for `staff_changes.tipo` and `staff_changes.changes_json`
- Update `IStaffConnector.get_changes()` signature if you want to expose the new event type

### Priority note
**Low-Medium**: no users reporting missing transfer info (the feature doesn't exist yet, no one is asking for it). But it is a data completeness gap that can bite when someone does longitudinal analysis of legislative staff. Consider it when planning a "staff movement analysis" feature or similar.

---

## Final notes

- **Suggested implementation order** (considering dependencies and impact):
  1. `FIX-003` (timezone) — 30 minutes, 2 lines, low risk
  2. `FIX-002` (BCRA dedup) — 1-2 hours, hygiene, low risk
  3. `FIX-001` (BCRA errors) — 2-3 hours, visible UX, medium risk (tests needed)
  4. `FIX-006` (token counting) — 3-4 hours, requires changes to the LLM port, medium risk
  5. `FIX-004` (NL2SQL integration) — 4-6 hours, structural refactor, medium risk
  6. `FIX-005` (JWT validation) — **days**, requires backend+frontend coordination, high risk but critical for security

- **When to start**: at the user's discretion. This backlog is the actionable guide. There is no self-imposed urgency; the fixes wait for an explicit decision.

- **How to update this file**: when a fix is implemented, change `Status` to `In progress` / `Completed` in the summary table, and add `Completed: YYYY-MM-DD` + link to the commit at the end of the fix's section.

---

**End of FIX_BACKLOG.md**
