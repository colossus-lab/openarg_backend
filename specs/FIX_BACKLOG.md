# OpenArg Fix Backlog

**Purpose**: Ordered list of pending fixes derived from the April 2026 reverse-SDD. Each entry has priority, affected module, problem description, chosen resolution, acceptance criteria, and affected files.

**Convention**:
- `FIX-NNN` IDs are **global to the project** (not per module).
- Priority: **High** (security or user-visible correctness) / **Medium** (structural debt) / **Low** (nice-to-fix).
- Status: `Pending` | `In progress` | `Completed`.

**Overall status**: Updated 2026-04-12 after multiple fix passes under strict §0.5 spec-first discipline. **15 of 18** fixes applied: FIX-001 through FIX-013, FIX-017, and FIX-018. FIX-005 (Google JWT server-side validation) was implemented and deployed to staging in `dual` mode first and then flipped to `enforced` on 2026-04-11 — all auth now runs through `Authorization: Bearer <google_id_token>`, the legacy `X-User-Email` header path was deleted, and admin-key endpoints are exempt per FR-007a. FIX-017 (defensive JSON encoder) was added on 2026-04-12 after a prod regression: the post-deploy cache flush exposed a latent `datetime is not JSON serializable` crash in three serialization sinks (Redis, semantic cache, WebSocket v2). Three items remain **deferred**: FIX-014 (DB-first refactor for `series_tiempo` + `bcra`, architectural follow-up), FIX-015 (full RENABAP ingest, data expansion), and FIX-016 (DDJJ scope expansion to senadores/ejecutivo/jueces).

**Relation to specs**: Each fix is referenced from its corresponding `spec.md` in the "Open Questions" section (now with marker `[RESOLVED CL-XXX]` → `specs/FIX_BACKLOG.md#FIX-NNN`).

---

## Summary

| ID | Priority | Module | Title | Status |
|---|---|---|---|---|
| FIX-001 | **High** | `002a-bcra` | BCRA silent degradation → surface errors | ✅ Completed 2026-04-10 |
| FIX-002 | Medium | `002a-bcra` | BCRA deduplicate broken methods | ✅ Completed 2026-04-10 |
| FIX-003 | Low | `002a-bcra` + Celery config | BCRA schedule timezone → ART | ✅ Completed (already applied) |
| FIX-004 | Medium | `001-query-pipeline` + `010-sandbox-sql` | NL2SQL subgraph integration | ✅ Completed 2026-04-11 |
| FIX-005 | **High (security)** | `003-auth` | X-User-Email → JWT server-side validation | ✅ Completed 2026-04-11 (enforced + FR-007a admin exemption) |
| FIX-006 | Medium | `001-query-pipeline` | Token counting via Bedrock stream metadata | ✅ Completed 2026-04-10 |
| FIX-007 | Medium | `004-semantic-cache` | Semantic cache cleanup task (expired entries never deleted) | ✅ Completed 2026-04-10 |
| FIX-008 | Low-Medium | `002f-staff` | Staff diff misses field changes (transfers between offices invisible) | ✅ Completed 2026-04-11 |
| FIX-009 | Medium | `002h-ddjj` + `001-query-pipeline` | DDJJ top-N requests return partial results (compact ranking) | ✅ Completed 2026-04-11 |
| FIX-010 | Medium | `001-chat-bridge` + `001-query-pipeline` | Chart rendering regression (finalize return missing chart_data) | ✅ Completed 2026-04-11 |
| FIX-011 | Medium | `001-query-pipeline/001d-analysis` | Analyst leaks `cache_*` internal identifiers in answers | ✅ Completed 2026-04-11 |
| FIX-012 | Medium | `001-query-pipeline/001d-analysis` | Analyst apologetic preface masks real data | ✅ Completed 2026-04-11 |
| FIX-013 | Medium | `002-connectors/002b-series-tiempo` | Series fetch retry without date range on empty result | ✅ Completed 2026-04-11 |
| FIX-014 | Medium | `001-query-pipeline` + `002a-bcra` + `002b-series-tiempo` | DB-first refactor for connectors with daily snapshots | ⏸ Deferred (architectural) |
| FIX-015 | Low | `002-connectors` (ingestion) | Full RENABAP per-barrio ingest | ⏸ Deferred (data expansion) |
| FIX-016 | Low | `002-connectors/002h-ddjj` | DDJJ scope expansion to senadores / ejecutivo / jueces | ⏸ Deferred (data expansion) |
| FIX-017 | **High** | `004-semantic-cache` + `001-query-pipeline` + `004-caching` | JSON serialization crash on non-primitive state values | ✅ Completed 2026-04-12 |

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
**Status**: **DONE — enforced 2026-04-11**
**Module**: `003-auth/`
**Type**: Hardening
**Spec**: `specs/003-auth/spec.md` FR-007, CL-001
**Related debt**: `003-auth/DEBT-002` (closed)

The backend now validates the Google OAuth ID token on every authenticated
request via JWKS. The legacy ``X-User-Email`` header path has been deleted
from both backend and frontend. The ``GOOGLE_JWT_VALIDATION_MODE`` rollout
flag is gone — validation is always enforced.

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

## FIX-009: DDJJ top-N requests return partial results

**Priority**: Medium (correctness)
**Status**: **DONE 2026-04-11** — ranking rows now use a compact shape
**Module**: `002-connectors/` (DDJJ connector) + `001-query-pipeline/`
**Type**: Correctness / data completeness

### Problem
When the user asks for a ranked top-N (e.g. "¿Quiénes son los 10 diputados con
mayor patrimonio declarado?"), the pipeline returns fewer rows than requested.
Observed on 2026-04-11 against staging: asked for top 10, got only 4 complete
records (Brugge, Kirchner, Carrizo, Ritondo) followed by an admission that
"datos del 5º al 10º registro incompletos en la recolección".

Root cause suspected in one of:

- DDJJ adapter limiting rows too aggressively in its internal paging or
  by truncating the JSON slice loaded into memory (we only have 195 diputados
  in the local dataset — 10 top by patrimonio should always be reachable).
- Planner allocating fewer PlanSteps than needed when the ask is "top 10"
  and then stopping early.
- Analyst prompt dropping rows when synthesizing the final answer.

### Acceptance criteria
- [ ] "¿Quiénes son los 10 diputados con mayor patrimonio declarado?" returns
      exactly 10 rows with numeric patrimonio values, sorted descending.
- [ ] A regression test in `tests/integration/` or the analyst-prompt fixture
      set pins the top-10 behavior so it cannot silently drop back to top-4.
- [ ] Same check for senadores once the DDJJ coverage expands beyond the
      current 195 diputados sample (see `000-architecture/` CL-005).

### Investigation hint
Start by logging the row count returned at each step in
`app/application/pipeline/` for the DDJJ path, then compare with the
analyst prompt's final rendered table.

---

## FIX-010: Chart rendering regression in chat bridge

**Priority**: Medium (visible UX regression)
**Status**: **DONE 2026-04-11** — finalize_node now returns chart_data in its update dict
**Module**: `001-chat-bridge/` + `001-query-pipeline/`
**Type**: Regression (latent since LangGraph Phase 1)

### Problem
Charts no longer render in the chat UI for queries that historically produced
them. Confirmed broken on 2026-04-11 with "¿Cómo viene la inflación en los
últimos meses?" and "Mostrame la evolución de las reservas del BCRA". Both
used to return `chart_data` that the frontend renders as interactive charts;
current behavior is text-only.

Unclear whether the breakage is backend (pipeline no longer emitting
``chart_data`` in the SmartResult) or frontend (stream event whitelist or
SSE mapping dropping the payload). Note: 2026-04-11 backend logs contain
a warning `stream_payload dropped keys ['connector'] (type='status') —
add them to _STREAM_ALLOWED_PAYLOAD_KEYS if they should reach the browser`
which suggests the allowed-keys whitelist may also be stripping
``chart_data`` in some paths.

### Acceptance criteria
- [ ] The two reference queries above produce charts in the browser on
      staging, end to end.
- [ ] A regression test (frontend vitest + backend unit) pins that
      ``chart_data`` is present in the SmartResult and reaches the
      browser intact through both WS primary and HTTP fallback.
- [ ] `_STREAM_ALLOWED_PAYLOAD_KEYS` audited: any legitimate payload keys
      that are currently being dropped get added.

### Investigation hint
Start at `app/presentation/http/controllers/query/smart_query_v2_router.py`
where `_STREAM_ALLOWED_PAYLOAD_KEYS` lives, then trace the chart payload
back through the NL2SQL subgraph and forward to the frontend's chat bridge
(`src/lib/chat/wsBridge.ts` + `syncFallback.ts`).

---

## FIX-011: Analyst leaks internal `cache_*` table names in answers

**Priority**: Medium (UX / internal-detail leak)
**Status**: **DONE 2026-04-11** — analyst clean_answer now scrubs `cache_*`/`dataset_chunks`/`pgvector`/`query_cache` tokens
**Module**: `001-query-pipeline/001d-analysis`
**Type**: Regression / hygiene
**Spec**: `specs/001-query-pipeline/001d-analysis/spec.md` FR-025e

### Problem
The analyst LLM sometimes cited internal sandbox table names verbatim when summarizing which source answered a query, e.g. *"(Fuente: cache_leyes_sancionadas)"*. These are internal infrastructure identifiers that should never reach the user. Discovered by the E2E suite (`ask()` helper's `_INTERNAL_LEAKS` check) on `test_leyes_sancionadas` and `test_empleados_publicos`.

### Resolution
Added a post-processing scrub step in `analyst_node` that removes any token matching `cache_*`, `dataset_chunks`, `pgvector`, `query_cache` or `cached_datasets` from `clean_answer` after the LLM response is assembled. Unit-tested in `tests/unit/test_analyst_scrub.py`.

---

## FIX-012: Analyst apologetic preface masks real data

**Priority**: Medium (UX)
**Status**: **DONE 2026-04-11** — prompt + post-process strip leading "No tengo X pero..." when data follows
**Module**: `001-query-pipeline/001d-analysis`
**Type**: UX polish
**Spec**: `specs/001-query-pipeline/001d-analysis/spec.md` FR-025f

### Problem
The analyst sometimes opens its answer with *"No tengo datos específicos sobre X, pero el Índice de Salarios creció 7.869%..."* — burying the real data under a disclaimer. The `analyst.txt` prompt already forbids the construction, but the LLM ignores the rule often enough that the E2E `answer_contains()` helper flags it (it looks for negation phrases near the topic keyword).

### Resolution
1. Strengthened the `analyst.txt` prompt with a "lead with data, not with apologies" rule.
2. Added a post-processing step that drops a leading sentence matching `^[^.!?]*\b(No tengo|No encontré|No pude acceder|No dispongo|No cuento con)\b[^.!?]*[.!?]\s*` **when the remaining text contains numeric data**. Genuinely data-less answers (RENABAP, Adorni ejecutivo) keep their honest disclaimer.

Unit-tested in `tests/unit/test_analyst_scrub.py`.

---

## FIX-013: Series retry without date range on empty result

**Priority**: Medium (robustness)
**Status**: **DONE 2026-04-11** — `execute_series_step` retries once without start/end when a catalog-matched fetch returns empty
**Module**: `001-query-pipeline` + `002-connectors/002b-series-tiempo`
**Type**: Graceful degradation
**Spec**: `specs/002-connectors/002b-series-tiempo/spec.md` FR-009

### Problem
A question like *"IPC de inflación mensual en febrero 2026?"* made the planner emit `start_date=2026-02-01` / `end_date=2026-02-28`. But INDEC had only published up to January 2026 at the time of the query, so the API returned `{"data": []}` and the adapter raised `ConnectorError("API respondió sin datos")`. The user got a "no data" answer even though the pipeline had January 2026 available.

### Resolution
After a catalog-matched fetch returns `None`, `execute_series_step` now retries the same `series.fetch()` call once with `start_date=None`, `end_date=None`. The analyst then describes the latest available data and can tell the user *"los datos más recientes son de enero 2026: 2.88%"*.

---

## FIX-014: DB-first refactor for connectors with daily snapshots (series_tiempo + bcra)

**Priority**: Medium (architectural, performance, resilience)
**Status**: **Open** — deferred
**Module**: `001-query-pipeline/001c-execution` + `002-connectors/002b-series-tiempo` + `002-connectors/002a-bcra`
**Type**: Architectural drift from spec (violates top-level `002-connectors/spec.md` **FR-013**)
**Spec**: `specs/002-connectors/spec.md` FR-013, `specs/002-connectors/002b-series-tiempo/spec.md` FR-007

### Problem
The top-level connectors spec now has **FR-013 (DB-first norm)**: any connector with a daily snapshot MUST be queried from the cached PG tables first, falling through to the live API only for misses/stale data. Two connectors currently violate this rule.

**`series_tiempo`**: FR-007 of the Series de Tiempo connector says the system MUST daily-snapshot 12 key series into `cache_series_*` PG tables. The beat task is running and the tables exist with fresh data (`cache_series_inflacion_ipc`: 118 rows, `cache_series_salarios`: 111, `cache_series_reservas_internacionales`: 1000, etc.). But the query-path `SeriesTiempoAdapter.fetch()` **never reads from those tables** — it pegs the live datos.gob.ar API on every request.

**`bcra`**: similar pattern — `cache_bcra_*` tables exist (39 rows, smaller but real) and the beat task populates them, but `BCRAAdapter` always goes through `httpx.AsyncClient.get()` against the BCRA statistics API.

Consequences for both:

1. **Slower** (remote HTTP + JSON parsing per query).
2. **Less resilient** (any upstream hiccup takes out inflation / unemployment / EMAE / cotizaciones queries).
3. **Misaligned with spec FR-013** that says the daily snapshot is the source of truth.

### Proposed resolution
Add a new method on `ISeriesTiempoConnector`: `fetch_from_cache(catalog_key: str, start_date: str | None, end_date: str | None) -> DataResult | None`. Implement it in the adapter (or in a new `SeriesCacheReader`) by querying `cache_series_{catalog_key}` via a read-only PG session. In `execute_series_step`, after `find_catalog_match()` succeeds, try `fetch_from_cache` first and fall through to `series.fetch()` (live API) only when the cache is missing, stale (older than ~48h), or the series isn't in the curated catalog.

Same pattern for BCRA: expose `BCRAAdapter.fetch_from_cache(currency, start, end)` that reads from `cache_bcra_*`, and only hit the live BCRA API on miss/stale.

Acceptance:
- [ ] Unit tests for `fetch_from_cache` covering hit, miss, stale, for series_tiempo AND bcra.
- [ ] `execute_series_step` + `execute_bcra_step` unit tests asserting cache-first preference.
- [ ] E2E regressions: `test_inflacion_uses_series_tiempo`, `test_reservas_bcra` and `test_dolar` pass **without** hitting the network (or at least without depending on it).
- [ ] Top-level `002-connectors/spec.md` FR-013 compliance matrix updated from ❌ to ✅ for both connectors.

### Why deferred
The cache-first refactor requires wiring a PG read session into the step executor, defining staleness semantics (how old is "stale"?), and handling the daily-snapshot → query-path column mapping for each series. FIX-013's retry-without-date-range is the minimal change that makes `test_inflacion_uses_series_tiempo` pass today; FIX-014 is the architectural follow-up that closes the FR-013 compliance gap for good.

---

## FIX-015: RENABAP dataset is a 2-column summary, not a per-barrio registry

**Priority**: Low (data expansion)
**Status**: **Open** — deferred
**Module**: `002-connectors` (ingestion) + `010-collector-tasks`
**Type**: Data coverage gap

### Problem
The test `test_dv04_terrenos_renabap` asks *"Que porcentaje de los terrenos registrados en el RENABAP son de privados?"*. The cached table `cache_registro_nacional_de_barrios_populares` exists but has **58 rows × 2 columns** (`Título de Propiedad`, `Unnamed: 1`) — it's a metadata summary, not the per-barrio registry with ownership info. The pipeline correctly answers *"no tengo info del RENABAP"* but the E2E test flags it as a failure.

### Proposed resolution
Ingest the full RENABAP shapefile/CSV (the public one on datos.gob.ar has per-barrio fields: título propiedad, cantidad de familias, localización, etc.) so ownership-percentage queries can be answered from actual rows. Until then, `test_dv04_terrenos_renabap` is marked `xfail(reason="needs full RENABAP ingest — FIX-015")`.

---

## FIX-016: DDJJ dataset scope — ejecutivo / senadores / jueces

**Priority**: Low (data expansion)
**Status**: **Open** — deferred
**Module**: `002-connectors/002h-ddjj`
**Type**: Data coverage gap (documented assumption)

### Problem
`test_adorni_empresas_estado` asks about companies linked to Manuel Adorni (Secretary/Vocero, ejecutivo). The DDJJ dataset only contains **195 National Diputados** per `specs/002-connectors/002h-ddjj/spec.md` FR-001 and the architectural assumption. Adorni, senadores, ministros, ex-presidentes and jueces are **not** in the dataset. The answer is an honest "not found" but the test fails.

### Proposed resolution
Extend the DDJJ ingest to include senadores + ejecutivo declarations from the Oficina Anticorrupción portal (they publish all three branches). Until then, the test is marked `xfail(reason="DDJJ scope is diputados only per 002h-ddjj/spec.md FR-001 — FIX-016")`.

---

## FIX-017: JSON serialization crash on non-primitive state values

**Priority**: **High** (user-visible correctness)
**Status**: ✅ **Completed 2026-04-12**
**Modules**: `004-semantic-cache` + `001-query-pipeline` (WebSocket v2) + `004-caching` (Redis)
**Type**: Defensive serialization

### Problem

After the 2026-04-12 prod deploy + cache flush, the first fresh execution of simple queries that touch the BCRA / series-tiempo / sandbox connectors (e.g. *"Mostrame la evolución de las reservas del BCRA"*) started returning `[error: respuesta no disponible]`. Backend logs showed three stacked failures on the same pipeline run:

```
Redis cache write failed after 3 attempts: Object of type datetime is not JSON serializable
Semantic cache write failed     TypeError('Object of type datetime is not JSON serializable')
WebSocket v2 error              TypeError('Object of type datetime is not JSON serializable')
```

The pipeline itself finished correctly (`duration_ms ~17s`, audit log emitted), but all three serialization sinks (Redis cache write, semantic cache insert, WebSocket `complete` event) use `json.dumps` without a `default=` encoder, so any non-primitive value in the finalized state aborts the write. The WebSocket failure was the user-visible symptom — the `complete` event never reached the browser.

**Why it only surfaced now**: the semantic cache had accumulated pre-serialized responses for popular BCRA/reservas queries over time. Those queries always returned from cache, so the fresh pipeline path was never exercised end-to-end. The admin-flush on deploy removed the cached entries and exposed the latent bug on the next query.

**Origin of the `datetime` object**: could not be identified from the logs (structlog did not render the traceback inline). It could come from any field added to the state across the pipeline — pandas-derived records, `metadata.fetched_at` converted by a middleware, a `datetime.date` value snuck in from a DB read, etc. Regardless of origin, the serialization layer must not crash on common Python types.

### Resolution

Centralized defensive JSON encoder in a new `infrastructure/serialization/` package:

- `json_default(obj)` handles `datetime.datetime`, `datetime.date`, `datetime.time`, `decimal.Decimal`, `uuid.UUID`, `bytes`, `set`, `frozenset`, `pathlib.Path`, and objects with `__json__` / `model_dump` / `to_dict`. Falls through to `TypeError` for genuinely unknown types.
- `safe_dumps(obj, **kwargs)` is a thin wrapper over `json.dumps` that plugs in `json_default` as `default=`.
- `to_json_safe(obj)` walks a nested structure and returns a copy with the same conversions applied — used where we need to call `ws.send_json` (Starlette's `send_json` uses `json.dumps` internally with no `default=` hook).

Three serialization sites now use the helper:

1. `infrastructure/adapters/cache/redis_cache_adapter.py:25` — `safe_dumps(value)` instead of `json.dumps(value)`.
2. `infrastructure/adapters/cache/semantic_cache.py:178` — `safe_dumps(response, ensure_ascii=False)`.
3. `presentation/http/controllers/query/smart_query_v2_router.py` — all `ws.send_json(payload)` calls routed through a local `_safe_send_json(ws, payload)` helper that applies `to_json_safe` before delegating.

### Acceptance criteria

- [x] New module `src/app/infrastructure/serialization/json_safe.py` with `json_default`, `safe_dumps`, `to_json_safe`.
- [x] Unit tests covering every supported type + nested structures + unknown-type fallthrough.
- [x] Three serialization sites updated.
- [x] Regression scenario: a pipeline payload carrying `datetime` / `date` / `Decimal` inside `records` no longer crashes Redis write, semantic cache write, or WebSocket send.

### Files

- `src/app/infrastructure/serialization/__init__.py` (new, re-exports the helper)
- `src/app/infrastructure/serialization/json_safe.py` (new)
- `tests/unit/test_json_safe.py` (new)
- `src/app/infrastructure/adapters/cache/redis_cache_adapter.py` (patched)
- `src/app/infrastructure/adapters/cache/semantic_cache.py` (patched)
- `src/app/presentation/http/controllers/query/smart_query_v2_router.py` (patched)
- `specs/004-semantic-cache/spec.md` (note — updated)
- `specs/FIX_BACKLOG.md` (this entry)

### Follow-up (not part of FIX-017)

The *origin* of the `datetime` value injected into the state remains unidentified. FIX-017 is a defensive fix at the serialization layer — future work should add a debug log in the WebSocket v2 error handler that reports which top-level state key first failed the probe, so the upstream producer can be traced and fixed at the source (e.g., by converting to `isoformat()` in the connector). Tracked as an open DEBT note in `specs/001-query-pipeline/spec.md`.

---

## FIX-018: Collector execution hardening for duplicate runs and retry churn

**Priority**: **High** (throughput / staging stability)
**Status**: ✅ **Completed 2026-04-12**
**Modules**: `006-datasets/006b-ingestion` + `010-collector-tasks`
**Type**: Operational hardening

### Problem

Investigation on staging on **2026-04-12** showed three concrete collector bottlenecks:

1. **Overlapping `bulk_collect_all` runs** timing out after 120s and failing with `psycopg.OperationalError: another command is already in progress`.
2. **Duplicate `collect_dataset(dataset_id)` runs** for the same resource executing simultaneously on different workers, causing repeated downloads / parses / writes for identical datasets.
3. **Deterministic ingestion failures** such as `file_too_large`, empty CSV payloads, and unsupported Excel payloads consuming the retry budget and occupying collector slots repeatedly.

Observed staging evidence:

- `cached_datasets`: `ready=9529`, `error=2227`, `permanently_failed=85`
- dominant errors included `Table missing: marked for re-download` (468), `zip_no_parseable_file` (156), `Exhausted retries while stuck in downloading` (141), `file_too_large...` (23), `No columns to parse from file` (57), and invalid Excel payloads (50)
- no old stuck downloads at inspection time (`downloading > 30 min = 0`), indicating churn rather than a dead queue

### Resolution

Implemented three targeted mitigations in `collector_tasks.py`:

1. **Per-dataset single-flight lock**
   - `collect_dataset(dataset_id)` now acquires a PostgreSQL advisory lock derived from `dataset_id`.
   - Duplicate invocations exit immediately with `{"status": "already_collecting"}`.

2. **Singleton lock for `bulk_collect_all`**
   - `bulk_collect_all()` now acquires a dedicated advisory lock before reconciliation + dispatch.
   - Concurrent overlapping runs exit as `{"status": "skipped_already_running"}`.
   - The task timeout was raised from `120/180s` to `300/420s` to better match the amount of orchestration work now performed before dispatch completes.

3. **Deterministic failure short-circuit**
   - `file_too_large`
   - `No columns to parse from file`
   - `Excel file format cannot be determined`
   now bypass retries and go directly through `_set_error_status(...)` as non-retryable collector failures.

### Acceptance criteria

- [x] Duplicate `collect_dataset` runs for the same dataset short-circuit instead of doing duplicate work.
- [x] Overlapping `bulk_collect_all` runs short-circuit instead of dispatching overlapping waves.
- [x] Deterministic oversized / empty / invalid-format payloads no longer consume retry churn.
- [x] Focused unit validation passes.

### Validation

Executed on 2026-04-12:

- `pytest -q tests/unit/test_pipeline_p2_tasks.py` → `16 passed`
- `pytest -q tests/unit/test_error_scenarios.py tests/unit/test_pipeline_p2_tasks.py` → `37 passed`

Follow-up completed the same day after staging verification:

4. **Nested ZIP parsing**
   - The collector now descends one ZIP layer when portals wrap the real payload in an inner `.zip`.
   - This directly targets `zip_no_parseable_file` cases observed in staging where the outer ZIP contained `geojson`/`shp` bundles instead of raw files.

5. **Legacy sandbox-table compatibility**
   - Planner hints and NL2SQL now normalize stale aliases seen in staging and prompts:
     - `cache_series_tiempo_ipc` -> `cache_series_inflacion_ipc`
     - `cache_bcra_principales_variables` -> `cache_bcra_cotizaciones`
     - `cache_presupuesto_nacional` -> current `cache_presupuesto_*` tables
   - `coparticipacion` routing was changed from a non-existent dedicated table to generic budget tables plus domain notes.

Additional validation:

- `pytest -q tests/unit/test_pipeline_p2_tasks.py tests/unit/test_dataset_index.py tests/unit/test_nl2sql_subgraph.py` → `74 passed`
- `pytest -q tests/unit/test_error_scenarios.py tests/unit/test_pipeline_p2_tasks.py tests/unit/test_dataset_index.py tests/unit/test_nl2sql_subgraph.py` → `95 passed`

### Files

- `src/app/infrastructure/celery/tasks/collector_tasks.py`
- `tests/unit/test_pipeline_p2_tasks.py`
- `specs/006-datasets/006b-ingestion/spec.md`
- `specs/006-datasets/006b-ingestion/plan.md`
- `specs/FIX_BACKLOG.md`

---

## Final notes

- **Deferred items** — the next ones to pick up whenever there is time:
  1. `FIX-014` (DB-first refactor for series_tiempo + bcra) — architectural follow-up. Requires wiring a PG read session into `execute_series_step` / `execute_bcra_step`, defining staleness semantics, and mapping daily-snapshot columns to the query-path shape. Estimated 2–3 hours of focused work. High value: closes the FR-013 compliance gap and makes both connectors faster, more resilient, and less dependent on upstream availability.
  2. `FIX-015` (full RENABAP per-barrio ingest) — data expansion. The current `cache_registro_nacional_de_barrios_populares` table is a 58-row × 2-column summary. Ingesting the full shapefile/CSV with per-barrio ownership and location fields would unlock queries like "qué porcentaje de terrenos son privados" that today fall back to an honest "no data".
  3. `FIX-016` (DDJJ scope expansion: senadores / ejecutivo / jueces) — data expansion. Today the dataset only has 195 diputados per `002h-ddjj/spec.md` FR-001. Expanding to the other branches opens the Adorni / Bullrich / Kicillof / etc. query paths currently answered with "out of scope".

- **When to start**: at the user's discretion. This backlog is the actionable guide. There is no self-imposed urgency; the fixes wait for an explicit decision.

- **How to update this file**: when a fix is implemented, change `Status` to `In progress` / `Completed` in the summary table, and add `Completed: YYYY-MM-DD` + link to the commit at the end of the fix's section.

---

**End of FIX_BACKLOG.md**
