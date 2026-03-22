# API Reference

Base URL: `http://localhost:8081` (dev) or as configured.

All API endpoints are under `/api/v1` except health checks.

---

## Health

### `GET /health`

Basic health check.

**Response:** `200 OK`
```json
{ "status": "ok", "service": "openarg" }
```

### `GET /health/ready`

Readiness probe.

**Response:** `200 OK`
```json
{ "status": "ready" }
```

---

## Users

### `POST /api/v1/users/sync`

Upsert a user from Google OAuth (NextAuth frontend). Creates the user if they don't exist, updates name/image if they do.

**Request Body:**
```json
{
  "email": "user@example.com",
  "name": "John Doe",
  "image": "https://lh3.googleusercontent.com/..."
}
```

**Response:** `200 OK`
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "email": "user@example.com",
  "name": "John Doe",
  "image_url": "https://lh3.googleusercontent.com/...",
  "created_at": "2026-02-28T12:00:00+00:00"
}
```

### `GET /api/v1/users/me?email={email}`

Get user by email.

**Query Parameters:**
| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `email` | string | yes | User email address |

**Response:** `200 OK` — Same shape as sync response.

**Errors:** `404` if user not found.

---

## Conversations

### `GET /api/v1/conversations/`

List conversations for a user, ordered by most recently updated.

**Query Parameters:**
| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `user_email` | string | no | — | Filter by user email |
| `limit` | int | no | 20 | Max results |
| `offset` | int | no | 0 | Pagination offset |

**Response:** `200 OK`
```json
[
  {
    "id": "550e8400-...",
    "title": "Inflacion en Argentina",
    "created_at": "2026-02-28T12:00:00+00:00",
    "updated_at": "2026-02-28T12:05:00+00:00"
  }
]
```

Returns `[]` if no `user_email` provided or user not found.

### `POST /api/v1/conversations/`

Create a new conversation.

**Request Body:**
```json
{
  "user_email": "user@example.com",
  "title": "Inflacion en Argentina"
}
```

**Response:** `200 OK`
```json
{
  "id": "550e8400-...",
  "title": "Inflacion en Argentina",
  "created_at": "2026-02-28T12:00:00+00:00",
  "updated_at": "2026-02-28T12:00:00+00:00",
  "messages": []
}
```

**Errors:** `404` if user not found (must sync first).

### `GET /api/v1/conversations/{conversation_id}`

Get a conversation with all its messages.

**Response:** `200 OK`
```json
{
  "id": "550e8400-...",
  "title": "Inflacion en Argentina",
  "created_at": "2026-02-28T12:00:00+00:00",
  "updated_at": "2026-02-28T12:05:00+00:00",
  "messages": [
    {
      "id": "660e8400-...",
      "conversation_id": "550e8400-...",
      "role": "user",
      "content": "Como viene la inflacion?",
      "sources": [],
      "created_at": "2026-02-28T12:00:00+00:00"
    },
    {
      "id": "770e8400-...",
      "conversation_id": "550e8400-...",
      "role": "assistant",
      "content": "Segun los datos del INDEC...",
      "sources": [{"title": "IPC INDEC", "portal": "datos_gob_ar"}],
      "created_at": "2026-02-28T12:00:05+00:00"
    }
  ]
}
```

**Errors:** `404` if conversation not found.

### `DELETE /api/v1/conversations/{conversation_id}`

Delete a conversation and all its messages (CASCADE).

**Response:** `200 OK`
```json
{ "status": "deleted", "id": "550e8400-..." }
```

### `PATCH /api/v1/conversations/{conversation_id}`

Update conversation title.

**Request Body:**
```json
{ "title": "New Title" }
```

**Response:** `200 OK` — `ConversationSummary` object.

### `POST /api/v1/conversations/{conversation_id}/messages`

Add a message to a conversation.

**Request Body:**
```json
{
  "role": "user",
  "content": "Como viene la inflacion?",
  "sources": []
}
```

**Response:** `200 OK`
```json
{
  "id": "660e8400-...",
  "conversation_id": "550e8400-...",
  "role": "user",
  "content": "Como viene la inflacion?",
  "sources": [],
  "created_at": "2026-02-28T12:00:00+00:00"
}
```

### `GET /api/v1/conversations/{conversation_id}/messages`

Get messages for a conversation (ordered by `created_at ASC`).

**Query Parameters:**
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `limit` | int | 100 | Max messages |
| `offset` | int | 0 | Pagination offset |

**Response:** `200 OK` — Array of `MessageResponse` objects.

---

## Query

### `POST /api/v1/query/`

Submit a query for asynchronous analysis via Celery workers.

**Request Body:**
```json
{
  "question": "Cuanto gasta el gobierno en educacion?",
  "user_id": "optional-session-id"
}
```

**Response:** `200 OK`
```json
{
  "query_id": "550e8400-...",
  "status": "pending",
  "message": "Query submitted. Use GET /query/{query_id} to check status."
}
```

### `GET /api/v1/query/{query_id}`

Check status and results of a submitted query.

**Response:** `200 OK`
```json
{
  "query_id": "550e8400-...",
  "status": "completed",
  "question": "Cuanto gasta el gobierno en educacion?",
  "analysis_result": "Segun los datos del presupuesto...",
  "sources": [
    { "title": "Presupuesto Nacional 2025", "portal": "datos_gob_ar", "score": 0.87 }
  ],
  "tokens_used": 1500,
  "duration_ms": 4200
}
```

**Possible statuses:** `pending`, `planning`, `collecting`, `analyzing`, `completed`, `error`, `not_found`

### `POST /api/v1/query/quick`

Synchronous query — searches datasets, optionally fetches real cached data, and returns an LLM-generated answer immediately. Results are cached in Redis for 1 hour.

**Rate limit:** 15 requests/minute

**Request Body:**
```json
{
  "question": "Cuanto gasta el gobierno en educacion?",
  "user_id": "optional-session-id"
}
```

**Response:** `200 OK`
```json
{
  "answer": "Segun los datos del presupuesto nacional...",
  "sources": [
    { "title": "Presupuesto 2025", "portal": "datos_gob_ar", "score": 0.87, "has_data": true }
  ],
  "tokens_used": 1200,
  "has_real_data": true,
  "cached": false
}
```

### `DELETE /api/v1/query/cache/{question_hash}`

Invalidate a cached query result.

**Response:** `200 OK`
```json
{ "status": "deleted", "key": "openarg:query:abc123..." }
```

### `WS /api/v1/query/ws/stream`

WebSocket endpoint for streaming query responses (legacy).

**Client sends:**
```json
{ "question": "Cuanto gasta el gobierno en educacion?" }
```

**Server streams:**
```json
{ "type": "status", "content": "Buscando datasets..." }
{ "type": "status", "content": "Analizando..." }
{ "type": "chunk", "content": "Segun los datos..." }
{ "type": "chunk", "content": " del presupuesto..." }
{ "type": "complete", "sources": [...] }
```

### `POST /api/v1/query/smart`

LangGraph pipeline endpoint. Executes the full multi-agent query pipeline: classify, cache, plan, execute, analyze, and finalize.

**Rate limit:** 15 requests/minute

**Request Body:**
```json
{
  "question": "Cuanto gasta el gobierno en educacion?",
  "user_email": "user@example.com",
  "conversation_id": "optional-uuid",
  "policy_mode": false
}
```

**Response:** `200 OK`
```json
{
  "answer": "Segun los datos del presupuesto nacional...",
  "sources": [
    { "name": "Presupuesto 2025", "url": "https://...", "portal": "datos_gob_ar", "accessed_at": "..." }
  ],
  "chart_data": [...],
  "tokens_used": 1200,
  "confidence": 0.85,
  "citations": [...],
  "documents": [...],
  "warnings": []
}
```

### `WS /api/v1/query/ws/smart`

LangGraph pipeline via WebSocket. Streams real-time progress events as the pipeline executes.

**Client sends (after connecting):**
```json
{
  "question": "Cuanto gasta el gobierno en educacion?",
  "conversation_id": "optional-uuid",
  "policy_mode": false
}
```

**Server streams:**
```json
{ "type": "status", "step": "classifying" }
{ "type": "status", "step": "planning" }
{ "type": "status", "step": "planned", "intent": "economic_data", "steps_count": 2 }
{ "type": "status", "step": "searching" }
{ "type": "status", "step": "generating" }
{ "type": "chunk", "content": "Segun los datos..." }
{ "type": "complete", "answer": "...", "sources": [...], "chart_data": [...], "confidence": 0.85, "citations": [...] }
```

---

## Datasets

### `GET /api/v1/datasets/`

List indexed datasets.

**Query Parameters:**
| Param | Type | Default | Description |
|-------|------|---------|-------------|
| `portal` | string | — | Filter by portal (datos_gob_ar, caba) |
| `limit` | int | 50 | Max results |
| `offset` | int | 0 | Pagination offset |

**Response:** `200 OK`
```json
[
  {
    "id": "550e8400-...",
    "title": "Presupuesto Nacional 2025",
    "description": "Datos del presupuesto...",
    "organization": "Ministerio de Economia",
    "portal": "datos_gob_ar",
    "format": "csv",
    "is_cached": true,
    "row_count": 15000
  }
]
```

### `GET /api/v1/datasets/stats`

Get dataset counts per portal.

**Response:** `200 OK`
```json
[
  { "portal": "datos_gob_ar", "count": 1200 },
  { "portal": "caba", "count": 450 }
]
```

### `POST /api/v1/datasets/scrape/{portal}`

Trigger a catalog scrape for a portal.

**Rate limit:** 2 requests/hour

**Valid portals:** `datos_gob_ar`, `caba`

**Response:** `200 OK`
```json
{ "task_id": "celery-task-id", "portal": "datos_gob_ar", "status": "scraping_started" }
```

---

## Sandbox

### `POST /api/v1/sandbox/query`

Execute a raw SQL SELECT query against cached dataset tables. Only SELECT statements are allowed. Results are capped at 1000 rows.

**Rate limit:** 10 requests/minute

**Request Body:**
```json
{ "sql": "SELECT * FROM cache_presupuesto_2025 LIMIT 10" }
```

**Response:** `200 OK`
```json
{
  "columns": ["anio", "monto", "jurisdiccion"],
  "rows": [
    { "anio": 2025, "monto": 1500000, "jurisdiccion": "Educacion" }
  ],
  "row_count": 1,
  "truncated": false,
  "error": null
}
```

### `GET /api/v1/sandbox/tables`

List all cached dataset tables with their columns.

**Response:** `200 OK`
```json
[
  {
    "table_name": "cache_presupuesto_2025",
    "dataset_id": "550e8400-...",
    "row_count": 15000,
    "columns": ["anio", "monto", "jurisdiccion", "programa"]
  }
]
```

### `POST /api/v1/sandbox/ask`

Natural language to SQL. Takes a question, generates a SELECT query via LLM, executes it, and returns results.

**Rate limit:** 10 requests/minute

**Request Body:**
```json
{ "question": "Cuanto se gasto en educacion en 2025?" }
```

**Response:** `200 OK`
```json
{
  "sql": "SELECT SUM(monto) FROM cache_presupuesto_2025 WHERE jurisdiccion = 'Educacion'",
  "columns": ["sum"],
  "rows": [{ "sum": 45000000 }],
  "row_count": 1,
  "truncated": false,
  "error": null
}
```

---

## Error Responses

All errors follow a consistent format:

```json
{
  "code": "DS_001",
  "message": "Dataset not found",
  "details": { ... },
  "field": "dataset_id"
}
```

### Error Codes

| Code | HTTP | Description |
|------|------|-------------|
| `DS_001` | 404 | Dataset not found |
| `DS_002` | 502 | Failed to download dataset |
| `DS_003` | 422 | Failed to parse dataset |
| `QR_001` | 400 | Query cannot be empty |
| `QR_002` | 400 | Query exceeds maximum length |
| `QR_003` | 500 | Query processing failed |
| `AG_001` | 504 | Agent execution timed out |
| `AG_002` | 502 | LLM provider error |
| `AG_003` | 404 | No relevant datasets found |
| `SC_001` | 502 | Data portal unreachable |
| `SC_002` | 429 | Rate limited by data portal |
| `SR_001` | 500 | Failed to generate embedding |
