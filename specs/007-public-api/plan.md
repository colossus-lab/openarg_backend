# Plan: Public API (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| Presentation | `ask_router.py` | `presentation/http/controllers/public_api/ask_router.py` |
| Application | `api_key_service.py` | `application/services/api_key_service.py` (or similar) |
| Domain Ports | `IApiKeyRepository`, `ICacheService` | `domain/ports/` |
| Infrastructure | `ApiKeyRepositorySQLA` | `infrastructure/adapters/` |

## 2. Endpoint

```
POST /api/v1/ask
Authorization: Bearer oarg_sk_<random>
Content-Type: application/json

{
  "question": "string",
  "policy_mode": false  // optional
}
```

**Response (200)**:
```json
{
  "answer": "...",
  "sources": [{"name": "...", "url": "...", "portal": "..."}],
  "chart_data": [...],
  "map_data": {...},
  "confidence": 0.95,
  "citations": [...],
  "warnings": [],
  "tokens_used": 0,
  "duration_ms": 3200
}
```

**Error responses**: 401 (invalid key), 429 (rate limited), 500.

## 3. Authentication Flow

```python
async def verify_api_key(token: str, repo: IApiKeyRepository) -> ApiKey:
    if not token.startswith("oarg_sk_"):
        raise HTTPException(401)
    key_hash = sha256(token.encode()).hexdigest()
    api_key = await repo.get_by_key_hash(key_hash)
    if not api_key or not api_key.is_active:
        raise HTTPException(401)
    if api_key.expires_at and api_key.expires_at < now():
        raise HTTPException(401)
    return api_key
```

Uses `secrets.compare_digest()` for constant-time comparison.

## 4. Rate Limiting (3 layers)

### Per-key (per-minute + per-day)

```python
PLAN_LIMITS = {
    "free":  {"per_min": 2,  "per_day": 5},
    "basic": {"per_min": 15, "per_day": 200},
    "pro":   {"per_min": 30, "per_day": 1000},
}
```

Redis keys:
- `rl:user:{user_id}:min` (TTL 60s)
- `rl:user:{user_id}:day` (TTL 86400s)

### Per-IP (per-day)
Redis key: `rl:ip:{client_ip}:day` (TTL 86400s), max 20 per day.

### Global free cap
Redis key: `rl:global:free:day` (TTL 86400s), max 5000 per day.

### Fail-open
If Redis fails → allow the request, log a warning.

## 5. Persistence

### `api_keys` table (see also `008-developers-keys/`)
- `id` UUID
- `user_id` FK
- `key_hash` TEXT UNIQUE (SHA-256 hex)
- `key_prefix` TEXT (for display)
- `plan` TEXT (free | basic | pro)
- `is_active` BOOLEAN
- `expires_at` TIMESTAMPTZ | null
- `last_used_at` TIMESTAMPTZ
- `created_at` TIMESTAMPTZ

### `api_usage` (append-only)
- `api_key_id` FK
- `endpoint` TEXT
- `tokens_used` INT
- `duration_ms` INT
- `status_code` INT
- `timestamp` TIMESTAMPTZ

## 6. External Dependencies

- **Query Pipeline** (`001-query-pipeline`) — invoked stateless
- **Redis** — rate limit counters
- **PostgreSQL** — `api_keys` + `api_usage`

## 7. Source Files

- `presentation/http/controllers/public_api/ask_router.py`
- API key service (application module)
- `infrastructure/adapters/` for repositories

## 8. Deviations from Constitution

- **Principle XII (Security)**: satisfied — SHA-256 hashing, constant-time compare, multi-layer rate limit.
- Fail-open trade-off during Redis outages accepted.

---

**End of plan.md**
