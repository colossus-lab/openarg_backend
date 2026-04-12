# Plan: Semantic Cache (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File (grep hint) |
|---|---|---|
| Infrastructure Adapter | `SemanticCache` | likely `infrastructure/adapters/cache/semantic_cache.py` or `application/pipeline/cache_manager.py` |
| Domain Port | ❌ Missing | No port |

## 2. API (de facto interface)

```python
class SemanticCache:
    async def get(self, question: str, embedding: list[float] | None = None) -> dict | None: ...
    async def set(self, question: str, embedding: list[float] | None, response: dict, ttl: int) -> None: ...
    async def cleanup(self) -> int: ...

def _normalize(question: str) -> str:
    return unicodedata.normalize("NFC", question.lower().strip())

def _hash(question: str) -> str:
    return sha256(_normalize(question).encode()).hexdigest()

def ttl_for_intent(intent: str) -> int: ...
```

## 3. Persistence — `query_cache` table

| Column | Type | Notes |
|---|---|---|
| `question_hash` | TEXT PK | SHA-256 hex |
| `question` | TEXT | Plaintext normalized |
| `embedding` | `vector(1024)` | HNSW index |
| `response` | JSONB | answer + sources + chart_data + tokens |
| `ttl_seconds` | INT | Stored for reference |
| `expires_at` | TIMESTAMPTZ | Checked on read |
| `created_at` | TIMESTAMPTZ | |

**HNSW index**: `CREATE INDEX ON query_cache USING hnsw (embedding vector_cosine_ops)`

## 4. Get Logic

```python
async def get(question, embedding=None):
    # 1. Exact hash match
    row = await db.fetch_one("""
        SELECT response FROM query_cache
        WHERE question_hash = :hash AND expires_at > now()
    """, {"hash": _hash(question)})
    if row:
        return row.response

    # 2. Similarity match (if embedding provided)
    if embedding:
        emb_str = "[" + ",".join(map(str, embedding)) + "]"
        row = await db.fetch_one("""
            SELECT response, 1 - (embedding <=> CAST(:emb AS vector)) AS score
            FROM query_cache
            WHERE expires_at > now()
              AND 1 - (embedding <=> CAST(:emb AS vector)) >= :threshold
            ORDER BY embedding <=> CAST(:emb AS vector)
            LIMIT 1
        """, {"emb": emb_str, "threshold": 0.92})
        if row:
            return row.response

    return None
```

## 5. Set Logic (Mar 2026 fix)

**Bug 1 (historical)**: the code used `:embedding::vector`, which failed on writes.
**Fix applied** (Mar 2026): replaced with `CAST(:embedding AS vector)`.

```python
async def set(question, embedding, response, ttl):
    # Validate embedding
    if embedding and any(math.isnan(v) or math.isinf(v) for v in embedding):
        logger.warning("Invalid embedding (NaN/Inf), skipping cache set")
        return

    emb_str = "[" + ",".join(map(str, embedding)) + "]" if embedding else None

    await db.execute("""
        INSERT INTO query_cache (question_hash, question, embedding, response, ttl_seconds, expires_at)
        VALUES (:hash, :q, CAST(:emb AS vector), :resp, :ttl, now() + make_interval(secs => :ttl))
        ON CONFLICT (question_hash) DO UPDATE SET
            response = EXCLUDED.response,
            expires_at = EXCLUDED.expires_at
    """, {...})
```

## 6. TTL Mapping (ttl_for_intent)

```python
REAL_TIME_TOKENS = {"dolar", "cotizacion", "riesgo_pais"}
DAILY_TOKENS = {"inflacion", "series", "emae", "tipo_cambio", "reservas", "desempleo"}
STATIC_TOKENS = {"ddjj", "sesiones", "ckan", "catalogo", "legisladores", "georef"}

def ttl_for_intent(intent: str) -> int:
    tokens = intent.lower().split("_")
    if any(t in REAL_TIME_TOKENS for t in tokens):
        return 300
    if any(t in DAILY_TOKENS for t in tokens):
        return 1800
    if any(t in STATIC_TOKENS for t in tokens):
        return 7200
    return 1800  # default
```

## 7. Source Files

- `application/pipeline/cache_manager.py` — wrapping helpers (`check_cache`, `write_cache`)
- `infrastructure/adapters/cache/semantic_cache.py` — main adapter (likely location)
- Migration for the `query_cache` table in `alembic/versions/2026_02_28_0005_add_query_cache.py`

## 8. Deviations from Constitution

- **Principle I (Hexagonal)**: no abstract port. **[DEBT-006]**
- **Principle VII (Observability)**: no hit-rate metrics.
- **Principle VIII (Cleanup via Alembic)**: no automated cleanup task.

---

**End of plan.md**
