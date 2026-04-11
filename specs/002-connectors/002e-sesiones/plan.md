# Plan: Connector Sesiones (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Type**: Reverse-engineered
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| Domain Port | `ISesionesConnector` | `domain/ports/connectors/sesiones.py:8` |
| Application | `execute_sesiones_step` | `application/pipeline/connectors/sesiones.py:13` |
| Infrastructure Adapter | `SesionesAdapter` | `infrastructure/adapters/connectors/sesiones_adapter.py:32` |
| Data Files | Chunks JSON | `src/app/infrastructure/data/chunks/*.json` |

## 2. Ports & Contracts

```python
class ISesionesConnector(ABC):
    async def search(
        self,
        query: str,
        periodo: int | None = None,
        orador: str | None = None,
        limit: int = 15,
    ) -> DataResult | None: ...
```

## 3. Dual-Mode Retrieval

### 3.1 Primary: pgvector

```sql
SELECT *, 1 - (embedding <=> :embedding) AS score
FROM sesion_chunks
WHERE (:periodo IS NULL OR periodo = :periodo)
ORDER BY embedding <=> :embedding
LIMIT :limit
```

Post-filter by `orador` in Python after the query (`sesiones_adapter.py:145-149`).

Embedding via Bedrock Cohere Embed Multilingual v3:
- `input_type="search_query"`
- query truncated to 2000 chars
- Blocking boto3 call wrapped in `asyncio.to_thread()`

### 3.2 Fallback: Keyword Search

Chunks loaded from `src/app/infrastructure/data/chunks/*.json` (lazy via `_ensure_loaded()`).

Pre-built indexes:
- `_dataset: list[dict]` — raw chunks
- `_term_index: dict[str, list[int]]` — term → chunk IDs
- `_period_index: dict[int, list[int]]` — periodo → chunk IDs

Query-time scoring (`sesiones_adapter.py:236-254`):
1. Extract terms with >2 chars from the query
2. Look up candidate chunks via inverted index
3. Score by term frequency
4. +10 if the term appears in the speaker name
5. +20 if the orador param matches exactly
6. Top-limit via heap

## 4. External Dependencies

| Dependency | Use |
|---|---|
| AWS Bedrock Cohere Embed v3 | Query embedding |
| PostgreSQL + pgvector HNSW | Primary store |
| Local JSON files | Fallback store |

## 5. Pipeline Step Logic

`execute_sesiones_step(step, adapter)`:
- Params: `query`, `periodo`, `orador`, `limit`
- Calls `adapter.search(query, periodo, orador, limit)`
- Returns `[result]` if not `None`, otherwise `[]`

## 6. Persistence

`sesion_chunks` table (inferred from code):
- `id`, `periodo`, `reunion`, `fecha`, `tipo_sesion`, `pdf_url`, `total_pages`, `speaker`, `text`, `embedding vector(1024)`
- Indexed with HNSW on `embedding`

## 7. Scheduled Work

**No visible ingestion task**. Chunks are maintained offline (external process).

Health check: `HealthCheckService` checks `COUNT(*) FROM sesion_chunks` as an availability indicator.

## 8. Source Files

| File | Role |
|---|---|
| `domain/ports/connectors/sesiones.py` | Port ABC |
| `application/pipeline/connectors/sesiones.py` | Pipeline step |
| `infrastructure/adapters/connectors/sesiones_adapter.py` | Dual-mode adapter |
| `infrastructure/data/chunks/*.json` | Fallback chunk files |

## 9. Deviations from Constitution

- **Principle IV (Async-first)**: use of `asyncio.to_thread()` for blocking boto3; acceptable.
- **Principle VI (Circuit breaker)**: not applied to Bedrock Cohere.
- **Principle VII (Observability)**: mode (vector vs keyword) not logged in a structured way.

---

**End of plan.md**
