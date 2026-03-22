# Infrastructure Layer

Concrete implementations of domain ports. Location: `src/app/infrastructure/`

## Adapters

### Database Adapters

#### UserRepositorySQLA (`adapters/user/user_repository_sqla.py`)

Implements `IUserRepository`. Receives `MainAsyncSession` via DI.

- `upsert_by_email()` — Checks if user exists by email; updates name/image if found, creates if not.
- `get_by_email()` — SELECT with `User.email == email`.
- `get_by_id()` — `session.get(User, user_id)`.

#### ChatRepositorySQLA (`adapters/chat/chat_repository_sqla.py`)

Implements `IChatRepository`. Receives `MainAsyncSession` via DI.

- `create_conversation()` — `session.add()` + flush + commit.
- `list_conversations()` — SELECT filtered by `user_id`, ordered by `updated_at DESC`.
- `get_conversation()` — `session.get(Conversation, id)`.
- `delete_conversation()` — Loads entity then `session.delete()` (triggers CASCADE on messages).
- `update_conversation_title()` — Core `update()` with explicit `updated_at=func.now()`.
- `add_message()` — `session.add()` + flush + commit.
- `get_messages()` — SELECT filtered by `conversation_id`, ordered by `created_at ASC`.

#### DatasetRepositorySQLA (`adapters/dataset/dataset_repository_sqla.py`)

Implements `IDatasetRepository`. Standard CRUD with an `upsert()` that checks `source_id + portal`.

### LLM Adapters

All implement `ILLMProvider` (chat + chat_stream) with different backends.

#### BedrockLLMAdapter (`adapters/llm/bedrock_llm_adapter.py`)

**Primary LLM adapter.** Uses `boto3` Bedrock Converse API. Model: `anthropic.claude-3-5-haiku-20241022-v1:0`.

- Uses AWS credentials (env vars or instance profile).
- Handles system messages via Converse API system parameter.
- Supports streaming via `converse_stream()`.
- Runs boto3 sync calls in a thread pool for async compatibility.

#### BedrockEmbeddingAdapter (`adapters/llm/bedrock_embedding_adapter.py`)

**Primary embedding adapter.** Implements `IEmbeddingProvider`. Model: `cohere.embed-multilingual-v3`, 1024 dimensions.

- `embed()` — Single text embedding via Bedrock `invoke_model()`.
- `embed_batch()` — Batch embedding (all texts in one API call).
- Uses `input_type` parameter (search_query vs search_document) for optimal retrieval.

#### AnthropicLLMAdapter (`adapters/llm/anthropic_adapter.py`)

**Fallback LLM adapter.** Uses `anthropic` SDK (async). Model: `claude-sonnet-4-20250514`.

- Extracts system prompt from messages (Anthropic requires separate `system` parameter).
- Streaming via `client.messages.stream()`.
- Used as fallback when Bedrock is unavailable.

### Search Adapter

#### PgVectorSearchAdapter (`adapters/search/pgvector_search_adapter.py`)

Implements `IVectorSearch` using pgvector extension.

- `search_datasets()` — Cosine distance search (`<=>` operator) via raw SQL:
  ```sql
  SELECT dc.dataset_id, d.title, d.description, d.portal, d.columns,
         1 - (dc.embedding <=> :query_vec) AS score
  FROM dataset_chunks dc JOIN datasets d ON ...
  ORDER BY dc.embedding <=> :query_vec
  LIMIT :limit
  ```
- `index_dataset()` — Inserts embedding chunks with raw SQL (pgvector type).
- `delete_dataset_chunks()` — Deletes all chunks for a dataset.

### Sandbox Adapter

#### PgSandboxAdapter (`adapters/sandbox/pg_sandbox_adapter.py`)

Implements `ISQLSandbox`. Read-only SQL execution against cached datasets.

**Security measures:**
- SQL validation: blocks INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE, GRANT, REVOKE, COPY.
- Transaction set to READ ONLY.
- Statement timeout (configurable, default 30s).
- Max 1000 rows returned.
- Runs in a `ThreadPoolExecutor` for async wrapping.

**Methods:**
- `execute_readonly(sql, timeout_seconds)` — Validates SQL, executes in read-only transaction, returns `SandboxResult`.
- `list_cached_tables()` — Queries `cached_datasets` + `information_schema.columns` to build table metadata.

### Data Source Adapters

#### DatosGobArAdapter (`adapters/source/datos_gob_ar_adapter.py`)

Implements `IDataSource` for datos.gob.ar (CKAN API).

- `fetch_catalog()` — Calls `package_search` action, maps to `CatalogEntry`.
- `fetch_catalog_count()` — Calls `package_search` with `rows=0`.
- `download_dataset()` — Downloads resource via HTTPX.

#### CABADataAdapter (`adapters/source/caba_adapter.py`)

Implements `IDataSource` for Buenos Aires city CKAN portal.

Same interface as DatosGobArAdapter but targeting `data.buenosaires.gob.ar`.

### Cache Adapter

#### RedisCacheAdapter (`adapters/cache/redis_cache_adapter.py`)

Implements `ICacheService`. JSON serialization with optional TTL.

- `get()` — `redis.get()` + `json.loads()`.
- `set()` — `json.dumps()` + `redis.setex()` (with TTL) or `redis.set()`.
- `delete()` — `redis.delete()`.
- `exists()` — `redis.exists()`.

---

## Persistence Layer

Location: `src/app/infrastructure/persistence_sqla/`

### Provider (`provider.py`)

Creates async SQLAlchemy engine and session factory.

```python
MainAsyncSession = NewType("MainAsyncSession", AsyncSession)
```

- `get_async_engine(config)` — Creates `create_async_engine()` with pool settings.
- `get_async_session_factory(engine)` — `async_sessionmaker(expire_on_commit=False)`.
- `get_main_async_session(factory)` — Async generator yielding sessions (for DI).

### Config (`config.py`)

- `PostgresDsn` — DSN URL wrapper.
- `SqlaEngineConfig` — Echo, pool_size, max_overflow from settings.

### Registry (`registry.py`)

```python
mapping_registry = registry(
    metadata=MetaData(naming_convention=NAMING_CONVENTIONS),
)
```

Naming conventions: `ix_*` for indexes, `uq_*` for unique, `fk_*` for foreign keys, `pk_*` for primary keys.

### Mappings (`mappings/`)

Imperative ORM mapping: SQLAlchemy `Table` objects mapped to domain dataclasses via `mapping_registry.map_imperatively()`.

Each mapping module follows the same pattern:
1. Define `Table` with columns
2. Call `mapping_registry.map_imperatively(Entity, table)`
3. Guard with `_mapped` flag to prevent double-mapping

**Registration order matters** — `all.py` calls mappings in dependency order:
1. `map_dataset_tables()` — datasets, dataset_chunks, cached_datasets
2. `map_query_tables()` — user_queries, query_dataset_links
3. `map_agent_tables()` — agent_tasks
4. `map_user_tables()` — users
5. `map_chat_tables()` — conversations (FK → users), messages (FK → conversations)

### Alembic (`alembic/`)

Migration runner configured in `env.py`:
- Loads app settings for DB connection.
- Calls `map_tables()` to register all mappings.
- Runs async migrations via `run_async_migrations()`.

Config: `alembic.ini` at project root.
