# Debug backend pipeline: $ARGUMENTS

Diagnose the described problem in the scraper → embedding → collector → analyst pipeline. Follow this checklist:

## Base infrastructure

- [ ] PostgreSQL running: `make db.up` or check docker (`docker ps | grep postgres`)
- [ ] Redis running: verify connectivity on port 6381
- [ ] Migrations applied: `make db.migrate`
- [ ] Environment variables: `OPENAI_API_KEY`, `DATABASE_URL`, `CELERY_BROKER_URL`
- [ ] Workers active: check in Flower (`make flower` → http://localhost:5556)

## Scraper (`src/app/infrastructure/celery/tasks/scraper_tasks.py`)

- [ ] `scrape_catalog(portal)` receives a valid portal (datos_gob_ar, caba, etc.)
- [ ] The corresponding `IDataSource` adapter is registered in Dishka
- [ ] `fetch_catalog()` returns `CatalogEntry[]` — is the CKAN portal responding?
- [ ] Upsert to `datasets` works (check UNIQUE constraint on source_id + portal)
- [ ] Dispatches `index_dataset_embedding` for each dataset
- [ ] If failing: check worker-scraper logs, verify HTTPX timeout

## Embedding (`src/app/infrastructure/celery/tasks/embedding_tasks.py` + `scraper_tasks.py`)

- [ ] `index_dataset_embedding(dataset_id)` receives a valid UUID
- [ ] The dataset exists in the `datasets` table
- [ ] 3 chunks are generated: main, columns, contextual
- [ ] OpenAI embedding API responds (verify `OPENAI_API_KEY`)
- [ ] Chunks are saved in `dataset_chunks` with 1536-dim embedding
- [ ] HNSW index works: `SELECT * FROM dataset_chunks ORDER BY embedding <=> $1 LIMIT 5`
- [ ] If failing: check retries (max 3), review worker-embedding logs

## Collector (`src/app/infrastructure/celery/tasks/collector_tasks.py`)

- [ ] `collect_dataset(dataset_id)` downloads the file from `download_url`
- [ ] Format is supported (CSV, JSON, XLSX, XLS)
- [ ] Pandas parses correctly (check encoding, separators)
- [ ] Cache table `cache_{id}` is created in PostgreSQL
- [ ] `cached_datasets` is updated: status downloading → ready
- [ ] `datasets.is_cached = true` and `row_count` updated
- [ ] Row limit: 500k max
- [ ] If failing: check download URL, file format, worker-collector logs

## Analyst (`src/app/infrastructure/celery/tasks/analyst_tasks.py`)

- [ ] `analyze_query(query_id, question)` — query exists in `user_queries`

### Step 1: Planning
- [ ] LLM (GPT-4o-mini) generates valid plan JSON
- [ ] Plan has `datasets_needed`, `analysis_steps`, `output_format`
- [ ] Saved in `user_queries.plan_json`

### Step 2: Vector Search
- [ ] Question embedding works (OpenAI)
- [ ] PgVector similarity search returns results (top 5)
- [ ] If no results: are there indexed datasets? `SELECT count(*) FROM dataset_chunks`

### Step 3: Gather Data
- [ ] Relevant datasets have cache (is_cached = true, cached_datasets.status = ready)
- [ ] If not cached: dispatches `collect_dataset` — does it complete in time?
- [ ] Sample rows extracted correctly (20 row limit)

### Step 4: Analysis
- [ ] LLM (GPT-4o) receives context with real data
- [ ] System prompt instructs to respond in Spanish, cite sources
- [ ] Result saved in `user_queries`: analysis_result, sources_json, tokens_used, duration_ms
- [ ] Final status: completed

## API Layer

- [ ] `POST /api/v1/query/` returns query_id and status pending
- [ ] `GET /api/v1/query/{id}` shows progress (pending → planning → collecting → analyzing → completed)
- [ ] `POST /api/v1/query/quick` works synchronously (timeout 60s)
- [ ] WebSocket `/api/v1/query/ws/stream` emits events correctly
- [ ] Rate limiting doesn't block legitimate requests

## Diagnostic steps

1. Check Flower (http://localhost:5556) for pending/failed tasks
2. Read logs from the specific failing worker
3. Verify `datasets` table has data: `SELECT count(*), portal FROM datasets GROUP BY portal`
4. Verify embeddings: `SELECT count(*) FROM dataset_chunks`
5. Test health endpoint: `GET /health/ready`
