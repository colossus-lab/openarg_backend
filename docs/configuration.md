# Configuration

OpenArg uses a layered configuration system that merges environment-specific TOML files with secret files and environment variable overrides.

## Config Resolution Order

```mermaid
graph TD
    Default[Base Config: config/{env}/config.toml] --> Secrets[Secret Config: config/{env}/.secrets.toml]
    Secrets --> EnvVars[Environment Variable Overrides]
    EnvVars --> Final[Final AppSettings Model]
```

## Config Hierarchy

```
config/
├── local/config.toml     # Local dev (Postgres on localhost:5435, as in docker-compose.yaml)
├── dev/config.toml       # Development server
├── prod/config.toml      # Production (the real DSN comes from DATABASE_URL)
├── test/config.toml      # Test suite
├── marts/*.yaml          # Mart definitions (copied into every image)
└── curated_sources.json  # Curated sources loaded by refresh_curated_sources
```

`.secrets.toml` next to each `config.toml` is optional and gitignored. The active environment is set via `APP_ENV` (`local`, `dev`, `prod`, `test`; default `local`; any other value raises at startup).

Many runtime switches are not in the TOML at all: they are read straight from the environment with `os.getenv` where they are used (answer engine, verifier, quotas, beat, ingestion knobs). They are listed under [Environment Variables](#environment-variables).

## Settings Structure

All settings are Pydantic models defined in `src/app/setup/config/settings.py`.

### AppSettings (root)

```python
class AppSettings:
    postgres: PostgresSettings
    sqla: SqlaEngineSettings
    security: SecuritySettings
    logs: LoggingSettings
    agents: AgentSettings
    scraper: ScraperSettings
    gemini: GeminiSecrets
    anthropic: AnthropicSecrets
    bedrock: BedrockSettings
    s3: S3Settings
```

### PostgresSettings

| Field | Type | Default | Env Override |
|-------|------|---------|-------------|
| `USER` | str | `"postgres"` | `DATABASE_URL` (full DSN) |
| `PASSWORD` | str | `""` | |
| `DB` | str | `"openarg_db"` | |
| `HOST` | str | `"localhost"` | |
| `PORT` | int | `5432` | |
| `DRIVER` | str | `"psycopg"` | |

The `dsn` property builds `postgresql+{DRIVER}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}`. If `DATABASE_URL` is set, it overrides the entire DSN.

### SqlaEngineSettings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `ECHO` | bool | `false` | Log all SQL |
| `ECHO_POOL` | bool | `false` | Log connection pool events |
| `POOL_SIZE` | int | `20` | Connection pool size |
| `MAX_OVERFLOW` | int | `10` | Max extra connections |

### AgentSettings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `EMBEDDING_MODEL` | str | `"cohere.embed-multilingual-v3"` | Not read by the embedding provider (see below) |
| `EMBEDDING_DIMENSIONS` | int | `1024` | Passed to the Bedrock embedding adapter, which does not use it: Cohere v3 returns 1024 dimensions |
| `MAX_CONCURRENT_COLLECTORS` | int | `5` | |
| `SANDBOX_TIMEOUT_SECONDS` | int | `30` | |

The TOML files still set `[agents] EMBEDDING_MODEL = "gemini-embedding-001"` and `EMBEDDING_DIMENSIONS = 768`. Neither changes anything: the embedding provider uses `bedrock.EMBEDDING_MODEL` (`provider_registry.py`), and the vector columns are `vector(1024)` since migration 0024.

### ScraperSettings

| Field | Type | Default |
|-------|------|---------|
| `DATOS_GOB_AR_BASE_URL` | str | `"https://datos.gob.ar/api/3/action"` |
| `CABA_BASE_URL` | str | `"https://data.buenosaires.gob.ar/api/3/action"` |
| `SCRAPE_INTERVAL_HOURS` | int | `24` (the actual schedule is the beat's: one `scrape-<portal>` entry per portal, daily) |
| `SERIES_TIEMPO_BASE_URL` | str | `"https://apis.datos.gob.ar/series/api"` |
| `ARGENTINA_DATOS_BASE_URL` | str | `"https://api.argentinadatos.com/v1"` |
| `GEOREF_BASE_URL` | str | `"https://apis.datos.gob.ar/georef/api"` |

### SecuritySettings

| Field | Type | Default | Env Override |
|-------|------|---------|-------------|
| `JWT_SECRET_KEY` | str | `""` | |
| `JWT_ALGORITHM` | str | `"HS256"` | |
| `ACCESS_TOKEN_EXPIRE_MINUTES` | int | `60` | |
| `REFRESH_TOKEN_EXPIRE_DAYS` | int | `7` | |
| `BACKEND_API_KEY` | str | `""` | `BACKEND_API_KEY` |
| `CORS_ALLOWED_ORIGINS` | list | `[]` | `CORS_ALLOWED_ORIGINS` (comma-separated) |
| `GOOGLE_OAUTH_CLIENT_ID` | str | `""` | `GOOGLE_OAUTH_CLIENT_ID` |

### LoggingSettings

| Field | Type | Default |
|-------|------|---------|
| `LEVEL` | str | `"INFO"` |

### BedrockSettings

| Field | Type | Default | Env Override |
|-------|------|---------|-------------|
| `REGION` | str | `"us-east-1"` | `AWS_REGION` |
| `LLM_MODEL` | str | `"us.anthropic.claude-haiku-4-5-20251001-v1:0"` | `BEDROCK_LLM_MODEL` |
| `LLM_MODEL_DEEP` | str | falls back to `LLM_MODEL` | `BEDROCK_LLM_MODEL_DEEP` |
| `AGENT_MODEL` | str | `"us.anthropic.claude-sonnet-4-6"` | `BEDROCK_AGENT_MODEL` |
| `EMBEDDING_MODEL` | str | `"cohere.embed-multilingual-v3"` | `BEDROCK_EMBEDDING_MODEL` |

`LLM_MODEL` is the legacy engine's model (wrapped with the Gemini fallback) and the default of the workers' LLM tasks; `AGENT_MODEL` is the agent engine's (`ANSWERS_ENGINE=agent`), called through the Anthropic SDK's `AsyncAnthropicBedrock`.

### GeminiSecrets / AnthropicSecrets

| Field | Default | Env Override | Used by |
|-------|---------|-------------|---------|
| `gemini.API_KEY` | `""` | `GEMINI_API_KEY` | Fallback of the Bedrock LLM adapter (legacy engine) |
| `gemini.MODEL` | `"gemini-2.5-flash"` | `GEMINI_MODEL` | |
| `anthropic.API_KEY` | `""` | `ANTHROPIC_API_KEY` | Nothing: `AnthropicAdapter` exists but is not wired in `provider_registry.py` |
| `anthropic.MODEL` | `"claude-sonnet-4-20250514"` | `ANTHROPIC_MODEL` | |

### S3Settings

| Field | Type | Default | Env Override |
|-------|------|---------|-------------|
| `BUCKET` | str | `"openarg-datasets"` | `S3_BUCKET` |
| `REGION` | str | `"us-east-1"` | `AWS_REGION` |

## Environment-Specific Configs

What differs between the TOML files (see the files for the rest):

| Setting | local | dev | prod | test |
|---------|-------|-----|------|------|
| `postgres.HOST:PORT` | `localhost:5435` | `postgres:5432` | `localhost:5432` (overridden by `DATABASE_URL`) | `localhost:5432` |
| `postgres.DB` | `openarg_db` | `openarg_db` | `openarg_db` | `openarg_test` |
| `sqla.ECHO` | `true` | `false` | `false` | `false` |
| `sqla.POOL_SIZE` / `MAX_OVERFLOW` | 10 / 5 | 30 / 15 | 20 / 10 | 5 / — |
| `logs.LEVEL` | `DEBUG` | `INFO` | `INFO` | `WARNING` |

## Environment Variables

These override TOML settings or are read directly by the code. Names only: values live in each server's `.env` (template: `.env.example`).

### Core

| Variable | Description | Required |
|----------|-------------|----------|
| `APP_ENV` | Environment (`local`/`dev`/`prod`/`test`) | No (default: `local`) |
| `DATABASE_URL` | Full PostgreSQL DSN | No (overrides postgres config) |
| `SANDBOX_DATABASE_URL` | DSN of the read-only role the SQL sandbox uses | Yes in prod |
| `CELERY_BROKER_URL` / `CELERY_RESULT_BACKEND` | Redis broker and results | Yes (for workers) |
| `REDIS_CACHE_URL` | Redis cache, quotas and rate limits | Yes (default `redis://localhost:6379/2`) |
| `LOG_LEVEL`, `SENTRY_DSN` | Logging level, error tracking | No |

### Answers and models

| Variable | Description | Default |
|----------|-------------|---------|
| `ANSWERS_ENGINE` | `legacy` (LangGraph graph) or `agent` (tool-using agent). Read on every turn; an unknown value logs `ERROR` and uses `legacy` | `legacy` |
| `ANSWERS_VERIFY_MODE` | Figure verifier: `off`, `shadow` (log only) or `correct` (cite only what was used, one corrective round, notice for unbacked figures) | `shadow` |
| `AWS_REGION` | AWS region for Bedrock and S3 | `us-east-1` |
| `BEDROCK_AGENT_MODEL` | Agent model | `us.anthropic.claude-sonnet-4-6` |
| `BEDROCK_LLM_MODEL` | Legacy engine and worker LLM tasks | `us.anthropic.claude-haiku-4-5-20251001-v1:0` |
| `BEDROCK_LLM_MODEL_DEEP` | Legacy deep mode only. Left unset, deep mode runs on the same model as everything else. Whichever you pick, confirm the account can actually invoke it — a profile can be listed as ACTIVE and still be denied at invocation | `BEDROCK_LLM_MODEL` |
| `BEDROCK_MODEL_ID` | Old name read first by `analyst_tasks` and `catalog_enrichment_tasks` (`constants.bedrock_llm_model`) | unset |
| `BEDROCK_EMBEDDING_MODEL` | Embedding model | `cohere.embed-multilingual-v3` |
| `GEMINI_API_KEY`, `GEMINI_MODEL` | Fallback of the legacy LLM | `gemini-2.5-flash` |
| `S3_BUCKET` | S3 bucket for original files | `openarg-datasets` |

**AWS credentials.** No client is built with explicit keys: boto3 and `AsyncAnthropicBedrock` use the default credential chain, so on the servers the EC2 instance role provides them and the `.env` needs no keys. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` still work (for example, locally), since the chain reads them first.

### Auth

| Variable | Description |
|----------|-------------|
| `BACKEND_API_KEY` | Shared service key the frontend sends in `X-API-Key`. When set, `APIKeyMiddleware` protects every non-public route |
| `GOOGLE_OAUTH_CLIENT_ID` | Enables `GoogleJwtAuthMiddleware` (user's Google ID token in `Authorization: Bearer`). Mandatory in prod: the app refuses to start without it |
| `ADMIN_API_KEY` | `X-Admin-Key` for `/api/v1/admin/*` and the transparency POSTs. Mandatory in prod and must differ from `BACKEND_API_KEY` |
| `DATA_SERVICE_TOKEN` | Bearer token of the internal `/api/v1/data/*` API |
| `CORS_ALLOWED_ORIGINS` | Comma-separated origins |

### Public API, MCP and web quotas

| Variable | Description | Default |
|----------|-------------|---------|
| `PUBLIC_API_MONTHLY_PREGUNTAS` / `PUBLIC_API_MONTHLY_DATOS` | Free monthly questions (`/ask`) / data-mode requests (`/catalogo/*`, `/fuentes`) per person | 10 / 200 |
| `PUBLIC_API_FOUNDER_PREGUNTAS` / `PUBLIC_API_FOUNDER_DATOS` | Same, for Fundadores | 100 / 2000 |
| `PUBLIC_API_GLOBAL_DAILY_CAP` | Free-plan questions per day for everybody together (Bedrock spend ceiling) | 300 |
| `PUBLIC_API_IP_DAILY_LIMIT` | Questions per day per client IP | 30 |
| `PUBLIC_API_USER_UNBILLED_DAILY_LIMIT` | Unbilled model runs (timeout, error, clarification, empty answer) per person per UTC day | 20 |
| `PUBLIC_API_TIMEOUT_SECONDS` | Turn timeout of `/ask`; keep it below the MCP's timeout minus 10 s | 30 |
| `PUBLIC_API_COST_PER_ANSWER_USD` | Per-answer cost estimate used by `/admin/analytics/mcp/overview` | 0.034 |
| `PUBLIC_WEB_MONTHLY_PREGUNTAS` / `PUBLIC_WEB_FOUNDER_PREGUNTAS` | Web chat monthly questions | 30 / 100 |
| `PUBLIC_WEB_GLOBAL_DAILY_CAP` | Web chat answers per day for everybody together | 1000 |
| `BACKEND_URL`, `MCP_ALLOWED_HOSTS`, `MCP_BACKEND_TIMEOUT_SECONDS` | Public MCP container (see `mcp_publico/README.md`) | `http://backend:8080`, see README, 75 |

`PUBLIC_API_CATALOG_DAILY_LIMIT`, still in `.env.example`, is not read by the code: the data-mode limits are the monthly quota above plus 30 requests per minute (`api_key_service.CATALOG_MINUTE_LIMIT`).

### Workers, beat and ingestion

| Variable | Description | Default |
|----------|-------------|---------|
| `OPENARG_BEAT_DESACTIVADAS` | Entradas del beat que no se agendan, separadas por comas: las claves de `beat_schedule` (p. ej. `ingest-series-tiempo,check-series-freshness,snapshot-bcra`), no los nombres de las tareas. Sirve para desplegar sin que esas tareas corran solas y correrlas a mano cuando se decida. Se lee al crear la app de Celery, así que va en el `.env` que comparten el beat y los workers (los workers la usan para no esperar el latido de una tarea frenada) y toma efecto al reiniciarlos. Un nombre que no existe sale como `ERROR` en el log y no frena nada. Ver `docs/deploy-produccion.md` | vacía (la agenda entera) |
| `OPENARG_ENABLE_STARTUP_BOOTSTRAP` | Dispatch the initial scrape / bulk collect when a worker starts | off |
| `OPENARG_COLLECTOR_CONCURRENCY` | Concurrency of `worker-collector` in `docker-compose.prod.yml` | 8 |
| `OPENARG_HEAVY_COLLECT_QUEUE` / `OPENARG_HEAVY_RETRY_QUEUE` | Queue names of the heavy collectors | `collector-heavy` / `collector-heavy-retry` |
| `OPENARG_HEADER_FROM_DATA_SEVERITY` | Severity of the "header taken from data" detector: `critical` hides the table from the sandbox while the finding is open; `warn` only records it | `critical` |
| `OPENARG_MART_MAX_UNION_TABLES` | Cap of `live_tables_by_*` macros in mart YAMLs | 200 |
| `OPENARG_TELEGRAM_TOKEN`, `OPENARG_TELEGRAM_CHAT_ID` | Destination of the quality alerts | unset (no alerts sent) |
| `SANDBOX_MAX_ROWS`, `SANDBOX_TIMEOUT_MS`, `SANDBOX_TABLE_PREFIX`, `INJECTION_THRESHOLD`, `CACHE_SIMILARITY_THRESHOLD` | Security tuning (see `.env.example`) | |

The collector and parser have many more `OPENARG_*` knobs (download size caps, chunk sizes, heavy routing); grep `os.getenv("OPENARG_` under `src/app/infrastructure/celery/tasks/` for the full list.

## Config Loading

`load_settings()` in `setup/config/settings.py`:

1. Reads `APP_ENV` (default `"local"`).
2. Loads `config/{env}/config.toml`.
3. Merges `config/{env}/.secrets.toml` (if it exists).
4. Builds the `AppSettings` Pydantic model; each section applies its environment-variable overrides in `model_post_init`.
