# Configuration

OpenArg uses a layered configuration system: TOML files per environment + environment variable overrides.

## Config Hierarchy

```
config/
├── local/
│   ├── config.toml       # Local dev settings
│   └── .secrets.toml     # API keys (gitignored)
├── dev/
│   └── config.toml       # Development server
├── prod/
│   └── config.toml       # Production
└── test/
    └── config.toml       # Test suite
```

The active environment is set via `APP_ENV` (default: `local`).

## Settings Structure

All settings are Pydantic models defined in `setup/config/settings.py`.

### AppSettings (root)

```python
class AppSettings:
    postgres: PostgresSettings
    sqla: SqlaEngineSettings
    security: SecuritySettings
    logs: LoggingSettings
    agents: AgentSettings
    scraper: ScraperSettings
    openai: OpenAISecrets
    gemini: GeminiSecrets
    anthropic: AnthropicSecrets
```

### PostgresSettings

| Field | Type | Default | Env Override |
|-------|------|---------|-------------|
| `USER` | str | `"postgres"` | `DATABASE_URL` (full DSN) |
| `PASSWORD` | str | `"postgres"` | |
| `DB` | str | `"openarg_db"` | |
| `HOST` | str | `"localhost"` | |
| `PORT` | int | `5435` | |
| `DRIVER` | str | `"postgresql+psycopg"` | |

The `dsn` property builds: `{DRIVER}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}`

If `DATABASE_URL` env var is set, it overrides the entire DSN.

### SqlaEngineSettings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `ECHO` | bool | `false` | Log all SQL |
| `ECHO_POOL` | bool | `false` | Log connection pool events |
| `POOL_SIZE` | int | `5` | Connection pool size |
| `MAX_OVERFLOW` | int | `10` | Max extra connections |

### AgentSettings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `DEFAULT_LLM_PROVIDER` | str | `"gemini"` | Active LLM (gemini/openai/anthropic) |
| `EMBEDDING_MODEL` | str | `"text-embedding-3-small"` | OpenAI embedding model |
| `EMBEDDING_DIMENSIONS` | int | `1536` | Vector dimensions |
| `SANDBOX_TIMEOUT` | int | `30` | SQL sandbox timeout (seconds) |

### ScraperSettings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `DATOS_GOB_AR_BASE_URL` | str | `"https://datos.gob.ar/api/3/action"` | CKAN API |
| `CABA_BASE_URL` | str | `"https://data.buenosaires.gob.ar/api/3/action"` | CKAN API |
| `SCRAPE_INTERVAL_HOURS` | int | `12` | Auto-scrape interval |

### SecuritySettings

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `JWT_SECRET_KEY` | str | — | JWT signing key |
| `JWT_ALGORITHM` | str | `"HS256"` | |
| `ACCESS_TOKEN_EXPIRE_MINUTES` | int | `60` | |

### LoggingSettings

| Field | Type | Default |
|-------|------|---------|
| `LEVEL` | str | `"INFO"` |

### API Key Secrets

Set via `.secrets.toml` or environment variables:

| Setting | Env Variable | Used By |
|---------|-------------|---------|
| `openai.API_KEY` | `OPENAI_API_KEY` | Embeddings (text-embedding-3-small) |
| `gemini.API_KEY` | `GEMINI_API_KEY` | LLM (Gemini 2.5 Flash) |
| `anthropic.API_KEY` | `ANTHROPIC_API_KEY` | LLM (Claude Sonnet) |

## Environment-Specific Configs

### Local (`config/local/config.toml`)

```toml
[app]
DEBUG = true

[agents]
DEFAULT_LLM_PROVIDER = "gemini"

[postgres]
HOST = "localhost"
PORT = 5435
DB = "openarg_db"

[sqla]
ECHO = true
POOL_SIZE = 5
```

### Dev (`config/dev/config.toml`)

```toml
[agents]
DEFAULT_LLM_PROVIDER = "anthropic"

[sqla]
POOL_SIZE = 30

[scraper]
SCRAPE_INTERVAL_HOURS = 12
```

### Prod (`config/prod/config.toml`)

```toml
[agents]
DEFAULT_LLM_PROVIDER = "gemini"

[sqla]
POOL_SIZE = 20

[scraper]
SCRAPE_INTERVAL_HOURS = 24
```

## Environment Variables

These override TOML settings:

| Variable | Description | Required |
|----------|-------------|----------|
| `APP_ENV` | Environment (local/dev/prod/test) | No (default: local) |
| `DATABASE_URL` | Full PostgreSQL DSN | No (overrides postgres config) |
| `CELERY_BROKER_URL` | Redis broker URL | Yes (for workers) |
| `CELERY_RESULT_BACKEND` | Redis results URL | Yes (for workers) |
| `REDIS_CACHE_URL` | Redis cache URL | Yes |
| `GEMINI_API_KEY` | Google AI API key | Yes (if using Gemini LLM) |
| `OPENAI_API_KEY` | OpenAI API key | Yes (always, for embeddings) |
| `ANTHROPIC_API_KEY` | Anthropic API key | Only if using Claude |

## Config Loading

The `load_settings()` function in `setup/config/settings.py`:

1. Reads `APP_ENV` (default `"local"`).
2. Loads `config/{env}/config.toml`.
3. Merges `config/{env}/.secrets.toml` (if exists).
4. Applies environment variable overrides.
5. Returns `AppSettings` Pydantic model.
