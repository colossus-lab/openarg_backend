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
    gemini: GeminiSecrets
    anthropic: AnthropicSecrets
    bedrock: BedrockSettings
    s3: S3Settings
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
| `EMBEDDING_MODEL` | str | `"cohere.embed-multilingual-v3"` | Bedrock embedding model |
| `EMBEDDING_DIMENSIONS` | int | `1024` | Vector dimensions |
| `MAX_CONCURRENT_COLLECTORS` | int | `5` | Max parallel collector tasks |
| `SANDBOX_TIMEOUT_SECONDS` | int | `30` | SQL sandbox timeout (seconds) |

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
| `bedrock.REGION` | `AWS_REGION` | AWS Bedrock region (default: us-east-1) |
| `bedrock.LLM_MODEL` | `BEDROCK_LLM_MODEL` | Bedrock LLM model (default: claude-3-5-haiku) |
| `bedrock.EMBEDDING_MODEL` | `BEDROCK_EMBEDDING_MODEL` | Bedrock embedding model (default: cohere.embed-multilingual-v3) |
| `gemini.API_KEY` | `GEMINI_API_KEY` | LLM (Gemini 2.5 Flash, optional) |
| `anthropic.API_KEY` | `ANTHROPIC_API_KEY` | LLM fallback (Claude Sonnet via Anthropic API) |
| `s3.BUCKET` | `S3_BUCKET` | S3 bucket for dataset storage |

## Environment-Specific Configs

### BedrockSettings

| Field | Type | Default | Env Override |
|-------|------|---------|-------------|
| `REGION` | str | `"us-east-1"` | `AWS_REGION` |
| `LLM_MODEL` | str | `"anthropic.claude-3-5-haiku-20241022-v1:0"` | `BEDROCK_LLM_MODEL` |
| `EMBEDDING_MODEL` | str | `"cohere.embed-multilingual-v3"` | `BEDROCK_EMBEDDING_MODEL` |

### S3Settings

| Field | Type | Default | Env Override |
|-------|------|---------|-------------|
| `BUCKET` | str | `"openarg-datasets"` | `S3_BUCKET` |
| `REGION` | str | `"us-east-1"` | `AWS_REGION` |

## Environment-Specific Configs

### Local (`config/local/config.toml`)

```toml
[app]
DEBUG = true

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

[sqla]
POOL_SIZE = 30

[scraper]
SCRAPE_INTERVAL_HOURS = 12
```

### Prod (`config/prod/config.toml`)

```toml
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
| `AWS_REGION` | AWS region for Bedrock | No (default: us-east-1) |
| `AWS_ACCESS_KEY_ID` | AWS credentials | Yes (for Bedrock + S3) |
| `AWS_SECRET_ACCESS_KEY` | AWS credentials | Yes (for Bedrock + S3) |
| `BEDROCK_LLM_MODEL` | Bedrock LLM model ID | No (default: claude-3-5-haiku) |
| `BEDROCK_EMBEDDING_MODEL` | Bedrock embedding model ID | No (default: cohere.embed-multilingual-v3) |
| `GEMINI_API_KEY` | Google AI API key | No (optional, if using Gemini) |
| `ANTHROPIC_API_KEY` | Anthropic API key | No (fallback LLM) |
| `S3_BUCKET` | S3 bucket for datasets | No (default: openarg-datasets) |

## Config Loading

The `load_settings()` function in `setup/config/settings.py`:

1. Reads `APP_ENV` (default `"local"`).
2. Loads `config/{env}/config.toml`.
3. Merges `config/{env}/.secrets.toml` (if exists).
4. Applies environment variable overrides.
5. Returns `AppSettings` Pydantic model.
