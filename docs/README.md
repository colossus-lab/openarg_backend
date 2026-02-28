# OpenArg Backend Documentation

OpenArg is an AI-powered multi-agent platform for analyzing Argentine government open data. It scrapes public data portals, generates vector embeddings, caches datasets in PostgreSQL, and answers natural-language queries using LLMs.

## Documentation Index

| Document | Description |
|----------|-------------|
| [Architecture](./architecture.md) | Hexagonal architecture, layer responsibilities, DI wiring |
| [API Reference](./api-reference.md) | All HTTP endpoints with request/response schemas |
| [Database Schema](./database-schema.md) | Tables, columns, indexes, relationships, migrations |
| [Domain Layer](./domain-layer.md) | Entities, ports (interfaces), exceptions |
| [Infrastructure Layer](./infrastructure-layer.md) | Adapters, persistence, Celery workers |
| [Configuration](./configuration.md) | Settings, TOML config, environment variables |
| [Deployment](./deployment.md) | Docker Compose, services, Make commands |
| [Worker Pipeline](./worker-pipeline.md) | Celery tasks, queues, scheduled jobs |

## Quick Start

```bash
# 1. Start infrastructure
make db.up

# 2. Run migrations
make db.migrate

# 3. Start dev server
make dev

# 4. Start workers (in separate terminals)
make workers.scraper
make workers.collector
make workers.embedding
make workers.analyst
```

## Tech Stack

- **Runtime:** Python 3.12+
- **Framework:** FastAPI 0.115 + Uvicorn (async, UVLoop)
- **Database:** PostgreSQL 16 + pgvector (HNSW indexing, 1536-dim embeddings)
- **ORM:** SQLAlchemy 2.0 (async) + Alembic migrations
- **DI:** Dishka 1.6 (IoC container)
- **Workers:** Celery 5.4 + Redis 7 (broker + cache + results)
- **AI:** Google Generative AI (Gemini 2.5 Flash) + OpenAI (text-embedding-3-small) + Anthropic (Claude Sonnet)
- **HTTP Client:** HTTPX (async)
- **Rate Limiting:** SlowAPI
- **Config:** TOML + Pydantic settings
- **Logging:** structlog
