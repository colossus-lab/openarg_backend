<p align="center">
  <img src="docs/openarg-logo.png" alt="OpenArg Logo" width="120" />
</p>

<h1 align="center">OpenArg Backend</h1>

<p align="center">
  <b>AI-powered analysis engine for Argentine open government data</b><br/>
  Query pipeline, data connectors, and LLM orchestration for openarg.org
</p>

<p align="center">
  <img src="https://img.shields.io/badge/FastAPI-0.115-009688?style=for-the-badge&logo=fastapi" />
  <img src="https://img.shields.io/badge/PostgreSQL-16+pgvector-4169E1?style=for-the-badge&logo=postgresql" />
  <img src="https://img.shields.io/badge/Gemini_2.5-AI-blue?style=for-the-badge&logo=google" />
  <img src="https://img.shields.io/badge/Python-3.12-3776AB?style=for-the-badge&logo=python" />
</p>

---

## Overview

OpenArg Backend is the analysis engine behind [openarg.org](https://openarg.org) -- a platform that answers natural-language questions about Argentine public data. It orchestrates a multi-step query pipeline that classifies user intent, searches across 9 data connectors, generates SQL against cached datasets, and produces structured analyses with citations and chart data. Built with FastAPI, PostgreSQL + pgvector, Redis, Celery, and Gemini 2.5 Flash (with Claude Sonnet fallback).

---

## Architecture

Hexagonal (Ports & Adapters) architecture with four layers:

| Layer | Responsibility | Key Components |
|-------|---------------|----------------|
| **Presentation** | HTTP + WebSocket endpoints, auth middleware | FastAPI controllers, SlowAPI rate limiting |
| **Application** | Query orchestration, pipeline coordination | SmartQueryService, connectors |
| **Domain** | Pure entities, ports (interfaces), value objects | Dataclass entities, ABC port definitions |
| **Infrastructure** | External system adapters, persistence | PostgreSQL/pgvector, Redis, Celery, Gemini/Claude LLMs |

Domain ports define abstract interfaces (`IDataSource`, `ILLMProvider`, `IVectorSearch`, `ISQLSandbox`, `ICacheService`). Infrastructure adapters implement them. All wiring is handled by Dishka (IoC container).

---

## Query Pipeline

When a user submits a question, the smart query pipeline executes these steps:

1. **Classification** -- Categorize as casual, meta, injection, or off-topic (0 LLM calls, regex + keyword scoring)
2. **Semantic cache lookup** -- Check Redis + pgvector for a similar recent answer
3. **Query preprocessing** -- Expand acronyms, resolve temporal references, normalize province names
4. **Planning** -- Gemini 2.5 Flash generates a structured execution plan (1 LLM call)
5. **Parallel data collection** -- Dispatch to 9 connectors concurrently
6. **Table catalog matching** -- Match collected data against cached tables via vector search
7. **NL2SQL generation** -- Generate read-only SQL queries against matched tables
8. **SQL validation** -- 3-layer validation (regex, table allowlist, AST parsing via sqlglot)
9. **Analysis generation** -- Gemini 2.5 Flash synthesizes findings (1 LLM call)
10. **Response assembly** -- Return analysis with citations, chart data, and source links

---

## Data Sources

| Connector | Data | Source |
|-----------|------|--------|
| **Series Tiempo** | 30,000+ economic time-series (INDEC, BCRA) | apis.datos.gob.ar/series |
| **Argentina Datos** | Dollar rates (7 types), country risk | argentinadatos.com |
| **BCRA** | Central bank indicators, reserves, monetary base | bcra.gob.ar |
| **Sesiones** | Legislative session transcripts (vector search) | Congressional records |
| **DDJJ** | Patrimonial declarations (195 deputies) | Oficina Anticorrupcion |
| **Staff** | Congressional employee data (HCDN + Senado) | datos.hcdn.gob.ar |
| **Georef** | Geographic entities (provinces, departments, localities) | apis.datos.gob.ar/georef |
| **CKAN** | 3,000+ datasets across 20 government portals | Multiple CKAN portals |
| **SQL Sandbox** | Read-only queries against cached dataset tables | Internal PostgreSQL |

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Framework | FastAPI 0.115 + Uvicorn (async, UVLoop) |
| Database | PostgreSQL 16 + pgvector (HNSW indexing, 1536-dim) |
| ORM | SQLAlchemy 2.0 (async) + Alembic migrations |
| Cache / Broker | Redis 7 |
| Task Queue | Celery 5.4 (7 workers + beat scheduler) |
| AI Models | Gemini 2.5 Flash (primary) + Claude Sonnet (fallback) |
| Embeddings | gemini-embedding-001 (1536-dim) |
| DI Container | Dishka 1.6 |
| Auth | PyJWT + bcrypt + SlowAPI rate limiting |
| Monitoring | structlog + in-memory metrics + health checks |
| Config | TOML files + Pydantic settings |
| Deploy | Docker Compose on EC2, Caddy reverse proxy |

---

## Quick Start

```bash
# Clone and configure
git clone https://github.com/ColossusLab/openarg-backend.git
cd openarg-backend
cp .env.example .env
# Edit .env with your API keys (GEMINI_API_KEY, DATABASE_URL, etc.)

# Start with Docker (recommended)
docker compose up -d

# Or local development
make install        # Install dependencies (requires uv)
make db.up          # Start PostgreSQL + Redis containers
make db.migrate     # Run Alembic migrations
make dev            # Start API with hot reload on port 8080
```

---

## API Endpoints

| Category | Method | Path | Purpose |
|----------|--------|------|---------|
| Health | GET | `/health` | Component-level health check |
| Health | GET | `/health/ready` | Readiness probe |
| Query | POST | `/api/v1/query/smart` | Smart pipeline (plan + collect + analyze) |
| Query | POST | `/api/v1/query/quick` | Synchronous single-step query |
| Query | WS | `/ws/smart` | Smart pipeline with SSE streaming |
| Query | POST | `/api/v1/query/` | Submit async query |
| Query | GET | `/api/v1/query/{query_id}` | Check query status |
| Datasets | GET | `/api/v1/datasets/` | List indexed datasets |
| Datasets | GET | `/api/v1/datasets/stats` | Dataset counts per portal |
| Datasets | POST | `/api/v1/datasets/scrape/{portal}` | Trigger catalog scrape |
| Sandbox | POST | `/api/v1/sandbox/query` | Execute read-only SQL |
| Sandbox | POST | `/api/v1/sandbox/ask` | Natural language to SQL |
| Sandbox | GET | `/api/v1/sandbox/tables` | List cached tables |
| Monitoring | GET | `/api/v1/metrics` | Request, connector, cache, and token metrics |

---

## Workers

| Worker | Queue | Concurrency | Purpose |
|--------|-------|-------------|---------|
| **Scraper** | `scraper` | 2 | Scrape CKAN portal catalogs |
| **Collector** | `collector` | 4 | Download datasets, parse with pandas, cache in PostgreSQL |
| **Embedding** | `embedding` | 8 | Generate vector embeddings (3 chunks per dataset, 1536-dim) |
| **Analyst** | `analyst` | 2 | Execute query analysis pipeline |
| **Transparency** | `transparency` | 2 | Process transparency/budget data |
| **Ingest** | `ingest` | 2 | Ingest structured data sources (512MB memory limit) |
| **S3** | `s3` | 2 | Handle S3 storage operations |
| **Beat** | -- | 1 | Celery Beat scheduler for periodic tasks |

---

## Development

```bash
make install            # Install dependencies (uv pip)
make dev                # Start API with hot reload
make db.up              # Start PostgreSQL + Redis containers
make db.migrate         # Run Alembic migrations
make db.revision msg="add xyz"  # Create new migration

make workers.scraper    # Start scraper worker
make workers.collector  # Start collector worker
make workers.embedding  # Start embedding worker
make workers.analyst    # Start analyst worker
make workers.transparency  # Start transparency worker
make workers.ingest     # Start ingest worker
make workers.s3         # Start S3 worker
make beat               # Start Celery Beat scheduler
make flower             # Start Flower monitoring UI

make code.format        # Format with Ruff
make code.lint          # Ruff check + mypy
make code.test          # Pytest with coverage
make code.check         # Lint + tests
```

---

## Testing

```bash
make code.test              # Run all tests with coverage
pytest tests/unit/ -v       # Unit tests only
pytest tests/integration/   # Integration tests (requires DB + Redis)
```

CI runs unit tests, integration tests, and type checking against PostgreSQL 16 + pgvector and Redis 7. See `.github/workflows/test.yml`.

---

## Documentation

- [Architecture](docs/architecture.md)
- [API Reference](docs/api-reference.md)
- [Database Schema](docs/database-schema.md)
- [Worker Pipeline](docs/worker-pipeline.md)
- [Query Pipeline](docs/pipeline-map.md)
- [Deployment](docs/deployment.md)
- [Configuration](docs/configuration.md)
- [Domain Layer](docs/domain-layer.md)
- [Infrastructure Layer](docs/infrastructure-layer.md)
- [Backup & Restore](docs/backup-restore.md)
- [Runbook](docs/runbook.md)

---

## License

[MIT](LICENSE)

---

<p align="center">
  Created by <b>Dante De Agostino</b><br/>
  Powered by <a href="https://colossuslab.tech"><b>ColossusLab</b></a>
</p>
