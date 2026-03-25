# Contributing to OpenArg

Thank you for your interest in contributing to OpenArg. This guide will help you get started.

## Getting Started

1. Fork the repository
2. Clone your fork:
   ```bash
   git clone https://github.com/<your-username>/openarg-backend.git
   cd openarg-backend
   ```
3. Copy `.env.example` to `.env` and configure your environment variables (see [Environment Setup](#environment-setup) below)
4. Install dependencies and start services:
   ```bash
   make install
   make db.up
   make db.migrate
   ```
5. Run the API:
   ```bash
   make dev
   ```

The API will be available at `http://localhost:8081`.

## Environment Setup

```bash
cp .env.example .env
```

### Database & Redis (Required)

Started automatically by `make db.up` (Docker). Just set passwords:

| Variable | How to set it |
|----------|--------------|
| `POSTGRES_PASSWORD` | Generate with `openssl rand -base64 32` |
| `REDIS_PASSWORD` | Generate with `openssl rand -base64 32` |
| `POSTGRES_USER` | Default: `openarg` (leave as-is) |
| `POSTGRES_DB` | Default: `openarg_db` (leave as-is) |

### AI Keys (At least one required)

The system uses a fallback chain: AWS Bedrock (primary) -> Gemini (fallback). For local dev, a free Gemini key is enough:

| Variable | How to get it |
|----------|--------------|
| `GEMINI_API_KEY` | Get a free key at [Google AI Studio](https://aistudio.google.com/apikey). Easiest option for local dev. |
| `GEMINI_MODEL` | Default: `gemini-2.5-flash` (leave as-is) |
| `AWS_ACCESS_KEY_ID` | Only if using AWS Bedrock (Claude). Create IAM credentials with Bedrock access. |
| `AWS_SECRET_ACCESS_KEY` | Same as above. |
| `ANTHROPIC_API_KEY` | Optional. Direct Anthropic API, not used by default. |

### Security

| Variable | How to set it |
|----------|--------------|
| `BACKEND_API_KEY` | Generate with `openssl rand -base64 24`. The frontend's `OPENARG_BACKEND_API_KEY` must match this. |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:3000` for local dev (the frontend URL). |

### Frontend Auth (Optional — only needed if running the frontend too)

| Variable | How to get it |
|----------|--------------|
| `NEXTAUTH_SECRET` | Generate with `openssl rand -base64 32` |
| `GOOGLE_CLIENT_ID` | [Google Cloud Console](https://console.cloud.google.com) > APIs & Services > Credentials > Create OAuth Client ID (Web app). Add `http://localhost:3000/api/auth/callback/google` as redirect URI. |
| `GOOGLE_CLIENT_SECRET` | Same OAuth client as above. |
| `ALLOWED_EMAILS` | Comma-separated Google emails allowed to log in: `you@gmail.com` |
| `ADMIN_EMAILS` | Comma-separated admin emails. |

### Minimal Dev Config (Quick Start)

For the fastest setup, you only need these in your `.env`:

```env
POSTGRES_PASSWORD=devpassword
REDIS_PASSWORD=devpassword
GEMINI_API_KEY=your-gemini-key-from-aistudio
BACKEND_API_KEY=devkey
CORS_ALLOWED_ORIGINS=http://localhost:3000
APP_ENV=local
```

Then:
```bash
make install && make db.up && make db.migrate && make dev
```

## Related Repository

OpenArg has two repositories:
- **Backend** (this repo): Python/FastAPI API, Celery workers, data pipeline
- **Frontend**: [OpenArg Frontend](https://github.com/colossus-lab/openarg_frontend) — Next.js 16, React 19, TypeScript

## Development Workflow

1. Create a feature branch from `staging`:
   ```bash
   git checkout staging
   git pull origin staging
   git checkout -b feature/my-feature
   ```
2. Make your changes
3. Run the full check suite:
   ```bash
   make code.check    # Runs lint + tests
   ```
4. Commit with a clear, meaningful message
5. Push to your fork and open a Pull Request **against the `staging` branch** (not `main`)

> **Important**: All PRs must target `staging`. The `main` branch is reserved for production releases and is only updated via merges from `staging`.

## Code Style

- **Formatter/Linter**: Ruff (line length 100)
- **Type checking**: mypy in strict mode
- **Type hints**: Required on all function signatures
- **Architecture**: Follow hexagonal architecture -- domain ports (ABC) in `domain/ports/`, infrastructure adapters in `infrastructure/adapters/`
- **Async-first**: All I/O operations must use async/await
- **Tests**: Required for new features and bug fixes

Run formatting and linting before committing:

```bash
make code.format    # Auto-format with Ruff
make code.lint      # Ruff check + mypy
```

## Testing

```bash
make code.test              # Run all tests with coverage
pytest tests/unit/ -v       # Unit tests only
pytest tests/integration/   # Integration tests (requires DB + Redis)
```

## Commit Messages

- Use imperative mood: "Add feature" not "Added feature"
- Keep the subject line under 72 characters
- Reference issue numbers where applicable: "Fix query timeout (#42)"

## Reporting Issues

- Use GitHub Issues for bug reports and feature requests
- Include: steps to reproduce, expected vs. actual behavior, environment details
- For security vulnerabilities, see [SECURITY.md](SECURITY.md) -- do NOT open a public issue

## Questions?

Open a Discussion on GitHub or reach out to the maintainers.
