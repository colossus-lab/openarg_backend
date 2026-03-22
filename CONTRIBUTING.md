# Contributing to OpenArg

Thank you for your interest in contributing to OpenArg. This guide will help you get started.

## Getting Started

1. Fork the repository
2. Clone your fork:
   ```bash
   git clone https://github.com/<your-username>/openarg-backend.git
   cd openarg-backend
   ```
3. Copy `.env.example` to `.env` and configure your environment variables
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
