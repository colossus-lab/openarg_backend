.PHONY: help install dev db.up db.migrate db.revision api workers beat flower docker.up docker.down mcp.up mcp.down mcp.logs docker.all code.format code.lint code.test code.check

help: ## Show this help
	@grep -E '^[a-zA-Z_.-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	pip install uv && uv pip install --system -e '.[dev,test]'

dev: ## Run dev server with reload
	APP_ENV=local uvicorn app.run:make_app --factory --host 0.0.0.0 --port 8080 --reload

db.up: ## Start PostgreSQL + Redis
	docker compose up -d postgres redis

db.migrate: ## Run all migrations
	APP_ENV=local alembic -c alembic.ini upgrade head

db.revision: ## Create new migration (usage: make db.revision msg="add xyz")
	APP_ENV=local alembic -c alembic.ini revision --autogenerate -m "$(msg)"

api: ## Run API server
	APP_ENV=local uvicorn app.run:make_app --factory --host 0.0.0.0 --port 8080 --loop uvloop

workers.scraper: ## Run scraper worker
	APP_ENV=local celery -A app.infrastructure.celery.app:celery_app worker -Q scraper -c 2 -n scraper@%h

workers.collector: ## Run collector worker
	APP_ENV=local celery -A app.infrastructure.celery.app:celery_app worker -Q collector -c 4 -n collector@%h

workers.analyst: ## Run analyst worker
	APP_ENV=local celery -A app.infrastructure.celery.app:celery_app worker -Q analyst -c 2 -n analyst@%h

workers.embedding: ## Run embedding worker
	APP_ENV=local celery -A app.infrastructure.celery.app:celery_app worker -Q embedding -c 8 -n embedding@%h

beat: ## Run Celery Beat scheduler
	APP_ENV=local celery -A app.infrastructure.celery.app:celery_app beat --loglevel=info

flower: ## Run Flower monitoring
	celery --broker=redis://localhost:6379/0 flower --port=5555

docker.up: ## Start core services (API + workers)
	docker compose up --build

docker.down: ## Stop core services
	docker compose down

mcp.up: ## Start MCP servers (standalone containers)
	docker compose -f docker-compose-mcp.yml up --build -d

mcp.down: ## Stop MCP servers
	docker compose -f docker-compose-mcp.yml down

mcp.logs: ## Tail MCP server logs
	docker compose -f docker-compose-mcp.yml logs -f

docker.all: ## Start everything (core + MCP servers)
	docker compose up --build -d
	docker compose -f docker-compose-mcp.yml up --build -d

code.format: ## Format code with ruff
	ruff format src/ tests/

code.lint: ## Lint code
	ruff check src/ tests/ --fix
	mypy src/

code.test: ## Run tests
	pytest --cov=app --cov-report=term-missing

code.check: code.lint code.test ## Run lint + tests
