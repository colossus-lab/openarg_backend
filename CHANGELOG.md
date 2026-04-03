# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- **Automatic Skill System**: 5 auto-detected skills (verificar, comparar, presupuesto, perfil, precio) with regex-based detection and prompt injection into planner/analyst
- **Coordinator Mode**: Heuristic coordinator node with up to 2 replans using strategies (broaden, narrow, switch_source), 20s time budget, smart escalation
- **Public API**: `POST /api/v1/ask` endpoint with Bearer token auth, per-user rate limiting, per-IP abuse protection, global free cap
- **API Key Management**: Create/revoke keys from frontend UI, 1 key per user, SHA-256 hashing, usage tracking
- **API Key Dialog**: Frontend dialog with key display, curl example, copy button, "Free Beta" chip
- `GET /api/v1/skills` endpoint for listing available skills
- `GET /api/v1/developers/usage` endpoint for usage stats

### Fixed
- Resettable reducer for data_results/step_warnings (pre-existing bug causing data accumulation on replans)
- Safety net: override LLM clarification when `_ALWAYS_CLEAR_RE` regex matches
- Fix `extract_numbers` decimal parsing in e2e helpers (64.72% → 64.72 instead of 6472)
- Rate limit counters now keyed by user_id (survives key rotation)
- Global free cap only incremented after all checks pass (no counter leak on rejected requests)
- Redis fail-open: rate limiting gracefully degrades if Redis is down
- Generic auth error messages (prevents key enumeration attacks)
- Pipeline timeout (30s) on public API endpoint

### Security
- Move hardcoded security values to environment variables
- Add topic guard for off-topic query rejection
- Enable CI test suite (unit, integration, type-check)
- Block /docs, /redoc, /admin endpoints in production (Caddyfile)
- Fail-closed SQL validation (sqlglot)
- Dynamic PUBLIC_PATHS in auth middleware

### Added
- SECURITY.md with deployment checklist
- Parametrized Docker registry and domains
- CONTRIBUTING.md with development guidelines
- Comprehensive README with architecture documentation

### Fixed
- Fix 52 failing unit tests
- Fix sqlglot compatibility (AlterTable to Alter for v30+)
- Add "cba" province alias in geographic normalization
- Add prompt injection keywords ("forget everything", "do anything now")
