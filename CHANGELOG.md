# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

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
