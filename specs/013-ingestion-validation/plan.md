# Plan 013 — Ingestion Validation (WS0)

## File map

| Layer | File | Purpose |
|---|---|---|
| Application | `src/app/application/validation/detector.py` | `Detector` ABC, `Finding`, `Severity`, `Mode`, `ResourceContext` |
| Application | `src/app/application/validation/ingestion_validator.py` | `IngestionValidator` Strategy, `default_validator()`, `input_hash()` |
| Application | `src/app/application/validation/findings_repository.py` | UPSERT/list/resolve persistence |
| Application | `src/app/application/validation/collector_hooks.py` | `validate_pre_parse` / `validate_post_parse` / `validate_retrospective`, feature-flag aware |
| Application | `src/app/application/validation/detectors/{content,metadata,naming,preingest}.py` | The 14 detectors |
| Migration | `alembic/versions/2026_04_25_0033_add_ingestion_findings.py` | `ingestion_findings` table + idempotency unique constraint |
| Worker | `src/app/infrastructure/celery/tasks/ingestion_findings_sweep.py` | Modo 3 retrospective sweep, beat every 30 min as a bridge for non-WS0 paths |
| Worker | `src/app/infrastructure/celery/tasks/collector_tasks.py` | Shared `_finalize_cached_dataset()` helper used by specialized collectors before flipping `ready` |
| Tests | `tests/unit/test_ingestion_validator.py` | 31 unit tests covering all 14 detectors |

## Wire-in points (`collector_tasks.py`)
- After `_stream_download` returns: `validate_pre_parse(...)` — short-circuits on critical findings before S3 upload.
- Before main `UPDATE status='ready'`: `validate_post_parse(...)` — flips to error on critical findings.
- Specialized collectors (`bcra_tasks`, `indec_tasks`, `senado_tasks`, `mapa_estado_tasks`, `georef_tasks`, `presupuesto_tasks`, `gobernadores_tasks`, `dkan_tasks`, `series_tiempo_tasks`, `cordoba_leg_tasks`, `bac_tasks`) MUST call `_finalize_cached_dataset(...)` instead of writing `status='ready'` inline.

## Beat schedule
```python
"ws0-retrospective-sweep": {
    "task": "openarg.ws0_retrospective_sweep",
    "schedule": crontab(minute="12,42"),
    "options": {"queue": "ingest"},
}
```

## Feature flags
- `OPENARG_DISABLE_INGESTION_VALIDATOR=1` — disable Modos 1 + 2 (Modo 3 still runs).
- `OPENARG_SWEEP_AUTOFLIP=1` — enable auto-flip of `materialization_status` from Modo 3 (default off; soak for 1 week before turning on).
- `OPENARG_SWEEP_BATCH_SIZE` — sweep batch size (default 500).
- `OPENARG_SWEEP_PORTALS` — comma-separated allowlist of portals to sweep.

## Rollout
1. Deploy with all flags off.
2. Watch `ingestion_findings` populate via Modo 3; manual review of warn/critical distribution.
3. After 1 week: enable `OPENARG_SWEEP_AUTOFLIP=1` in staging, then prod.
