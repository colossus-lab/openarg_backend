# Plan 014 — `cached_datasets` State Machine (WS0.5)

## File map

| Layer | File | Purpose |
|---|---|---|
| Application | `src/app/application/state_machine/cached_dataset_enforcer.py` | `Status`, `Transition`, `ALLOWED_TRANSITIONS`, `StateMachineEnforcer` |
| Migration | `alembic/versions/2026_04_25_0034_add_error_category_to_cached_datasets.py` | `error_category` column + CHECK + backfill + trigger + batched heal historical |
| Worker | `src/app/infrastructure/celery/tasks/state_invariants_sweep.py` | Periodic sweep + finding persistence |
| Worker (M1+M2) | `src/app/infrastructure/celery/tasks/ops_fixes.py:cleanup_invariants` | M1 (`retry>=5 + age>6h`) + M2 (`retry=0 + msg + age>24h`) zombie sweeps. Returns `fixed_zombies` and `fixed_zero_retry_zombies`. |
| Tests | `tests/unit/test_state_machine_enforcer.py` | Transition table + invariant scan |

## Beat schedule
```python
"ws0-5-state-invariants-sweep": {
    "task": "openarg.ws0_5_state_invariants_sweep",
    "schedule": crontab(minute="7,37"),
    "options": {"queue": "default"},
}
```

## Feature flags
- `OPENARG_WS0_5_AUTO_ENFORCE=1` — enable auto-correction (default off; soak 1 week first).

## Rollout
1. Deploy migration 0034 — backfills `error_category`, installs trigger, heals stuck rows historically in batches of 100.
2. Sweep runs every 30 min in dry-run mode for 1 week.
3. Validate violations distribution; enable `OPENARG_WS0_5_AUTO_ENFORCE=1`.
