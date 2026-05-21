# dbt project — OpenArg

This is the **optional** dbt layer of the OpenArg medallion architecture
(MASTERPLAN Fase 5).

## Why optional

The medallion (raw → staging → mart) runs end-to-end without dbt via the
Celery tasks in `app.application.marts`. The 5 mart YAMLs at
`config/marts/*.yaml` materialize via `openarg.build_mart` /
`openarg.refresh_mart`.

dbt is here for what it adds **on top**:
- Lineage graph auto-generated from `{{ source() }}` and `{{ ref() }}` calls.
- Built-in tests (`not_null`, `unique`, `accepted_values`, plus custom).
- `dbt docs serve` for the team to browse model documentation.
- Incremental materializations when models grow large.

If your marts stay small and stable, you can ignore this directory entirely.

## Layout

```
dbt/
├── dbt_project.yml           # project config
├── profiles.yml.example      # template — real profile lives in ~/.dbt/
├── models/
│   ├── staging/sources.yml   # staging.* tables (post-contract validation)
│   └── marts/
│       ├── series_economicas.sql   # example mart (dbt-managed)
│       └── schema.yml              # tests for mart models
├── macros/                   # reusable Jinja/SQL macros
└── tests/                    # custom data tests
```

## How to run

### One-time setup

```bash
# 1. Install the dbt extra
pip install -e ".[dbt]"

# 2. Copy the example profile
cp dbt/profiles.yml.example ~/.dbt/profiles.yml
# Edit ~/.dbt/profiles.yml with your actual DB credentials

# 3. Verify the project parses
cd dbt
dbt parse
```

### Daily commands

```bash
# Run all models in dependency order
dbt run

# Run + test (recommended for the full pipeline)
dbt build

# Run only the marts
dbt run --select marts.*

# Run tests
dbt test

# Browse the docs locally
dbt docs generate && dbt docs serve
```

### From the Celery cluster

The wrappers in `app.infrastructure.celery.tasks.dbt_tasks` invoke dbt
from the same workers that run the rest of the pipeline:

```bash
# Trigger a dbt run from any node with celery installed
celery -A app.infrastructure.celery.app call openarg.dbt_build
celery -A app.infrastructure.celery.app call openarg.dbt_test
celery -A app.infrastructure.celery.app call openarg.dbt_run --args='["marts.series_economicas"]'
```

These tasks are NOT auto-triggered. Operators run them manually (or
schedule via beat) so dbt's pace stays decoupled from the
contract → staging → mart auto-refresh chain.

## When to use dbt vs. the YAML marts

| Scenario | Use |
|---|---|
| Single mart, stable SQL, refresh on upstream change | YAML mart |
| Multi-step transformation with intermediate models | dbt |
| Need lineage docs for stakeholders | dbt |
| Need column-level tests beyond pandera contracts | dbt |
| Quick prototype | YAML mart (faster iteration) |

You **can** run both, but not on the same target table. The YAML marts
materialize to `mart.<id>`; dbt models materialize to `mart.<id>` too
unless you change `+schema` in `dbt_project.yml`. Pick one per mart.

## CI

`dbt parse` is the recommended gate in CI — it catches `{{ ref() }}` typos,
missing sources, and YAML errors without needing a database connection
beyond the `--profiles-dir` setup.

## Limits

- dbt models in this project are postgres-only. The macros and types
  assume `dbt-postgres` (1.8+).
- `dbt deps` is not configured by default — add `packages.yml` if you
  need third-party macros.
