"""Mart data quality invariants — runs against a live database.

These tests catch the class of bug Sprint 1.5 introduced and Sprint 1.6
went back to fix manually:

  - escuelas_argentina v0.3.0 reported 28,424 rows but only 23,826 were
    unique CUEs (duplicate matrícula records inflated the count by 19%).
  - demo_energia_pozos v0.1.2 reported 499,670 rows but the GROUP BY was
    silently broken (538× inflation against the real 927 logical rows).

Both bugs would have been caught by a single assertion: `COUNT(*) ==
COUNT(DISTINCT business_key)`. Add an entry to `MART_INVARIANTS` per
mart with its business key + soft NULL bounds and the suite runs the
checks against `mart.<view>` for every CI / smoke run.

These are integration tests (need a live PG with the mart schema
populated). They skip when `OPENARG_SKIP_MART_INVARIANTS=1` or when the
DB is unreachable — that way they're harmless in pure-unit CI but they
fail loudly when wired against staging.

Add new marts to `MART_INVARIANTS` when they ship. The default cost of
a new entry is ~0 (the test harness handles iteration), so there's no
excuse to leave a mart without an invariant after the bugs we just hit.
"""

from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, text

# ── Per-mart invariants ──────────────────────────────────────────────────────


MART_INVARIANTS: dict[str, dict] = {
    "escuelas_argentina": {
        # Business key: cue is unique within a province. A row per CUE.
        "business_key": ["cue", "provincia"],
        # Min rows we expect once the 4 portales aterrizaron. Tripwire
        # below this likely means a portal silently disappeared.
        "min_rows": 20_000,
        # Coverage assertions — the mart promises 4 jurisdictions today.
        # If the count drops below that, we lost data.
        "min_distinct_provincia": 4,
        # NULL bounds — `gestion` should be set for every row (we filter
        # on `cue IS NOT NULL`); NULL gestion would be a regression.
        "max_null_rate": {
            "cue": 0.0,
            "nombre": 0.05,  # tolerance for legacy records without name
            "provincia": 0.0,
            "gestion": 0.05,
        },
    },
    "legislatura_actividad": {
        "business_key": ["organismo", "nombre"],
        "min_rows": 200,  # at least HCDN's 256
        "min_distinct_organismo": 2,  # HCDN + Senado
        "max_null_rate": {
            "organismo": 0.0,
            "nombre": 0.0,
            "bloque": 0.0,
        },
    },
    "staff_estado": {
        "business_key": ["organismo", "nombre", "cargo"],
        "min_rows": 1_000,
        "min_distinct_organismo": 2,
        "max_null_rate": {
            "organismo": 0.0,
            "nombre": 0.0,
        },
    },
    "presupuesto_consolidado": {
        "business_key": ["anio", "jurisdiccion", "programa"],
        "min_rows": 1_000,
        # We expect 11 years of data (2016..2026) and ~16 jurisdicciones.
        "min_distinct_anio": 5,
        "max_null_rate": {
            "anio": 0.0,
            "jurisdiccion": 0.0,
            "programa": 0.0,
        },
    },
    "demo_energia_pozos": {
        "business_key": ["anio", "mes", "empresa", "provincia"],
        "min_rows": 100,
        "max_null_rate": {
            "anio": 0.0,
            "mes": 0.0,
            "empresa": 0.10,  # raw can have empty empresa
        },
    },
    "series_economicas": {
        "business_key": ["serie_id"],
        "min_rows": 30,  # ~39 cotizaciones BCRA hoy
        "max_null_rate": {
            "serie_id": 0.0,
            "fuente": 0.0,
        },
    },
}


# ── Test harness ─────────────────────────────────────────────────────────────


def _engine_or_skip():
    """Return a SQLAlchemy sync engine targeting the same DB the workers
    use, or skip the test when there's no usable connection.

    Allows running the suite locally without staging credentials and
    keeps CI green when the DB is unreachable. Set
    `OPENARG_SKIP_MART_INVARIANTS=1` to force-skip even when the DB
    answers (useful for offline development).
    """
    if os.getenv("OPENARG_SKIP_MART_INVARIANTS", "").lower() in ("1", "true", "yes"):
        pytest.skip("OPENARG_SKIP_MART_INVARIANTS set")
    url = os.getenv("DATABASE_URL", "")
    if not url:
        pytest.skip("DATABASE_URL not set — mart invariants need a live DB")
    try:
        engine = create_engine(url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).scalar()
        return engine
    except Exception as exc:  # pragma: no cover — environmental
        pytest.skip(f"DB unreachable: {exc}")


def _quote_ident(name: str) -> str:
    """Defensive double-quote so a schema/view name with weird chars
    can't break the f-string. We control these names but treating them
    as untrusted is cheap insurance."""
    return '"' + name.replace('"', '""') + '"'


@pytest.fixture(scope="module")
def engine():
    return _engine_or_skip()


# ── Invariants ───────────────────────────────────────────────────────────────


@pytest.mark.parametrize("mart_id,spec", list(MART_INVARIANTS.items()))
def test_mart_no_duplicate_business_keys(engine, mart_id, spec):
    """The single most important assertion — `COUNT(*) == COUNT(DISTINCT
    business_key)`. Catches the v0.3.0 escuelas_argentina bug
    (matrícula multi-período inflating CUE count) and the v0.1.2
    demo_energia_pozos bug (broken GROUP BY producing 538× duplication).
    """
    bk = ", ".join(_quote_ident(c) for c in spec["business_key"])
    view = _quote_ident(mart_id)
    with engine.connect() as conn:
        row = conn.execute(
            text(f"SELECT COUNT(*) AS r, COUNT(DISTINCT ({bk})) AS u FROM mart.{view}")  # noqa: S608
        ).one()
    assert row.r == row.u, (
        f"Mart {mart_id} has duplicate business keys: {row.r} rows vs "
        f"{row.u} distinct ({bk}). Likely missing DISTINCT ON or broken "
        f"GROUP BY — see Sprint 1.5/1.6 for similar shape."
    )


@pytest.mark.parametrize("mart_id,spec", list(MART_INVARIANTS.items()))
def test_mart_minimum_rows(engine, mart_id, spec):
    """Tripwire on row_count to catch silent data loss (a portal dropped
    a dataset, or a build_mart silently produced zero rows). The
    threshold is conservative — the real count is well above."""
    view = _quote_ident(mart_id)
    with engine.connect() as conn:
        rows = conn.execute(text(f"SELECT COUNT(*) FROM mart.{view}")).scalar()  # noqa: S608
    assert rows >= spec["min_rows"], (
        f"Mart {mart_id} has {rows} rows, below floor {spec['min_rows']}. "
        f"Possible silent data loss."
    )


@pytest.mark.parametrize("mart_id,spec", list(MART_INVARIANTS.items()))
def test_mart_null_rates_within_bounds(engine, mart_id, spec):
    """Per-column NULL rate ceiling. Catches the case where a portal
    aterrizó pero el cast falló (e.g. all-NULL `provincia` because the
    upstream column got renamed)."""
    view = _quote_ident(mart_id)
    with engine.connect() as conn:
        total = conn.execute(text(f"SELECT COUNT(*) FROM mart.{view}")).scalar()  # noqa: S608
        assert total > 0, f"Mart {mart_id} is empty — separate failure"
        for col, max_rate in spec.get("max_null_rate", {}).items():
            null_count = conn.execute(
                text(f"SELECT COUNT(*) FROM mart.{view} WHERE {_quote_ident(col)} IS NULL")  # noqa: S608
            ).scalar()
            actual_rate = null_count / total
            assert actual_rate <= max_rate, (
                f"Mart {mart_id} column {col!r}: NULL rate {actual_rate:.2%} "
                f"exceeds bound {max_rate:.2%} ({null_count}/{total})"
            )


def test_escuelas_covers_at_least_4_provinces(engine):
    """Coverage tripwire specific to escuelas_argentina v0.3.x: the
    mart promises BA + CABA + Mendoza + Corrientes. If a UNION block
    silently drops out (e.g. live_table macro can't resolve), the count
    drops and the user gets a half-truth."""
    with engine.connect() as conn:
        n = conn.execute(
            text("SELECT COUNT(DISTINCT provincia) FROM mart.escuelas_argentina")
        ).scalar()
    assert n >= MART_INVARIANTS["escuelas_argentina"]["min_distinct_provincia"], (
        f"escuelas_argentina has only {n} distinct provincia — UNION block missing?"
    )


def test_legislatura_has_both_chambers(engine):
    """Sister assertion to the escuelas one: legislatura must have HCDN
    and Senado after Sprint 1.5. Without this, a regression that drops
    the Senado UNION block would silently halve the mart and the LLM
    would only ever recommend HCDN."""
    with engine.connect() as conn:
        n = conn.execute(
            text("SELECT COUNT(DISTINCT organismo) FROM mart.legislatura_actividad")
        ).scalar()
    assert n >= MART_INVARIANTS["legislatura_actividad"]["min_distinct_organismo"], (
        f"legislatura_actividad has only {n} organismo — Senado UNION dropped?"
    )


def test_escuelas_geo_within_valid_range(engine):
    """Sprint 1.7 detected 19 escuelas with lat/lng outside the
    [-90,90]/[-180,180] envelope. Mendoza upstream had `lati`↔`longitud`
    swapped + decimal-comma quirks; the v0.3.2 parser swaps + filters.
    Any future regression that re-introduces invalid geo trips this."""
    with engine.connect() as conn:
        invalid = conn.execute(
            text(
                """
                SELECT COUNT(*) FROM mart.escuelas_argentina
                WHERE (lat IS NOT NULL AND (lat < -90 OR lat > 90))
                   OR (lng IS NOT NULL AND (lng < -180 OR lng > 180))
                """
            )
        ).scalar()
    assert invalid == 0, (
        f"escuelas_argentina has {invalid} rows with geo outside valid range — "
        f"likely a regex regression in the Mendoza locale parser"
    )


def test_presupuesto_devengado_never_exceeds_vigente(engine):
    """Definitional invariant of Argentine budget law: you cannot
    execute (devengar) more than was approved (crédito vigente).
    Sprint 1.7 audit caught 1 row violating this; the v0.2.1 mart
    filters them out at SQL level. If a future API change ever
    re-introduces over-execution rows, this test catches it."""
    with engine.connect() as conn:
        violations = conn.execute(
            text(
                """
                SELECT COUNT(*) FROM mart.presupuesto_consolidado
                WHERE credito_devengado IS NOT NULL
                  AND credito_vigente IS NOT NULL
                  AND credito_devengado > credito_vigente
                """
            )
        ).scalar()
    assert violations == 0, (
        f"presupuesto_consolidado has {violations} rows where devengado > "
        f"vigente — definitional bug, the SQL WHERE should filter them"
    )


def test_no_dataset_has_double_ready_cd(engine):
    """A dataset_id should have at most ONE `cached_datasets` row in
    status='ready'. Sprint 1.7 caught 63 datasets with both a legacy
    `cache_*` ready and a raw-promoted ready. The cleanup_invariants
    invariant #6.5 demotes the legacy one when a current rtv exists.
    Drift back into double-ready means the cleanup didn't catch up
    or a writer is creating extra rows without going through the
    state machine."""
    with engine.connect() as conn:
        n = conn.execute(
            text(
                """
                SELECT COUNT(*) FROM (
                    SELECT dataset_id FROM cached_datasets
                    WHERE status = 'ready'
                    GROUP BY dataset_id
                    HAVING COUNT(*) > 1
                ) sub
                """
            )
        ).scalar()
    # Soft tolerance — cleanup_invariants runs every hour, so a small
    # drift between writes is expected. Real regression is sustained
    # double-ready growth above this floor.
    assert n <= 5, (
        f"{n} datasets have multiple `cached_datasets` ready rows — "
        f"cleanup_invariants invariant #6.5 isn't keeping up or a "
        f"writer is bypassing the state machine"
    )


def test_mart_definitions_metadata_in_sync(engine):
    """`mart_definitions.last_row_count` should match the matview row
    count (the cleanup_invariants invariant from Sprint 0.3 keeps this
    aligned). Drift means the discovery filter
    `COALESCE(last_row_count,0) > 0` could hide a healthy mart from the
    serving port — see DEBT-019-002 history."""
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT mart_id, mart_schema, mart_view_name, last_row_count
                FROM mart_definitions
                WHERE COALESCE(last_row_count, 0) > 0
                """
            )
        ).fetchall()
    assert rows, "no marts with last_row_count > 0 — sweep regression?"
    for r in rows:
        actual = conn = engine.connect()
        try:
            actual_count = conn.execute(
                text(
                    f"SELECT COUNT(*) FROM "
                    f"{_quote_ident(r.mart_schema)}.{_quote_ident(r.mart_view_name)}"  # noqa: S608
                )
            ).scalar()
        finally:
            conn.close()
        # Tolerance: the matview is REFRESHed asynchronously and
        # `last_row_count` is updated AFTER the build. A small drift is
        # fine; >5% means cleanup_invariants didn't catch up yet.
        ratio = abs(actual_count - r.last_row_count) / max(actual_count, 1)
        assert ratio <= 0.05, (
            f"Mart {r.mart_id}: last_row_count={r.last_row_count} but "
            f"matview has {actual_count} rows (drift {ratio:.1%})"
        )
