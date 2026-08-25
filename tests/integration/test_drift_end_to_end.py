"""End-to-end: does the system actually notice a dataset changing shape?

Everything else about drift detection is tested with mocks, which prove the
pieces behave as written and prove nothing about whether they add up. This test
plays out the real sequence against a real Postgres:

1. A resource is collected. The baseline records its shape.
2. The portal changes the file. The collector's `to_sql` fails on the column
   mismatch and `_record_cache_drop` captures the old shape before dropping —
   **under the same table name**, which is what `schema_mismatch_recreate` does.
3. The table is recreated with the new shape.
4. The baseline runs again and must capture the new shape.
5. The report must pair the two and reach a verdict.

Step 4 is the one that was broken and the reason this test exists. The baseline
skipped any table that already carried a snapshot, so after a genuine format
change the new shape went unrecorded until the *next* drop. One snapshot, no
pair, nothing detected — the system would have caught a resource's second format
change and missed its first.

Runs only against a live PG with migrations 0056–0058 applied, and skips cleanly
otherwise so unit runs stay green.
"""

from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, text

SCHEMA = "raw"


def _engine_or_skip():
    url = os.getenv("DATABASE_URL", "")
    if not url:
        pytest.skip("DATABASE_URL not set — drift end-to-end needs a live DB")
    try:
        engine = create_engine(url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).scalar()
        return engine
    except Exception as exc:  # pragma: no cover — environmental
        pytest.skip(f"DB unreachable: {exc}")


def _skip_unless_snapshots_exist(engine) -> None:
    with engine.connect() as conn:
        present = conn.execute(text("SELECT to_regclass('raw.raw_schema_snapshots')")).scalar()
    if not present:
        pytest.skip("migration 0056 has not run on this DB")


@pytest.fixture
def resource(request):
    """A throwaway resource, cleaned up whatever the test does."""
    engine = _engine_or_skip()
    _skip_unless_snapshots_exist(engine)

    suffix = uuid.uuid4().hex[:10]
    table = f"cache_drifttest_{suffix}"
    identity = f"drifttest::{suffix}"

    def _cleanup():
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS {SCHEMA}."{table}" CASCADE'))
            conn.execute(
                text("DELETE FROM raw.raw_schema_snapshots WHERE table_name = :t"),
                {"t": table},
            )
            conn.execute(
                text("DELETE FROM public.raw_table_versions WHERE resource_identity = :r"),
                {"r": identity},
            )

    _cleanup()
    request.addfinalizer(_cleanup)
    return engine, table, identity


def _create_with(engine, table: str, columns: dict[str, list[str]]) -> None:
    """Create the physical table and fill it, so `pg_stats` has something to see."""
    cols_sql = ", ".join(f'"{c}" text' for c in columns)
    with engine.begin() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS {SCHEMA}."{table}" CASCADE'))
        conn.execute(text(f'CREATE TABLE {SCHEMA}."{table}" ({cols_sql})'))
        names = ", ".join(f'"{c}"' for c in columns)
        for row in zip(*columns.values(), strict=True):
            values = ", ".join(f"'{v}'" for v in row)
            conn.execute(text(f'INSERT INTO {SCHEMA}."{table}" ({names}) VALUES ({values})'))
        conn.execute(text(f'ANALYZE {SCHEMA}."{table}"'))


def _register(engine, identity: str, table: str, version: int) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO public.raw_table_versions
                    (resource_identity, version, schema_name, table_name,
                     parser_version, normalization_version, created_at)
                VALUES (:r, :v, :s, :t, :pv, :nv, NOW())
                ON CONFLICT (resource_identity, version) DO UPDATE
                    SET table_name = EXCLUDED.table_name,
                        parser_version = EXCLUDED.parser_version
                """
            ),
            {
                "r": identity,
                "v": version,
                "s": SCHEMA,
                "t": table,
                # A real fingerprint on both sides, so G1 can actually run and
                # the verdict is UNEXPLAINED rather than UNATTRIBUTABLE.
                "pv": "p:testfingerprint",
                "nv": "n:testfingerprint",
            },
        )


def _snapshots(engine, table: str) -> list[tuple]:
    with engine.connect() as conn:
        return [
            (r.column_count, r.reason)
            for r in conn.execute(
                text(
                    """
                    SELECT column_count, reason FROM raw.raw_schema_snapshots
                    WHERE table_name = :t ORDER BY captured_at
                    """
                ),
                {"t": table},
            ).fetchall()
        ]


PROVINCIAS = ["Buenos Aires", "Córdoba", "Santa Fe", "Mendoza", "Salta", "Jujuy"]
MONTOS = ["1200", "980", "34000", "12", "7788", "450"]
AÑOS = ["2021", "2022", "2023", "2024", "2025", "2026"]


def test_a_first_format_change_is_captured_and_classified(resource):
    """The whole point, played out once.

    A resource is collected, the portal adds a column, the table is recreated
    under the same name, and the system has to end up holding two comparable
    shapes and a verdict about the difference.
    """
    from app.application.catalog.schema_snapshot import capture_table_snapshot, snapshot_from_row
    from app.application.drift import DriftContext, Verdict, classify_change

    engine, table, identity = resource

    # ── 1. collected, and the baseline records the shape ──────────
    _create_with(engine, table, {"provincia": PROVINCIAS, "monto": MONTOS})
    _register(engine, identity, table, version=1)
    first = capture_table_snapshot(
        engine, table_name=table, schema_name=SCHEMA, reason="baseline", actor="test"
    )
    assert first is not None, "the baseline must record the shape it found"
    assert _snapshots(engine, table) == [(2, "baseline")]

    # ── 2 & 3. the portal changes the file; drop + recreate, same name ──
    # This is exactly what `_to_sql_safe` does on a column mismatch: capture the
    # old shape, then DROP and rewrite under the identical table name.
    pre_drop = capture_table_snapshot(
        engine,
        table_name=table,
        schema_name=SCHEMA,
        reason="schema_mismatch_recreate",
        actor="test",
    )
    assert pre_drop is not None, "the old shape must survive the drop"
    _create_with(engine, table, {"provincia": PROVINCIAS, "monto": MONTOS, "anio": AÑOS})
    # No new version row. `uq_raw_table_versions_table_name` is UNIQUE on
    # (schema_name, table_name), so a recreate under the same name cannot
    # register a second version — the registry keeps one row and the two shapes
    # are related by being consecutive snapshots of the same table, which is
    # what `_PAIRS_SQL` pairs on. Discovered by this test failing, which is the
    # kind of thing mocks would have agreed with instead.

    # ── 4. the baseline must notice the table no longer matches its snapshot ──
    # This is the step that was broken: it used to skip on "has a snapshot".
    with engine.connect() as conn:
        needs_capture = conn.execute(
            text(
                """
                SELECT (
                    SELECT array_agg(x ORDER BY x COLLATE "C")
                    FROM raw.raw_schema_snapshots s,
                         jsonb_array_elements(s.columns_profile) e,
                         LATERAL (SELECT e->>'name' AS x) q
                    WHERE s.table_name = :t
                      AND s.captured_at = (SELECT max(captured_at)
                                           FROM raw.raw_schema_snapshots
                                           WHERE table_name = :t)
                ) IS DISTINCT FROM (
                    SELECT array_agg(c.column_name::text ORDER BY c.column_name::text COLLATE "C")
                    FROM information_schema.columns c
                    WHERE c.table_schema = :s AND c.table_name = :t
                )
                """
            ),
            {"t": table, "s": SCHEMA},
        ).scalar()
    assert needs_capture, "a recreated table must be eligible for a fresh snapshot"

    third = capture_table_snapshot(
        engine, table_name=table, schema_name=SCHEMA, reason="baseline", actor="test"
    )
    assert third is not None
    assert _snapshots(engine, table) == [
        (2, "baseline"),
        (2, "schema_mismatch_recreate"),
        (3, "baseline"),
    ], "three snapshots: the original, the one taken as it was replaced, and the new shape"

    # ── 5. the pair is comparable and the verdict is right ────────
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT schema_name, table_name, resource_identity, version,
                       row_count_estimate, stats_available, columns_profile,
                       parser_version, normalization_version, layout_profile,
                       header_quality, is_truncated
                FROM raw.raw_schema_snapshots
                WHERE table_name = :t ORDER BY captured_at
                """
            ),
            {"t": table},
        ).fetchall()

    before, after = snapshot_from_row(rows[1]), snapshot_from_row(rows[2])
    verdict = classify_change(before, after, DriftContext(same_identity=True, same_source_url=True))

    assert verdict.diff["schema_changed"], "the shapes differ and the diff must say so"
    assert verdict.diff["added"] == ["anio"]
    assert verdict.diff["removed"] == []
    # Same parser on both sides, same identity, same URL: nothing can explain it
    # away, so it is upstream drift and the cascade must say so plainly.
    assert verdict.verdict is Verdict.UNEXPLAINED
    assert verdict.is_actionable


def test_our_own_parser_moving_is_exonerated_not_reported(resource):
    """The counter-case, and the one the first real measurement was made of.

    All five findings on 2026-08-21 were our parser, not the portals. A change
    across a fingerprint bump must be exonerated by G1 — otherwise the system
    spends its first actions adapting to a portal that never moved.
    """
    from app.application.catalog.schema_snapshot import (
        capture_table_snapshot,
        snapshot_from_row,
    )
    from app.application.drift import DriftContext, Verdict, classify_change

    engine, table, identity = resource

    _create_with(engine, table, {"col_1": PROVINCIAS, "col_2": MONTOS})
    _register(engine, identity, table, version=1)
    capture_table_snapshot(
        engine, table_name=table, schema_name=SCHEMA, reason="baseline", actor="test"
    )

    # The parser learns to recover the real headers — our change, not theirs.
    _create_with(engine, table, {"provincia": PROVINCIAS, "monto": MONTOS})
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE public.raw_table_versions
                SET parser_version = 'p:afterthefix', normalization_version = 'n:afterthefix'
                WHERE resource_identity = :r
                """
            ),
            {"r": identity},
        )
    capture_table_snapshot(
        engine, table_name=table, schema_name=SCHEMA, reason="baseline", actor="test"
    )

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT schema_name, table_name, resource_identity, version,
                       row_count_estimate, stats_available, columns_profile,
                       parser_version, normalization_version, layout_profile,
                       header_quality, is_truncated
                FROM raw.raw_schema_snapshots
                WHERE table_name = :t ORDER BY captured_at
                """
            ),
            {"t": table},
        ).fetchall()

    verdict = classify_change(
        snapshot_from_row(rows[0]),
        snapshot_from_row(rows[1]),
        DriftContext(same_identity=True, same_source_url=True),
    )

    assert verdict.verdict is Verdict.EXONERATED
    assert verdict.exonerated_by == "G1_provenance"
    assert not verdict.is_actionable


def test_a_change_we_cannot_attribute_is_not_reported_as_drift(resource):
    """Placeholders are not provenance.

    Production carries `legacy:unknown` for 26,435 snapshots and a bare date for
    21,989 registry rows. Treating either as a real version is what let the
    cascade report our own regressions as upstream change.
    """
    from app.application.catalog.schema_snapshot import (
        capture_table_snapshot,
        snapshot_from_row,
    )
    from app.application.drift import DriftContext, Verdict, classify_change

    engine, table, identity = resource

    _create_with(engine, table, {"provincia": PROVINCIAS, "monto": MONTOS})
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO public.raw_table_versions
                    (resource_identity, version, schema_name, table_name,
                     parser_version, created_at)
                VALUES (:r, 1, :s, :t, 'legacy:unknown', NOW())
                """
            ),
            {"r": identity, "s": SCHEMA, "t": table},
        )
    capture_table_snapshot(
        engine, table_name=table, schema_name=SCHEMA, reason="baseline", actor="test"
    )
    _create_with(engine, table, {"provincia": PROVINCIAS, "monto": MONTOS, "anio": AÑOS})
    capture_table_snapshot(
        engine, table_name=table, schema_name=SCHEMA, reason="baseline", actor="test"
    )

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT schema_name, table_name, resource_identity, version,
                       row_count_estimate, stats_available, columns_profile,
                       parser_version, normalization_version, layout_profile,
                       header_quality, is_truncated
                FROM raw.raw_schema_snapshots
                WHERE table_name = :t ORDER BY captured_at
                """
            ),
            {"t": table},
        ).fetchall()

    verdict = classify_change(
        snapshot_from_row(rows[0]),
        snapshot_from_row(rows[1]),
        DriftContext(same_identity=True, same_source_url=True),
    )

    assert verdict.verdict is Verdict.UNATTRIBUTABLE
    assert not verdict.is_actionable
