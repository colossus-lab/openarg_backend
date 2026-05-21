"""DB-level invariant: `mart_definitions.domain` is always normalized.

Migration 0052 installs a BEFORE INSERT/UPDATE trigger on
`mart_definitions.domain` so that accented or whitespace-noisy values
get canonicalized regardless of the entry path (loader, raw SQL,
admin tools, future migrations). Without that, the Python loader's
`_normalize_domain` could be silently bypassed by any raw UPSERT and
the routing layer would see duplicated dimensions
(`economía` vs `economia`) — the recurring drift we hit in 2026-05.

These tests run only against a live PG that has migration 0052 applied
(it's the staging/prod CI shape). They skip cleanly otherwise so unit
runs stay green.
"""

from __future__ import annotations

import os
import uuid

import pytest
from sqlalchemy import create_engine, text


def _engine_or_skip():
    """Return a sync engine to the DB the workers use, or skip."""
    url = os.getenv("DATABASE_URL", "")
    if not url:
        pytest.skip("DATABASE_URL not set — domain trigger test needs a live DB")
    try:
        engine = create_engine(url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).scalar()
        return engine
    except Exception as exc:  # pragma: no cover — environmental
        pytest.skip(f"DB unreachable: {exc}")


def _skip_if_trigger_missing(engine) -> None:
    """Skip cleanly when migration 0052 hasn't run yet (e.g. an older DB)."""
    with engine.connect() as conn:
        present = conn.execute(
            text(
                "SELECT 1 FROM pg_trigger "
                "WHERE tgname = 'mart_definitions_normalize_domain_trg'"
            )
        ).scalar()
    if not present:
        pytest.skip("Migration 0052 not applied — trigger missing")


def _cleanup_test_rows(engine, mart_id_prefix: str) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM mart_definitions WHERE mart_id LIKE :p"),
            {"p": f"{mart_id_prefix}%"},
        )


@pytest.fixture(scope="module")
def engine():
    eng = _engine_or_skip()
    _skip_if_trigger_missing(eng)
    return eng


def _insert_minimal_mart(conn, mart_id: str, domain_value: str | None) -> None:
    """Insert a minimal mart row exercising the trigger.

    `mart_definitions` has several NOT NULL columns we don't care about here;
    we feed placeholders so the INSERT succeeds and the trigger fires on
    `domain`. The view never gets materialized — this is purely a row test.
    """
    conn.execute(
        text(
            "INSERT INTO mart_definitions ("
            "  mart_id, mart_schema, mart_view_name, sql_definition, "
            "  yaml_version, domain, source_portals"
            ") VALUES ("
            "  :id, 'mart', :id, 'SELECT 1', '0.0.0', :dom, '{test}'::text[]"
            ")"
        ),
        {"id": mart_id, "dom": domain_value},
    )


@pytest.mark.parametrize(
    "input_value,expected",
    [
        ("economía", "economia"),
        ("Geografía", "geografia"),
        ("EDUCACIÓN", "educacion"),
        ("   politica   ", "politica"),
        ("Salud Pública", "salud_publica"),
        ("economia", "economia"),  # idempotent
        ("ñandú", "nandu"),
    ],
)
def test_trigger_normalizes_on_insert(engine, input_value, expected):
    """A raw INSERT with an accented/noisy domain is normalized in storage."""
    mart_id = f"test_trigger_{uuid.uuid4().hex[:8]}"
    try:
        with engine.begin() as conn:
            _insert_minimal_mart(conn, mart_id, input_value)
            stored = conn.execute(
                text("SELECT domain FROM mart_definitions WHERE mart_id = :id"),
                {"id": mart_id},
            ).scalar()
        assert stored == expected, (
            f"trigger should normalize {input_value!r} → {expected!r}, "
            f"got {stored!r}"
        )
    finally:
        _cleanup_test_rows(engine, mart_id)


def test_trigger_normalizes_on_update(engine):
    """An UPDATE that sets domain to an accented value gets normalized."""
    mart_id = f"test_trigger_update_{uuid.uuid4().hex[:8]}"
    try:
        with engine.begin() as conn:
            _insert_minimal_mart(conn, mart_id, "economia")
            conn.execute(
                text("UPDATE mart_definitions SET domain = :d WHERE mart_id = :id"),
                {"d": "economía", "id": mart_id},
            )
            stored = conn.execute(
                text("SELECT domain FROM mart_definitions WHERE mart_id = :id"),
                {"id": mart_id},
            ).scalar()
        assert stored == "economia", (
            f"trigger should normalize on UPDATE; got {stored!r}"
        )
    finally:
        _cleanup_test_rows(engine, mart_id)


def test_trigger_handles_null(engine):
    """NULL domain stays NULL (the trigger must not crash on NULL input)."""
    mart_id = f"test_trigger_null_{uuid.uuid4().hex[:8]}"
    try:
        with engine.begin() as conn:
            _insert_minimal_mart(conn, mart_id, None)
            stored = conn.execute(
                text("SELECT domain FROM mart_definitions WHERE mart_id = :id"),
                {"id": mart_id},
            ).scalar()
        assert stored is None
    finally:
        _cleanup_test_rows(engine, mart_id)


def test_trigger_collapses_empty_string_to_null(engine):
    """All-whitespace domain → NULL (consistent with Python `_normalize_domain`)."""
    mart_id = f"test_trigger_empty_{uuid.uuid4().hex[:8]}"
    try:
        with engine.begin() as conn:
            _insert_minimal_mart(conn, mart_id, "   ")
            stored = conn.execute(
                text("SELECT domain FROM mart_definitions WHERE mart_id = :id"),
                {"id": mart_id},
            ).scalar()
        assert stored is None
    finally:
        _cleanup_test_rows(engine, mart_id)


def test_existing_rows_already_normalized(engine):
    """After 0052 backfill, no row in mart_definitions should have an
    accented or differently-cased domain."""
    with engine.connect() as conn:
        offenders = conn.execute(
            text(
                "SELECT mart_id, domain "
                "FROM mart_definitions "
                "WHERE domain IS DISTINCT FROM normalize_domain_token(domain)"
            )
        ).fetchall()
    assert not offenders, (
        f"{len(offenders)} row(s) violate the trigger invariant: "
        f"{[(r.mart_id, r.domain) for r in offenders[:5]]}"
    )
