"""The unsplit-CSV repair, against a real Postgres.

This class is 210 tables in production and the largest single parse defect in
the corpus. It is also the one where a bad repair is silent: split on a
delimiter that also appears inside a quoted value and every field after it
shifts by one, producing a table that looks fine and is wrong in every row.

So the tests that matter are the refusals.
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
        pytest.skip("DATABASE_URL not set — unsplit-CSV repair needs a live DB")
    try:
        engine = create_engine(url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).scalar()
        return engine
    except Exception as exc:  # pragma: no cover — environmental
        pytest.skip(f"DB unreachable: {exc}")


@pytest.fixture
def table(request):
    engine = _engine_or_skip()
    name = f"cache_unsplit_{uuid.uuid4().hex[:8]}"

    def _cleanup():
        with engine.begin() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS {SCHEMA}."{name}" CASCADE'))
            conn.execute(text("DELETE FROM parse_repair_audit WHERE table_name = :t"), {"t": name})

    request.addfinalizer(_cleanup)
    return engine, name


def _make(engine, name, header, rows):
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))
        conn.execute(text(f'DROP TABLE IF EXISTS {SCHEMA}."{name}" CASCADE'))
        conn.execute(text(f'CREATE TABLE {SCHEMA}."{name}" ("{header}" text)'))
        for r in rows:
            conn.execute(text(f'INSERT INTO {SCHEMA}."{name}" VALUES (:v)'), {"v": r})


def _columns(engine, name):
    with engine.connect() as conn:
        return [
            r.column_name
            for r in conn.execute(
                text(
                    """
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema = :s AND table_name = :t ORDER BY ordinal_position
                    """
                ),
                {"s": SCHEMA, "t": name},
            ).fetchall()
        ]


def test_it_splits_a_clean_csv_back_into_columns(table):
    """The real shape of the defect, taken from production:
    `caba__casos_de_sida` held one column named `anio,sexo,casos_SIDA,grupo_edad`
    and rows like `2003,m,1,0 - 4`."""
    from app.application.repair.parse_repair import repair_unsplit_csv_table

    engine, name = table
    _make(
        engine,
        name,
        "anio,sexo,casos_SIDA,grupo_edad",
        ["2003,m,1,0 - 4", "2003,v,3,0 - 4", "2004,m,2,5 - 9"],
    )

    out = repair_unsplit_csv_table(engine, table_schema=SCHEMA, table_name=name, dry_run=False)

    assert out.ok, out.reason
    assert _columns(engine, name) == ["anio", "sexo", "casos_sida", "grupo_edad"]
    with engine.connect() as conn:
        row = conn.execute(
            text(f'SELECT * FROM {SCHEMA}."{name}" ORDER BY anio, sexo LIMIT 1')
        ).fetchone()
    assert tuple(row) == ("2003", "m", "1", "0 - 4")


def test_a_genuinely_malformed_table_is_still_refused(table):
    """Quoting is now handled, so a refusal has to mean the rows really do not
    agree with the header — not merely that SQL could not split them.

    The dangerous case remains the same: a proposal that looks like a properly
    split table and is wrong in every row is worse than no repair at all.
    """
    from app.application.repair.parse_repair import repair_unsplit_csv_table

    engine, name = table
    _make(
        engine,
        name,
        "ciudad,provincia,poblacion",
        ["Buenos Aires,CABA,3000000", "Rosario,Santa Fe", "Córdoba,Córdoba,1400000,extra"],
    )

    out = repair_unsplit_csv_table(engine, table_schema=SCHEMA, table_name=name, dry_run=False)

    assert not out.ok
    assert out.reason.startswith("inconsistent_field_count")
    assert _columns(engine, name) == ["ciudad,provincia,poblacion"], "table untouched"


def test_dry_run_changes_nothing(table):
    from app.application.repair.parse_repair import repair_unsplit_csv_table

    engine, name = table
    _make(engine, name, "a;b;c", ["1;2;3", "4;5;6"])

    out = repair_unsplit_csv_table(engine, table_schema=SCHEMA, table_name=name)

    assert out.ok and out.reason == "dry_run"
    assert out.new_columns == ["a", "b", "c"]
    assert _columns(engine, name) == ["a;b;c"], "dry run must not touch the table"


def test_a_table_that_is_not_an_unsplit_csv_is_left_alone(table):
    from app.application.repair.parse_repair import repair_unsplit_csv_table

    engine, name = table
    with engine.begin() as conn:
        conn.execute(text(f'CREATE TABLE {SCHEMA}."{name}" (provincia text, monto text)'))

    out = repair_unsplit_csv_table(engine, table_schema=SCHEMA, table_name=name, dry_run=False)

    assert not out.ok
    assert out.reason == "not_an_unsplit_csv"


def test_the_repair_is_audited_and_therefore_reversible(table):
    """parse_repair_audit is what makes revert_repair possible, so a repair that
    does not record itself cannot be undone."""
    from app.application.repair.parse_repair import repair_unsplit_csv_table

    engine, name = table
    _make(engine, name, "x;y", ["1;2", "3;4"])
    repair_unsplit_csv_table(engine, table_schema=SCHEMA, table_name=name, dry_run=False)

    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT phase, operation, ok, old_columns, new_columns
                FROM parse_repair_audit WHERE table_name = :t
                """
            ),
            {"t": name},
        ).fetchone()

    assert row is not None, "the repair must record itself"
    assert row.phase == "unsplit_csv"
    assert row.operation == "apply" and row.ok
    assert row.new_columns == ["x", "y"]


def test_it_now_splits_a_quoted_csv_correctly(table):
    """The 87 production tables the SQL path had to decline.

    `Rosario,"Santa Fe, Argentina",1300000` splits into four fields under
    string_to_array and three under a reader that honours quoting. The first
    shifts every field after the quoted one and produces a table that looks
    correctly split and is wrong in every row.
    """
    from app.application.repair.parse_repair import repair_unsplit_csv_table

    engine, name = table
    _make(
        engine,
        name,
        "ciudad,provincia,poblacion",
        [
            "Buenos Aires,CABA,3000000",
            'Rosario,"Santa Fe, Argentina",1300000',
            'Córdoba,"Córdoba, Argentina",1400000',
        ],
    )

    out = repair_unsplit_csv_table(engine, table_schema=SCHEMA, table_name=name, dry_run=False)

    assert out.ok, out.reason
    assert out.reason == "split_quote_aware"
    assert _columns(engine, name) == ["ciudad", "provincia", "poblacion"]
    with engine.connect() as conn:
        row = conn.execute(
            text(f"SELECT * FROM {SCHEMA}.\"{name}\" WHERE ciudad = 'Rosario'")
        ).fetchone()
    assert tuple(row) == ("Rosario", "Santa Fe, Argentina", "1300000"), (
        "the quoted comma must stay inside its field"
    )


def test_a_row_beyond_the_sample_that_does_not_fit_rolls_the_whole_table_back(table):
    """Verification samples. A row past the sample can still be malformed, and
    leaving half the table split is worse than leaving none of it."""
    from app.application.repair.parse_repair import repair_unsplit_csv_table

    engine, name = table
    rows = ['a,"b,c",d'] * 3 + ["only,three,fields,plus,extra"]
    _make(engine, name, "one,two,three", rows)

    out = repair_unsplit_csv_table(
        engine, table_schema=SCHEMA, table_name=name, dry_run=False, sample_rows=3
    )

    assert not out.ok
    assert _columns(engine, name) == ["one,two,three"], "table must be untouched"
