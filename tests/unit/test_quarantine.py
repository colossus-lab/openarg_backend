"""Tests for the third outcome: withholding a table nobody could repair.

The asymmetry that justifies it: a table whose columns are `col_1` / `Unnamed: 3`
is not merely unhelpful to a system that writes SQL, it is dangerous — the model
infers a meaning for `col_1` and answers fluently, with a citation, from a column
nobody has read. "No tengo el dato" beats a sourced wrong number.

So what these tests pin hardest is the *narrowness*: only the defect classes that
make a column unreadable, only tables currently being served, and always
reversible.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from app.application.repair.quarantine import (
    is_quarantinable,
    quarantine,
    release,
)


def _engine(rowcount=1, raises=False):
    engine = MagicMock()
    if raises:
        engine.begin.side_effect = RuntimeError("db caída")
        return engine, None
    ctx = engine.begin.return_value.__enter__.return_value
    ctx.execute.return_value.rowcount = rowcount
    return engine, ctx


# ── a qué se le aplica ─────────────────────────────────────────


def test_placeholder_columns_are_quarantinable():
    assert is_quarantinable(["col_n"])
    assert is_quarantinable(["unnamed"])


def test_a_delimiter_left_in_a_name_is_quarantinable():
    assert is_quarantinable(["delimiter_in_name"])


def test_a_merely_long_name_is_not():
    # Es feo de leer y perfectamente usable. Incluirlo retiraría miles de tablas
    # que funcionan.
    assert not is_quarantinable(["long_name"])


def test_no_symptoms_means_nothing_to_withhold():
    assert not is_quarantinable([])


def test_one_unreadable_symptom_among_several_is_enough():
    assert is_quarantinable(["long_name", "col_n"])


# ── retirar ────────────────────────────────────────────────────


def test_a_served_table_is_withheld():
    engine, ctx = _engine(rowcount=1)
    assert quarantine(engine, "raw.t") is True
    sql = str(ctx.execute.call_args[0][0])
    assert "materialization_corrupted" in sql
    assert "= 'ready'" in sql, "sólo mueve lo que hoy se sirve"


def test_a_table_that_is_not_being_served_is_left_alone():
    # Ya está `failed` o `pending`: reescribir su estado perdería por qué llegó
    # ahí, y no se está sirviendo igual.
    engine, _ = _engine(rowcount=0)
    assert quarantine(engine, "raw.t") is False


def test_a_database_error_does_not_break_the_sweep():
    engine, _ = _engine(raises=True)
    assert quarantine(engine, "raw.t") is False


# ── devolver ───────────────────────────────────────────────────


def test_a_repaired_table_goes_back_into_service():
    engine, ctx = _engine(rowcount=1)
    assert release(engine, "raw.t") is True
    sql = str(ctx.execute.call_args[0][0])
    assert "= 'ready'" in sql


def test_release_only_undoes_what_quarantine_did():
    # Un recurso que llegó a `materialization_corrupted` por otra vía conserva su
    # estado hasta que se satisfaga lo que lo puso ahí.
    engine, ctx = _engine(rowcount=1)
    release(engine, "raw.t")
    assert "materialization_corrupted" in str(ctx.execute.call_args[0][0])


def test_releasing_something_that_was_not_withheld_changes_nothing():
    engine, _ = _engine(rowcount=0)
    assert release(engine, "raw.t") is False


def test_a_database_error_on_release_is_swallowed():
    engine, _ = _engine(raises=True)
    assert release(engine, "raw.t") is False
