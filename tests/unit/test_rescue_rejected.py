"""Repair the refused table, then ask whether it is actually usable.

`parse_repair.py` has held fixes for these shapes since May and applied them
thousands of times — always because a person asked. The repair and the
resource's status never met: a table could be fixed and the resource would stay
`error` forever, because nothing looked again. 546 sat like that.
"""

from __future__ import annotations

import inspect

from app.infrastructure.celery.tasks import rescue_rejected
from app.infrastructure.celery.tasks.rescue_rejected import _REJECTED_SQL


def test_it_only_takes_resources_whose_table_still_exists():
    """The repair operates on a table. Without one there is nothing to fix and
    the resource needs collecting, not rescuing."""
    sql = str(_REJECTED_SQL)
    assert "JOIN information_schema.tables t" in sql
    assert "t.table_type = 'BASE TABLE'" in sql


def test_it_targets_the_validator_verdict_it_can_answer():
    assert "placeholder" in str(_REJECTED_SQL)


def test_promotion_is_gated_on_the_column_names_not_on_having_tried():
    """A repair that ran and changed nothing must leave the resource rejected.

    Flipping it to `ready` on the strength of having tried would serve `col_3`
    to someone asking about poverty — worse than serving nothing and saying so.
    """
    src = inspect.getsource(rescue_rejected.rescue_rejected_resources)
    # The check reads the table, not the repair's report of itself.
    assert "_column_names(conn, schema, table)" in src
    assert "any(is_garbage_column(n) for n in names)" in src
    # And the refusal comes before any promotion.
    check_at = src.index("still_unusable")
    promote_at = src.index("SET status = 'ready'")
    assert check_at < promote_at


def test_an_empty_column_list_is_not_clean():
    """A table we cannot read the columns of is not a table we can promote."""
    src = inspect.getsource(rescue_rejected.rescue_rejected_resources)
    assert "not names" in src
    assert "is_smeared_title(names)" in src


def test_the_cheaper_repair_is_tried_first():
    """`title_as_columns` is the shape 546 rejected tables actually have; `col_n`
    is the fallback. Ordering matters because the first success stops the loop."""
    src = inspect.getsource(rescue_rejected.rescue_rejected_resources)
    assert "(repair_title_as_columns_table, repair_col_n_table)" in src


def test_dry_run_neither_repairs_nor_promotes():
    src = inspect.getsource(rescue_rejected.rescue_rejected_resources)
    assert "if not dry_run:" in src
    assert "if dry_run:\n            continue" in src


def test_a_smeared_title_is_not_clean():
    """The defect `is_garbage_column` does not know, and the one most rejected
    tables actually have.

    The first production run promoted a table whose columns were
    `['Conformación Cartográfica de Localidades Censales 2008 por De',
      '... por _2', '... por _3', ...]` — precisely what the gate exists to
    refuse. A gate that passes what it was built to stop launders the result.
    """
    from app.application.pipeline.parsers.column_normalization import is_smeared_title

    smeared = [
        "Conformación Cartográfica de Localidades Censales 2008 por De",
        "Conformación Cartográfica de Localidades Censales 2008 por _2",
        "Conformación Cartográfica de Localidades Censales 2008 por _3",
    ]
    assert is_smeared_title(smeared)


def test_real_column_names_are_not_mistaken_for_a_smeared_title():
    from app.application.pipeline.parsers.column_normalization import is_smeared_title

    assert not is_smeared_title(["Apellido", "Nombre", "Cargo", "Tipo"])
    assert not is_smeared_title(["Departamento", "Población", "Nacidos vivos"])
    # A legitimately repeated short name is odd, not this defect.
    assert not is_smeared_title(["valor", "valor_2", "valor_3"])
    # And real names that share a short prefix must survive.
    assert not is_smeared_title(["fecha_inicio", "fecha_fin", "fecha_alta"])


def test_the_detector_ignores_column_count():
    """`repair_title_as_columns_table` needs thirty columns before it acts. A
    six-column table with the same defect is just as unusable."""
    from app.application.pipeline.parsers.column_normalization import is_smeared_title

    tres = [
        "Superficie sembrada por departamento y campaña agrícola",
        "Superficie sembrada por departamento y campaña agrícola_2",
        "Superficie sembrada por departamento y campaña agrícola_3",
    ]
    assert is_smeared_title(tres)
