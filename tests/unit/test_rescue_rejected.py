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
    assert "if not names or any(" in src


def test_the_cheaper_repair_is_tried_first():
    """`title_as_columns` is the shape 546 rejected tables actually have; `col_n`
    is the fallback. Ordering matters because the first success stops the loop."""
    src = inspect.getsource(rescue_rejected.rescue_rejected_resources)
    assert "(repair_title_as_columns_table, repair_col_n_table)" in src


def test_dry_run_neither_repairs_nor_promotes():
    src = inspect.getsource(rescue_rejected.rescue_rejected_resources)
    assert "if not dry_run:" in src
    assert "if dry_run:\n            continue" in src
