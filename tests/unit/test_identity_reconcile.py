"""Reconciling identities CKAN regenerated — and what must never be merged.

The whole risk here is fusing two resources that are not the same thing. In
production 113 URL groups carry different titles; those are genuinely distinct
and merging them would destroy the distinction silently.
"""

from __future__ import annotations

from app.infrastructure.celery.tasks.identity_reconcile import _RECONCILE_SQL


def test_the_grouping_requires_url_AND_title():
    """The URL alone is not enough.

    Two real resources can be published from one endpoint that takes
    parameters. Requiring the title to agree is what separates a rename from a
    coincidence — and 113 groups in production are exactly that coincidence.
    """
    sql = str(_RECONCILE_SQL)
    assert "GROUP BY download_url, title" in sql
    assert "d.download_url = a.download_url" in sql
    assert "d.title = a.title" in sql


def test_only_groups_with_more_than_one_identifier_are_touched():
    """A resource with one name has nothing to reconcile, and writing its own id
    into the column would be noise that looks like a finding."""
    assert "HAVING count(DISTINCT source_id) > 1" in str(_RECONCILE_SQL)


def test_the_anchor_is_the_earliest_row_not_the_newest():
    """The point is a name that predates the renaming, so a row arriving under a
    fresh CKAN id can be recognised as something we already hold."""
    sql = str(_RECONCILE_SQL)
    assert "ORDER BY d.download_url, d.title, d.created_at ASC" in sql


def test_it_never_deletes_or_merges():
    """What to do about the 7,201 redundant tables is a decision with an owner.

    This records which rows are the same resource; it does not act on that.
    """
    sql = str(_RECONCILE_SQL).upper()
    assert "DELETE" not in sql
    assert "DROP" not in sql
    # The only write is to the new column.
    assert "SET ORIGINAL_IDENTIFIER" in sql


def test_rows_that_already_carry_the_right_anchor_are_left_alone():
    """Re-running must not churn every row; `IS DISTINCT FROM` makes the task
    idempotent and keeps `updated_at` from moving for no reason."""
    assert "IS DISTINCT FROM" in str(_RECONCILE_SQL)
