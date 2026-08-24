"""Dropping tables is the most dangerous thing in this codebase.

Every test here is about a condition under which the drop must NOT happen. The
2026-08-03 incident was a sweep doing exactly what it was told against a premise
that had stopped being true, and it cost sixteen days of collection.
"""

from __future__ import annotations

from app.application.catalog.duplicate_cleanup import (
    _NEVER_DROP,
    _SAFE_CANDIDATES_SQL,
)


def test_users_and_conversations_can_never_be_dropped():
    """The standing instruction, written into the code rather than remembered."""
    for name in (
        "users",
        "conversations",
        "conversation_messages",
        "messages",
        "checkpoints",
        "checkpoint_writes",
        "checkpoint_blobs",
        "user_queries",
        "api_keys",
    ):
        assert name in _NEVER_DROP, name


def test_groups_whose_row_counts_differ_are_excluded():
    """Same URL, same title, different content: the file changed between
    collections. 791 groups in production are like this, and dropping either
    side loses something."""
    assert "min(cd.row_count) = max(cd.row_count)" in str(_SAFE_CANDIDATES_SQL)


def test_tables_a_mart_reads_are_excluded():
    """620 candidates are named in a mart's SQL. Dropping them breaks serving."""
    sql = str(_SAFE_CANDIDATES_SQL)
    assert "FROM mart_definitions m" in sql
    assert "NOT EXISTS" in sql


def test_it_never_drops_the_last_copy():
    """The survivor has to exist on disk and hold rows. Dropping the copy of
    something that is not there would leave the resource with nothing."""
    sql = str(_SAFE_CANDIDATES_SQL)
    assert "s.rn = 1 AND s.row_count > 0" in sql
    assert "table_type = 'BASE TABLE'" in sql


def test_only_rows_beyond_the_first_are_candidates():
    assert "WHERE r.rn > 1" in str(_SAFE_CANDIDATES_SQL)


def test_empty_tables_are_not_treated_as_copies():
    """A zero-row table is not evidence that the data lives elsewhere."""
    assert str(_SAFE_CANDIDATES_SQL).count("cd.row_count > 0") >= 2


def test_the_drop_retires_the_registry_row_in_the_same_transaction():
    """A dropped table whose registry row still says live is a phantom, and
    those had to be swept out of production the same morning this was written."""
    import inspect

    from app.application.catalog import duplicate_cleanup

    src = inspect.getsource(duplicate_cleanup.cleanup_duplicate_tables)
    drop_at = src.index("DROP TABLE")
    supersede_at = src.index("SET superseded_at")
    begin_at = src.index("with engine.begin() as conn:")
    # Both inside the same `begin()` block, and the registry retired after the
    # drop rather than in a separate pass that could not run.
    assert begin_at < drop_at < supersede_at


def test_it_rechecks_the_survivor_inside_the_transaction():
    """The candidate list is built earlier; a sweep may have moved since."""
    import inspect

    from app.application.catalog import duplicate_cleanup

    src = inspect.getsource(duplicate_cleanup.cleanup_duplicate_tables)
    assert "survivor_vanished" in src


def test_is_cached_is_left_alone_so_nothing_re_collects_the_duplicate():
    """The dispatcher selects on `is_cached = false`. Flipping it would rebuild
    exactly what this removed — a treadmill rather than a cleanup."""
    import inspect

    from app.application.catalog import duplicate_cleanup

    src = inspect.getsource(duplicate_cleanup.cleanup_duplicate_tables)
    assert "is_cached" not in src
