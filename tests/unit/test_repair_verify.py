"""Tests for the repair verifier.

What matters is what it refuses. A verifier that accepts is doing nothing a
caller could not have done; the value is entirely in the proposals it stops,
and the case it exists for is the plausible one that is wrong.
"""

from __future__ import annotations

from app.application.catalog.schema_snapshot import ColumnProfile, TableSnapshot
from app.application.repair.verify import (
    verify_against_previous_version,
    verify_rename,
)

PROVINCIAS = ["Buenos Aires", "Córdoba", "Santa Fe", "Mendoza", "Salta"]
MONTOS = ["1200.5", "980.0", "34000.75", "12.4", "7788.1"]
FECHAS = ["2021-01-01", "2021-02-01", "2021-03-01", "2021-04-01", "2021-05-01"]


def _col(name, ordinal=1, mcv=None, pg_type="text"):
    return ColumnProfile(
        name=name,
        ordinal=ordinal,
        pg_type=pg_type,
        null_frac=0.0,
        n_distinct=-0.5,
        most_common_vals=list(mcv or []),
        histogram_sample=[],
    )


def _snap(cols, version=1, table="t"):
    return TableSnapshot(
        schema_name="raw",
        table_name=table,
        columns=cols,
        row_count_estimate=100,
        stats_available=True,
        resource_identity="portal::x",
        version=version,
    )


# ── acepta lo que corresponde ──────────────────────────────────


def test_it_accepts_a_rename_that_restores_the_reference_names():
    """The `col_n` case: the values never moved, only the labels were lost."""
    reference = _snap([_col("provincia", 1, PROVINCIAS), _col("monto", 2, MONTOS)])
    current = _snap([_col("col_0", 1, PROVINCIAS), _col("col_1", 2, MONTOS)], version=2)

    result = verify_rename(
        current=current, proposed_names=["provincia", "monto"], reference=reference
    )

    assert result.accepted
    assert result.reason == "improves_alignment"
    assert result.improvement > 0.5


# ── se niega, que es donde está el valor ───────────────────────


def test_it_refuses_names_the_reference_never_used():
    """A proposal that invents plausible labels scores zero, not generously.
    This is the LLM tier's failure mode and the reason it is gated."""
    reference = _snap([_col("provincia", 1, PROVINCIAS), _col("monto", 2, MONTOS)])
    current = _snap([_col("col_0", 1, PROVINCIAS), _col("col_1", 2, MONTOS)], version=2)

    result = verify_rename(
        current=current,
        proposed_names=["jurisdiccion_administrativa", "importe_total"],
        reference=reference,
    )

    assert not result.accepted
    assert result.reason == "repaired_table_still_does_not_match_the_reference"


def test_it_refuses_a_proposal_that_puts_the_right_names_in_the_wrong_places():
    """Names drawn from the reference, mapped to the wrong columns. Every name
    exists, so a check on names alone would pass it; the values are what catch
    it."""
    reference = _snap([_col("provincia", 1, PROVINCIAS), _col("monto", 2, MONTOS)])
    current = _snap([_col("col_0", 1, PROVINCIAS), _col("col_1", 2, MONTOS)], version=2)

    result = verify_rename(
        current=current, proposed_names=["monto", "provincia"], reference=reference
    )

    assert not result.accepted


def test_it_refuses_to_trade_a_column_that_was_already_right():
    """A net average gain that breaks a correct column is not an improvement
    worth taking unattended. The average would have hidden it."""
    reference = _snap(
        [_col("provincia", 1, PROVINCIAS), _col("monto", 2, MONTOS), _col("fecha", 3, FECHAS)]
    )
    current = _snap(
        [_col("provincia", 1, PROVINCIAS), _col("col_1", 2, MONTOS), _col("col_2", 3, FECHAS)],
        version=2,
    )

    result = verify_rename(
        # Fixes two, breaks the one that was already correct.
        current=current,
        proposed_names=["fecha", "monto", "provincia"],
        reference=reference,
    )

    assert not result.accepted
    assert "regress" in result.reason


def test_it_refuses_a_proposal_that_does_not_cover_the_table():
    reference = _snap([_col("provincia", 1, PROVINCIAS)])
    current = _snap([_col("col_0", 1, PROVINCIAS), _col("col_1", 2, MONTOS)], version=2)

    result = verify_rename(current=current, proposed_names=["provincia"], reference=reference)

    assert not result.accepted
    assert result.reason == "proposal_does_not_cover_the_table"


def test_it_refuses_when_the_reference_carries_no_values():
    """`profile_similarity` returns 0.0 without evidence, so a verdict built on
    such a reference would be arithmetic rather than evidence."""
    reference = _snap([_col("provincia", 1, []), _col("monto", 2, [])])
    current = _snap([_col("col_0", 1, PROVINCIAS), _col("col_1", 2, MONTOS)], version=2)

    result = verify_rename(
        current=current, proposed_names=["provincia", "monto"], reference=reference
    )

    assert not result.accepted
    assert result.reason == "reference_has_no_identifiable_columns"


def test_a_rename_that_changes_nothing_is_not_an_improvement():
    reference = _snap([_col("provincia", 1, PROVINCIAS)])
    current = _snap([_col("provincia", 1, PROVINCIAS)], version=2)

    result = verify_rename(current=current, proposed_names=["provincia"], reference=reference)

    assert not result.accepted
    assert result.reason == "no_demonstrated_improvement"


# ── elección de referencia ─────────────────────────────────────


def test_the_earliest_usable_version_is_chosen_as_reference():
    """The PAMI case: v1 was correct and v2 carried a title row promoted to
    headers. A verifier pointed at v2 would have confirmed the damage."""
    v1 = _snap([_col("destino", 1, PROVINCIAS), _col("monto", 2, MONTOS)], version=1)
    v2 = _snap(
        [_col("LISTADO / G_1", 1, PROVINCIAS), _col("LISTADO / G_2", 2, MONTOS)], version=2
    )
    current = _snap([_col("col_0", 1, PROVINCIAS), _col("col_1", 2, MONTOS)], version=3)

    result = verify_against_previous_version(
        current=current, proposed_names=["destino", "monto"], candidates=[v2, v1]
    )

    assert result.accepted, "should have verified against v1, the clean version"


def test_no_usable_reference_is_a_refusal_not_a_guess():
    current = _snap([_col("col_0", 1, PROVINCIAS)], version=2)
    empty = _snap([_col("provincia", 1, [])], version=1)

    result = verify_against_previous_version(
        current=current, proposed_names=["provincia"], candidates=[empty]
    )

    assert not result.accepted
    assert result.reason == "no_earlier_snapshot_can_serve_as_a_reference"


def test_the_outcome_carries_the_evidence_for_its_answer():
    """An operator reading a refusal has to be able to see why without rerunning
    it."""
    reference = _snap([_col("provincia", 1, PROVINCIAS), _col("monto", 2, MONTOS)])
    current = _snap([_col("col_0", 1, PROVINCIAS), _col("col_1", 2, MONTOS)], version=2)

    result = verify_rename(
        current=current, proposed_names=["provincia", "monto"], reference=reference
    )
    log = result.as_log_dict()

    assert log["columns_total"] == 2
    assert log["columns_improved"] == 2
    assert log["improvement"] > 0
    assert result.columns[0].current_name == "col_0"
    assert result.columns[0].proposed_name == "provincia"


# ── verificación sin referencia ────────────────────────────────


def test_intrinsic_accepts_recovering_real_names_from_placeholders():
    """The `col_N` case: the heuristic found the buried header row."""
    from app.application.repair.verify import verify_intrinsic

    result = verify_intrinsic(
        current_names=["col_0", "col_1", "col_2"],
        proposed_names=["provincia", "monto", "anio"],
    )

    assert result.accepted
    assert result.reason == "removes_all_garbage_names"


def test_intrinsic_refuses_to_touch_a_healthy_table():
    """Renaming a table that was fine is how a repair sweep becomes damage.
    It runs unattended over 1,118 tables, so this is the guard that matters."""
    from app.application.repair.verify import verify_intrinsic

    result = verify_intrinsic(
        current_names=["provincia", "monto"], proposed_names=["a", "b"]
    )

    assert not result.accepted
    assert result.reason == "nothing_wrong_with_the_current_names"


def test_intrinsic_refuses_a_proposal_that_still_contains_placeholders():
    """Inventing `col_9` to replace `Unnamed: 1` is motion, not repair. The bar
    is none left, not fewer."""
    from app.application.repair.verify import verify_intrinsic

    result = verify_intrinsic(
        current_names=["col_0", "col_1"], proposed_names=["provincia", "col_9"]
    )

    assert not result.accepted
    assert result.reason == "proposal_still_contains_garbage_names"


def test_intrinsic_refuses_a_proposal_that_collapses_two_columns():
    """Two columns sharing a name lose a column's worth of meaning, and the
    rename would fail or shadow one of them."""
    from app.application.repair.verify import verify_intrinsic

    result = verify_intrinsic(
        current_names=["col_0", "col_1"], proposed_names=["provincia", "provincia"]
    )

    assert not result.accepted
    assert "collapses" in result.reason


def test_intrinsic_refuses_a_proposal_that_does_not_cover_the_table():
    from app.application.repair.verify import verify_intrinsic

    result = verify_intrinsic(current_names=["col_0", "col_1"], proposed_names=["x"])

    assert not result.accepted
    assert result.reason == "proposal_does_not_cover_the_table"


def test_intrinsic_needs_no_reference_which_is_the_whole_point():
    """`verify_rename` compares against a snapshot held to be correct. Measured
    on production: of 1,118 tables carrying these defects, 26 have another
    version of the resource and none have a second snapshot. There is no correct
    past to compare with, so the question has to be answerable from the names
    alone."""
    import inspect

    from app.application.repair.verify import verify_intrinsic

    params = set(inspect.signature(verify_intrinsic).parameters)
    assert params == {"current_names", "proposed_names"}


def test_intrinsic_refuses_to_rename_the_collectors_own_columns():
    """`_source_dataset_id` is how a table links back to its dataset.

    The model tier's first production dry run proposed renaming all five
    `_source_*` / `_parser_version` columns to `metadata_<i>` — the prompt tells
    it to do exactly that for columns with no semantic value, and from the
    values alone they look like precisely that. Applying it would have cut every
    repaired table loose from its origin.

    The proposer holds them out now, and this refuses the proposal outright. A
    second line of defence on a data-integrity invariant is worth more than the
    duplication costs.
    """
    from app.application.repair.verify import verify_intrinsic

    result = verify_intrinsic(
        current_names=["col_0", "_source_dataset_id", "_source_url"],
        proposed_names=["tipo_caso", "metadata_0", "metadata_1"],
    )

    assert not result.accepted
    assert result.reason.startswith("proposal_renames_collector_columns")
    assert "_source_dataset_id" in result.reason


def test_intrinsic_allows_a_repair_that_leaves_the_internal_columns_alone():
    from app.application.repair.verify import verify_intrinsic

    result = verify_intrinsic(
        current_names=["col_0", "_source_dataset_id"],
        proposed_names=["tipo_caso", "_source_dataset_id"],
    )

    assert result.accepted
