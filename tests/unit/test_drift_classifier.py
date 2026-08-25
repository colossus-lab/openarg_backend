"""Tests for the exoneration cascade.

The property under test is not "does it detect drift" — it is the inverse:
**every gate must be able to clear a diff, and none may ever accuse one.** A
test that asserts the classifier called something drift is testing the wrong
thing; what matters is that nothing survives the cascade unless no gate could
explain it away.
"""

from __future__ import annotations

import pytest

from app.application.catalog.schema_snapshot import (
    ColumnProfile,
    Provenance,
    TableSnapshot,
    is_identifiable,
)
from app.application.drift import (
    ChangeClass,
    DriftContext,
    Verdict,
    classify_change,
    summarize,
)

# Five values so the column clears the identifiability floor of four.
PROVINCIAS = ["Buenos Aires", "Córdoba", "Santa Fe", "Mendoza", "Salta"]
MONTOS = ["1000", "2500", "3700", "4200", "5900"]


def _col(name, ordinal=1, pg_type="text", *, mcv=None, null_frac=0.0):
    return ColumnProfile(
        name=name,
        ordinal=ordinal,
        pg_type=pg_type,
        null_frac=null_frac,
        most_common_vals=list(mcv or []),
    )


def _snap(columns, *, prov=None):
    return TableSnapshot(
        schema_name="raw",
        table_name="t",
        columns=columns,
        row_count_estimate=1000,
        stats_available=True,
        provenance=prov or Provenance(parser_version="phase4-v1", layout_profile="simple_tabular"),
    )


# ── G1 · procedencia ───────────────────────────────────────────


def test_g1_exonerates_when_our_parser_changed():
    """The most productive gate.

    Our parser rewrites schemas for reasons that have nothing to do with the
    portal — ~9.500 `parse_repair` renames, five schema-rewriting transforms.
    Attributing those to the source is the single largest source of noise.
    """
    before = _snap([_col("col_1", mcv=PROVINCIAS)], prov=Provenance(parser_version="phase3-v2"))
    after = _snap([_col("provincia", mcv=PROVINCIAS)], prov=Provenance(parser_version="phase4-v1"))

    result = classify_change(before, after)

    assert result.verdict is Verdict.EXONERATED
    assert result.exonerated_by == "G1_provenance"
    assert not result.is_actionable


def test_g1_does_not_exonerate_when_the_parser_is_the_same():
    before = _snap([_col("provincia", mcv=PROVINCIAS)])
    after = _snap([_col("jurisdiccion_nueva", mcv=MONTOS)])

    result = classify_change(before, after)

    assert result.verdict is Verdict.UNEXPLAINED


def test_g1_does_not_exonerate_on_unknown_provenance():
    """Absence of evidence must not clear a diff.

    Most of production predates provenance tracking. Treating `None` as "same
    parser" would exonerate nearly everything and quietly disable the cascade.

    It reports UNATTRIBUTABLE rather than UNEXPLAINED, which is the sharper
    claim: we do not know whose change this was, which calls for recording
    provenance — not for adapting the parser to a portal that may not have
    moved.
    """
    before = _snap([_col("a", mcv=PROVINCIAS)], prov=Provenance())
    after = _snap([_col("b", mcv=MONTOS)], prov=Provenance(parser_version="phase4-v1"))

    verdict = classify_change(before, after)
    assert verdict.verdict is Verdict.UNATTRIBUTABLE
    assert not verdict.is_actionable
    assert "G1_provenance" in verdict.gates_not_evaluated


def test_a_placeholder_is_not_provenance():
    """`legacy:unknown` and a bare date look like values and carry nothing.
    Treating them as real is what let the cascade report our own regressions as
    upstream drift — 5 of 5 on 2026-08-21."""
    for placeholder in ("legacy:unknown", "2026-05-04"):
        before = _snap([_col("a", mcv=PROVINCIAS)], prov=Provenance(parser_version=placeholder))
        after = _snap([_col("b", mcv=MONTOS)], prov=Provenance(parser_version=placeholder))

        assert classify_change(before, after).verdict is Verdict.UNATTRIBUTABLE, placeholder


def test_two_real_fingerprints_that_match_leave_the_change_unexplained():
    """When provenance is real on both sides and identical, the parser did not
    move — so the change is genuinely not ours, and the cascade may say so."""
    before = _snap([_col("a", mcv=PROVINCIAS)], prov=Provenance(parser_version="p:ac538ee9d1a7"))
    after = _snap(
        [_col("a", mcv=PROVINCIAS), _col("nueva", 2, mcv=MONTOS)],
        prov=Provenance(parser_version="p:ac538ee9d1a7"),
    )

    verdict = classify_change(before, after)
    assert verdict.verdict is Verdict.UNEXPLAINED
    assert verdict.is_actionable


# ── G3 · comportamiento del pipeline ───────────────────────────


def test_g3_exonerates_when_the_parse_took_a_different_path():
    """`unpivot_if_time_pivoted` fires at ≥50 % time columns, so one extra year
    of data flips a wide table to long with no upstream change at all."""
    before = _snap(
        [_col("etiqueta", mcv=PROVINCIAS), _col("2023", 2, mcv=MONTOS)],
        prov=Provenance(parser_version="phase4-v1", layout_profile="simple_tabular"),
    )
    after = _snap(
        [
            _col("etiqueta", mcv=PROVINCIAS),
            _col("periodo", 2, mcv=["2023", "2024", "2025", "2026"]),
        ],
        prov=Provenance(parser_version="phase4-v1", layout_profile="wide_csv"),
    )

    result = classify_change(before, after)

    assert result.verdict is Verdict.EXONERATED
    assert result.exonerated_by == "G3_pipeline"


def test_g3_exonerates_on_truncation():
    before = _snap(
        [_col("a", mcv=PROVINCIAS)],
        prov=Provenance(parser_version="phase4-v1", is_truncated=False),
    )
    after = _snap(
        [_col("a", mcv=PROVINCIAS), _col("b", 2, mcv=MONTOS)],
        prov=Provenance(parser_version="phase4-v1", is_truncated=True),
    )

    assert classify_change(before, after).exonerated_by == "G3_pipeline"


# ── G0 y G2 · lo que el snapshot no sabe ───────────────────────


def test_g0_exonerates_a_diff_between_different_resources():
    """5.485 URLs en producción están registradas bajo más de un `source_id`
    tras la migración a CKAN 2.11."""
    before = _snap([_col("a", mcv=PROVINCIAS)])
    after = _snap([_col("b", mcv=MONTOS)])

    result = classify_change(before, after, DriftContext(same_identity=False))

    assert result.exonerated_by == "G0_identity"


def test_g2_exonerates_siblings():
    """De 78 familias multi-variante medidas, 29 nacieron el mismo día."""
    before = _snap([_col("a", mcv=PROVINCIAS)])
    after = _snap([_col("b", mcv=MONTOS)])

    result = classify_change(before, after, DriftContext(same_source_url=False))

    assert result.exonerated_by == "G2_sibling"


def test_gates_without_input_are_reported_not_assumed():
    """A gate that cannot run must say so.

    Waving a case through silently is how a cascade stops being trustworthy —
    the caller has to be able to see which checks did not happen.
    """
    result = classify_change(_snap([_col("a", mcv=PROVINCIAS)]), _snap([_col("b", mcv=MONTOS)]))

    assert set(result.gates_not_evaluated) == {"G0_identity", "G2_sibling"}


# ── clasificación de lo que sobrevive ──────────────────────────


def test_no_change_is_reported_as_such():
    snap = _snap([_col("a", mcv=PROVINCIAS)])
    result = classify_change(snap, _snap([_col("a", mcv=PROVINCIAS)]))
    assert result.verdict is Verdict.NO_CHANGE
    assert not result.is_actionable


@pytest.mark.parametrize(
    ("before_cols", "after_cols", "expected"),
    [
        (
            [_col("a", mcv=PROVINCIAS)],
            [_col("a", mcv=PROVINCIAS), _col("b", 2, mcv=MONTOS)],
            ChangeClass.ADDITIVE,
        ),
        (
            [_col("a", mcv=PROVINCIAS), _col("b", 2, mcv=MONTOS)],
            [_col("a", mcv=PROVINCIAS)],
            ChangeClass.REMOVAL,
        ),
        (
            [_col("monto", mcv=MONTOS, pg_type="text")],
            [_col("monto", mcv=MONTOS, pg_type="numeric")],
            ChangeClass.TYPE_CHANGE,
        ),
        (
            [_col("provincia", mcv=PROVINCIAS)],
            [_col("jurisdiccion", mcv=PROVINCIAS)],
            ChangeClass.RENAME,
        ),
    ],
)
def test_change_classes(before_cols, after_cols, expected):
    result = classify_change(_snap(before_cols), _snap(after_cols))
    assert result.verdict is Verdict.UNEXPLAINED
    assert result.change_class is expected


def test_unrelated_add_and_remove_is_a_reshape_not_a_rename():
    """Both directions at once with no confident match between them means the
    table was restructured, which calls for different handling than an edit."""
    result = classify_change(
        _snap([_col("provincia", mcv=PROVINCIAS)]),
        _snap([_col("importe", mcv=MONTOS)]),
    )
    assert result.change_class is ChangeClass.RESHAPE


# ── G5 · suficiencia ───────────────────────────────────────────


def test_g5_rejects_a_low_cardinality_column():
    """`sexo` (M/F) looks like every other two-valued column."""
    assert is_identifiable(_col("sexo", mcv=["M", "F"])) is False


def test_g5_rejects_a_column_whose_values_are_all_empty_markers():
    """Extremely common in Argentine public data — the production `pg_stats`
    is full of columns whose top values are `""` and `-`."""
    assert is_identifiable(_col("obs", mcv=["", "-", "s/d", "N/A", "null"])) is False


def test_g5_rejects_a_mostly_null_column():
    assert is_identifiable(_col("x", mcv=PROVINCIAS, null_frac=0.99)) is False


def test_g5_accepts_a_column_with_real_values():
    assert is_identifiable(_col("provincia", mcv=PROVINCIAS)) is True


def test_g5_keeps_unidentifiable_columns_out_of_rename_matching():
    """The column still shows as removed — we just refuse to guess what it
    became, which is the difference between silence and a wrong answer."""
    result = classify_change(
        _snap([_col("activo", mcv=["S", "N"])]),
        _snap([_col("habilitado", mcv=["S", "N"])]),
    )
    assert result.diff["renamed_candidates"] == []
    assert result.diff["removed"] == ["activo"]


# ── G6 · unicidad ──────────────────────────────────────────────


def test_g6_refuses_an_ambiguous_rename():
    """The case the gate exists for.

    `provincia_origen` and `provincia_destino` share the same domain, so both
    score ~1.0 against a removed `provincia`. A high score is not evidence when
    several candidates share it.
    """
    result = classify_change(
        _snap([_col("provincia", mcv=PROVINCIAS)]),
        _snap(
            [_col("provincia_origen", mcv=PROVINCIAS), _col("provincia_destino", 2, mcv=PROVINCIAS)]
        ),
    )

    assert result.diff["renamed_candidates"] == []
    assert len(result.diff["ambiguous_renames"]) == 1
    ambiguous = result.diff["ambiguous_renames"][0]
    assert ambiguous["from"] == "provincia"
    assert {c["to"] for c in ambiguous["candidates"]} == {
        "provincia_origen",
        "provincia_destino",
    }


def test_g6_allows_an_unambiguous_rename():
    result = classify_change(
        _snap([_col("provincia", mcv=PROVINCIAS)]),
        _snap([_col("jurisdiccion", mcv=PROVINCIAS), _col("importe", 2, mcv=MONTOS)]),
    )
    assert [r["from"] for r in result.diff["renamed_candidates"]] == ["provincia"]
    assert result.diff["renamed_candidates"][0]["to"] == "jurisdiccion"


# ── agregación ─────────────────────────────────────────────────


def test_summarize_breaks_down_by_gate():
    """The per-gate count is the point: if G1 clears 90 % of diffs, the noise
    was ours, and that changes what to fix."""
    verdicts = [
        classify_change(
            _snap([_col("a", mcv=PROVINCIAS)], prov=Provenance(parser_version="v1")),
            _snap([_col("b", mcv=MONTOS)], prov=Provenance(parser_version="v2")),
        ),
        classify_change(_snap([_col("a", mcv=PROVINCIAS)]), _snap([_col("a", mcv=PROVINCIAS)])),
        classify_change(
            _snap([_col("a", mcv=PROVINCIAS)]),
            _snap([_col("a", mcv=PROVINCIAS), _col("nuevo", 2, mcv=MONTOS)]),
        ),
    ]

    result = summarize(verdicts)

    assert result["evaluated"] == 3
    assert result["actionable"] == 1
    assert result["exonerated_by_gate"] == {"G1_provenance": 1}
    assert result["actionable_by_class"] == {"additive": 1}
