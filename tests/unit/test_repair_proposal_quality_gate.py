"""A rename into differently-useless names is not a repair.

`propose_col_n_rename` already refused when the garbage ratio failed to drop,
which catches `col_N` surviving the rename. It did not catch a rename into
names that are *differently* useless, and on staging 2026-07-31 that was most
of what got through: of 240 candidates, 17 produced a "viable" proposal and
roughly three in five of those were worse than doing nothing.

    Cuadro 1.2: Matriz de Utilización…  ->  Periodo: Ańo 2003
    Cuadro 1.2: Matriz de Utilización…  ->  Periodo: Ańo 2003_2
    Cuadro 1.2: Matriz de Utilización…  ->  Periodo: Ańo 2003_3      (x148)

One useless title swapped for another, mojibake included, at the cost of
deleting rows — which the audit log cannot undo, because it stores column names
and not deleted data.

Each check here is one observed failure. With all three in place the gate
accepts 2 of the 240, which is the honest size of what this repair can do
safely; the rest need a human or a different repair.
"""

from __future__ import annotations

import pytest

from app.application.repair.parse_repair import assess_rename_proposal

# What a genuine recovery looks like — measured, then applied on staging.
_REAL = ["Departamento", "1998_Total", "1999_Total", "2000_Total", "2001_Total"]
# A three-level header (year / sex / value) folded into compound names.
_MULTINIVEL = [
    "Grupo de Edad (en años)",
    "Tipo de cobertura de salud",
    "Año_2023_Sexo_TOTAL",
    "Año_2023_Sexo_Mujer",
    "Año_2023_Sexo_Varón",
]


class TestRepeatedTitle:
    def test_one_title_repeated_with_suffixes_is_rejected(self) -> None:
        cols = ["col_0"] + [f"Periodo: Anio 2003_{i}" for i in range(2, 30)]
        assert assess_rename_proposal(cols, rows_to_delete=2, total_rows=500) == (
            "proposal_is_one_repeated_title"
        )

    def test_truncation_does_not_hide_the_repetition(self) -> None:
        """Postgres cuts identifiers at 63 bytes, so `_2` shifts the cut.

        `…Proyecciones de Pob` and `…Proyecciones de P_2` are the same title
        truncated at different points. Comparing whole bases saw them as
        distinct and let the proposal through; comparing a bounded prefix does
        not.
        """
        base = "Provincia de Cordoba segun departamentos. Proyecciones de Pob"
        cols = [base] + [f"{base[: -len(str(i)) - 1]}_{i}" for i in range(2, 25)]
        assert assess_rename_proposal(cols, rows_to_delete=2, total_rows=500) == (
            "proposal_is_one_repeated_title"
        )

    def test_distinct_names_sharing_a_short_head_are_kept(self) -> None:
        """`1998_Total` / `1999_Total` diverge early and must survive."""
        assert assess_rename_proposal(_REAL, rows_to_delete=3, total_rows=67) is None

    def test_a_multilevel_header_is_kept(self) -> None:
        assert assess_rename_proposal(_MULTINIVEL, rows_to_delete=3, total_rows=39) is None


class TestDecodingArtefacts:
    """The recovered header comes out of the same badly-decoded file.

    A name can be "recovered" into something worse than what it replaced.
    """

    @pytest.mark.parametrize(
        "bad",
        [
            "Periodo: Ańo 2003",
            "Periodo: AÒo 2003",
            "Cuadro 1.3: Matriz de Utilizaci—n",
            "Indicadores socioeconůmicos",
            "Matriz de C¾rdoba a precios bßsicos",
            "2016_1¤ Sem._%",
        ],
    )
    def test_mojibake_is_rejected(self, bad: str) -> None:
        cols = ["Departamento", bad, "otra_columna"]
        assert assess_rename_proposal(cols, rows_to_delete=1, total_rows=500) == (
            "proposal_has_decoding_artefacts"
        )

    def test_real_spanish_accents_are_not_mojibake(self) -> None:
        """Rejecting `Año` or `Córdoba` would reject most of the corpus."""
        cols = ["Año", "Córdoba", "Población", "Educación", "¿Cuántos?", "Nº", "1º Sem."]
        assert assess_rename_proposal(cols, rows_to_delete=1, total_rows=500) is None


class TestHeaderRowCost:
    """Deleting header rows is the one part of the repair that cannot be undone.

    `parse_repair_audit` stores old and new column names, so a rename is
    recoverable by hand. The deleted rows are not stored anywhere.
    """

    def test_eating_a_fifth_of_a_small_table_is_rejected(self) -> None:
        """`caba__fecundidad`: 9 rows, proposal consumed 2 — one of them data."""
        assert assess_rename_proposal(_REAL, rows_to_delete=2, total_rows=9) == (
            "header_cost_too_high:2/9"
        )

    def test_the_same_rows_are_free_on_a_large_table(self) -> None:
        assert assess_rename_proposal(_REAL, rows_to_delete=3, total_rows=100_000) is None

    def test_an_unknown_row_count_skips_the_check(self) -> None:
        """Callers without a count must not be blocked by a cost they can't see."""
        assert assess_rename_proposal(_REAL, rows_to_delete=99, total_rows=None) is None

    def test_deleting_nothing_is_always_fine(self) -> None:
        assert assess_rename_proposal(_REAL, rows_to_delete=0, total_rows=3) is None


class TestTheGateIsWiredIntoTheProposal:
    def test_propose_col_n_rename_consults_it(self) -> None:
        """A gate only the bulk driver called would be bypassed by the admin
        endpoint, which is how single-table repairs actually get run today."""
        from pathlib import Path

        source = (
            Path(__file__).resolve().parents[2]
            / "src"
            / "app"
            / "application"
            / "repair"
            / "parse_repair.py"
        ).read_text(encoding="utf-8")
        body = source.split("def propose_col_n_rename(", 1)[1].split("\ndef ", 1)[0]
        assert "assess_rename_proposal(" in body
