"""Guardrail against reporting a total that silently dropped rows.

Prod, 2026-07-26: the chat produced a confident ranking of budget transfers
that was wrong — a small university above UBA, a total orders of magnitude
too low. The generated SQL carried `WHERE credito_devengado ~ '^\\-?\\d+\\.?\\d*$'`,
which excludes every comma-decimal row from the SUM. Nothing caught it:
`_classify_result_kind` called it an "aggregate", `ground_citations` verified
the number (it *was* in the returned rows), and confidence came out 0.9.

For a fact-checking platform a confident wrong answer is the worst outcome,
so the guardrail refuses the number rather than hedging it.
"""

from __future__ import annotations

from types import SimpleNamespace

from app.application.pipeline.citation_guard import _quality_ceiling, ground_citations
from app.application.pipeline.context_builder import (
    _COVERAGE_WARNING_HEAD,
    _coverage_warning_note,
    build_data_context,
)
from app.application.pipeline.subgraphs.nl2sql import find_lossy_numeric_filters

# The exact query shape that produced the wrong prod answer.
_BAD_SQL = (
    "SELECT jurisdiccion_desc, SUM(CAST(credito_devengado AS NUMERIC)) AS total "
    "FROM mart.presupuesto_nacional_ejecutado "
    "WHERE credito_devengado IS NOT NULL "
    "AND credito_devengado ~ '^\\-?\\d+\\.?\\d*$' "
    "GROUP BY jurisdiccion_desc ORDER BY total DESC LIMIT 10"
)

# Shape-branching normalisation, the form the prompt now teaches. The earlier
# fixture here used `replace(replace(col,'.',''),',','.')`, which this test
# labelled "good" — it is not: stripping every dot multiplies by 100 any row
# where the dot is the decimal separator (measured 2026-07-31 at 75% of the rows
# in mart.caba_departamentos_en_venta). It is not *lossy* in the sense this
# detector looks for, which is why the test kept passing; it is simply wrong,
# and a fixture named _GOOD_SQL teaches whoever reads it next.
_GOOD_SQL = (
    "SELECT jurisdiccion_desc, SUM(CASE "
    "WHEN credito_devengado ~ '^\\s*-?[0-9]{1,3}(\\.[0-9]{3})+,[0-9]+\\s*$' "
    "THEN replace(replace(btrim(credito_devengado),'.',''),',','.') "
    "WHEN credito_devengado ~ '^\\s*-?[0-9]+,[0-9]+\\s*$' "
    "THEN replace(btrim(credito_devengado),',','.') "
    "WHEN credito_devengado ~ '^\\s*-?[0-9]{1,3}(\\.[0-9]{3})+\\s*$' "
    "THEN replace(btrim(credito_devengado),'.','') "
    "WHEN credito_devengado ~ '^\\s*-?[0-9]+(\\.[0-9]+)?\\s*$' "
    "THEN btrim(credito_devengado) "
    "END::numeric) AS total "
    "FROM mart.presupuesto_nacional_ejecutado "
    "GROUP BY jurisdiccion_desc ORDER BY total DESC LIMIT 10"
)


class TestFindLossyNumericFilters:
    def test_detects_the_prod_failure(self) -> None:
        found = find_lossy_numeric_filters(_BAD_SQL)
        assert found, "the shape guard that broke the budget ranking must be detected"
        assert found[0][0] == "credito_devengado"

    def test_normalizing_query_is_clean(self) -> None:
        assert find_lossy_numeric_filters(_GOOD_SQL) == []

    def test_shape_filter_without_aggregate_is_allowed(self) -> None:
        """Narrowing a plain listing is a legitimate filter, not silent loss."""
        sql = "SELECT anio, monto FROM t WHERE monto ~ '^[0-9]+$' LIMIT 50"
        assert find_lossy_numeric_filters(sql) == []

    def test_empty_sql(self) -> None:
        assert find_lossy_numeric_filters("") == []


class TestCoverageNote:
    def test_measured_note_states_the_number(self) -> None:
        note = _coverage_warning_note(
            {
                "columns": ["credito_devengado"],
                "measured": True,
                "total_rows": 1000,
                "excluded_rows": 210,
                "excluded_pct": 21.0,
            }
        )
        assert _COVERAGE_WARNING_HEAD in note
        assert "210 de 1000" in note
        assert "21.0 %" in note
        # It must forbid the ranking, not merely caveat it.
        assert "NO los presentes como total, ranking ni porcentaje" in note

    def test_unmeasured_note_still_refuses(self) -> None:
        note = _coverage_warning_note({"columns": ["monto"], "measured": False})
        assert _COVERAGE_WARNING_HEAD in note
        assert "indeterminado" in note

    def test_note_reaches_the_analyst_context(self) -> None:
        result = SimpleNamespace(
            records=[{"jurisdiccion": "X", "total": 123}],
            source="sandbox:nl2sql",
            metadata={
                "coverage_warning": {
                    "columns": ["credito_devengado"],
                    "measured": True,
                    "total_rows": 100,
                    "excluded_rows": 40,
                    "excluded_pct": 40.0,
                },
                "total_records": 1,
            },
            dataset_title="t",
            portal_name="p",
            portal_url="u",
            format="json",
        )
        context = build_data_context([result])
        assert _COVERAGE_WARNING_HEAD in context


class TestQualityCeiling:
    @staticmethod
    def _result(metadata: dict):
        return SimpleNamespace(
            records=[{"a": 1}],
            source="sandbox:nl2sql",
            metadata=metadata,
            dataset_title="t",
            portal_name="p",
            portal_url="u",
            format="json",
        )

    def test_coverage_warning_caps_hard(self) -> None:
        assert _quality_ceiling([self._result({"coverage_warning": {"measured": True}})]) == 0.35

    def test_clean_result_is_uncapped(self) -> None:
        assert _quality_ceiling([self._result({"result_kind": "aggregate"})]) == 1.0

    def test_grounded_answer_no_longer_scores_high_when_coverage_failed(self) -> None:
        """The prod bug: a transcribed-correctly number from a broken SQL got 0.9."""
        results = [
            self._result(
                {
                    "coverage_warning": {"measured": True, "excluded_rows": 5, "total_rows": 10},
                    "result_kind": "aggregate",
                }
            )
        ]
        _, warnings, confidence = ground_citations(
            "El total es 123.", [{"claim": "El total es 123.", "source": "t"}], results, 1.0
        )
        assert confidence <= 0.35
        assert any("Total incompleto" in w for w in warnings), (
            "the coverage failure must reach the user through `warnings` — "
            "`confidence` is stripped from the API response"
        )
