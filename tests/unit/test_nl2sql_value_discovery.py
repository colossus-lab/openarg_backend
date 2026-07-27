"""A zero-row answer must not be reported as "we don't have the data".

Measured on staging 2026-07-26. Asked how much went to national
universities in 2024, the model wrote a structurally perfect query:

    SELECT SUM(credito_devengado) FROM mart.presupuesto_consolidado
    WHERE anio = 2024 AND programa ILIKE '%universidad%'

and got nothing — the program is filed as "Desarrollo de la Educacion
Superior", which contains no form of the word "universidad". The data was
there (2,59 billones, 4,24 % of the 2024 budget, 5th largest program). The
vocabulary was not. The pipeline answered "los datos disponibles no tienen
registros procesados".

Guessing a search literal that does not exist is a whole class of recall
failure, independent of the table. One discovery pass puts the column's
real values in front of the model before we deflect.

The guard rails matter as much as the retry:
  - the model sees EVERY value or none at all — a truncated list invites
    picking the best of a bad slice;
  - it is told to answer NONE rather than force a doubtful match;
  - when a substitution does happen, it is recorded and surfaced, because
    answering about a different concept than the one asked is exactly the
    kind of quiet reinterpretation this project cannot afford.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.application.pipeline.context_builder import _value_substitution_note
from app.application.pipeline.subgraphs.nl2sql import (
    _MAX_DISCOVERABLE_VALUES,
    _fetch_column_values,
    _is_effectively_empty,
    _route_after_execute,
    _swappable_text_predicates,
    discover_values_node,
)

_FAILED_SQL = (
    "SELECT SUM(credito_devengado) AS total FROM mart.presupuesto_consolidado "
    "WHERE anio = 2024 AND programa ILIKE '%universidad%' "
    "AND credito_devengado IS NOT NULL"
)


def _result(rows, error=None):
    return SimpleNamespace(
        rows=rows,
        row_count=len(rows),
        error=error,
        columns=[],
        truncated=False,
    )


class _Sandbox:
    """Records the SQL it is asked to run and replays canned results."""

    def __init__(self, results):
        self._results = list(results)
        self.executed: list[str] = []

    async def execute_readonly(self, sql, *a, **kw):
        self.executed.append(sql)
        return self._results.pop(0)


class _LLM:
    def __init__(self, reply):
        self._reply = reply
        self.prompts: list[str] = []

    async def chat(self, messages, **kw):
        self.prompts.append(messages[0].content)
        return SimpleNamespace(content=self._reply)


class TestEmptinessDetection:
    def test_zero_rows_is_empty(self) -> None:
        assert _is_effectively_empty(_result([])) is True

    def test_aggregate_of_nulls_is_empty(self) -> None:
        """SUM() over no matching rows yields one row holding NULL."""
        assert _is_effectively_empty(_result([{"total": None}])) is True

    def test_a_real_zero_is_not_empty(self) -> None:
        """0 is an answer; NULL is the absence of one."""
        assert _is_effectively_empty(_result([{"total": 0}])) is False

    def test_rows_with_data_are_not_empty(self) -> None:
        assert _is_effectively_empty(_result([{"programa": "x", "total": 5}])) is False

    def test_an_errored_result_is_not_empty(self) -> None:
        """Errors belong to the retry path, not the discovery path."""
        assert _is_effectively_empty(_result([], error="boom")) is False


class TestPredicateExtraction:
    def test_finds_the_ilike_literal(self) -> None:
        found = _swappable_text_predicates(_FAILED_SQL)
        assert [(p["column"], p["literal"]) for p in found] == [("programa", "%universidad%")]

    def test_skips_numeric_literals(self) -> None:
        """A year that matches nothing is absent, not misspelled."""
        sql = "SELECT 1 FROM t WHERE anio = '2024'"
        assert _swappable_text_predicates(sql) == []

    def test_handles_equality_on_text(self) -> None:
        sql = "SELECT 1 FROM t WHERE jurisdiccion = 'Ministerio de Salud'"
        found = _swappable_text_predicates(sql)
        assert found[0]["column"] == "jurisdiccion"

    def test_whole_predicate_is_captured_for_substitution(self) -> None:
        found = _swappable_text_predicates(_FAILED_SQL)
        assert found[0]["whole"] == "programa ILIKE '%universidad%'"


class TestRouting:
    def test_empty_result_routes_to_discovery(self) -> None:
        state = {"result": _result([{"total": None}]), "generated_sql": _FAILED_SQL}
        assert _route_after_execute(state) == "discover_values"

    def test_discovery_runs_only_once(self) -> None:
        """The second pass must exit, or the graph loops forever."""
        state = {
            "result": _result([{"total": None}]),
            "generated_sql": _FAILED_SQL,
            "value_discovery_done": True,
        }
        assert _route_after_execute(state) == "success"

    def test_result_with_rows_skips_discovery(self) -> None:
        state = {"result": _result([{"total": 42}]), "generated_sql": _FAILED_SQL}
        assert _route_after_execute(state) == "success"

    def test_empty_result_without_text_filters_skips_discovery(self) -> None:
        """Nothing to re-aim: the table genuinely has no rows for that year."""
        state = {
            "result": _result([]),
            "generated_sql": "SELECT SUM(x) FROM t WHERE anio = '1899'",
        }
        assert _route_after_execute(state) == "success"

    def test_errors_still_route_to_retry(self) -> None:
        state = {
            "result": _result([], error="syntax error"),
            "generated_sql": _FAILED_SQL,
            "attempt": 0,
            "max_attempts": 2,
        }
        assert _route_after_execute(state) == "retry"


class TestValueFetching:
    @pytest.mark.asyncio
    async def test_probe_preserves_the_other_filters(self) -> None:
        """Asking about 2024 must not surface the values of every year."""
        sandbox = _Sandbox([_result([{"value": "Desarrollo de la Educacion Superior"}])])
        await _fetch_column_values(
            sandbox=sandbox,
            served_table="mart.presupuesto_consolidado",
            predicate=_swappable_text_predicates(_FAILED_SQL)[0],
            failed_sql=_FAILED_SQL,
        )
        probe = sandbox.executed[0]
        assert "anio = 2024" in probe, "the year scope must survive"
        assert "TRUE" in probe, "only the offending predicate is neutralised"
        assert "universidad" not in probe

    @pytest.mark.asyncio
    async def test_high_cardinality_column_is_refused(self) -> None:
        """Better no list than a truncated one — see the module docstring."""
        too_many = [{"value": f"v{i}"} for i in range(_MAX_DISCOVERABLE_VALUES + 1)]
        sandbox = _Sandbox([_result(too_many)])
        values = await _fetch_column_values(
            sandbox=sandbox,
            served_table="mart.presupuesto_consolidado",
            predicate=_swappable_text_predicates(_FAILED_SQL)[0],
            failed_sql=_FAILED_SQL,
        )
        assert values is None

    @pytest.mark.asyncio
    async def test_probe_error_yields_nothing(self) -> None:
        sandbox = _Sandbox([_result([], error="nope")])
        values = await _fetch_column_values(
            sandbox=sandbox,
            served_table="mart.presupuesto_consolidado",
            predicate=_swappable_text_predicates(_FAILED_SQL)[0],
            failed_sql=_FAILED_SQL,
        )
        assert values is None


class TestDiscoveryNode:
    @pytest.mark.asyncio
    async def test_rewrites_with_a_real_value(self) -> None:
        rewritten = (
            "SELECT SUM(credito_devengado) AS total FROM mart.presupuesto_consolidado "
            "WHERE anio = 2024 AND programa = 'Desarrollo de la Educacion Superior'"
        )
        sandbox = _Sandbox([_result([{"value": "Desarrollo de la Educacion Superior"}])])
        llm = _LLM(rewritten)

        out = await discover_values_node(
            {
                "nl_query": "¿Cuánto se transfirió a las universidades nacionales en 2024?",
                "generated_sql": _FAILED_SQL,
                "sandbox": sandbox,
                "llm": llm,
            }
        )

        assert out["generated_sql"] == rewritten
        assert out["value_discovery_done"] is True
        assert out["value_substitution"]["column"] == "programa"
        assert out["value_substitution"]["searched_for"] == "%universidad%"
        assert "Desarrollo de la Educacion Superior" in llm.prompts[0], (
            "the model must be shown the real values"
        )

    @pytest.mark.asyncio
    async def test_none_reply_keeps_the_empty_result(self) -> None:
        """A doubtful match is worse than no answer — the model may decline."""
        sandbox = _Sandbox([_result([{"value": "Servicio Penitenciario Federal"}])])
        out = await discover_values_node(
            {
                "nl_query": "¿Cuánto se transfirió a universidades en 2024?",
                "generated_sql": _FAILED_SQL,
                "sandbox": sandbox,
                "llm": _LLM("NONE"),
            }
        )
        assert out == {"value_discovery_done": True}
        assert "generated_sql" not in out
        assert "value_substitution" not in out

    @pytest.mark.asyncio
    async def test_marks_done_even_when_nothing_is_discoverable(self) -> None:
        """Otherwise the router would send us back here forever."""
        sandbox = _Sandbox([_result([], error="boom")])
        out = await discover_values_node(
            {
                "nl_query": "x",
                "generated_sql": _FAILED_SQL,
                "sandbox": sandbox,
                "llm": _LLM("NONE"),
            }
        )
        assert out["value_discovery_done"] is True


class TestSubstitutionIsSurfaced:
    def test_note_names_the_searched_term_and_column(self) -> None:
        note = _value_substitution_note({"column": "programa", "searched_for": "%universidad%"})
        assert "universidad" in note
        assert "programa" in note

    def test_note_instructs_the_analyst_to_tell_the_user(self) -> None:
        """A silent reinterpretation is the failure mode being guarded."""
        note = _value_substitution_note({"column": "programa", "searched_for": "%x%"})
        assert "USUARIO" in note.upper()
