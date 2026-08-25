"""Watching the model that renames columns in production.

The LLM tier has written to 33 tables. Every proposal answers to
`verify_intrinsic`, which checks *shape* — that a name is identifier-like,
distinct, and less broken than what it replaces. It cannot check *meaning*: a
column of CUITs named `fecha` passes every structural test there is.

Its own tests mock the response, which proves the plumbing and says nothing
about the answers. These tests are about the watcher, not the model.
"""

from __future__ import annotations

import asyncio

from app.application.quality.model_canary import _FIXTURES, run_canary


def _proposer_returning(names, reason="applied"):
    async def _p(old_cols, sample, *, llm):
        return list(names), len(names), reason

    return _p


def _run(proposer):
    return asyncio.run(run_canary(llm=object(), proposer=proposer))


def test_applied_is_the_success_reason_these_proposers_return() -> None:
    """`propose_llm_assisted_rename` devuelve `applied`, no `ok`.

    Lo asumí mal la primera vez y el canario reportó que el modelo declinaba
    las tres cuando había respondido bien. Un vigía que se equivoca sobre lo
    que vigila es peor que ninguno: enseña a ignorarlo.
    """
    assert _run(_proposer_returning(["cuit", "fecha", "monto"], reason="applied")).ok
    assert _run(_proposer_returning(["cuit", "fecha", "monto"], reason="ok")).ok
    assert not _run(_proposer_returning(["cuit", "fecha", "monto"], reason="llm_bad_json")).ok


def test_a_model_that_names_the_three_fixtures_passes() -> None:
    """CUITs, ISO dates and peso amounts. A person names these without
    hesitating; a model that cannot has no business renaming the ambiguous
    columns it is pointed at in production."""
    r = _run(_proposer_returning(["cuit", "fecha_operacion", "monto_pesos"]))
    assert r.ok, r.detail


def test_synonyms_are_accepted_because_this_watches_competence_not_vocabulary() -> None:
    """`cuit` and `identificador_fiscal` are both right. Pinning one exact
    string would fail the canary on a model that is working fine."""
    r = _run(_proposer_returning(["identificador_fiscal", "periodo", "importe_total"]))
    assert r.ok, r.detail


def test_a_plausible_but_wrong_name_is_caught() -> None:
    """The failure mode that matters: a model that degrades keeps producing
    well-formed names for the wrong columns, and every structural check passes.
    Here the date column is named as if it held money."""
    r = _run(_proposer_returning(["cuit", "monto_total", "monto_pesos"]))
    assert not r.ok
    assert "mal nombradas" in r.reason
    assert "monto_total" in r.detail


def test_names_that_are_not_identifiers_are_caught() -> None:
    r = _run(_proposer_returning(["CUIT del titular", "fecha", "monto"]))
    assert not r.ok
    assert "identificadores válidos" in r.reason


def test_repeated_names_are_caught() -> None:
    """Three columns collapsing to two loses a column's worth of meaning, and
    the repair would apply it."""
    r = _run(_proposer_returning(["cuit", "fecha", "fecha"]))
    assert not r.ok
    assert "repetidos" in r.reason


def test_declining_all_three_is_a_failure_not_a_pass() -> None:
    """Declining is legitimate on an ambiguous column and not on these."""
    r = _run(_proposer_returning([], reason="too_many_cols"))
    assert not r.ok
    assert "declinó" in r.reason


def test_a_proposer_that_raises_is_reported_not_swallowed() -> None:
    """A throttled endpoint or a credential problem must surface. A canary that
    goes quiet when the thing it watches breaks is worse than no canary."""

    async def _boom(old_cols, sample, *, llm):
        raise RuntimeError("ThrottlingException")

    r = _run(_boom)
    assert not r.ok
    assert "ThrottlingException" in r.detail


def test_the_fixtures_are_unambiguous_to_a_person() -> None:
    """Guard on the fixtures themselves: if someone adds one whose answer is
    debatable, the canary starts failing on a healthy model and gets muted."""
    assert len(_FIXTURES) == 3
    for _, valores, esperados in _FIXTURES:
        assert len(valores) >= 4, "pocos ejemplos para inferir"
        assert esperados, "un fixture sin respuesta aceptable no se puede juzgar"
