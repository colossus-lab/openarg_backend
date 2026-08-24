"""Tests for the LLM repair tier.

This tier spends money and rewrites schemas from a model's output, so what is
tested is the restraint: it must stay off, stay bounded, and never let a
proposal past the verifier just because a model produced it.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


class _Row:
    def __init__(self, i, broken=1, total=30):
        self.table_schema = "raw"
        self.table_name = f"t{i}"
        self.broken = broken
        self.total = total


class _Outcome:
    def __init__(self, ok=True, reason="llm_applied"):
        self.ok = ok
        self.reason = reason


@pytest.fixture(autouse=True)
def _off(monkeypatch):
    monkeypatch.delenv("OPENARG_LLM_REPAIR", raising=False)


def _run(rows, outcomes, monkeypatch, **kw):
    from app.application.repair import parse_repair
    from app.infrastructure.adapters.llm import bedrock_llm_adapter
    from app.infrastructure.celery.tasks import llm_repair_tasks as mod

    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value = MagicMock(
        fetchall=lambda: rows
    )
    mod.get_sync_engine = lambda: engine
    monkeypatch.setattr(bedrock_llm_adapter, "BedrockLLMAdapter", lambda *a, **k: MagicMock())

    calls: list[dict] = []
    it = iter(outcomes)

    async def _repair(_engine, **kwargs):
        calls.append(kwargs)
        result = next(it)
        if isinstance(result, Exception):
            raise result
        return result

    monkeypatch.setattr(parse_repair, "repair_with_llm_assist", _repair)

    # El canario corre antes de escribir y llama al modelo de verdad. Estos
    # tests son sobre el bucle de reparación, no sobre el modelo, así que se lo
    # da por sano. Que haya que mockearlo es la señal de que la compuerta está
    # puesta: sin este stub, ninguna de estas pruebas escribiría nada.
    from app.application.quality import model_canary

    async def _canary_ok(llm, proposer):
        return model_canary.CanaryResult(ok=True, reason="ok")

    monkeypatch.setattr(model_canary, "run_canary", _canary_ok)
    return mod.repair_columns_with_llm.run(**kw), calls, engine


def test_it_does_nothing_unless_deliberately_switched_on(monkeypatch):
    """Two deliberate acts to start: the env switch and dry_run=False. A tier
    that costs money per table should not be one flag away from running."""
    from app.infrastructure.celery.tasks import llm_repair_tasks as mod

    result = mod.repair_columns_with_llm.run(dry_run=False)

    assert result["enabled"] is False
    assert result["reason"] == "not_enabled"
    assert result["repaired"] == 0


def test_it_reports_without_acting_by_default(monkeypatch):
    monkeypatch.setenv("OPENARG_LLM_REPAIR", "1")
    result, calls, _ = _run([_Row(1)], [_Outcome(reason="dry_run_proposal")], monkeypatch)

    assert result["dry_run"] is True
    assert calls[0]["dry_run"] is True
    assert result["repaired"] == 0


def test_it_targets_tables_that_are_mostly_healthy(monkeypatch):
    """The population the heuristics cannot reach: a few lost names among many
    good ones. Measured on production — 428 tables, 231 with a single broken
    column, one of them 1 broken out of 841."""
    monkeypatch.setenv("OPENARG_LLM_REPAIR", "1")
    _, _, engine = _run([_Row(1)], [_Outcome()], monkeypatch, dry_run=False)

    params = engine.connect.return_value.__enter__.return_value.execute.call_args.args[1]
    assert params["min_broken"] == 1
    assert params["max_ratio"] == 0.40, "above this the deterministic repairs should own it"


def test_it_stays_within_its_budget(monkeypatch):
    monkeypatch.setenv("OPENARG_LLM_REPAIR", "1")
    _, _, engine = _run(
        [_Row(i) for i in range(3)], [_Outcome()] * 3, monkeypatch, limit=3, dry_run=False
    )

    params = engine.connect.return_value.__enter__.return_value.execute.call_args.args[1]
    assert params["limit"] == 3


def test_proposals_the_verifier_threw_out_are_counted_separately(monkeypatch):
    """The number that says whether this tier earns its cost. A high count means
    we are paying for plausible nonsense."""
    monkeypatch.setenv("OPENARG_LLM_REPAIR", "1")
    outcomes = [
        _Outcome(),
        _Outcome(ok=False, reason="verification_refused:proposal_still_contains_garbage_names"),
        _Outcome(ok=False, reason="verification_refused:proposal_collapses_two_columns"),
    ]
    result, _, _ = _run([_Row(i) for i in range(3)], outcomes, monkeypatch, dry_run=False)

    assert result["repaired"] == 1
    assert result["refused_by_verifier"] == 2


def test_a_failing_model_call_costs_only_its_own_table(monkeypatch):
    """Throttling, a timeout, a transient credential problem — none of which are
    the table's fault."""
    monkeypatch.setenv("OPENARG_LLM_REPAIR", "1")
    outcomes = [_Outcome(), RuntimeError("throttled"), _Outcome()]
    result, _, _ = _run([_Row(i) for i in range(3)], outcomes, monkeypatch, dry_run=False)

    assert result["repaired"] == 2
    assert result["by_reason"]["raised"] == 1


def test_it_does_not_spend_a_slot_on_a_table_the_proposer_will_refuse(monkeypatch):
    """`propose_llm_assisted_rename` declines past 100 columns, and the first
    ordering sent it exactly those.

    Ordering by the lowest broken ratio put the 841-column tables at the front —
    the widest ones, which are precisely the ones refused. The first production
    run returned three candidates and three `too_many_cols`, so the tier would
    have done nothing forever while looking like it ran.
    """
    monkeypatch.setenv("OPENARG_LLM_REPAIR", "1")
    _, _, engine = _run([_Row(1)], [_Outcome()], monkeypatch, dry_run=False)

    params = engine.connect.return_value.__enter__.return_value.execute.call_args.args[1]
    assert params["max_cols"] == 100

    from app.infrastructure.celery.tasks import llm_repair_tasks as mod

    sql = str(mod._CANDIDATES_SQL)
    assert "total <= :max_cols" in sql
    assert "ORDER BY broken ASC" in sql, "fewest broken columns first, not widest table"


def test_a_degraded_model_writes_nothing(monkeypatch):
    """La compuerta que faltaba.

    `verify_intrinsic` revisa la *forma* de una propuesta —que sea un
    identificador, distinto, menos roto que lo que reemplaza— y no su
    *significado*. Una columna de CUITs nombrada `fecha` pasa todos los
    chequeos estructurales que hay. Un modelo degradado sigue produciendo
    nombres bien formados para las columnas equivocadas, y eso el verificador
    no lo puede ver.
    """
    from app.application.quality import model_canary
    from app.infrastructure.adapters.llm import bedrock_llm_adapter
    from app.infrastructure.celery.tasks import llm_repair_tasks as mod

    monkeypatch.setenv("OPENARG_LLM_REPAIR", "1")
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value.execute.return_value = MagicMock(
        fetchall=lambda: [_Row(1)]
    )
    mod.get_sync_engine = lambda: engine
    monkeypatch.setattr(bedrock_llm_adapter, "BedrockLLMAdapter", lambda *a, **k: MagicMock())

    async def _canary_mal(llm, proposer):
        return model_canary.CanaryResult(ok=False, reason="2 de 3 columnas mal nombradas")

    monkeypatch.setattr(model_canary, "run_canary", _canary_mal)

    result = mod.repair_columns_with_llm.run(dry_run=False)
    assert result["reason"] == "canary_failed"
    assert result["repaired"] == 0
    assert "mal nombradas" in result["canary"]
