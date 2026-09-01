"""Tests for the loop that ties detection, repair, rebuild and the message.

What is being pinned here is the wiring, because that is where this task's
value is: it picks the right tables, hands the ladder a guard built from the
real mart SQL, rebuilds what it changed, and sends a message whose content
depends on which of the three outcomes happened.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.application.marts.consumers import ConsumerIndex
from app.application.repair.escalation import Escalation
from app.infrastructure.celery.tasks import self_repair_tasks as srt


class _Broken:
    def __init__(self, table_name, *, schema="raw", n_cols=10, col_n=True):
        self.schema_name = schema
        self.table_name = table_name
        self.n_cols = n_cols
        self.col_n = col_n
        self.unnamed = False
        self.long_name = False
        self.delimiter_in_name = False


def _index(mapping, sql_by_mart=None):
    return ConsumerIndex(by_table=mapping, sql_by_mart=sql_by_mart or {})


@pytest.fixture
def wired(monkeypatch):
    """The task with every edge faked, and a record of what it did."""
    state: dict = {"escalated": [], "dispatched": [], "alerts": [], "index": _index({})}

    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchall.return_value = state.setdefault("broken", [])
    monkeypatch.setattr(srt, "get_sync_engine", lambda: engine)

    monkeypatch.setattr(
        "app.application.marts.consumers.build_consumer_index",
        lambda e: state["index"],
    )

    def _escalate(engine, *, table_schema, table_name, guard, llm, run_id, dry_run):
        state["escalated"].append((table_name, llm, dry_run, guard))
        return state.get("outcome", lambda t: Escalation(table_schema, t, True, tier="col_n"))(
            table_name
        )

    monkeypatch.setattr("app.application.repair.escalation.escalate_table", _escalate)
    monkeypatch.setattr(
        "app.infrastructure.celery.tasks.mart_tasks.dispatch_build_mart",
        lambda mid, **kw: state["dispatched"].append(mid),
    )

    def _notify(engine, alerts, *, heading):
        state["alerts"] = list(alerts)
        return {"considered": len(alerts), "new": len(alerts), "sent": len(alerts)}

    monkeypatch.setattr("app.application.quality.alerting.notify", _notify)
    monkeypatch.setattr(srt, "_model_if_it_answers", lambda: (None, "no consultado"))

    state["conn"] = conn
    return state


def _feed(state, rows):
    state["conn"].execute.return_value.fetchall.return_value = rows


# ── a qué tablas se le dedica el presupuesto ───────────────────


def test_only_tables_a_mart_actually_reads_are_attempted(wired):
    _feed(wired, [_Broken("alimenta"), _Broken("huerfana")])
    wired["index"] = _index({("raw", "alimenta"): ("m1",)})

    report = srt.repair_mart_sources(dry_run=True, use_llm=False)

    assert [t for t, *_ in wired["escalated"]] == ["alimenta"]
    assert report["broken_tables_total"] == 2
    assert report["feeding_marts"] == 1


def test_the_widest_table_goes_first(wired):
    _feed(wired, [_Broken("angosta", n_cols=3), _Broken("ancha", n_cols=40)])
    wired["index"] = _index({("raw", "angosta"): ("m1",), ("raw", "ancha"): ("m1",)})

    srt.repair_mart_sources(dry_run=True, use_llm=False)

    assert [t for t, *_ in wired["escalated"]] == ["ancha", "angosta"]


def test_the_run_is_bounded(wired):
    _feed(wired, [_Broken(f"t{i}") for i in range(10)])
    wired["index"] = _index({("raw", f"t{i}"): ("m1",) for i in range(10)})

    report = srt.repair_mart_sources(dry_run=True, use_llm=False, limit=3)

    assert report["attempted"] == 3
    assert report["feeding_marts"] == 10


def test_a_public_schema_table_is_eligible_too(wired):
    # `escuelas_padron_nacional` reads a `public.cache_*` table in production.
    _feed(wired, [_Broken("cache_padron", schema="public")])
    wired["index"] = _index({("public", "cache_padron"): ("escuelas",)})

    srt.repair_mart_sources(dry_run=True, use_llm=False)

    assert [t for t, *_ in wired["escalated"]] == ["cache_padron"]


# ── el freno que se le pasa a la escalera ──────────────────────


def test_the_guard_handed_down_is_built_from_the_real_mart_sql(wired):
    _feed(wired, [_Broken("t")])
    wired["index"] = _index(
        {("raw", "t"): ("m1",)},
        sql_by_mart={"m1": 'SELECT col_1 FROM raw."t"'},
    )

    srt.repair_mart_sources(dry_run=True, use_llm=False)

    guard = wired["escalated"][0][3]
    assert guard(["col_1"]) == ["m1"], "una columna que el mart nombra frena la reparación"
    assert guard(["col_9"]) == [], "una columna que nadie nombra no frena nada"


# ── el modelo ──────────────────────────────────────────────────


def test_a_model_that_fails_the_canary_is_not_handed_to_the_ladder(wired, monkeypatch):
    monkeypatch.setattr(srt, "_model_if_it_answers", lambda: (None, "falló: nombró 1 de 3"))
    _feed(wired, [_Broken("t")])
    wired["index"] = _index({("raw", "t"): ("m1",)})

    report = srt.repair_mart_sources(dry_run=True, use_llm=True)

    assert wired["escalated"][0][1] is None, "sin modelo"
    assert report["canary"].startswith("falló")
    assert report["attempted"] == 1, "las heurísticas corren igual"


def test_a_model_that_answers_is_handed_down(wired, monkeypatch):
    sentinel = object()
    monkeypatch.setattr(srt, "_model_if_it_answers", lambda: (sentinel, "ok"))
    _feed(wired, [_Broken("t")])
    wired["index"] = _index({("raw", "t"): ("m1",)})

    srt.repair_mart_sources(dry_run=True, use_llm=True)

    assert wired["escalated"][0][1] is sentinel


def test_use_llm_false_never_even_asks_the_canary(wired, monkeypatch):
    def _boom():  # pragma: no cover — must not be reached
        raise AssertionError("no debería consultar el canario")

    monkeypatch.setattr(srt, "_model_if_it_answers", _boom)
    _feed(wired, [_Broken("t")])
    wired["index"] = _index({("raw", "t"): ("m1",)})

    srt.repair_mart_sources(dry_run=True, use_llm=False)


# ── reconstrucción ─────────────────────────────────────────────


def test_a_dry_run_rebuilds_nothing(wired):
    _feed(wired, [_Broken("t")])
    wired["index"] = _index({("raw", "t"): ("m1",)})

    report = srt.repair_mart_sources(dry_run=True, use_llm=False)

    assert wired["dispatched"] == []
    assert report["marts_rebuilt"] == []


def test_every_mart_reading_a_repaired_table_is_rebuilt_once(wired):
    _feed(wired, [_Broken("a"), _Broken("b")])
    wired["index"] = _index({("raw", "a"): ("m1", "m2"), ("raw", "b"): ("m1",)})

    report = srt.repair_mart_sources(dry_run=False, use_llm=False)

    # `m1` reads both repaired tables and is dispatched once, not twice.
    assert wired["dispatched"] == ["m1", "m2"]
    assert report["marts_rebuilt"] == ["m1", "m2"]


def test_a_table_that_was_not_fixed_does_not_trigger_a_rebuild(wired):
    _feed(wired, [_Broken("t")])
    wired["index"] = _index({("raw", "t"): ("m1",)})
    wired["outcome"] = lambda t: Escalation("raw", t, False, reason="declined")

    srt.repair_mart_sources(dry_run=False, use_llm=False)

    assert wired["dispatched"] == []


# ── el mensaje ─────────────────────────────────────────────────


def test_a_repair_says_which_rung_did_it(wired):
    _feed(wired, [_Broken("t")])
    wired["index"] = _index({("raw", "t"): ("m1",)})
    wired["outcome"] = lambda t: Escalation(
        "raw", t, True, tier="unsplit_csv", changed_columns=("a;b;c",)
    )

    srt.repair_mart_sources(dry_run=False, use_llm=False)

    alert = wired["alerts"][0]
    assert alert.kind == "repaired"
    assert alert.key == "raw.t::unsplit_csv", "la clave lleva el escalón"
    assert "unsplit_csv" in alert.detail
    assert "m1" in alert.detail


def test_something_nothing_could_fix_names_the_marts_it_affects(wired):
    _feed(wired, [_Broken("t")])
    wired["index"] = _index({("raw", "t"): ("m1", "m2")})
    wired["outcome"] = lambda t: Escalation(
        "raw", t, False, reason="heuristics_declined_and_no_model"
    )

    report = srt.repair_mart_sources(dry_run=False, use_llm=False)

    alert = wired["alerts"][0]
    assert alert.kind == "broken_unrepaired"
    assert alert.key == "raw.t"
    assert "m1, m2" in alert.detail
    assert report["unfixed"] == 1


def test_a_refusal_is_reported_as_a_refusal_not_as_a_failure(wired):
    _feed(wired, [_Broken("t")])
    wired["index"] = _index({("raw", "t"): ("m1",)})
    wired["outcome"] = lambda t: Escalation(
        "raw",
        t,
        False,
        reason="would_break_marts",
        blocked_by_marts=("m1",),
        changed_columns=("col_1",),
    )

    report = srt.repair_mart_sources(dry_run=False, use_llm=False)

    assert report["blocked_by_marts"] == 1
    assert report["unfixed"] == 0, "rechazar a propósito no es fallar"
    alert = wired["alerts"][0]
    assert alert.kind == "repair_would_break_mart"
    assert "col_1" in alert.detail and "m1" in alert.detail


def test_a_dry_run_tells_nobody(wired, monkeypatch):
    # The first production dry run sent five real messages and claimed all 25
    # fingerprints, which would have deduplicated the real run into silence.
    def _boom(engine, alerts, *, heading):  # pragma: no cover — must not be reached
        raise AssertionError("un ensayo no le avisa a nadie")

    monkeypatch.setattr("app.application.quality.alerting.notify", _boom)
    _feed(wired, [_Broken("t")])
    wired["index"] = _index({("raw", "t"): ("m1",)})

    report = srt.repair_mart_sources(dry_run=True, use_llm=False)

    assert report["alerting"]["sent"] == 0
    assert report["alerting"]["new"] == 0, "no debe consumir el dedup"
    # …pero sí muestra lo que diría.
    assert report["alerting"]["would_say"], "un ensayo tiene que ser legible"


def test_nothing_broken_sends_nothing(wired):
    _feed(wired, [])

    report = srt.repair_mart_sources(dry_run=False, use_llm=False)

    assert report["attempted"] == 0
    assert wired["alerts"] == []


def test_a_channel_that_is_down_does_not_cost_the_run(wired, monkeypatch):
    def _boom(engine, alerts, *, heading):
        raise RuntimeError("telegram caído")

    monkeypatch.setattr("app.application.quality.alerting.notify", _boom)
    _feed(wired, [_Broken("t")])
    wired["index"] = _index({("raw", "t"): ("m1",)})

    report = srt.repair_mart_sources(dry_run=False, use_llm=False)

    assert report["fixed"] == 1
    assert report["alerting"]["sent"] == 0


# ── síntomas ───────────────────────────────────────────────────


def test_symptoms_are_named_not_counted():
    assert srt._symptoms(_Broken("t", n_cols=2)) == ["col_n"]


def test_a_narrow_table_with_good_names_has_no_symptom():
    # `caba__estructura_demografica` entraba a la escalera seis veces por semana
    # con las columnas `BARRIO | POBLACION`. Dos columnas no es un defecto: es
    # cómo se ve una tabla de población por barrio. Medido 2026-09-01: ese
    # síntoma marcaba 1.383 tablas sanas.
    sana = _Broken("caba__estructura_demografica", n_cols=2, col_n=False)
    assert srt._symptoms(sana) == []


def test_a_narrow_table_that_is_also_broken_still_enters():
    # No se pierde cobertura: las 336 angostas-Y-rotas siguen entrando por su
    # defecto real.
    rota = _Broken("t", n_cols=2, col_n=True)
    assert "col_n" in srt._symptoms(rota)


# ── el tercer resultado ────────────────────────────────────────


def test_an_unrepairable_unreadable_table_is_withheld(wired, monkeypatch):
    retiradas: list[str] = []
    monkeypatch.setattr(
        "app.application.repair.quarantine.quarantine",
        lambda e, t: retiradas.append(t) or True,
    )
    _feed(wired, [_Broken("t", col_n=True)])
    wired["index"] = _index({("raw", "t"): ("m1",)})
    wired["outcome"] = lambda t: Escalation("raw", t, False, reason="declined")

    report = srt.repair_mart_sources(dry_run=False, use_llm=False)

    assert retiradas == ["t"]
    assert report["quarantined"] == ["t"]


def test_a_dry_run_withholds_nothing(wired, monkeypatch):
    def _boom(e, t):  # pragma: no cover — must not be reached
        raise AssertionError("un ensayo no retira nada del servicio")

    monkeypatch.setattr("app.application.repair.quarantine.quarantine", _boom)
    _feed(wired, [_Broken("t")])
    wired["index"] = _index({("raw", "t"): ("m1",)})
    wired["outcome"] = lambda t: Escalation("raw", t, False, reason="declined")

    srt.repair_mart_sources(dry_run=True, use_llm=False)


def test_a_repaired_table_is_returned_to_service(wired, monkeypatch):
    devueltas: list[str] = []
    monkeypatch.setattr(
        "app.application.repair.quarantine.release",
        lambda e, t: devueltas.append(t) or True,
    )
    _feed(wired, [_Broken("t")])
    wired["index"] = _index({("raw", "t"): ("m1",)})

    report = srt.repair_mart_sources(dry_run=False, use_llm=False)

    assert devueltas == ["t"]
    assert report["released"] == ["t"]


def test_a_long_name_alone_is_not_withheld(wired, monkeypatch):
    def _boom(e, t):  # pragma: no cover — must not be reached
        raise AssertionError("un nombre largo es feo, no peligroso")

    monkeypatch.setattr("app.application.repair.quarantine.quarantine", _boom)
    row = _Broken("t", col_n=False)
    row.long_name = True
    _feed(wired, [row])
    wired["index"] = _index({("raw", "t"): ("m1",)})
    wired["outcome"] = lambda t: Escalation("raw", t, False, reason="declined")

    srt.repair_mart_sources(dry_run=False, use_llm=False)


def test_the_number_withheld_in_one_run_is_capped(wired, monkeypatch):
    from app.application.repair import quarantine as q

    monkeypatch.setattr(q, "MAX_PER_RUN", 2)
    monkeypatch.setattr("app.application.repair.quarantine.quarantine", lambda e, t: True)
    _feed(wired, [_Broken(f"t{i}") for i in range(5)])
    wired["index"] = _index({("raw", f"t{i}"): ("m1",) for i in range(5)})
    wired["outcome"] = lambda t: Escalation("raw", t, False, reason="declined")

    report = srt.repair_mart_sources(dry_run=False, use_llm=False)

    assert len(report["quarantined"]) == 2, "el alcance es la lección de todos los incidentes"


def test_the_message_says_it_was_withheld(wired, monkeypatch):
    monkeypatch.setattr("app.application.repair.quarantine.quarantine", lambda e, t: True)
    _feed(wired, [_Broken("t")])
    wired["index"] = _index({("raw", "t"): ("m1",)})
    wired["outcome"] = lambda t: Escalation("raw", t, False, reason="declined")

    srt.repair_mart_sources(dry_run=False, use_llm=False)

    assert "retiré del servicio" in wired["alerts"][0].detail


# ── validación del conjunto objetivo y firma ───────────────────


def test_an_implausible_candidate_set_aborts_before_touching_anything(wired):
    # Azure y Diskerase: la acción estaba bien y el alcance estaba mal.
    _feed(wired, [_Broken(f"t{i}") for i in range(60)])
    wired["index"] = _index({("raw", f"t{i}"): ("m1",) for i in range(60)})

    report = srt.repair_mart_sources(dry_run=False, use_llm=False, limit=5)

    assert report["aborted"] == "implausible_target_set"
    assert report["attempted"] == 0
    assert wired["escalated"] == [], "no escala ni una"


def test_a_normal_candidate_set_proceeds(wired):
    _feed(wired, [_Broken(f"t{i}") for i in range(8)])
    wired["index"] = _index({("raw", f"t{i}"): ("m1",) for i in range(8)})

    report = srt.repair_mart_sources(dry_run=False, use_llm=False, limit=5)

    assert "aborted" not in report
    assert report["attempted"] == 5


def test_waiting_for_a_signature_is_not_a_failure(wired, monkeypatch):
    # Contarlo como falla lo mandaría a cuarentena: retiraríamos del servicio una
    # tabla que tiene arreglo listo esperando una firma.
    monkeypatch.setattr(
        "app.application.repair.quarantine.quarantine",
        lambda e, t: (_ for _ in ()).throw(AssertionError("no debe aislarse")),
    )
    _feed(wired, [_Broken("t")])
    wired["index"] = _index({("raw", "t"): ("m1",)})
    wired["outcome"] = lambda t: Escalation(
        "raw",
        t,
        False,
        reason="needs_approval:delete_rows",
        proposal_id="p1",
        action_detail="borraría 3 fila(s)",
    )

    report = srt.repair_mart_sources(dry_run=False, use_llm=False)

    assert report["unfixed"] == 0
    assert report["quarantined"] == []
    assert report["awaiting_approval"][0]["id"] == "p1"
    assert wired["alerts"][0].kind == "repair_needs_approval"
