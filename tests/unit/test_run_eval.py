"""La batería de regresión: su lógica de scoring y de comparación.

Lo que se prueba acá es que la batería sepa distinguir una regresión de una
mejora, y que no invente números sobre casos que no puede juzgar. Correr el
pipeline entero es otra cosa y no pertenece a un test unitario.
"""

from __future__ import annotations

from typing import Any

from tests.evaluation.run_eval import (
    INTENT_MAP,
    compare_to_baseline,
    summarise,
    validate_dataset,
)


def _r(**over: Any) -> dict:
    base = {
        "id": "x_001",
        "category": "series_tiempo",
        "question": "q",
        "error": None,
        "latency_ms": 1000,
        "answered": True,
        "answer_chars": 500,
        "tokens_used": 1000,
        "answer_head": "…",
        "classification": None,
        "plan_actions": ["query_series"],
        "sources": ["INDEC"],
        "keyword_score": 1.0,
        "retrieval_precision": 1.0,
        "intent_scored": False,
        "intent_match": False,
        "connector_scored": True,
        "connector_match": True,
    }
    base.update(over)
    return base


# ── qué cuenta como regresión ──────────────────────────────


def test_detecta_un_caso_que_ahora_falla() -> None:
    reg = compare_to_baseline(
        {"results": [_r(error="Boom: x", answered=False)]}, {"results": [_r()]}
    )
    assert any("now errors" in p for p in reg)


def test_detecta_un_caso_que_se_quedo_mudo() -> None:
    reg = compare_to_baseline(
        {"results": [_r(answered=False, answer_chars=3)]}, {"results": [_r()]}
    )
    assert any("answered before" in p for p in reg)


def test_detecta_que_dejo_de_rutear_al_conector_correcto() -> None:
    peor = _r(connector_match=False, plan_actions=["search_ckan"])
    reg = compare_to_baseline({"results": [peor]}, {"results": [_r()]})
    assert any("no longer routes" in p for p in reg)


def test_detecta_latencia_que_mas_que_duplico() -> None:
    reg = compare_to_baseline(
        {"results": [_r(latency_ms=2100)]}, {"results": [_r(latency_ms=1000)]}
    )
    assert any("latency" in p for p in reg)


def test_una_latencia_apenas_peor_no_es_regresion() -> None:
    """Estas corridas pegan a portales y a un modelo vivos: un umbral apretado
    gritaría en cada corrida y la batería se volvería ruido que nadie mira."""
    assert compare_to_baseline({"results": [_r(latency_ms=1900)]}, {"results": [_r()]}) == []


# ── qué NO cuenta ──────────────────────────────────────────


def test_una_mejora_nunca_es_regresion() -> None:
    mejor = _r(latency_ms=100, keyword_score=1.0, answer_chars=900)
    peor_base = _r(latency_ms=5000, keyword_score=0.5, answered=False, answer_chars=2)
    assert compare_to_baseline({"results": [mejor]}, {"results": [peor_base]}) == []


def test_un_caso_nuevo_no_rompe_la_comparacion() -> None:
    """Agregar un caso al dataset no puede hacer fallar el gate: no hay contra
    qué compararlo todavía."""
    assert compare_to_baseline({"results": [_r(id="nuevo_001")]}, {"results": []}) == []


def test_un_caso_que_ya_venia_roto_sigue_sin_ser_regresion() -> None:
    roto = _r(error="Boom", answered=False)
    assert compare_to_baseline({"results": [roto]}, {"results": [roto]}) == []


# ── el resumen no esconde el denominador ───────────────────


def test_las_tasas_dicen_sobre_cuántos_casos_se_calcularon() -> None:
    """32 de los 50 casos tienen conector esperado y sólo 18 tienen un intent
    comparable. Un porcentaje pelado ocultaría eso."""
    res = [
        _r(id="a", intent_scored=True, intent_match=True, connector_scored=False),
        _r(id="b", intent_scored=False, connector_scored=True, connector_match=True),
        _r(id="c", intent_scored=False, connector_scored=False),
    ]
    s = summarise(res, "normal")
    assert s["intent_accuracy"] == {"rate": 1.0, "scored_over": 1}
    assert s["connector_accuracy"] == {"rate": 1.0, "scored_over": 1}
    assert s["total"] == 3


def test_sin_casos_comparables_la_tasa_es_nula_y_no_cero() -> None:
    """Cero sería mentira: no es que falló todo, es que no había qué medir."""
    s = summarise([_r(intent_scored=False, connector_scored=False)], "deep")
    assert s["intent_accuracy"]["rate"] is None
    assert s["connector_accuracy"]["rate"] is None


def test_el_resumen_cuenta_errores_por_categoria() -> None:
    s = summarise([_r(id="a", error="Boom", answered=False), _r(id="b")], "normal")
    assert s["errors"] == 1
    assert s["by_category"]["series_tiempo"] == {"total": 2, "answered": 1, "errors": 1}


# ── el dataset real ────────────────────────────────────────


def test_el_dataset_que_esta_en_el_repo_es_valido() -> None:
    from tests.evaluation.run_eval import DEFAULT_DATASET, load_golden_dataset

    entries = load_golden_dataset(DEFAULT_DATASET)
    assert len(entries) == 50
    assert validate_dataset(entries) == []


def test_solo_se_puntua_el_intent_que_el_clasificador_puede_emitir() -> None:
    """El dataset mezcla intents reales con etiquetas temáticas ("inflacion",
    "dolar"). Puntuar las temáticas fabricaría un fallo garantizado."""
    from app.application.pipeline.classifiers import classify_request  # noqa: F401
    from tests.evaluation.run_eval import DEFAULT_DATASET, load_golden_dataset

    entries = load_golden_dataset(DEFAULT_DATASET)
    comparables = [e for e in entries if e["expected_intent"] in INTENT_MAP]
    assert len(comparables) == 18
    # y todo lo que mapeamos tiene que ser algo que el clasificador devuelva
    emitidos = {"casual", "meta", "internal_table", "injection", "off_topic", "educational"}
    assert set(INTENT_MAP.values()) <= emitidos


def test_la_bateria_nunca_escribe_en_una_conversacion() -> None:
    """Restricción dura: el pipeline persiste memoria cuando hay
    conversation_id. La batería tiene que pasar vacío siempre."""
    import inspect

    from tests.evaluation.run_eval import evaluate_entry

    src = inspect.getsource(evaluate_entry)
    assert '"conversation_id": ""' in src


def test_no_compara_latencia_entre_modos_distintos() -> None:
    """El modo profundo tiene que tardar más: quejarse de eso es una falsa
    alarma por diseño. Medido, produjo 11 'regresiones' que no eran nada."""
    lento = {"mode": "deep", "results": [_r(latency_ms=50_000)]}
    base = {"mode": "normal", "results": [_r(latency_ms=10_000)]}
    assert compare_to_baseline(lento, base) == []
    # pero contra un baseline del mismo modo sí tiene que saltar
    base_deep = {"mode": "deep", "results": [_r(latency_ms=10_000)]}
    assert any("latency" in p for p in compare_to_baseline(lento, base_deep))


def test_una_respuesta_peor_sí_cruza_entre_modos() -> None:
    """Lo que no depende del modo —que una respuesta empeore— se compara igual."""
    peor = {"mode": "deep", "results": [_r(keyword_score=0.5)]}
    base = {"mode": "normal", "results": [_r(keyword_score=1.0)]}
    assert any("keyword score" in p for p in compare_to_baseline(peor, base))
