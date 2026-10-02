"""Los chequeos de calidad de la batería, sobre respuestas reales que pasaban.

Cada caso de acá es una falla que la batería anterior aprobaba (relevamiento
del 01-oct-2026) o una buena respuesta que no puede fallar por cómo está
escrita. Si un chequeo deja de distinguirlas, la batería vuelve a medir sólo
que hubo respuesta.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.evaluation.engines import UsageMeter, price_for
from tests.evaluation.evaluator import parse_judge_score
from tests.evaluation.quality_checks import (
    assess,
    find_value,
    is_deflection,
    keyword_score,
    leaked_errors,
    leaked_identifiers,
    numbers_in_answer,
    source_kinds,
)
from tests.evaluation.run_eval import aggregate_entry, rescore, summarise, validate_dataset

DATASET = Path(__file__).parents[1] / "evaluation" / "golden_dataset.json"


def _entry(case_id: str) -> dict:
    data = json.loads(DATASET.read_text(encoding="utf-8"))
    return next(e for e in data["entries"] if e["id"] == case_id)


SERIES = [
    {
        "name": "PIB",
        "portal": "API de Series de Tiempo",
        "url": "https://datos.gob.ar/series/api/series/?ids=x",
    }
]
ESTUDIO = [
    {
        "name": "Estudio Nacional sobre el Perfil…",
        "portal": "datos.gob.ar",
        "url": "https://datos.gob.ar/dataset/x",
    }
]


# ── números ────────────────────────────────────────────────


def test_las_fechas_y_los_anios_no_son_cifras() -> None:
    assert numbers_in_answer("En 2024, al 30/09/2025 y el 3 de marzo de 2023 a las 10:30.") == []


def test_una_cifra_en_millones_se_expande() -> None:
    [n] = numbers_in_answer("son US$ 44.516 millones")
    assert n.value == 44_516_000_000


def test_el_redondeo_de_la_respuesta_cuenta_como_tolerancia() -> None:
    assert find_value({"value": 3_675_564}, "estima 3,7 millones de personas")
    assert not find_value({"value": 3_675_564}, "estima 3,9 millones de personas")


def test_un_valor_admite_alternativas() -> None:
    spec = {"value": [3_675_564, 3_571_983], "rel_tolerance": 0.01}
    assert find_value(spec, "3.571.983 personas de 6 años y más")
    assert find_value(spec, "3.675.564 personas")
    assert not find_value(spec, "7.944 personas")


def test_la_unidad_tiene_que_estar_cerca_de_la_cifra() -> None:
    spec = {"value": 257, "unit_patterns": ["banca"]}
    assert find_value(spec, "La Cámara tiene 257 bancas.")
    assert not find_value(spec, "Hay 257 registros en la tabla.")


# ── deflexión y filtraciones ───────────────────────────────


def test_una_deflexion_sin_cifras_es_deflexion() -> None:
    assert is_deflection("No encontré datos sobre el CUD en la provincia en 2023.")


def test_aclarar_lo_que_falta_y_dar_el_dato_no_es_deflexion() -> None:
    assert not is_deflection(
        "No hay dato por partido: el estudio es nacional y estima 3.675.564 personas."
    )


def test_detecta_un_error_interno_que_llego_al_usuario() -> None:
    assert leaked_errors("Hubo un problema: operación no permitida") == ["operación no permitida"]
    assert leaked_errors('psycopg.errors: relation "x" does not exist')


def test_un_nombre_de_tabla_interno_es_aviso() -> None:
    texto = "según raw.datos_gob_ar__estudio_nacional_sobre_el_perfil_de__08ca74a9__v1"
    assert leaked_identifiers(texto)
    assert not leaked_identifiers("según el Estudio Nacional sobre el Perfil (INDEC, 2018)")


# ── fuentes por tipo, no por título ────────────────────────


def test_la_serie_oficial_no_cuenta_tambien_como_dataset_del_portal() -> None:
    assert source_kinds(SERIES) == ["series_tiempo"]


def test_un_dataset_del_portal_es_datos_gob_ar() -> None:
    assert source_kinds(ESTUDIO) == ["datos_gob_ar"]


def test_las_cotizaciones_de_dolarapi_son_argentina_datos() -> None:
    """Línea base del 01-oct: el conector sirve las cotizaciones del día desde
    DolarApi y tres casos salían en rojo por un detector incompleto."""
    assert source_kinds([{"portal": "DolarApi", "url": "https://dolarapi.com"}]) == [
        "argentina_datos"
    ]


def test_la_descarga_en_vivo_del_indec_se_distingue() -> None:
    kinds = source_kinds(
        [{"portal": "INDEC (descarga en vivo)", "url": "https://www.indec.gob.ar/x"}]
    )
    assert "indec_live" in kinds


# ── palabras clave con alternativas ────────────────────────


@pytest.mark.parametrize(
    "saludo",
    [
        "¡Hola! ¿Qué querés saber sobre datos abiertos de Argentina?",
        "¡Buenas! Estoy listo para ayudarte con datos públicos argentinos.",
        "¡Hola! Preguntame sobre economía, presupuesto, educación o cualquier dato público.",
    ],
)
def test_las_tres_variantes_del_saludo_aprueban(saludo: str) -> None:
    assert assess(_entry("casual_001"), saludo, []).passed


def test_una_palabra_clave_simple_sigue_funcionando() -> None:
    assert keyword_score("la inflación fue 2%", ["inflación", "%"]) == 1.0
    assert keyword_score("la inflación", ["inflación", "%"]) == 0.5


# ── las fallas reales del relevamiento ─────────────────────


def test_pbi_respondido_con_el_emae_falla() -> None:
    q = assess(_entry("series_003"), "El PBI: la actividad económica (EMAE) cayó -6,28 %.", SERIES)
    assert not q.passed
    assert any("EMAE" in f for f in q.failures)


def test_pbi_bien_respondido_aprueba() -> None:
    q = assess(
        _entry("series_003"), "El PBI creció 4,2 % interanual en el segundo trimestre.", SERIES
    )
    assert q.passed, q.failures


def test_reservas_en_pesos_falla_y_en_dolares_aprueba() -> None:
    bcra = [{"portal": "Banco Central de la República Argentina", "url": "https://www.bcra.gob.ar"}]
    assert not assess(_entry("series_004"), "Las reservas son $44.516 millones.", bcra).passed
    ok = assess(_entry("series_004"), "Las reservas del BCRA son US$ 44.516 millones.", bcra)
    assert ok.passed, ok.failures


def test_256_legisladores_falla() -> None:
    q = assess(_entry("ckan_004"), "Hay 256 legisladores en la Cámara de Diputados.", [])
    assert not q.passed


def test_la_linea_de_pobreza_no_es_la_tasa() -> None:
    mal = assess(
        _entry("complex_004"), "La línea de pobreza subió 32 % y la canasta básica también.", SERIES
    )
    bien = assess(
        _entry("complex_004"),
        "La pobreza bajó al 31,6 % de las personas; la canasta básica total subió 32 %.",
        SERIES,
    )
    assert not mal.passed
    assert bien.passed, bien.failures


def test_pinamar_con_las_filas_de_la_muestra_falla() -> None:
    q = assess(_entry("negativo_003"), "En Pinamar: total nacional 7.944 personas.", ESTUDIO)
    assert not q.passed
    assert any("filas de la muestra" in f for f in q.failures)


def test_pinamar_bien_respondido_aprueba() -> None:
    respuesta = (
        "El Estudio Nacional sobre el Perfil de las Personas con Discapacidad (INDEC, 2018) "
        "no tiene datos por partido, así que no hay una cifra para Pinamar. A nivel nacional "
        "estima 3.675.564 personas con alguna dificultad."
    )
    q = assess(_entry("negativo_003"), respuesta, ESTUDIO)
    assert q.passed, q.failures


def test_la_deflexion_con_la_tabla_en_la_mano_falla() -> None:
    ddjj = [{"portal": "Declaraciones Juradas Patrimoniales — Oficina Anticorrupción", "url": ""}]
    q = assess(
        _entry("ddjj_004"), "Lamentablemente no tengo forma de contar patrimonio negativo.", ddjj
    )
    assert not q.passed
    assert q.deflected


def test_un_error_del_motor_es_falla_y_no_se_mira_nada_mas() -> None:
    q = assess(_entry("series_003"), "", [], error="TimeoutError: x")
    assert [c.name for c in q.checks] == ["sin_error"]
    assert not q.passed


# ── el dataset declara bien sus chequeos ───────────────────


def test_el_dataset_no_tiene_tipos_de_fuente_inventados() -> None:
    entries = json.loads(DATASET.read_text(encoding="utf-8"))["entries"]
    assert validate_dataset(entries) == []


def test_un_tipo_de_fuente_mal_escrito_se_rechaza() -> None:
    bad = {**_entry("series_003"), "expected_source_by": [["series_tiemp"]]}
    assert any("series_tiemp" in p for p in validate_dataset([bad]))


def test_un_valor_esperado_sin_numero_se_rechaza() -> None:
    bad = {**_entry("series_003"), "expected_values": [{"value": "257"}]}
    assert validate_dataset([bad])


# ── corridas repetidas ─────────────────────────────────────


def _run(answer: str, run: int = 0, passed: bool = True, cost: float | None = 0.01) -> dict:
    return {
        "run": run,
        "error": None,
        "latency_ms": 1000 + run,
        "answer": answer,
        "sources": SERIES,
        "usage": {
            "llm_calls": 3,
            "tokens": {"input": 1000, "output": 100, "cache_read": 0, "cache_write": 0},
            "cost_usd": cost,
        },
        "tokens_reported": 400,
        "quality": {
            "passed": passed,
            "failures": [] if passed else ["valor:x: no aparece"],
            "checks": [{"name": "valor:x", "ok": passed, "detail": ""}],
            "warnings": [],
        },
        "diagnostics": {"classification": None, "plan_actions": ["query_series"]},
    }


def test_un_caso_que_a_veces_falla_se_ve_como_inestable() -> None:
    entry = _entry("series_003")
    agg = aggregate_entry(
        entry, [_run("a" * 50, 0), _run("b" * 50, 1, passed=False), _run("c" * 50, 2)]
    )
    assert agg["pass_rate"] == pytest.approx(0.667, abs=0.001)
    assert agg["failures"] == {"valor:x: no aparece": 1}
    s = summarise([agg], "normal")
    assert s["quality"]["cases_flaky"] == 1
    assert s["quality"]["numeric_accuracy"] == {"rate": 0.667, "scored_over": 3}
    assert s["n_runs"] == 3


def test_el_reporte_guarda_la_respuesta_completa() -> None:
    larga = "x" * 5_000
    agg = aggregate_entry(_entry("series_003"), [_run(larga)])
    assert agg["runs"][0]["answer"] == larga
    assert len(agg["answer_head"]) == 160


def test_rescore_aplica_las_expectativas_nuevas_sin_tocar_lo_medido() -> None:
    entry = _entry("series_003")
    viejo = summarise([aggregate_entry(entry, [_run("El EMAE cayó 6,28 %.")])], "normal")
    # La respuesta guardada aprobaba con un veredicto viejo; con el dataset
    # actual (que prohíbe el EMAE) tiene que fallar, y la latencia y el costo
    # quedan como se midieron.
    nuevo = rescore(viejo, [entry])
    [r] = nuevo["results"]
    assert r["pass_rate"] == 0.0
    assert r["runs"][0]["latency_ms"] == 1000
    assert nuevo["cost"]["total_usd"] == viejo["cost"]["total_usd"]


def test_un_modelo_sin_precio_no_inventa_un_costo() -> None:
    agg = aggregate_entry(_entry("series_003"), [_run("a" * 50, cost=None)])
    s = summarise([agg], "normal")
    assert s["cost"]["runs_without_price"] == 1
    assert s["cost"]["avg_per_answer_usd"] is None


# ── costo y jueces ─────────────────────────────────────────


def test_el_costo_suma_todas_las_llamadas_por_modelo() -> None:
    m = UsageMeter()
    m.add("us.anthropic.claude-haiku-4-5-20251001-v1:0", 1_000_000, 0)
    m.add("us.anthropic.claude-haiku-4-5-20251001-v1:0", 0, 100_000)
    m.add("us.anthropic.claude-sonnet-4-6", 100_000, 0, cache_read=1_000_000)
    # Haiku: 1,00 + 0,50; Sonnet: 0,30 + 0,30 de lectura de caché.
    assert m.cost_usd() == pytest.approx(2.10)
    assert m.calls == 3


def test_un_modelo_desconocido_deja_el_costo_en_none() -> None:
    m = UsageMeter()
    m.add("gemini-2.5-flash", 1000, 100)
    assert price_for("gemini-2.5-flash") is None
    assert m.cost_usd() is None


@pytest.mark.parametrize(
    ("texto", "esperado"),
    [
        ("Las cifras salen de la tabla.\nPUNTAJE: 0.0", 0.0),
        ("Cambia de indicador: habla del EMAE.\n**PUNTAJE: 0,2**", 0.2),
        ("Las 2 cifras coinciden con el 1 de marzo.\nPUNTAJE: 1", 1.0),
        ("0.8", 0.8),
        ("I need to evaluate the 3 figures", None),
        ("PUNTAJE: 7", None),
        ("", None),
    ],
)
def test_el_puntaje_del_juez_sale_de_la_linea_final(texto: str, esperado: float | None) -> None:
    assert parse_judge_score(texto) == esperado


def test_una_sugerencia_de_seguimiento_no_cuenta_como_respuesta() -> None:
    """Caso real del 01-oct: aprobó porque la tasa estaba en una sugerencia."""
    respuesta = (
        "**La línea de pobreza subió 32% en un año.** La canasta básica también.\n\n"
        "**¿Querés profundizar?**\n"
        "- ¿Cómo evolucionó la tasa de pobreza (% de personas/hogares) en el mismo período?\n"
    )
    q = assess(_entry("complex_004"), respuesta, SERIES)
    assert not q.passed
    assert any("tasa de pobreza" in f for f in q.failures)


def test_lo_prohibido_se_busca_tambien_en_las_sugerencias() -> None:
    respuesta = "El PBI creció 4,2 %.\n- ¿Querés ver el EMAE de este mes?"
    assert not assess(_entry("series_003"), respuesta, SERIES).passed
