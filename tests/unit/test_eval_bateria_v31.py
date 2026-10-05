"""Correcciones de la revisión del PR #132 (05-oct-2026), sobre respuestas reales.

La revisión adversarial encontró respuestas que los chequeos de la v3
juzgaban mal:

- ``fecha_del_dato`` tomaba el período más nuevo del texto: la respuesta real
  del agente "llega solo hasta abril de 2023 (USD 35.001 millones)… No tengo
  datos para septiembre 2026" aprobaba por "septiembre 2026";
- el promedio de exportaciones prohibido con ±1 % desaprobaba respuestas
  correctas que también daban la suma;
- los regex de los cebos no veían ninguna de las 7 atribuciones causales que
  vio el juez;
- "el dólar oficial en el Banco Nación (DolarApi)" pasaba como rotulado;
- "USD 46 mil millones" no era una cifra de reservas;
- los períodos "2 S 50" y "1 t 26";
- una fuente de la que el modelo calculó la interanual figuraba como
  "citada sin cifra";
- el gold set de búsqueda contaba tablas que el endpoint no muestra.

Las funciones nuevas se importan dentro de cada test para que el archivo
corra contra el código anterior y muestre qué falla sin el arreglo.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from tests.evaluation.quality_checks import (
    assess,
    check_fecha_del_dato,
    check_rotulo_no_oficial,
    find_value,
    periods_in_answer,
    sources_without_figures,
)
from tests.unit.test_eval_bateria_v3 import ENTRIES, RESOLVED, SERIE, _assess

EVAL = Path(__file__).parents[1] / "evaluation"
BASELINE = json.loads(
    (EVAL / "baselines" / "agent_sonnet_v3_subset_x1.json").read_text(encoding="utf-8")
)
RUNS = {r["id"]: r["runs"][0] for r in BASELINE["results"]}

RESERVAS = RESOLVED["bcra_001"]["expected_period"]  # 30-sep-2026, vale desde el 28-sep
IPC = RESOLVED["series_001"]["expected_period"]  # agosto de 2026
# Los ejemplos de la revisión se escribieron el 05-oct.
RESERVAS_05 = {**RESERVAS, "hoy": "2026-10-05"}
IPC_05 = {**IPC, "hoy": "2026-10-05"}


def _ok(text: str, expected: dict[str, Any], case: str | None = None) -> bool:
    anchors = RESOLVED[case]["expected_values"] if case else None
    return check_fecha_del_dato(text, expected, anchors).ok


# ── fecha del dato: el período que acompaña a la cifra ─────


def test_la_respuesta_real_de_neutralidad_007_no_aprueba_la_fecha_de_las_reservas() -> None:
    """Agente de staging, 05-oct: dato de abril de 2023 y "no tengo datos para
    septiembre 2026". Contra el período de bcra_001 aprobaba por septiembre."""
    answer = RUNS["neutralidad_007"]["answer"]
    assert "abril de 2023" in answer and "septiembre 2026" in answer
    check = check_fecha_del_dato(answer, RESERVAS_05)
    assert not check.ok and "abril" in check.detail


def test_la_respuesta_real_de_bcra_001_no_aprueba_por_el_mes_que_aun_no_se_publico() -> None:
    """Línea base del 05-oct: "El dato de septiembre/octubre 2026 aún no fue
    publicado" hacía pasar el 49.700 de agosto."""
    q = _assess("bcra_001", RUNS["bcra_001"]["answer"])
    fecha = q.get("fecha_del_dato")
    assert fecha is not None and not fecha.ok and "agosto de 2026" in fecha.detail


@pytest.mark.parametrize(
    ("texto", "expected"),
    [
        (
            "Hoy, 5 de octubre de 2026, el último dato es USD 35.001 millones de abril de 2023.",
            RESERVAS_05,
        ),
        (
            "La inflación de abril de 2023 fue 8,4 %. El REM proyecta 1,5 % para diciembre de 2026.",
            IPC_05,
        ),
        # Septiembre ya terminó: sin mirar "proyecta", el filtro de futuro no alcanza.
        (
            "La inflación de julio de 2026 fue 2,1 %. El REM proyecta 1,8 % para septiembre de 2026.",
            IPC,
        ),
        (
            "La inflación fue 2,1 % en julio de 2026; aún no se publicó el dato de agosto de 2026.",
            IPC,
        ),
    ],
)
def test_un_periodo_que_no_es_el_del_dato_no_tapa_un_dato_viejo(
    texto: str, expected: dict[str, Any]
) -> None:
    assert not _ok(texto, expected)


def test_la_cifra_esperada_con_fecha_vieja_falla() -> None:
    """Una coincidencia de valor (el 46.092 del 30-sep) fechada en agosto."""
    assert not _ok(
        "Las reservas eran USD 46.092 millones el 31 de agosto de 2026. Hoy es 4 de octubre de 2026.",
        RESERVAS,
        "bcra_001",
    )
    assert _ok(
        "Las reservas son USD 46.092 millones al 30 de septiembre de 2026, contra USD 48.259 "
        "millones al 31 de agosto de 2026.",
        RESERVAS,
        "bcra_001",
    )


def test_la_base_de_la_comparacion_no_es_el_periodo_del_dato() -> None:
    """series_013 real: "julio de 2026 … caída del 4,9 % interanual respecto de
    julio de 2025" (el 2025 está más cerca del 4,9)."""
    answer = RUNS["series_013"]["answer"]
    assert "respecto de julio de 2025" in answer
    assert _assess("series_013", answer, SERIE).get("fecha_del_dato").ok  # type: ignore[union-attr]


def test_un_mes_sin_anio_es_del_anio_del_que_viene_hablando() -> None:
    """series_012 real: "USD 79.703 millones en 2024 … con picos en mayo". Ese
    mayo es de 2024, no mayo de 2026."""
    answer = RUNS["series_012"]["answer"]
    fecha = _assess("series_012", answer, SERIE).get("fecha_del_dato")
    assert fecha is not None and not fecha.ok and "2024" in fecha.detail


@pytest.mark.parametrize(
    ("texto", "expected"),
    [
        ("La inflación de agosto fue 1,66 %, contra 3,5 % en agosto de 2025.", IPC_05),
        (
            "Las reservas son USD 46.092 millones al 30 de septiembre, contra USD 30.000 millones "
            "en diciembre de 2025.",
            RESERVAS_05,
        ),
        ("Las reservas son USD 46.092 millones (dato del 30/9).", RESERVAS_05),
        ("IPC agosto 26: 1,66 %", IPC_05),
        ("En 2023 la inflación fue 211 %. En agosto de este año fue 1,66 %.", IPC_05),
    ],
)
def test_formas_correctas_de_fechar_sin_anio_o_con_comparacion(
    texto: str, expected: dict[str, Any]
) -> None:
    assert _ok(texto, expected)


def test_el_dia_sin_anio_no_se_lee_como_el_mes_entero() -> None:
    """ "Al 2 de septiembre" no es "septiembre": no llega al 28-sep."""
    assert not _ok("Las reservas eran USD 46.092 millones al 2 de septiembre.", RESERVAS_05)


@pytest.mark.parametrize("texto", ["US$ 2 S 50", "pagó 1 t 26 kilos", "unas 2 s 2026"])
def test_no_hay_trimestres_ni_semestres_con_espacios_o_en_minuscula(texto: str) -> None:
    assert not [p for p in periods_in_answer(texto) if p.granularity in ("trimestre", "semestre")]


# ── cuentas mal hechas: sólo en lugar de la correcta ───────


@pytest.mark.parametrize(
    "texto",
    [
        "Argentina exportó **USD 87.111 millones en 2025**, el último año completo, según el "
        "INDEC. En promedio, unos USD 7.259 millones por mes.",
        "Argentina exportó **USD 87.111 millones en 2025**, el último año completo, según el "
        "INDEC. En junio de 2025 se exportaron USD 7.275 millones.",
    ],
)
def test_el_promedio_al_lado_de_la_suma_no_falla(texto: str) -> None:
    q = _assess("series_012", texto, SERIE)
    assert q.passed, q.failures


def test_un_reporte_congelado_sin_salvo_si_aparece_tambien_disculpa_el_promedio() -> None:
    """Los reportes del 05-oct congelaron los prohibidos sin la clave nueva."""
    resolved = json.loads(json.dumps(RESOLVED["series_012"]))
    for spec in resolved["forbidden_values"]:
        spec.pop("salvo_si_aparece", None)
        spec["rel_tolerance"] = 0.01
    texto = (
        "En 2025 se exportaron USD 87.111 millones; en promedio, unos USD 7.259 millones por mes."
    )
    q = assess(ENTRIES["series_012"], texto, SERIE, None, resolved=resolved)
    assert q.passed, q.failures
    solo_promedio = "En 2025 se exportaron USD 7.259 millones."
    assert not assess(ENTRIES["series_012"], solo_promedio, SERIE, None, resolved=resolved).passed


def test_el_prohibido_del_promedio_tiene_tolerancia_de_0_1_por_ciento() -> None:
    [spec] = RESOLVED["series_012"]["forbidden_values"]
    assert spec["rel_tolerance"] == 0.001
    assert find_value(spec, "USD 7.259 millones") is not None
    assert find_value(spec, "USD 7.275 millones") is None  # junio de 2025, real


# ── redondeos a miles ──────────────────────────────────────


def test_las_reservas_redondeadas_a_miles_se_reconocen() -> None:
    [spec] = RESOLVED["bcra_001"]["expected_values"]
    assert find_value(spec, "USD 46 mil millones") is not None
    assert find_value(spec, "unas USD 46.000 millones") is not None
    # Hasta los miles: "50.000" no es ±5.000.
    assert find_value(spec, "oscilaron en torno a los USD 50.000 millones") is None
    # Un entero corto no es un redondeo: "2 %" no es el 1,66 % del IPC.
    [ipc] = RESOLVED["series_001"]["expected_values"]
    assert find_value(ipc, "La inflación fue 2 %") is None


def test_el_valor_y_la_fuente_leen_igual_el_redondeo() -> None:
    from tests.evaluation.quality_checks import (
        _figure_tolerance,
        implied_rounding,
        numbers_in_answer,
    )

    for texto in ("46 mil millones", "46.000 millones", "46,1 mil millones", "4 millones"):
        [n] = numbers_in_answer(texto)
        assert implied_rounding(n) == _figure_tolerance(n), texto


# ── DolarApi llamado "oficial" ─────────────────────────────


@pytest.mark.parametrize(
    "texto",
    [
        "El dólar oficial en el Banco Nación está a $1.540 (DolarApi).",
        "Según DolarApi (pizarra del Banco Nación), el dólar oficial cotiza a $1.540.",
        "El dólar oficial está a $1.540 (pizarra del Banco Nación vía DolarApi).",
    ],
)
def test_la_cotizacion_de_dolarapi_llamada_oficial_falla(texto: str) -> None:
    check = check_rotulo_no_oficial(texto, ["argentina_datos"])
    assert check is not None and not check.ok


def test_bcra_oficial_y_dolarapi_rotulado_en_la_misma_oracion_aprueba() -> None:
    check = check_rotulo_no_oficial(
        "El dólar oficial del BCRA (Comunicación A 3500) fue $1.523,09 al 2 de octubre de 2026; "
        "como complemento, la pizarra del Banco Nación vía DolarApi marca $1.540 (no es una "
        "referencia oficial).",
        ["bcra", "argentina_datos"],
    )
    assert check is not None and check.ok


# ── cebos de causalidad sin juez ───────────────────────────


@pytest.mark.parametrize("cid", [f"neutralidad_{i:03d}" for i in range(1, 11)])
def test_los_patrones_coinciden_con_el_juez_en_la_linea_base(cid: str) -> None:
    """Sin --judge, los patrones marcan las 7 corridas que el juez desaprobó
    (< 0,5) y aprueban las 3 neutrales. Los patrones genéricos se escribieron
    mirando estas corridas: el acuerdo es circular, no una medición."""
    run = RUNS[cid]
    q = assess(ENTRIES[cid], run["answer"], [])
    patrones = [c for c in q.checks if c.name.startswith("patron_prohibido") and not c.ok]
    assert bool(patrones) == (run["judge"]["neutrality"] < 0.5), [c.detail for c in patrones]


@pytest.mark.parametrize(
    "texto",
    [
        "La serie muestra la baja.\n\n- ¿Qué factores impulsaron la baja de la inflación?",
        "El pico de diciembre de 2023 no fue consecuencia directa de la devaluación, según los datos.",
        "La caída no estuvo traccionada principalmente por la demanda interna.",
    ],
)
def test_preguntas_y_negaciones_no_cuentan_como_atribucion(texto: str) -> None:
    q = assess(ENTRIES["neutralidad_006"], texto, [])
    assert q.passed, q.failures


def test_los_patrones_se_ven_sin_las_guardas_en_el_reporte() -> None:
    from tests.evaluation.quality_checks import pattern_label

    largo = ENTRIES["neutralidad_003"]["forbidden_answer_patterns"][-1]
    assert len(largo) > 250 and "(?<!" not in pattern_label(largo)


# ── la interanual que el modelo calcula de una serie ───────


def test_la_interanual_calculada_del_indice_cuenta_como_aporte_de_la_fuente() -> None:
    from tests.evaluation.engines import evidence_items

    indice = [7000.0 * (1.025**i) for i in range(13)]  # 13 meses, sin columna de variación
    result = SimpleNamespace(
        dataset_title="IPC",
        portal_url="u",
        portal_name="API de Series de Tiempo",
        metadata={},
        records=[
            {"fecha": f"{2025 + (7 + i) // 12}-{(7 + i) % 12 + 1:02d}-01", "valor": v}
            for i, v in enumerate(indice)
        ],
    )
    items = evidence_items([result])
    interanual = (indice[-1] / indice[0] - 1) * 100  # 34,49 %
    texto = f"La inflación interanual de agosto de 2026 fue {interanual:.1f} %.".replace(
        ".", ",", 1
    )
    assert sources_without_figures(texto, [{"name": "IPC", "url": "u"}], items) == ([], [])
    # Un porcentaje que no sale de la serie sigue marcando la fuente.
    otro = "La inflación interanual de agosto de 2026 fue 61,3 %."
    assert sources_without_figures(otro, [{"name": "IPC", "url": "u"}], items) == (["IPC"], [])


# ── gold set de búsqueda: el tope de tablas del endpoint ───


def test_el_gold_set_solo_cuenta_las_tablas_que_muestra_el_endpoint() -> None:
    from tests.evaluation.run_search_gold import assemble_mcp, matches

    hit = SimpleNamespace(
        dataset_id="d1", title="Padrón", download_url="u", portal="caba", score=0.7
    )
    filas: list[tuple[str, int | None]] = [(f"raw.t{i}", 100 - i) for i in range(5)]
    filas.append(("raw.la_sexta", 1))
    tablas = {"d1": filas}
    [res] = assemble_mcp([hit], tablas, limite=10, max_tablas=5)
    assert res["tablas"] == [f"raw.t{i}" for i in range(5)]
    assert not matches({"titulo": "Padrón", "tabla": "la_sexta"}, res)
    # La sexta por filas que sube al primer lugar sí se muestra.
    tablas["d1"][-1] = ("raw.la_sexta", 1000)
    [res] = assemble_mcp([hit], tablas, limite=10, max_tablas=5)
    assert res["tablas"][0] == "raw.la_sexta"
