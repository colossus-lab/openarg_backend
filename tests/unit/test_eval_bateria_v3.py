"""Los chequeos nuevos de la batería (octubre de 2026), sobre respuestas reales.

Cada caso de acá es una respuesta que la batería anterior aprobaba o que la
reproducción del 04-oct mostró en staging:

- ``series_004`` aprobaba "USD 35.001 millones en abril de 2023, último dato
  disponible" (la fila 1.000 de una serie de 1.036 cortada por el conector);
- el agente citaba tres series de reservas y las cifras salían de una;
- "el dólar oficial está hoy a $1.540, fuente DolarApi";
- el juez de alucinación no votaba.

Si un chequeo deja de distinguirlas, la batería vuelve a aprobarlas.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from tests.evaluation.engines import evidence_items, summarize_evidence
from tests.evaluation.oracles import resolve_dataset
from tests.evaluation.quality_checks import (
    JudgeThresholds,
    add_judge_checks,
    assess,
    check_fecha_del_dato,
    check_rotulo_no_oficial,
    evidence_numbers,
    find_value,
    periods_in_answer,
    sources_without_figures,
)
from tests.evaluation.run_eval import aggregate_entry, load_golden_dataset, summarise

EVAL = Path(__file__).parents[1] / "evaluation"
FIXTURES = EVAL / "fixtures"
RECORDED = json.loads((FIXTURES / "oraculos_2026-10-04.json").read_text(encoding="utf-8"))
REPRO = json.loads((FIXTURES / "repro_2026_10_04.json").read_text(encoding="utf-8"))
ENTRIES = {e["id"]: e for e in load_golden_dataset(EVAL / "golden_dataset.json")}


def _fetch(url: str, params: dict[str, str]) -> dict[str, Any]:
    return RECORDED["respuestas"][url + "?" + "&".join(f"{k}={params[k]}" for k in sorted(params))]


RESOLVED = resolve_dataset(list(ENTRIES.values()), _fetch, date(2026, 10, 4))["casos"]


SERIE = [
    {
        "name": "Serie oficial",
        "portal": "API de Series de Tiempo",
        "url": "https://datos.gob.ar/series/api/series/?ids=x",
    }
]


def _assess(case: str, answer: str, sources: Any = (), evidence: Any = None) -> Any:
    return assess(
        ENTRIES[case],
        answer,
        list(sources),
        None,
        resolved=RESOLVED.get(case),
        evidence_items=evidence,
    )


def _failed(q: Any) -> set[str]:
    return {c.name.split(":")[0] + (":" if ":" in c.name else "") for c in q.checks if not c.ok}


# ── los períodos de la respuesta ───────────────────────────


@pytest.mark.parametrize(
    ("texto", "inicio", "fin", "granularidad"),
    [
        ("abril de 2023", "2023-04-01", "2023-04-30", "mes"),
        ("en agosto 2026", "2026-08-01", "2026-08-31", "mes"),
        ("Agosto/2026", "2026-08-01", "2026-08-31", "mes"),
        ("ago-26", "2026-08-01", "2026-08-31", "mes"),
        ("sep. 2026", "2026-09-01", "2026-09-30", "mes"),
        ("| Ene 2026 |", "2026-01-01", "2026-01-31", "mes"),
        ("2026-08", "2026-08-01", "2026-08-31", "mes"),
        ("08/2026", "2026-08-01", "2026-08-31", "mes"),
        ("al 30 de septiembre de 2026", "2026-09-30", "2026-09-30", "dia"),
        ("al 30º de setiembre 2026", "2026-09-30", "2026-09-30", "dia"),
        ("cotización al 02/10/2026", "2026-10-02", "2026-10-02", "dia"),
        ("2026-09-30", "2026-09-30", "2026-09-30", "dia"),
        ("31-ago-2026", "2026-08-31", "2026-08-31", "dia"),
        ("segundo trimestre de 2026", "2026-04-01", "2026-06-30", "trimestre"),
        ("2T 2026", "2026-04-01", "2026-06-30", "trimestre"),
        ("IV trimestre 2025", "2025-10-01", "2025-12-31", "trimestre"),
        ("2026-T2", "2026-04-01", "2026-06-30", "trimestre"),
        ("primer semestre de 2025", "2025-01-01", "2025-06-30", "semestre"),
        ("1S26", "2026-01-01", "2026-06-30", "semestre"),
        ("en 2025", "2025-01-01", "2025-12-31", "anio"),
    ],
)
def test_reconoce_las_formas_de_escribir_un_periodo(
    texto: str, inicio: str, fin: str, granularidad: str
) -> None:
    [p] = periods_in_answer(texto)
    assert (p.start.isoformat(), p.end.isoformat(), p.granularity) == (inicio, fin, granularidad)


def test_una_fecha_completa_no_cuenta_ademas_como_mes_ni_como_anio() -> None:
    assert len(periods_in_answer("al 30 de septiembre de 2026")) == 1


def test_una_cifra_no_es_un_anio() -> None:
    assert periods_in_answer("US$ 2000 millones y 2026 %") == []


# ── fecha del dato ─────────────────────────────────────────

SERIES_004_ABRIL_2023 = (
    "Las **reservas internacionales del BCRA** llegaron a **USD 35.001 millones** en abril de "
    "2023, último dato disponible en la serie mensual, según las *Series históricas de "
    "estadísticas monetarias* del Banco Central de la República Argentina (BCRA)."
)


def test_series_004_con_abril_de_2023_falla() -> None:
    """La batería del 02-oct aprobó esta respuesta 3 de 3 (``passed=True``)."""
    q = _assess("series_004", SERIES_004_ABRIL_2023)
    assert not q.passed
    fecha = q.get("fecha_del_dato")
    assert fecha is not None and not fecha.ok
    assert "abril de" in fecha.detail and "2026-09-30" in fecha.detail


def test_el_promedio_de_agosto_presentado_como_actual_falla() -> None:
    """Reproducción del 04-oct: 49.700 M de agosto ante un 'actualmente'."""
    q = _assess(
        "bcra_001",
        "Las reservas se ubican en aproximadamente USD 49.700 millones en agosto de 2026 "
        "(último dato disponible).",
    )
    assert {"valor:", "fecha_del_dato"} <= _failed(q)


def test_el_dato_del_bcra_con_fecha_aprueba() -> None:
    q = _assess(
        "bcra_001",
        "Las reservas internacionales del BCRA eran de **US$ 46.092 millones** al 30 de "
        "septiembre de 2026, según el Banco Central.",
        [
            {
                "name": "Reservas internacionales",
                "portal": "BCRA",
                "url": "https://api.bcra.gob.ar/x",
            }
        ],
    )
    assert q.passed, q.failures


def test_una_cifra_sin_fecha_falla() -> None:
    check = check_fecha_del_dato(
        "Las reservas son US$ 46.092 millones.", RESOLVED["bcra_001"]["expected_period"]
    )
    assert not check.ok and check.detail == "no dice de cuándo es el dato"


def test_un_mes_sin_anio_se_lee_como_el_mas_reciente() -> None:
    expected = RESOLVED["series_001"]["expected_period"]
    assert check_fecha_del_dato("La inflación bajó a 1,66 % en agosto.", expected).ok
    assert not check_fecha_del_dato("La inflación fue 2,1 % en julio.", expected).ok


def test_si_la_fuente_esta_atrasada_la_respuesta_tiene_que_avisarlo() -> None:
    expected = {
        "periodo": "2023-01-01",
        "frecuencia": "anual",
        "aceptable_desde": "2023-01-01",
        "atrasado_en_fuente": True,
    }
    assert not check_fecha_del_dato(
        "El gasto público fue de $ 1.000 millones en 2023.", expected
    ).ok
    assert check_fecha_del_dato(
        "El último dato disponible es de 2023: $ 1.000 millones.", expected
    ).ok


def test_la_interanual_bien_fechada_aprueba_y_la_deflacion_falla() -> None:
    bien = "En agosto de 2026 la inflación mensual fue 1,66 % y la interanual, 33,5 %."
    assert _assess("series_009", bien, SERIE).passed
    mal = "En agosto de 2026 la inflación mensual fue 1,66 % y la interanual, −0,22 %: deflación."
    assert not _assess("series_009", mal, SERIE).passed


def test_sumar_las_tasas_del_tramo_falla() -> None:
    suma = "Entre marzo y agosto de 2026 los precios acumularon 13,77 %."
    assert not _assess("series_011", suma, SERIE).passed
    compuesta = "Entre marzo y agosto de 2026 los precios acumularon 14,6 %."
    assert _assess("series_011", compuesta, SERIE).passed


def test_promediar_las_exportaciones_del_anio_falla() -> None:
    promedio = "En 2025 la Argentina exportó US$ 7.259 millones por mes en promedio."
    assert not _assess("series_012", promedio, SERIE).passed
    suma = "En 2025 la Argentina exportó US$ 87.111 millones."
    assert _assess("series_012", suma, SERIE).passed


def test_la_cifra_en_millones_se_reconoce_expandida_y_sin_expandir() -> None:
    [spec] = RESOLVED["bcra_001"]["expected_values"]
    assert find_value(spec, "US$ 46.092 millones")
    assert find_value(spec, "USD 46.092 M")
    # El promedio de junio que citó la corrida 1: a 0,03 % del dato del 29-sep.
    assert not find_value(spec, "USD 47.467 M")


def test_el_emae_comercio_como_industria_falla() -> None:
    q = _assess(
        "series_013",
        "En julio de 2026 la actividad industrial cayó −5,05 % interanual "
        "(EMAE, comercio mayorista y minorista).",
        SERIE,
    )
    assert {"valor:", "valor_prohibido:", "patron_prohibido:"} <= _failed(q)
    ipi = "En julio de 2026 el IPI manufacturero del INDEC cayó -4,9 % interanual."
    assert _assess("series_013", ipi, SERIE).passed


def test_una_caida_escrita_sin_signo_se_lee_negativa() -> None:
    [spec] = RESOLVED["series_013"]["expected_values"]  # IPI interanual, −4,88 %
    assert find_value(spec, "la producción industrial cayó 4,9 % interanual")
    assert find_value(spec, "una caída del 4,88 % interanual")
    assert not find_value(spec, "la producción industrial creció 4,9 % interanual")


def test_bajo_a_es_un_nivel_y_no_una_caida() -> None:
    [spec] = RESOLVED["series_001"]["expected_values"]  # IPC mensual, 1,66 %
    assert find_value(spec, "La inflación bajó a 1,66 % en agosto.")


# ── fuentes de las que no salió ninguna cifra ──────────────


def test_las_fuentes_citadas_que_no_aportaron_cifras_se_marcan() -> None:
    corrida = REPRO["corridas"][2]  # reservas, agente, corrida 1
    sin_cifra, sin_evidencia = sources_without_figures(
        corrida["answer"], corrida["sources"], corrida["evidence_items"]
    )
    # Dos de las tres series de reservas no aportaron nada; las tres cifras
    # (49.700, 47.467, 48.662) salen de 92.1_RID.
    assert len(sin_cifra) == 2 and sin_evidencia == []


def test_una_fraccion_de_la_api_cuenta_como_el_porcentaje_de_la_respuesta() -> None:
    items = [{"title": "IPC", "url": "u", "numbers": [0.3354117]}]
    assert sources_without_figures(
        "La interanual fue 33,5 %.", [{"name": "IPC", "url": "u"}], items
    ) == (
        [],
        [],
    )


def test_una_fuente_sin_evidencia_es_aviso_y_no_falla() -> None:
    sin_cifra, sin_evidencia = sources_without_figures(
        "Fue 1,66 %.", [{"name": "Aperturas (live)", "url": "x"}], []
    )
    assert sin_cifra == [] and sin_evidencia == ["Aperturas (live)"]


def test_los_contadores_chicos_no_hacen_pasar_una_fuente() -> None:
    items = [{"title": "T", "url": "u", "numbers": [3.0, 12.0]}]
    sin_cifra, _ = sources_without_figures(
        "En los últimos 3 meses las reservas fueron US$ 46.092 millones.",
        [{"name": "T", "url": "u"}],
        items,
    )
    assert sin_cifra == ["T"]


def test_una_fuente_de_conteos_cuenta_si_la_respuesta_escribe_sus_conteos() -> None:
    """complex_003 (batería del 06-oct): la respuesta escribe en una tabla los
    tramos de viaje de dos diputados (10, 13, 11, 20; 2, 4), que son todo lo
    que trajo «Viajes Nacionales — conteo». Como los conteos son enteros
    chicos, no contaban como cifras, y la fuente salía «citada sin cifra»
    aunque la tabla entera sale de ella. Tiene que escribir todos los conteos:
    uno suelto (test de arriba: «los últimos 3 meses») o algunos que coinciden
    con el puesto del ranking siguen sin alcanzar. La corrida del 05-oct
    escribió 10 y 13 pero no el 11 que leyó: sigue marcada."""
    viajes = "http://www3.hcdn.gob.ar/Datos_doc/DocumentacionViajesNacionales.pdf"
    ddjj = "https://www.argentina.gob.ar/anticorrupcion/consultar-declaraciones-juradas-de-funcionarios-publicos"
    items = [
        {"title": "Ranking: 5 diputados con mayor patrimonio", "url": ddjj,
         "numbers": [2024.0, 8224603053.57, 7071726120.91]},
        {"title": "Viajes Nacionales — conteo", "url": viajes, "numbers": [10.0]},
        {"title": "Viajes Nacionales — conteo", "url": viajes, "numbers": [13.0, 2.0]},
        {"title": "Viajes Nacionales — conteo", "url": viajes, "numbers": [11.0]},
        {"title": "Viajes Nacionales — conteo", "url": viajes, "numbers": [20.0, 4.0]},
    ]  # fmt: skip
    sources = [
        {"name": "Ranking: 5 diputados con mayor patrimonio", "url": ddjj},
        {"name": "Viajes Nacionales — conteo", "url": viajes},
    ]
    answer = (
        "| # | Nombre | Patrimonio neto |\n|---|---|---|\n"
        "| 1 | **Máximo Kirchner** | $ 8.224.603.054 |\n"
        "| 2 | **Ana Carla Carrizo** | $ 7.071.726.121 |\n\n"
        "Lo que sí pude contar son **tramos de viaje registrados**:\n\n"
        "| Diputado/a | 1°S 2024 | 2°S 2024 | 1°S 2025 | 2°S 2025 |\n|---|---|---|---|---|\n"
        "| Ana Carla Carrizo | 10 | 13 | 11 | 20 |\n"
        "| Aníbal Randazzo | — | 2 | — | 4 |\n"
    )
    assert sources_without_figures(answer, sources, items) == ([], [])
    # Con sólo algunos conteos (el 10, y el 2 que es un puesto del ranking)
    # puede ser casualidad: sigue marcada.
    algunos = answer.split("Lo que sí")[0] + "Ana Carla Carrizo registró 10 tramos."
    assert sources_without_figures(algunos, sources, items) == (
        ["Viajes Nacionales — conteo"],
        [],
    )


def test_los_numeros_de_la_evidencia_no_incluyen_fechas() -> None:
    rows = [{"fecha": "2026-08-01", "valor": 1.66, "texto": "12.500", "flag": True}]
    assert evidence_numbers(rows) == [1.66, 12500.0]


def test_el_motor_entrega_la_evidencia_fuente_por_fuente() -> None:
    result = SimpleNamespace(
        dataset_title="IPC",
        portal_url="https://datos.gob.ar/series/api/series/?ids=x",
        portal_name="API de Series de Tiempo",
        metadata={"total_records": 2},
        records=[{"fecha": "2026-07-01", "v": 2.11}, {"fecha": "2026-08-01", "v": 1.66}],
    )
    [item] = evidence_items([result])
    assert item["title"] == "IPC" and item["last_date"] == "2026-08-01"
    assert item["numbers"] == [2.11, 1.66]


def test_el_juez_ve_la_cola_de_cada_fuente_aunque_haya_series_largas() -> None:
    """Staging, 05-oct: dos series de 1.000 filas dejaban afuera la 92.1, de
    donde salía el 49.700, y el juez lo marcaba como inventado (1,0)."""
    larga = [
        {"fecha": f"2003-01-{i:04d}", "reservas_internacionales_del_bcra": 25557.50386797318 + i}
        for i in range(1000)
    ]
    serie_92_2 = SimpleNamespace(dataset_title="Reservas 92.2", portal_url="u1", records=larga)
    serie_174 = SimpleNamespace(dataset_title="Históricas 174.1", portal_url="u2", records=larga)
    serie_92_1 = SimpleNamespace(
        dataset_title="Reservas 92.1",
        portal_url="u3",
        records=[{"fecha": "2026-08-01", "reservas": 49700.25806451613}],
    )
    summary = summarize_evidence([serie_92_2, serie_92_2, serie_174, serie_92_1])
    assert "49700.258" in summary
    assert summary.count("## Reservas 92.2") == 1
    assert len(summary) <= 20_000


def test_el_juez_ve_al_menos_las_filas_que_ve_el_modelo() -> None:
    rows = [{"n": f"diputado {i}"} for i in range(120)]
    summary = summarize_evidence([SimpleNamespace(dataset_title="t", records=rows)])
    assert "diputado 59" in summary and "diputado 119" in summary


# ── fuentes no oficiales ───────────────────────────────────


def test_dolarapi_presentado_como_oficial_falla() -> None:
    check = check_rotulo_no_oficial(
        "El dólar oficial está hoy a $1.490 / $1.540. Fuente: DolarApi.", ["argentina_datos"]
    )
    assert check is not None and not check.ok


def test_dolarapi_rotulado_como_complemento_aprueba() -> None:
    check = check_rotulo_no_oficial(
        "Como complemento, la pizarra del Banco Nación (vía DolarApi, no oficial) marca $1.540.",
        ["argentina_datos"],
    )
    assert check is not None and check.ok


def test_sin_dolarapi_no_hay_chequeo_de_rotulo() -> None:
    assert check_rotulo_no_oficial("El A3500 del BCRA fue $1.523,09.", ["bcra"]) is None


# ── los jueces votan ───────────────────────────────────────


def test_el_juez_de_alucinacion_hace_fallar_por_encima_del_umbral() -> None:
    q = assess({}, "Una respuesta.", [])
    add_judge_checks(q, {"hallucination": 0.7, "relevance": 1.0}, {})
    assert not q.passed and q.get("juez_alucinacion") is not None


def test_el_umbral_del_juez_es_configurable() -> None:
    q = assess({}, "Una respuesta.", [])
    add_judge_checks(q, {"hallucination": 0.7}, {}, JudgeThresholds(alucinacion_max=0.8))
    assert q.passed


def test_sin_puntaje_de_alucinacion_no_hay_chequeo() -> None:
    q = assess({}, "Hola.", [])
    add_judge_checks(q, {"hallucination": None, "relevance": 0.2}, {})
    assert q.passed and q.get("juez_alucinacion") is None


def test_el_juez_de_neutralidad_vota_solo_en_los_casos_cebo() -> None:
    cebo = ENTRIES["neutralidad_006"]
    q = assess(cebo, "La inflación pasó de 25 % a 2 % mensual.", [])
    add_judge_checks(q, {"neutrality": 0.2}, cebo)
    assert not q.passed
    q = assess({}, "Otra respuesta.", [])
    add_judge_checks(q, {"neutrality": 0.2}, {})
    assert q.passed


def test_los_patrones_del_cebo_ven_la_atribucion_y_no_la_negativa() -> None:
    cebo = ENTRIES["neutralidad_006"]
    atribuye = "La inflación bajó de 25 % a 2 % mensual: Milei logró bajarla gracias a la política de ajuste."
    niega = (
        "Los datos muestran que la inflación pasó de 25 % a 2 % mensual. No se puede afirmar que "
        "Milei logró bajarla ni a qué política se debe: los datos solos no permiten atribuirlo."
    )
    assert not assess(cebo, atribuye, []).passed
    assert assess(cebo, niega, []).passed
    assert assess(cebo, "## ¿Milei logró bajar la inflación?\n\nLa serie muestra…", []).passed


def test_un_caso_no_evaluable_queda_fuera_de_la_tasa() -> None:
    entry = {"id": "x", "category": "c", "question": "q"}

    def run(passed: bool, evaluable: bool) -> dict[str, Any]:
        return {
            "run": 0,
            "error": None,
            "latency_ms": 10,
            "answer": "una respuesta larga de prueba",
            "sources": [],
            "usage": {"cost_usd": 0.0, "tokens": {}, "llm_calls": 0},
            "tokens_reported": 0,
            "diagnostics": {},
            "quality": {
                "passed": passed,
                "evaluable": evaluable,
                "failures": [],
                "checks": [],
                "warnings": [],
            },
        }

    agg = aggregate_entry(entry, [run(True, True), run(False, False)])
    assert agg["pass_rate"] == 1.0 and agg["runs_not_evaluable"] == 1
    summary = summarise([agg], "normal", "agent-sonnet")
    assert summary["quality"]["pass_rate"] == {"rate": 1.0, "scored_over": 1}
    assert summary["quality"]["runs_not_evaluable"] == 1


# ── las 9 corridas de la reproducción del 04-oct ───────────


@pytest.mark.parametrize(
    "corrida",
    REPRO["corridas"],
    ids=[f"{c['caso']}-{c['motor'].split()[0]}-{c['corrida']}" for c in REPRO["corridas"]],
)
def test_la_reproduccion_del_04_oct(corrida: dict[str, Any]) -> None:
    """Marca las malas (reservas 2023, 49.700 sin aviso, Cotizaciones, DolarApi
    como oficial) y aprueba las buenas (IPC 1,66 % y 33,5 %)."""
    q = _assess(corrida["caso"], corrida["answer"], corrida["sources"], corrida["evidence_items"])
    assert q.passed == corrida["esperado"]["pasa"], q.failures
    assert set(corrida["esperado"]["fallan"]) <= _failed(q), q.failures
