"""El verificador de cifras del agente, con casos de la reproducción del 04-oct.

La evidencia imita lo que devuelven los conectores: la API de Series de Tiempo
escala ``percent_change`` a puntos (1,66) pero deja la interanual como
fracción (0,3354117); la reservas 92.1 viene en "Millones de dólares".
"""

from __future__ import annotations

from typing import Any

import pytest

from app.application.answers import verification as v
from app.application.answers.verification import (
    build_citations,
    correction_note,
    extract_figures,
    seen_numbers,
    select_evidence,
    unverified_notice,
    verify_figures,
)
from app.domain.entities.connectors.data_result import DataResult

IPC = "IPC Nacional, Nivel General"


def _dr(title: str, records: list[dict[str, Any]], url: str = "", **meta: Any) -> DataResult:
    return DataResult(
        source="series_tiempo",
        portal_name="API de Series de Tiempo",
        portal_url=url or f"https://datos.gob.ar/series/api/series/?ids={title[:8]}",
        dataset_title=title,
        format="time_series",
        records=records,
        metadata={"units": "", **meta},
    )


def _ipc_mensual() -> DataResult:
    # Agosto de 2025 a agosto de 2026, variación mensual en puntos.
    values = [1.88, 2.08, 2.34, 2.47, 2.84, 2.88, 2.91, 3.38, 2.58, 2.15, 1.89, 2.11, 1.66]
    months = [f"2025-{m:02d}-01" for m in range(8, 13)] + [f"2026-{m:02d}-01" for m in range(1, 9)]
    return _dr(
        "Índice de Precios al Consumidor Nacional (IPC). Base diciembre 2016.",
        [{"fecha": f, IPC: x} for f, x in zip(months, values, strict=True)],
        representation="percent_change",
        unit="percent",
    )


def _ipc_interanual() -> DataResult:
    return _dr(
        "Índice de Precios al Consumidor Nacional (IPC). Base diciembre 2016.",
        [{"fecha": "2026-07-01", IPC: 0.3241}, {"fecha": "2026-08-01", IPC: 0.3354117291414028}],
        url="https://datos.gob.ar/series/api/series/?ids=148.3_INIVELNAL_DICI_M_26&i",
        representation="percent_change_a_year_ago",
        units="Índice",
    )


def _reservas_921() -> DataResult:
    return _dr(
        "Reservas internacionales y pasivos del BCRA",
        [
            {"fecha": "2026-06-01", "Reservas": 47467.31},
            {"fecha": "2026-07-01", "Reservas": 48661.88},
            {"fecha": "2026-08-01", "Reservas": 49700.26},
        ],
        url="https://datos.gob.ar/series/api/series/?ids=92.1_RID_0_0_32",
        units="Millones de dólares",
    )


def _status(answer: str, evidence: list[DataResult], **kw: Any) -> dict[str, str]:
    check = verify_figures(answer, evidence, **kw)
    return {c.figure.raw: c.status for c in check.checks}


# ── normalización ──────────────────────────────────────────


def test_33_5_por_ciento_contra_la_fraccion_0_3354() -> None:
    """La interanual llega como fracción con unidades «Índice»: el 33,5 % correcto
    no puede quedar marcado."""
    assert _status("La interanual fue de **33,5%**.", [_ipc_interanual()]) == {"33,5%": "directa"}
    assert _status("La interanual fue de 33,54 %.", [_ipc_interanual()]) == {"33,54 %": "directa"}
    assert _status("La interanual fue de 34 %.", [_ipc_interanual()]) == {"34 %": "directa"}
    assert _status("La interanual fue de 33,6 %.", [_ipc_interanual()]) == {
        "33,6 %": "sin_respaldo"
    }


def test_formato_argentino_e_ingles() -> None:
    evidence = [_dr("Tabla", [{"monto": 1234.5}, {"texto": "12.500"}, {"otro": "1.543,18"}])]
    assert _status("Fueron 1.234,5 pesos.", evidence) == {"1.234,5": "directa"}
    assert _status("Fueron 1,234.5 pesos.", evidence) == {"1,234.5": "directa"}
    # "12.500" en la tabla es doce mil quinientos, no 12,5.
    assert _status("Hay 12.500 casos.", evidence) == {"12.500": "directa"}
    assert _status("El precio es $1.543,18.", evidence) == {"1.543,18": "directa"}


def test_49700_millones_contra_49700_26() -> None:
    """Transcripción redondeada de la reproducción (corrida 1 de reservas)."""
    answer = (
        "Las reservas se ubican en **aproximadamente USD 49.700 millones** en agosto de 2026. "
        "En junio eran USD 47.467 M, en julio USD 48.662 M y en agosto USD 49.700 M."
    )
    assert set(_status(answer, [_reservas_921()]).values()) == {"directa"}


def test_aproximadamente_admite_el_redondeo_pero_no_una_cuenta_mal_hecha() -> None:
    assert _status("Unos USD 50.000 millones.", [_reservas_921()]) == {"50.000 millones": "directa"}
    assert _status("Casi 50 mil millones de dólares.", [_reservas_921()]) == {
        "50 mil millones": "directa"
    }
    assert _status("Más de USD 49.000 millones.", [_reservas_921()]) == {
        "49.000 millones": "directa"
    }
    # Sin "aproximadamente", 50.000 no es 49.700.
    assert _status("Son USD 50.000 millones.", [_reservas_921()]) == {
        "50.000 millones": "sin_respaldo"
    }
    # El «≈14 %» de la batería: la suma de tasas (13,77) redondeada, y la
    # acumulada real es 14,58.
    assert _status("Acumula ≈14 % en seis meses.", [_ipc_mensual()]) == {"14 %": "sin_respaldo"}


# ── lo que tiene que atrapar ───────────────────────────────


def test_menos_0_22_interanual_por_resta_de_tasas_no_tiene_respaldo() -> None:
    """El −0,22 % del pipeline viejo: 1,66 − 1,88, dos mensuales restadas."""
    answer = "La mensual de agosto fue 1,66 % y la interanual, **-0,22 %**."
    status = _status(answer, [_ipc_mensual()])
    assert status == {"1,66 %": "directa", "-0,22 %": "sin_respaldo"}
    # Escrita como diferencia en puntos sí es una cuenta legítima.
    ok = "En agosto la mensual fue 1,66 % contra 1,88 % un año antes: 0,22 puntos porcentuales menos."
    assert _status(ok, [_ipc_mensual()])["0,22 puntos porcentuales"] == "derivada"


@pytest.mark.parametrize("factor", [1.03, 0.97])
def test_una_cifra_corrida_un_3_por_ciento_no_tiene_respaldo(factor: float) -> None:
    wrong = f"{49700.26 * factor:,.0f}".replace(",", ".")
    status = _status(f"Las reservas son USD {wrong} millones.", [_reservas_921()])
    assert list(status.values()) == ["sin_respaldo"]


def test_signo_invertido() -> None:
    evidence = [_ipc_mensual()]
    assert _status("La inflación de agosto fue -1,66 %.", evidence) == {"-1,66 %": "sin_respaldo"}
    assert _status("En agosto los precios cayeron 1,66 %.", evidence) == {"1,66 %": "sin_respaldo"}
    # "bajó a" es un nivel, no una variación negativa.
    assert _status("La inflación bajó a 1,66 % en agosto.", evidence) == {"1,66 %": "directa"}
    negativa = [_dr("EMAE", [{"fecha": "2026-07-01", "var": -1.2}])]
    assert _status("La actividad cayó un 1,2 % interanual.", negativa) == {"1,2 %": "directa"}
    assert _status("La actividad varió -1,2 %.", negativa) == {"-1,2 %": "directa"}
    assert _status("La actividad subió 1,2 %.", negativa) == {"1,2 %": "sin_respaldo"}
    # Un negativo escrito como positivo, sin ninguna señal en el texto.
    assert _status("La variación fue de 1,2 %.", negativa) == {"1,2 %": "sin_respaldo"}


# ── lo derivado que se reproduce ───────────────────────────


def test_una_diferencia_entre_cifras_de_la_respuesta() -> None:
    """El «4.181 millones por encima» del pipeline viejo: 44.516 − 40.335."""
    serie = _dr(
        "Series históricas de estadísticas monetarias",
        [
            {"fecha": "2025-11-01", "Reservas": 40335.0},
            {"fecha": "2026-02-01", "Reservas": 45566.0},
            {"fecha": "2026-04-01", "Reservas": 44516.0},
        ],
        units="Millones de dólares",
    )
    answer = (
        "Las reservas eran USD 44.516 millones en abril, desde USD 40.335 millones en "
        "noviembre: USD 4.181 millones más."
    )
    status = _status(answer, [serie])
    assert status["4.181 millones"] == "derivada"


def test_el_saldo_de_dos_columnas_de_una_fila_y_la_suma_anual() -> None:
    meses = [f"2025-{m:02d}-01" for m in range(1, 13)]
    expo = [7000.0 + i * 10 for i in range(11)] + [0.0]
    expo[-1] = 87111.2 - sum(expo[:-1])
    impo = [6300.0] * 11 + [75792.0 - 6300.0 * 11]
    serie = _dr(
        "Intercambio comercial argentino",
        [{"fecha": f, "expo": e, "impo": i} for f, e, i in zip(meses, expo, impo, strict=True)],
        units="Millones de dólares",
    )
    answer = "En 2025 las exportaciones sumaron USD 87.111 M y las importaciones USD 75.792 M."
    assert set(_status(answer, [serie]).values()) == {"directa"}
    fila = _dr("Balanza 2025", [{"anio": 2025, "expo": 87111.2, "impo": 75792.0}])
    saldo = "Exportaciones 87.111 M, importaciones 75.792 M: saldo de USD 11.319 M."
    assert _status(saldo, [fila])["11.319 M"] == "derivada"


def test_las_tasas_no_se_suman() -> None:
    """La acumulada de marzo a agosto por suma de tasas (13,77) no tiene respaldo."""
    answer = (
        "De marzo a agosto la inflación mensual fue 3,38 %, 2,58 %, 2,15 %, 1,89 %, 2,11 % y "
        "1,66 %: acumula 13,77 %."
    )
    status = _status(answer, [_ipc_mensual()])
    assert status["13,77 %"] == "sin_respaldo"


def test_una_variacion_porcentual_entre_dos_niveles_citados() -> None:
    indice = _dr(
        "IPC índice",
        [{"fecha": "2026-02-01", "ipc": 10714.6255}, {"fecha": "2026-08-01", "ipc": 12276.766}],
    )
    answer = "El índice pasó de 10.714,63 en febrero a 12.276,77 en agosto: 14,58 % más."
    assert _status(answer, [indice])["14,58 %"] == "derivada"


# ── lo que no es una cifra ─────────────────────────────────


def test_anios_fechas_ids_normas_y_enteros_chicos_no_son_cifras() -> None:
    text = (
        "Serie 148.3_INIVELNAL_DICI_M_26 (base dic-2016=100), dato del 02/10/2026 y del 2 de "
        "octubre de 2026; 2.º trimestre de 2026, 4T 2024, mayorista A3500. Ley 27.430. "
        "Los últimos 12 meses, 3 de cada 10, los 5 diputados, 157 días sin datos.\n"
        "1. Primero\n2) Segundo"
    )
    assert extract_figures(text) == []


def test_lo_que_el_modelo_no_vio_no_respalda() -> None:
    """La evidencia trae la serie entera; el modelo vio las últimas filas."""
    serie = _ipc_mensual()
    visto = seen_numbers('{"filas":[{"fecha":"2026-08-01","IPC":1.66}]}')
    # 2,84 está en la serie (diciembre de 2025) pero el modelo no lo vio.
    assert _status("En agosto 1,66 %; en diciembre 2,84 %.", [serie], seen=[visto]) == {
        "1,66 %": "directa",
        "2,84 %": "sin_respaldo",
    }
    assert _status("En diciembre 2,84 %.", [serie]) == {"2,84 %": "directa"}


# ── lo que encontró la calibración en staging (04-oct) ─────


def test_una_resta_con_los_valores_redondeados_de_la_tabla() -> None:
    """series_008: el saldo de febrero, 6.140 − 5.864 = 276, con los valores
    de la API 6.139,70 y 5.864,29 (que restados dan 275,42)."""
    fila = _dr(
        "Intercambio Comercial Argentino",
        [{"fecha": "2025-02-01", "expo": 6139.7040571, "impo": 5864.28756064}],
        units="Millones de dólares",
    )
    answer = "| Febrero | 6.140 | 5.864 | +276 |"
    assert _status(answer, [fila]) == {"6.140": "directa", "5.864": "directa", "+276": "derivada"}


def test_la_variacion_de_un_mes_con_los_indices_a_la_vista() -> None:
    """extra_acumulada: la mensual de marzo (3,38 %) con los índices de
    febrero y marzo, aunque la representación pedida perdió marzo."""
    indice = _dr(
        "IPC índice",
        [{"fecha": "2026-02-01", "ipc": 10714.6255}, {"fecha": "2026-03-01", "ipc": 11077.0608}],
    )
    assert _status("En marzo la inflación fue de 3,38 %.", [indice]) == {"3,38 %": "derivada"}
    # Con el signo invertido no sale: el índice subió.
    assert _status("En marzo la inflación fue de -3,38 %.", [indice]) == {"-3,38 %": "sin_respaldo"}


def test_un_rango_comparte_la_unidad() -> None:
    """series_007: "entre $40,5 y $43,3 billones" (la serie está en millones)."""
    base = _dr(
        "Base monetaria",
        [{"fecha": "2026-01-01", "base": 40475552.0}, {"fecha": "2026-02-01", "base": 43281363.0}],
        units="Millones de pesos",
    )
    answer = "La base se movió entre **$40,5 y $43,3 billones**."
    assert _status(answer, [base]) == {"40,5": "directa", "43,3 billones": "directa"}


def test_el_signo_antes_de_la_moneda_y_las_cotas_en_negrita() -> None:
    ddjj = _dr(
        "DDJJ",
        [{"patrimonio_cierre": -73441682.96, "variacion_patrimonial": 31218720307.64}],
    )
    assert _status("Patrimonio mínimo: **-$73.441.683**.", [ddjj]) == {"73.441.683": "directa"}
    assert _status("Patrimonio mínimo: **$73.441.683**.", [ddjj]) == {"73.441.683": "sin_respaldo"}
    assert _status("Una variación de más de **$31.218 millones**.", [ddjj]) == {
        "31.218 millones": "directa"
    }
    # Una cota no se estira: "más de 25.000 millones" queda lejos del 31.218.
    assert _status("Una variación de más de **$25.000 millones**.", [ddjj]) == {
        "25.000 millones": "sin_respaldo"
    }


def test_un_cero_no_vuelve_derivable_cualquier_cifra() -> None:
    """Con un campo en cero, a − 0 = a: el signo invertido pasaba como cuenta."""
    fila = _dr("DDJJ", [{"bienes": 56802358.11, "deudas_inicio": 0.0, "otro": 17009574.76}])
    assert _status("Tenía bienes por $56,8 millones.", [fila]) == {"56,8 millones": "directa"}
    assert _status("Tenía bienes por -$56,8 millones.", [fila]) == {"56,8 millones": "sin_respaldo"}


def test_lo_que_el_modelo_leyo_fuera_de_la_evidencia_es_contexto() -> None:
    """negativo_003: "localidades de 5.000 y más habitantes" sale de la
    descripción del estudio que mostró describir_tabla, no de una tabla."""
    estudio = _dr("Estudio", [{"valor": 3571983.0}])
    contexto = seen_numbers('{"descripcion":"Población en localidades de 5.000 y más habitantes"}')
    answer = "Hay **3.571.983 personas** en localidades de 5.000 y más habitantes."
    assert _status(answer, [estudio]) == {"3.571.983": "directa", "5.000": "sin_respaldo"}
    status = _status(answer, [estudio], context=contexto)
    assert status == {"3.571.983": "directa", "5.000": "contexto"}
    # El contexto no respalda una cifra corrida.
    assert _status("Son 5.150 localidades.", [estudio], context=contexto) == {
        "5.150": "sin_respaldo"
    }


def test_seen_numbers_lee_json_y_texto_argentino() -> None:
    seen = seen_numbers('{"a":49700.26,"b":"1.543,18","c":-0.22,"d":1e-05}')
    assert {49700.26, 1543.18, 0.22, 1e-05} <= set(seen)


# ── fuentes y citas ────────────────────────────────────────


def test_se_citan_solo_las_fuentes_que_aportaron_cifras() -> None:
    """Reservas, corrida 1: tres series citadas y sólo 92.1 aportó las cifras.
    92.2 se llama igual que 92.1: el título no alcanza para citarla."""
    diaria = _dr(
        "Reservas internacionales y pasivos del BCRA",
        [{"fecha": "2005-09-27", "Reservas": 25557.0}],
        url="https://datos.gob.ar/series/api/series/?ids=92.2_RESERVAS_IRES_0_0_32_40",
        units="Millones de dólares",
    )
    historica = _dr(
        "Series históricas de estadísticas monetarias",
        [{"fecha": "2023-04-01", "Reservas": 35001.0}],
        units="Millones de dólares",
    )
    answer = (
        "Las reservas internacionales del BCRA se ubican en **aproximadamente USD 49.700 "
        "millones** en agosto de 2026. En junio eran USD 47.467 M y en julio USD 48.662 M. "
        "Fuente: *Reservas internacionales y pasivos del BCRA* – BCRA."
    )
    evidence = [diaria, historica, _reservas_921()]
    cited, consulted = select_evidence(answer, evidence)
    assert [r.portal_url for r in cited] == [
        "https://datos.gob.ar/series/api/series/?ids=92.1_RID_0_0_32"
    ]
    assert len(consulted) == 2


def test_una_fuente_sin_cifras_se_cita_si_se_nombra() -> None:
    sesiones = DataResult(
        source="sesiones",
        portal_name="HCDN",
        portal_url="https://www.hcdn.gob.ar",
        dataset_title="Versiones taquigráficas de la Cámara de Diputados",
        format="json",
        records=[{"orador": "X", "texto": "..."}],
    )
    otra = _dr("Otra cosa", [{"fecha": "2026-01-01", "v": 5.0}])
    answer = "Según las Versiones taquigráficas de la Cámara de Diputados, se debatió el tema."
    cited, consulted = select_evidence(answer, [sesiones, otra])
    assert cited == [sesiones] and consulted == [otra]


def test_sin_cifras_ni_titulos_se_citan_todas_como_antes() -> None:
    evidence = [_reservas_921(), _ipc_mensual()]
    cited, consulted = select_evidence("No encontré ese dato.", evidence)
    assert cited == evidence and consulted == []


def test_las_citas_salen_de_las_coincidencias() -> None:
    answer = "En agosto de 2026 la interanual fue de **33,5 %**."
    check = verify_figures(answer, [_ipc_interanual()])
    [cita] = build_citations(answer, check, [_ipc_interanual()])
    assert cita["verified"] is True
    assert cita["claim"] == "En agosto de 2026 la interanual fue de 33,5 %."
    assert cita["grounding"][0]["path"] == f"records[1].{IPC}"
    assert cita["grounding"][0]["value"] == pytest.approx(0.3354117291414028)


def test_los_mensajes_nombran_las_cifras() -> None:
    [fig] = extract_figures("La interanual fue -0,22 %.")
    assert "-0,22 %" in correction_note([fig])
    assert "no sumes ni restes tasas" in correction_note([fig])
    assert unverified_notice([fig]).startswith("**Aviso:** no pude verificar")
    assert "-0,22 %" in unverified_notice([fig])


@pytest.mark.parametrize(
    ("raw", "mode"),
    [(None, "shadow"), ("off", "off"), ("CORRECT", "correct"), ("bogus", "shadow")],
)
def test_el_modo_sale_de_la_variable(
    monkeypatch: pytest.MonkeyPatch, raw: str | None, mode: str
) -> None:
    if raw is None:
        monkeypatch.delenv(v.VERIFY_ENV, raising=False)
    else:
        monkeypatch.setenv(v.VERIFY_ENV, raw)
    assert v.verify_mode() == mode
