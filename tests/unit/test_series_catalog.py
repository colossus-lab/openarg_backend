"""El catálogo curado de series contra lo que dice la API de cada id.

La metadata (``field.description``) de cada id se grabó de la API el
04-oct-2026 en ``tests/fixtures/series_tiempo_api/catalog_meta.json``; la del
IPIM y del IPC por región, el 06-oct, y la de la tasa de actividad y el PIB,
el 07-oct (``recorded_at`` de cada una).
Cambiar un id del catálogo obliga a regrabarla y a fijar su descripción: así
se coló ``11.3_AGCS_2004_M_41`` (EMAE comercio) rotulado como "actividad
industrial", y el agente lo recibía como serie verificada.

Para regrabar: el script de la sesión del 04-oct pide ``metadata=full`` con
``limit=1`` por id; ``catalog_mismatches`` sirve igual contra la API en vivo.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.infrastructure.adapters.connectors.series_tiempo_adapter import (
    SERIES_CATALOG,
    catalog_mismatches,
    find_catalog_match,
    match_catalog,
)

FIXTURE = (
    Path(__file__).resolve().parents[1] / "fixtures" / "series_tiempo_api" / "catalog_meta.json"
)


def _api() -> dict[str, dict]:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))["series"]


def test_cada_id_del_catalogo_es_la_serie_que_dice_ser() -> None:
    descriptions = {sid: meta["description"] for sid, meta in _api().items()}
    assert catalog_mismatches(descriptions) == []


def test_un_id_cambiado_se_detecta() -> None:
    descriptions = {sid: meta["description"] for sid, meta in _api().items()}
    # Lo que pasaba antes: el id de "actividad industrial" era el EMAE comercio.
    descriptions["453.1_SERIE_ORIGNAL_0_0_14_46"] = (
        "EMAE. Comercio mayorista y minorista y reparaciones"
    )
    problems = catalog_mismatches(descriptions)
    assert len(problems) == 1
    assert "actividad_industrial" in problems[0]


@pytest.mark.parametrize(
    ("key", "palabra"),
    [
        ("actividad_industrial", "IPI"),
        ("emae_comercio", "Comercio"),
        ("inflacion", "IPC"),
        ("reservas", "Reservas"),
        ("reservas_diarias", "Reservas"),
        ("tipo_cambio", "Tipo de cambio"),
        ("base_monetaria", "Base Monetaria"),
        ("desempleo", "desempleo"),
        ("exportaciones", "Exportaciones"),
        ("importaciones", "Importaciones"),
        ("salarios", "Salarios"),
        ("ipim", "IPIM"),
        ("ipc_gba", "GBA"),
        ("ipc_pampeana", "pampeana"),
        ("ipc_noreste", "noreste"),
        ("ipc_noroeste", "noroeste"),
        ("ipc_cuyo", "Cuyo"),
        ("ipc_patagonia", "Patagonia"),
        ("actividad", "Tasa de actividad"),
        ("pbi", "PIB"),
    ],
)
def test_la_serie_de_cada_tema_es_de_ese_tema(key: str, palabra: str) -> None:
    api = _api()
    for sid in SERIES_CATALOG[key]["ids"]:
        assert palabra in api[sid]["description"], (key, sid, api[sid]["description"])


def test_ninguna_palabra_de_industria_lleva_a_una_serie_de_comercio() -> None:
    api = _api()
    for texto in ("industria", "producción industrial", "manufactura", "fábricas"):
        entry = find_catalog_match(texto)
        assert entry is not None, texto
        for sid in entry["ids"]:
            assert "Comercio" not in api[sid]["description"], (texto, sid)


def test_leliq_no_responde_por_la_tasa_de_politica_monetaria() -> None:
    assert find_catalog_match("¿Cuál es la tasa de política monetaria?") is None


def test_la_serie_discontinuada_esta_rotulada() -> None:
    gasto = SERIES_CATALOG["presupuesto"]
    assert gasto["discontinued"] is True
    assert "discontinuada" in gasto["description"]
    assert _api()["451.3_GPNGPN_0_0_3_30"]["is_updated"] == "False"


def test_el_viejo_sigue_encontrando_lo_de_siempre() -> None:
    """`find_catalog_match` (pipeline viejo) devuelve la primera entrada, como antes."""
    assert find_catalog_match("inflación de agosto")["ids"] == ["148.3_INIVELNAL_DICI_M_26"]
    assert find_catalog_match("costo de vida")["ids"] == ["148.3_INIVELNAL_DICI_M_26"]
    assert find_catalog_match("balanza comercial 2025")["ids"] == [
        "74.3_IET_0_M_16",
        "74.3_IIT_0_M_25",
    ]


def _claves(texto: str) -> list[str]:
    matches = match_catalog(texto)
    return [key for key, entry in SERIES_CATALOG.items() if any(entry is m for m in matches)]


@pytest.mark.parametrize(
    ("texto", "key"),
    [
        # Los textos del chequeo sin LLM del 06-oct: todos daban la entrada
        # "inflacion" (IPC nacional) como serie verificada.
        ("inflación mayorista", "ipim"),
        ("¿De cuánto fue la inflación mayorista en agosto de 2026?", "ipim"),
        ("precios mayoristas", "ipim"),
        ("IPIM", "ipim"),
        ("índice de precios al por mayor", "ipim"),
        ("IPC GBA", "ipc_gba"),
        ("¿Cuánto fue la inflación en el Gran Buenos Aires entre enero y abril?", "ipc_gba"),
        ("IPC noreste", "ipc_noreste"),
        ("inflación Misiones", "ipc_noreste"),
        ("inflacion Misiones agosto 2026", "ipc_noreste"),
        ("inflación del NEA", "ipc_noreste"),
        ("inflación en Corrientes", "ipc_noreste"),
        ("inflación en Jujuy", "ipc_noroeste"),
        ("inflación de Salta", "ipc_noroeste"),
        # Revisión de #164: nueva_15 está escrita sin preposición, como en un
        # buscador; Corrientes y Salta, también palabras comunes, sólo
        # coincidían con «en» o «de» delante.
        ("inflación Corrientes agosto 2026", "ipc_noreste"),
        ("IPC Corrientes", "ipc_noreste"),
        ("inflación Salta agosto 2026", "ipc_noroeste"),
        ("ipc salta", "ipc_noroeste"),
        ("IPC Cuyo", "ipc_cuyo"),
        ("precios en Mendoza", "ipc_cuyo"),
        ("inflación de la región pampeana", "ipc_pampeana"),
        ("inflación en Córdoba", "ipc_pampeana"),
        ("IPC Patagonia", "ipc_patagonia"),
        ("inflación en Neuquén", "ipc_patagonia"),
    ],
)
def test_la_palabra_mas_especifica_excluye_el_ipc_nacional(texto: str, key: str) -> None:
    assert _claves(texto) == [key]
    assert find_catalog_match(texto) is SERIES_CATALOG[key]


@pytest.mark.parametrize(
    ("texto", "claves"),
    [
        ("inflación de agosto", ["inflacion"]),
        # El nacional pedido por nombre, o junto a otro índice, se queda.
        ("inflación nacional y del NEA", ["inflacion", "ipc_noreste"]),
        ("inflación y precios mayoristas", ["inflacion", "ipim"]),
        # Revisión de #164: «costo de vida» es keyword del IPC y de la
        # canasta. La misma frase en otra entrada no es más específica.
        ("costo de vida", ["inflacion", "canasta_basica"]),
        ("¿cuánto subió el costo de vida en 2025?", ["inflacion", "canasta_basica"]),
        ("dólar mayorista", ["tipo_cambio"]),
        ("comercio mayorista", ["emae_comercio"]),
        ("exportaciones de Misiones", ["exportaciones"]),
    ],
)
def test_sin_una_palabra_mas_especifica_el_ipc_nacional_sigue(
    texto: str, claves: list[str]
) -> None:
    assert _claves(texto) == claves


@pytest.mark.parametrize(
    ("texto", "region"),
    [
        ("PBI a precios corrientes", "ipc_noreste"),
        ("inflación en pesos corrientes", "ipc_noreste"),
        ("¿por qué salta la inflación?", "ipc_noroeste"),
        ("qué dijo la misión del FMI sobre la inflación", "ipc_noreste"),
        # Pegados a la palabra del IPC, pero como verbo o adjetivo.
        ("¿por qué la inflación salta en diciembre?", "ipc_noroeste"),
        ("si el IPC salta otra vez", "ipc_noroeste"),
        ("índice de precios corrientes", "ipc_noreste"),
    ],
)
def test_un_lugar_que_tambien_es_palabra_comun_no_trae_la_region(texto: str, region: str) -> None:
    assert region not in _claves(texto)


@pytest.mark.parametrize("texto", ["reservas del BCRA", "¿cuántas reservas tiene el BCRA hoy?"])
def test_el_viejo_responde_reservas_con_la_diaria(texto: str) -> None:
    # La mensual 174.1 está parada en la fuente en 2026-04 (is_updated False)
    # y el pipeline viejo no ve el aviso de atraso: la primera entrada que
    # encuentra tiene que ser la diaria, que llega a 2026-08-31.
    entry = find_catalog_match(texto)
    assert entry is not None
    assert entry["ids"] == ["92.2_RESERVAS_IRES_0_0_32_40"]
    assert "default_collapse" not in entry
    assert _api()["174.1_RRVAS_IDOS_0_0_36"]["is_updated"] == "False"


# ── tasa de actividad y PIB (prueba de calidad del 07-oct, nueva_10 y 11) ──


@pytest.mark.parametrize(
    "texto",
    [
        "¿Cuál es la tasa de actividad según la última EPH?",
        # Con «EPH» la /search de la API nunca trae la trimestral del total:
        # casi siempre, sólo la anual 42.1, que termina en 2025 (medido el
        # 07-oct contra la API).
        "tasa de actividad EPH",
        "EPH tasa de actividad trimestral",
        "tasas de actividad",
    ],
)
def test_la_tasa_de_actividad_verifica_la_trimestral_de_la_eph(texto: str) -> None:
    """nueva_10: con «EPH» en el texto, buscar_series no daba ninguna serie
    trimestral; el modelo leyó la anual (hasta 2025) y una copia guardada que
    terminaba en el 1.er trimestre de 2026, y lo dio como el último dato. El
    05-oct pasó lo mismo, con las mismas herramientas."""
    assert _claves(texto) == ["actividad"]
    sid = SERIES_CATALOG["actividad"]["ids"][0]
    assert _api()[sid]["frequency"] == "R/P3M"
    assert _api()[sid]["time_index_end"] == "2026-04-01"


@pytest.mark.parametrize(
    ("texto", "clave"),
    [
        ("actividad económica", "emae"),
        ("actividad industrial", "actividad_industrial"),
        ("actividad comercial", "emae_comercio"),
    ],
)
def test_otra_actividad_no_es_la_tasa_de_actividad(texto: str, clave: str) -> None:
    assert _claves(texto) == [clave]


@pytest.mark.parametrize(
    "texto",
    [
        "¿Cuánto creció el producto bruto interno de la Argentina en 2025?",
        "PBI 2025",
        "crecimiento del PIB",
        "producto interno bruto",
    ],
)
def test_el_pbi_verifica_el_pib_a_precios_constantes(texto: str) -> None:
    """nueva_11: la única verificada para «producto bruto interno» era el
    EMAE, y la primera de la /search, 166.2_PPIB_0_0_3, el PIB a precios
    corrientes, parado en el 4.º trimestre de 2025. Las tres corridas
    (05, 06 y 07-oct) la leyeron primero."""
    assert "pbi" in _claves(texto)
    sid = SERIES_CATALOG["pbi"]["ids"][0]
    assert _api()[sid]["units"] == "Millones de pesos a precios de 2004"
    assert _api()[sid]["frequency"] == "R/P3M"


def test_el_pbi_no_saca_al_emae_ni_cambia_lo_del_pipeline_viejo() -> None:
    # «producto bruto» sigue siendo también del EMAE, y el pipeline viejo,
    # que toma la primera entrada, sigue en el EMAE.
    assert _claves("producto bruto interno") == ["emae", "pbi"]
    assert find_catalog_match("producto bruto interno")["ids"] == ["143.3_NO_PR_2004_A_21"]


# Revisión de #173: «tasa de actividad» y «pbi» sueltos coinciden también
# cuando se pregunta por un lugar, un grupo, otro país, el per cápita o un
# «% del PBI». En staging esas búsquedas no tenían ninguna verificada, y con
# la nacional verificada arriba, la de Gran Córdoba que la /search traía
# primero quedaba debajo (07-oct, en vivo). Es la falla de nueva_15 del
# 06-oct: «inflación Misiones» verificaba el IPC nacional.
#
# Lo decidido, mínimo y sin decisión de producto: la entrada sigue
# coincidiendo y su descripción dice que es el total nacional y para qué no
# sirve. Sacarla cuando aparece un lugar, «per cápita» o «del PBI» (lo inverso
# de ``places``) lo decide Lucho; si se toma, cambia la primera aserción.
@pytest.mark.parametrize(
    ("texto", "clave", "aviso"),
    [
        ("tasa de actividad en Córdoba", "actividad", "provincia"),
        # «aglomerados urbanos» ya estaba: se pide la frase entera.
        ("tasa de actividad en GBA", "actividad", "región o aglomerado"),
        ("tasa de actividad de las mujeres", "actividad", "mujeres"),
        ("PBI de Córdoba", "pbi", "provincia"),
        ("PIB de Brasil", "pbi", "otro país"),
        ("PBI per cápita", "pbi", "per cápita"),
        ("gasto en educación como porcentaje del PBI", "pbi", "% del PBI"),
        ("deuda pública en % del PBI", "pbi", "% del PBI"),
    ],
)
def test_la_serie_nacional_avisa_que_no_es_la_de_un_lugar_ni_un_cociente(
    texto: str, clave: str, aviso: str
) -> None:
    assert _claves(texto) == [clave]
    descripcion = SERIES_CATALOG[clave]["description"]
    assert "total nacional" in descripcion
    assert aviso in descripcion
    assert "usá la de 'series' que mida eso" in descripcion
