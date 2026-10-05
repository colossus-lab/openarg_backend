"""El catálogo curado de series contra lo que dice la API de cada id.

La metadata (``field.description``) de cada id se grabó de la API el
04-oct-2026 en ``tests/fixtures/series_tiempo_api/catalog_meta.json``.
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
    assert find_catalog_match("balanza comercial 2025")["ids"] == [
        "74.3_IET_0_M_16",
        "74.3_IIT_0_M_25",
    ]


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
