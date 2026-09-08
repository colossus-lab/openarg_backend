"""El fallback en vivo del INDEC no puede inventar un dataset.

Contexto (2026-09): una pregunta sobre personas con Certificado Único de
Discapacidad terminó descargando IPC, EMAE y PIB en vivo y citándolos como
fuentes. El `keyword_map` no matcheaba nada y el código caía a un default
`["ipc", "emae", "pib"]`, así que el fallback SIEMPRE devolvía algo. Con
filas en mano, el pipeline deja de tratar la consulta como "sin datos" y el
analista pierde el prompt con guardrails — de ahí la cobertura inventada.

Este archivo era el que faltaba: `indec_live_fallback` no tenía cobertura.
"""

from __future__ import annotations

import pytest

from app.application.pipeline.connectors.sandbox import (
    _INDEC_KEYWORD_MAP,
    _INDEC_MAX_DATASETS,
    _match_indec_datasets,
    indec_live_fallback,
)

CUD = (
    "¿cuántas personas con Certificado Único de Discapacidad (CUD) "
    "hay en la provincia de Buenos Aires?"
)


@pytest.fixture
def descargas(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Espía las URLs pedidas, sin bajar nada.

    Espía y no `raise`: `indec_live_fallback` envuelve cada descarga en un
    `except Exception` que se tragaría la explosión y devolvería `[]` igual,
    o sea que un doble que falla haría pasar el test por la razón
    equivocada. La lista deja ver lo que de verdad pasó.
    """
    pedidas: list[str] = []

    def _spy(url: str) -> dict:
        pedidas.append(url)
        return {}

    monkeypatch.setattr(
        "app.infrastructure.celery.tasks.indec_tasks._download_and_parse",
        _spy,
    )
    return pedidas


@pytest.mark.asyncio
async def test_sin_keywords_no_descarga_nada(descargas: list[str]) -> None:
    """Ninguna keyword matchea ⇒ no se toca la red y no hay resultados.

    Devolver `[]` deja la consulta en el camino no-data, que ya tiene los
    guardrails cableados (prompt `analyst_no_data`, sin caché, y
    `success=False` en analytics).
    """
    resultados = await indec_live_fallback(CUD)

    assert descargas == [], f"no debería descargar nada y pidió: {descargas}"
    assert resultados == []


def test_la_pregunta_del_incidente_no_matchea_ningun_dataset() -> None:
    """El caso exacto del 2026-09. Antes devolvía ['ipc', 'emae', 'pib']."""
    assert _match_indec_datasets(CUD) == []


def test_nombrar_al_indec_no_alcanza_para_elegir_un_dataset() -> None:
    """La palabra "indec" identifica al publicador, no al dataset.

    Está en `INDEC_PATTERN` a propósito (rutea la pregunta hacia acá) y no
    en el keyword map (no dice *qué* bajar). Sin esta separación, "¿qué
    publicó el INDEC?" volvería a bajar algo arbitrario.
    """
    assert _match_indec_datasets("¿qué publicó el INDEC este mes?") == []


@pytest.mark.parametrize(
    ("pregunta", "esperado"),
    [
        ("cómo viene la inflación", "ipc"),
        ("datos de construcción", "isac"),
        ("distribución del ingreso por decil", "distribucion_ingreso"),
        ("cuánto sale la canasta básica", "canasta_basica"),
    ],
)
def test_las_tildes_no_afectan_el_matcheo(pregunta: str, esperado: str) -> None:
    """Las preguntas reales llevan tilde y las keywords están sin tilde.

    Esto no andaba: "inflación" nunca matcheaba la keyword "inflacion", y el
    default lo tapaba porque bajaba `ipc` igual. Sacar el default sin
    normalizar habría roto justo las consultas que sí funcionaban.
    """
    assert esperado in _match_indec_datasets(pregunta)


def test_el_orden_es_por_especificidad_no_por_declaracion() -> None:
    """Entre "canasta básica" y "precios" gana el más específico.

    Antes el orden lo daba la posición en el dict, donde `ipc` está
    declarado antes que `canasta_basica`.
    """
    ids = _match_indec_datasets("cuánto cuesta la canasta básica y los precios")

    assert ids[0] == "canasta_basica"
    assert "ipc" in ids


def test_nunca_devuelve_mas_del_tope() -> None:
    pregunta = "inflación, empleo, pobreza, construcción, turismo y comercio exterior"

    assert len(_match_indec_datasets(pregunta)) == _INDEC_MAX_DATASETS


def test_toda_keyword_apunta_a_un_dataset_que_existe() -> None:
    """Un id con typo nunca fetchea y falla en silencio.

    La dirección inversa (19 datasets, 15 con keywords) es deliberada: ver
    la regla de "keyword discriminante" en `_match_indec_datasets`.
    """
    from app.infrastructure.celery.tasks.indec_tasks import INDEC_DATASETS

    ids_reales = {d["id"] for d in INDEC_DATASETS}

    assert set(_INDEC_KEYWORD_MAP) <= ids_reales


@pytest.mark.asyncio
async def test_el_titulo_conserva_el_sufijo_live(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pincha el acoplamiento del que depende el gate de la batería.

    `finalize._extract_sources` descarta `DataResult.source`, así que el
    string "indec:live" no llega nunca al reporte de evaluación: lo único
    observable ahí es el `dataset_title`, y la expectativa de fuente
    prohibida ancla en el sufijo " (live)". Si alguien renombra el título,
    que rompa acá y no en silencio.
    """
    import pandas as pd

    monkeypatch.setattr(
        "app.infrastructure.celery.tasks.indec_tasks._download_and_parse",
        lambda url: {"hoja": pd.DataFrame([{"periodo": "2026-01", "valor": 1.5}])},
    )

    resultados = await indec_live_fallback("cómo viene la inflación")

    assert resultados
    assert resultados[0].dataset_title.endswith(" (live)")
    assert resultados[0].source == "indec:live"
