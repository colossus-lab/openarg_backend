"""`buscar_datos` del agente: una copia por archivo, como `/catalogo/buscar`.

El agente (motor de respuestas en prod desde el 03-oct) tenía su propia copia
del agrupado por (título, URL), que sumaba las tablas de todas las copias. Con
los gemelos de la migración de datos.gob.ar, Sonnet veía dos tablas de
"Reservas internacionales" (8.467 y 8.582 filas) y tenía que elegir.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from app.application.answers.engine import EngineRequest
from app.application.answers.tools.base import ToolContext
from app.application.answers.tools.catalogo import BuscarDatos
from app.domain.ports.sandbox.sql_sandbox import CachedTableInfo, TableProfile
from app.domain.ports.search.vector_search import SearchResult

_URL = "https://infra.datos.gob.ar/catalog/sspm/dataset/92/distribution/92.2/download/r.csv"


class _Sandbox:
    def __init__(self, tables: list[CachedTableInfo], profiles: dict[str, TableProfile]):
        self.tables = tables
        self.profiles = profiles

    async def find_tables(self, **kw: Any) -> list[CachedTableInfo]:
        ids = set(kw.get("dataset_ids") or [])
        return [t for t in self.tables if t.dataset_id in ids]

    async def table_profiles(self, names: list[str]) -> dict[str, TableProfile]:
        return {n.split(".")[-1]: self.profiles[n.split(".")[-1]] for n in names if n.split(".")[-1] in self.profiles}  # fmt: skip

    async def find_marts(self, emb: list[float], limit: int = 5) -> list[Any]:
        return []


def _deps(hits: list[SearchResult], sandbox: _Sandbox) -> MagicMock:
    deps = MagicMock()
    deps.embedding.embed = AsyncMock(return_value=[0.1] * 4)
    deps.vector_search.search_datasets_ann = AsyncMock(return_value=hits)
    deps.sandbox = sandbox
    return deps


async def test_twins_reach_the_model_as_one_dataset_with_one_table() -> None:
    hits = [
        SearchResult("vieja", "Reservas internacionales", "", "datos_gob_ar", _URL, "", 0.77),
        SearchResult("nueva", "Reservas internacionales", "", "datos_gob_ar", _URL, "", 0.76),
    ]
    sandbox = _Sandbox(
        [
            CachedTableInfo("raw.reservas__bebd015a__v1", "vieja", 8467, ["indice_tiempo"]),
            CachedTableInfo("reservas_rf3719b94d1", "nueva", 8582, ["indice_tiempo"]),
        ],
        {},
    )
    deps = _deps(hits, sandbox)

    out = await BuscarDatos().run({"texto": "reservas"}, ToolContext(deps, EngineRequest("q", "u")))

    [dataset] = json.loads(out.content)["datasets"]
    assert [t["tabla"] for t in dataset["tablas"]] == ["reservas_rf3719b94d1"]
    assert dataset["archivo"] == "r.csv"
    # Pide varios datasets por resultado: después se juntan las copias.
    assert deps.vector_search.search_datasets_ann.await_args.kwargs["limit"] == 32


async def test_live_version_rows_replace_a_zero_row_count() -> None:
    hits = [SearchResult("vn", "Votaciones Nominales", "", "diputados", "https://d/v.csv", "", 0.6)]
    sandbox = _Sandbox(
        [CachedTableInfo("raw.vn__v3", "vn", 0, ["voto"])],
        {"vn__v3": TableProfile("vn__v3", rows=231_043)},
    )

    out = await BuscarDatos().run(
        {"texto": "votaciones nominales"},
        ToolContext(_deps(hits, sandbox), EngineRequest("q", "u")),
    )

    [dataset] = json.loads(out.content)["datasets"]
    assert dataset["tablas"][0]["filas"] == 231_043


async def test_isac_sheets_with_the_same_shape_reach_the_model_separately() -> None:
    """H092 (revisión del 05-oct): el Cuadro 3.1 del ISAC (serie original) y el
    4.1 (desestacionalizada) tienen la misma forma; por la forma, el agente
    veía uno solo y respondía con la otra serie. Con la huella del contenido
    son dos datasets; los cuadros con la misma tabla siguen siendo uno."""
    url = "https://www.indec.gob.ar/ftp/cuadros/economia/sh_isac_2025.xls"
    title = "INDEC - ISAC — Actividad de la Construcción — Cuadro "
    cols = ["Período", "Período_2", "Asfalto", "Cales"]
    hits = [
        SearchResult("c3.1", title + "3.1", "", "indec", url, "", 0.70),
        SearchResult("c4.1", title + "4.1", "", "indec", url, "", 0.69),
        SearchResult("c1", title + "1", "", "indec", url, "", 0.68),
        SearchResult("c6.3", title + "6.3", "", "indec", url, "", 0.67),
    ]
    sandbox = _Sandbox(
        [
            CachedTableInfo("raw.cache_indec_isac_cuadro_3_1", "c3.1", 171, cols),
            CachedTableInfo("raw.cache_indec_isac_cuadro_4_1", "c4.1", 171, cols),
            CachedTableInfo("raw.isac_cuadro_1__v2", "c1", 20, ["Índice"]),
            CachedTableInfo("raw.isac_cuadro_6_3__v3", "c6.3", 20, ["Índice"]),
        ],
        {},
    )
    asked: list[list[str]] = []

    async def table_fingerprints(names: list[str]) -> dict[str, str]:
        asked.append(names)
        return {
            "cache_indec_isac_cuadro_3_1": "171:96674937454929861457",
            "cache_indec_isac_cuadro_4_1": "171:-94578655270205905329",
            "isac_cuadro_1__v2": "20:8138770247277535997",
            "isac_cuadro_6_3__v3": "20:8138770247277535997",
        }

    sandbox.table_fingerprints = table_fingerprints  # type: ignore[attr-defined]

    out = await BuscarDatos().run(
        {"texto": "construcción asfalto"},
        ToolContext(_deps(hits, sandbox), EngineRequest("q", "u")),
    )

    datasets = json.loads(out.content)["datasets"]
    assert [d["titulo"].rsplit("— ", 1)[1] for d in datasets] == [
        "Cuadro 3.1",
        "Cuadro 4.1",
        "Cuadro 1",
    ]
    assert [d["tablas"][0]["tabla"] for d in datasets[:2]] == [
        "raw.cache_indec_isac_cuadro_3_1",
        "raw.cache_indec_isac_cuadro_4_1",
    ]
    assert len(asked) == 1 and len(asked[0]) == 4


async def test_each_table_of_a_zip_reaches_the_model_even_from_another_copy() -> None:
    """Revisión de #177: las tres copias de "pj-penal-archivos-recibidos-2019.zip"
    (mismo título, misma URL) traen los casos, las personas y los delitos. Main
    le mostraba las tres tablas al agente; con sólo las de la copia elegida
    veía los casos y no encontraba a las personas."""
    url = (
        "https://datos.jus.gob.ar/dataset/90178b26/resource/9c978fd4/download/"
        "pj-penal-archivos-recibidos-2019.zip"
    )
    title = "Archivos recibidos de los poderes judiciales provinciales - Penal - 2019"
    hits = [
        SearchResult("casos", title, "", "datos_gob_ar", url, "", 0.70),
        SearchResult("personas", title, "", "justicia", url, "", 0.69),
        SearchResult("delitos", title, "", "datos_gob_ar", url, "", 0.68),
    ]
    sandbox = _Sandbox(
        [
            CachedTableInfo("raw.pj_casos", "casos", 35_796, ["caso_tipoisj", "id_caso"]),
            CachedTableInfo("raw.pj_personas", "personas", 27_117, ["persona_tipoisj", "id_caso"]),
            CachedTableInfo("raw.pj_delitos", "delitos", 15_575, ["evento_tipoisj", "id_caso"]),
        ],
        {},
    )

    out = await BuscarDatos().run(
        {"texto": "personas imputadas"}, ToolContext(_deps(hits, sandbox), EngineRequest("q", "u"))
    )

    [dataset] = json.loads(out.content)["datasets"]
    assert [t["tabla"] for t in dataset["tablas"]] == [
        "raw.pj_casos",
        "raw.pj_personas",
        "raw.pj_delitos",
    ]


async def test_an_unknown_portal_is_named_in_the_note() -> None:
    deps = _deps([], _Sandbox([], {}))
    deps.vector_search.known_portals = AsyncMock(return_value=["datos_gob_ar", "indec"])

    out = await BuscarDatos().run(
        {"texto": "soja", "portal": "INDEC"}, ToolContext(deps, EngineRequest("q", "u"))
    )

    nota = json.loads(out.content)["nota"]
    assert "'INDEC'" in nota and "indec" in nota


async def test_lo_lejos_del_mejor_no_llega_al_modelo() -> None:
    """Con Cohere v3 casi todo pasa 0,40: se quedan los datasets a 0,05 o menos
    del mejor y los marts a 0,05 o menos del mejor mart (calibrado 09-oct)."""
    from app.domain.ports.sandbox.sql_sandbox import MartInfo

    hits = [
        SearchResult("smvm", "Salario mínimo, vital y móvil", "", "datos_gob_ar", "u1", "", 0.75),
        SearchResult("smvm2", "Salario mínimo en dólares", "", "datos_gob_ar", "u2", "", 0.71),
        SearchResult("haber", "Haber mínimo jubilatorio", "", "datos_gob_ar", "u3", "", 0.54),
    ]
    sandbox = _Sandbox(
        [
            CachedTableInfo("raw.smvm__v1", "smvm", 400, ["indice_tiempo"]),
            CachedTableInfo("raw.smvm2__v1", "smvm2", 400, ["indice_tiempo"]),
            CachedTableInfo("raw.haber__v1", "haber", 300, ["indice_tiempo"]),
        ],
        {},
    )

    async def find_marts(emb: list[float], limit: int = 5) -> list[MartInfo]:
        return [
            MartInfo("mart.salarios", "salarios", "Salarios", "trabajo", 10, 0.62),
            MartInfo("mart.empleo", "empleo", "Empleo", "trabajo", 10, 0.58),
            MartInfo("mart.inflacion", "inflacion", "Inflación", "precios", 10, 0.55),
        ]

    sandbox.find_marts = find_marts  # type: ignore[method-assign]

    out = await BuscarDatos().run(
        {"texto": "salario mínimo"}, ToolContext(_deps(hits, sandbox), EngineRequest("q", "u"))
    )

    payload = json.loads(out.content)
    assert [d["titulo"] for d in payload["datasets"]] == [
        "Salario mínimo, vital y móvil",
        "Salario mínimo en dólares",
    ]
    assert [m["tabla"] for m in payload["tablas_curadas"]] == ["mart.salarios", "mart.empleo"]
    # El paso que ve la persona dice qué encontró, no sólo cuántos.
    assert out.summary == (
        "Encontró 2 datasets y 2 tablas curadas; el más parecido: «Salario mínimo, vital y móvil»"
    )


async def test_el_umbral_fijo_sigue_valiendo_debajo_del_mejor() -> None:
    from app.domain.ports.sandbox.sql_sandbox import MartInfo

    hits = [SearchResult("x", "Algo", "", "datos_gob_ar", "u", "", 0.42)]
    sandbox = _Sandbox([CachedTableInfo("raw.x__v1", "x", 5, ["a"])], {})

    async def find_marts(emb: list[float], limit: int = 5) -> list[MartInfo]:
        return [MartInfo("mart.m", "m", "M", "d", 1, 0.39)]

    sandbox.find_marts = find_marts  # type: ignore[method-assign]
    out = await BuscarDatos().run(
        {"texto": "algo"}, ToolContext(_deps(hits, sandbox), EngineRequest("q", "u"))
    )

    payload = json.loads(out.content)
    assert [d["titulo"] for d in payload["datasets"]] == ["Algo"]
    assert payload["tablas_curadas"] == []
