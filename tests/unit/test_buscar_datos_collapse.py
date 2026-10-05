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


async def test_an_unknown_portal_is_named_in_the_note() -> None:
    deps = _deps([], _Sandbox([], {}))
    deps.vector_search.known_portals = AsyncMock(return_value=["datos_gob_ar", "indec"])

    out = await BuscarDatos().run(
        {"texto": "soja", "portal": "INDEC"}, ToolContext(deps, EngineRequest("q", "u"))
    )

    nota = json.loads(out.content)["nota"]
    assert "'INDEC'" in nota and "indec" in nota
