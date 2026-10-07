"""nueva_06: el tope de la búsqueda de sesiones no es un total.

En la prueba de calidad del 06-oct (ola 3), a la pregunta por el régimen
penal juvenil el agente contestó que hubo "12 fragmentos registrados" de
intervenciones. En staging hay 51 fragmentos con "penal juvenil". El 12 es
el `limit` de la
herramienta: la búsqueda vectorial trae los 12 más parecidos, y el adaptador
lo informaba como `total_records` y como "Se encontraron 12 fragmentos
relevantes". Al modelo le llegaba `"filas_totales":12`.

La búsqueda es por parecido (pgvector): no hay un "total" que contar barato.
Una cuenta por palabras depende de qué frase se elija (con todos los términos
de la pregunta da 0; con cualquiera, cientos), así que la herramienta dice
que es el tope y no da un número que parezca la cantidad.

Lo que se prueba:

- el adaptador pide una fila de más y, si la hay, marca que llegó al tope y
  no informa `total_records`;
- con `orador`, el filtro va después del LIMIT: lo que queda es parte de los
  más parecidos, no todo lo que dijo esa persona;
- lo mismo con la búsqueda local por palabras;
- si no llegó al tope, el total es el de siempre;
- la descripción no dice "0 orador(es)" cuando el orador no está cargado
  (en staging, los 1.030 fragmentos tienen `speaker` NULL);
- al modelo le llega "al menos N (tope de la búsqueda)" y el aviso de no
  contar, antes que las filas y dentro del tope de caracteres.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from app.application.answers.engine import EngineRequest
from app.application.answers.tools.base import MAX_CONTENT_CHARS, ToolContext
from app.application.answers.tools.conectores import Sesiones
from app.domain.entities.connectors.data_result import DataResult
from app.infrastructure.adapters.connectors.sesiones_adapter import SesionesAdapter

PDF = "https://www3.hcdn.gob.ar/dependencias/dtaquigrafos/diarios/periodo-143/diario_2026021221.pdf"


def _fila(i: int, speaker: str | None = None) -> SimpleNamespace:
    """Una fila de `sesion_chunks` como la de staging: ~3.100 caracteres y sin orador."""
    return SimpleNamespace(
        periodo=143,
        reunion=21,
        fecha="2026-02-12",
        tipo_sesion="2° Sesión Extraordinaria Especial",
        pdf_url=PDF,
        total_pages=643,
        speaker=speaker,
        content=f"Fragmento {i} sobre el régimen penal juvenil. " + "texto " * 520,
        score=0.9 - i / 1000,
    )


class _Sesion:
    """Sesión de SQLAlchemy falsa: devuelve hasta `:limit` filas de las que hay."""

    def __init__(self, filas: list[SimpleNamespace], pedidos: list[dict[str, Any]]) -> None:
        self._filas = filas
        self._pedidos = pedidos

    async def __aenter__(self) -> _Sesion:
        return self

    async def __aexit__(self, *exc: object) -> None:
        return None

    async def execute(self, _sql: Any, params: dict[str, Any]) -> MagicMock:
        self._pedidos.append(params)
        res = MagicMock()
        res.fetchall.return_value = self._filas[: params["limit"]]
        return res


def _adapter(filas: list[SimpleNamespace]) -> tuple[SesionesAdapter, list[dict[str, Any]]]:
    pedidos: list[dict[str, Any]] = []
    adapter = SesionesAdapter(lambda: _Sesion(filas, pedidos))
    # Sin Bedrock: el embedding de la pregunta no importa, la base es falsa.
    adapter._generate_embedding = AsyncMock(return_value=[0.1, 0.2])  # noqa: SLF001
    return adapter, pedidos


PREGUNTA = "régimen penal juvenil baja edad de imputabilidad"


# ── el adaptador ───────────────────────────────────────────


async def test_con_mas_fragmentos_que_el_tope_no_informa_el_tope_como_total() -> None:
    adapter, pedidos = _adapter([_fila(i) for i in range(51)])

    result = await adapter.search(PREGUNTA, limit=12)

    assert result is not None
    assert len(result.records) == 12
    # Pidió una de más para saber si había más.
    assert pedidos[0]["limit"] == 13
    meta = result.metadata
    assert "total_records" not in meta
    assert meta["tope_alcanzado"] is True
    assert meta["tope_busqueda"] == 12
    assert "Se encontraron 12" not in meta["description"]
    assert "no es la cantidad" in meta["description"]


async def test_sin_llegar_al_tope_el_total_es_el_de_siempre() -> None:
    adapter, _ = _adapter([_fila(i) for i in range(5)])

    result = await adapter.search(PREGUNTA, limit=12)

    assert result is not None
    assert len(result.records) == 5
    assert result.metadata["total_records"] == 5
    assert result.metadata["tope_alcanzado"] is False
    assert result.metadata["description"].startswith("Se encontraron 5 fragmentos")


async def test_justo_el_tope_no_es_llegar_al_tope() -> None:
    adapter, _ = _adapter([_fila(i) for i in range(12)])

    result = await adapter.search(PREGUNTA, limit=12)

    assert result is not None
    assert result.metadata["total_records"] == 12
    assert result.metadata["tope_alcanzado"] is False


async def test_con_orador_lo_filtrado_sale_de_los_mas_parecidos() -> None:
    """El filtro por orador va después del LIMIT: 2 de los 13 más parecidos
    no son "las 2 intervenciones" de esa persona."""
    filas = [_fila(i, speaker="Pichetto" if i in (3, 7) else "Otro") for i in range(51)]
    adapter, _ = _adapter(filas)

    result = await adapter.search(PREGUNTA, orador="pichetto", limit=12)

    assert result is not None
    assert len(result.records) == 2
    assert "total_records" not in result.metadata
    assert result.metadata["tope_alcanzado"] is True


async def test_busqueda_local_tambien_avisa_el_tope() -> None:
    adapter, pedidos = _adapter([])  # pgvector sin filas: va a la búsqueda local
    adapter._chunks = [  # noqa: SLF001
        {"periodo": 143, "reunion": 21, "speaker": None, "text": f"penal juvenil {i}"}
        for i in range(20)
    ]
    adapter._chunk_term_counts = [{"penal": 1, "juvenil": 1} for _ in range(20)]  # noqa: SLF001
    adapter._speaker_lowers = [""] * 20  # noqa: SLF001
    adapter._term_index = {"penal": set(range(20)), "juvenil": set(range(20))}  # noqa: SLF001
    adapter._period_index = {143: list(range(20))}  # noqa: SLF001
    adapter._loaded = True  # noqa: SLF001

    result = await adapter.search("penal juvenil", limit=12)

    assert pedidos  # probó primero pgvector
    assert result is not None
    assert len(result.records) == 12
    assert "total_records" not in result.metadata
    assert result.metadata["tope_alcanzado"] is True


async def test_sin_orador_cargado_no_dice_cero_oradores() -> None:
    adapter, _ = _adapter([_fila(i) for i in range(51)])

    result = await adapter.search(PREGUNTA, limit=12)

    assert result is not None
    assert "0 orador" not in result.metadata["description"]
    assert "no traen el orador" in result.metadata["description"]


async def test_con_orador_cargado_los_cuenta() -> None:
    adapter, _ = _adapter([_fila(i, speaker=f"Diputado {i % 3}") for i in range(5)])

    result = await adapter.search(PREGUNTA, limit=12)

    assert result is not None
    assert "3 orador(es)" in result.metadata["description"]


# ── lo que lee el modelo ───────────────────────────────────


def _ctx(sesiones: Any) -> ToolContext:
    deps = MagicMock()
    deps.sesiones = sesiones
    return ToolContext(deps, EngineRequest("q", "u"))


async def test_herramienta_no_presenta_el_tope_como_total() -> None:
    """El caso de nueva_06: 51 fragmentos en la base, la herramienta pide 12."""
    adapter, _ = _adapter([_fila(i) for i in range(51)])

    out = await Sesiones().run({"texto": PREGUNTA}, _ctx(adapter))

    assert len(out.content) <= MAX_CONTENT_CHARS
    payload = json.loads(out.content)  # JSON válido: no se cortó nada
    assert payload["filas_totales"] != 12
    assert payload["filas_totales"] == "al menos 12 (tope de la búsqueda: 12)"
    assert "como máximo 12 fragmentos" in payload["aviso"]
    assert "«al menos 12»" in payload["aviso"]
    assert "no se contaron" in payload["aviso"]
    # El aviso llega antes que las filas (las filas van al final y enteras).
    assert out.content.index('"aviso"') < out.content.index('"filas"')
    [result] = out.results
    assert payload["filas"] == result.records[: len(payload["filas"])]


async def test_herramienta_con_orador_el_tope_es_el_de_la_busqueda() -> None:
    """Con orador quedan 2 de los más parecidos: "al menos 2", pero el tope
    sigue siendo 12 (no "como máximo 2")."""
    filas = [_fila(i, speaker="Pichetto" if i in (3, 7) else "Otro") for i in range(51)]
    adapter, _ = _adapter(filas)

    out = await Sesiones().run({"texto": PREGUNTA, "orador": "Pichetto"}, _ctx(adapter))

    payload = json.loads(out.content)
    assert payload["filas_totales"] == "al menos 2 (tope de la búsqueda: 12)"
    assert "como máximo 12 fragmentos" in payload["aviso"]
    assert "«al menos 2»" in payload["aviso"]


async def test_herramienta_sin_tope_da_el_total() -> None:
    adapter, _ = _adapter([_fila(i) for i in range(2)])

    out = await Sesiones().run({"texto": PREGUNTA}, _ctx(adapter))

    payload = json.loads(out.content)
    assert payload["filas_totales"] == 2
    assert "aviso" not in payload


async def test_herramienta_con_un_resultado_sin_marca_queda_igual() -> None:
    """Un conector que no informa el tope (otro adaptador, un doble de test)
    se muestra como antes."""
    result = DataResult(
        source="sesiones:diputados",
        portal_name="Diario de Sesiones",
        portal_url="",
        dataset_title="Transcripciones",
        format="json",
        records=[{"texto": "a"}, {"texto": "b"}],
        metadata={"total_records": 2},
    )
    sesiones = MagicMock()
    sesiones.search = AsyncMock(return_value=result)

    out = await Sesiones().run({"texto": "algo"}, _ctx(sesiones))

    payload = json.loads(out.content)
    assert payload["filas_totales"] == 2
    assert "aviso" not in payload
    assert sesiones.search.await_args.kwargs["limit"] == 12
