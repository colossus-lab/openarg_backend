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

Tampoco un mínimo (revisión de #158): pgvector ordena por distancia sin
umbral, así que con los 1.030 fragmentos de staging llega al tope siempre,
aunque el tema no tenga ninguno. "Al menos 12" y "estos no son todos" eran
falsos para "ocupación de Airbnb".

Lo que se prueba:

- el adaptador pide una fila de más y, si la hay, marca que llegó al tope y
  no informa `total_records`;
- con `orador`, el filtro va después del LIMIT: lo que queda es parte de los
  más parecidos, no todo lo que dijo esa persona;
- lo mismo con la búsqueda local por palabras;
- si no llegó al tope, el total es el de siempre;
- la descripción no dice "0 orador(es)" cuando el orador no está cargado
  (en staging, los 1.030 fragmentos tienen `speaker` NULL);
- al tope, las sesiones van por fecha y reunión, no como cantidad
  (sesiones_001: "12 fragmentos sobre educación en 3 sesiones");
- al modelo le llega que son los más parecidos, que pueden ser de otros
  temas y que no cuente, sin "al menos N", antes que las filas y dentro del
  tope de caracteres; con la tabla entera y un tema sin fragmentos, igual;
- con `orador` y `speaker` NULL (el respaldo local real), que ninguno está
  atribuido a esa persona.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.application.answers.engine import EngineRequest
from app.application.answers.tools.base import MAX_CONTENT_CHARS, ToolContext
from app.application.answers.tools.conectores import Sesiones
from app.domain.entities.connectors.data_result import DataResult
from app.infrastructure.adapters.connectors import sesiones_adapter
from app.infrastructure.adapters.connectors.sesiones_adapter import SesionesAdapter

PDF = "https://www3.hcdn.gob.ar/dependencias/dtaquigrafos/diarios/periodo-143/diario_2026021221.pdf"


def _fila(
    i: int, speaker: str | None = None, reunion: int = 21, fecha: str = "2026-02-12"
) -> SimpleNamespace:
    """Una fila de `sesion_chunks` como la de staging: ~3.100 caracteres y sin orador."""
    return SimpleNamespace(
        periodo=143,
        reunion=reunion,
        fecha=fecha,
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


@pytest.fixture(scope="module")
def tabla_de_staging() -> list[SimpleNamespace]:
    """`sesion_chunks` entera: los 1.030 fragmentos del repo son los de staging.

    Consulta de sólo lectura en staging (06-oct): 1.030 filas, todas del
    período 143, `speaker` NULL en todas y ninguna con "airbnb".
    """
    chunks = [
        c
        for f in sorted(Path(sesiones_adapter._CHUNKS_DIR).glob("*.json"))  # noqa: SLF001
        for c in json.loads(f.read_text(encoding="utf-8"))
    ]
    assert len(chunks) == 1030
    assert {c["periodo"] for c in chunks} == {143}
    assert not any(c.get("speaker") for c in chunks)
    assert not any("airbnb" in str(c.get("text")).lower() for c in chunks)
    return [
        SimpleNamespace(
            periodo=c["periodo"],
            reunion=c["reunion"],
            fecha=c.get("fecha"),
            tipo_sesion=c.get("tipoSesion"),
            pdf_url=c.get("pdfUrl"),
            total_pages=c.get("totalPages"),
            speaker=c.get("speaker"),
            content=c.get("text"),
            score=0.1,
        )
        for c in chunks
    ]


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


def _de_tres_reuniones() -> list[SimpleNamespace]:
    """Las reuniones de sesiones_001 (20, 22 y 21), más una fila para llegar al tope."""
    fechas = {20: "2025-12-17", 21: "2026-02-12", 22: "2026-02-19"}
    reuniones = [20] * 5 + [22] * 4 + [21] * 4
    return [_fila(i, reunion=r, fecha=fechas[r]) for i, r in enumerate(reuniones)]


async def test_al_tope_las_sesiones_van_por_fecha_y_no_como_cantidad() -> None:
    """sesiones_001 de la batería v3 (06-oct): "12 fragmentos sobre educación en
    3 sesiones". El 3 eran las sesiones de los 12 traídos; en staging
    "educación" aparece en 4. Con orador cargado, lo mismo con los oradores."""
    filas = _de_tres_reuniones()
    for i, f in enumerate(filas):
        f.speaker = f"Diputado {i % 5}"
    adapter, _ = _adapter(filas)

    result = await adapter.search("educación", limit=12)

    assert result is not None
    assert result.metadata["tope_alcanzado"] is True
    descripcion = result.metadata["description"]
    assert "3 sesi" not in descripcion
    assert "orador(es)" not in descripcion
    assert (
        "de las sesiones del 2025-12-17 (reunión 20), 2026-02-12 (reunión 21) "
        "y 2026-02-19 (reunión 22)."
    ) in descripcion
    assert "fragmentos, sesiones, intervenciones ni oradores" in descripcion


async def test_la_descripcion_entra_en_el_corte_con_las_cuatro_sesiones_de_staging() -> None:
    """`result_for_model` corta la descripción en 400 caracteres. Con las cuatro
    sesiones de staging (reuniones 19 a 22) y un orador sin atribuir, entra."""
    fechas = {19: "2025-12-03", 20: "2025-12-17", 21: "2026-02-12", 22: "2026-02-19"}
    filas = [_fila(i, reunion=r, fecha=fechas[r]) for i, r in enumerate([19, 20, 21, 22] * 4)]
    adapter, _ = _adapter([])  # pgvector sin filas: va a la búsqueda local
    adapter._loaded = True  # noqa: SLF001
    adapter._search_local = MagicMock(  # noqa: SLF001
        return_value=[
            {"periodo": f.periodo, "reunion": f.reunion, "fecha": f.fecha, "speaker": None}
            for f in filas[:13]
        ]
    )

    result = await adapter.search("jubilados", orador="Pichetto", limit=12)

    assert result is not None
    descripcion = result.metadata["description"]
    assert "2025-12-03 (reunión 19)" in descripcion
    assert "2026-02-19 (reunión 22)" in descripcion
    assert descripcion.endswith(
        "Ninguno trae a «Pichetto» como orador: no tienen el orador identificado."
    )
    assert len(descripcion) <= 400


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
    assert payload["filas_totales"] == "sin contar: son los 12 más parecidos (tope: 12)"
    assert "Son los 12 fragmentos más parecidos" in payload["aviso"]
    assert "no se contaron" in payload["aviso"]
    # Ni "al menos 12": la búsqueda llega al tope trate de lo que trate.
    assert "al menos" not in out.content
    # El aviso llega antes que las filas (las filas van al final y enteras).
    assert out.content.index('"aviso"') < out.content.index('"filas"')
    [result] = out.results
    assert payload["filas"] == result.records[: len(payload["filas"])]


async def test_herramienta_al_tope_no_cuenta_sesiones() -> None:
    """sesiones_001 y sesiones_004 (batería v3, 06-oct): "12 fragmentos sobre
    educación en 3 sesiones" y "12 fragmentos registrados sobre reforma
    laboral, todos de una sola sesión". El aviso tiene que incluir las
    sesiones entre lo que no se cuenta."""
    adapter, _ = _adapter(_de_tres_reuniones())

    out = await Sesiones().run({"texto": "educación"}, _ctx(adapter))

    payload = json.loads(out.content)
    assert "3 sesi" not in out.content
    assert "fragmentos, sesiones, intervenciones u oradores" in payload["aviso"]
    [result] = out.results
    assert payload["descripcion"] == result.metadata["description"]  # no se cortó


async def test_herramienta_con_orador_el_tope_es_el_de_la_busqueda() -> None:
    """Con orador quedan 2 de los más parecidos; el tope sigue siendo 12."""
    filas = [_fila(i, speaker="Pichetto" if i in (3, 7) else "Otro") for i in range(51)]
    adapter, _ = _adapter(filas)

    out = await Sesiones().run({"texto": PREGUNTA, "orador": "Pichetto"}, _ctx(adapter))

    payload = json.loads(out.content)
    assert payload["filas_totales"] == "sin contar: son los 2 más parecidos (tope: 12)"
    assert "Son los 2 fragmentos más parecidos" in payload["aviso"]
    assert "al menos" not in out.content
    # Los dos traen a Pichetto como orador: no hay nada que advertir sobre eso.
    assert "atribuido" not in payload["aviso"]


async def test_tema_sin_fragmentos_con_la_tabla_entera_no_pide_una_cantidad(
    tabla_de_staging: list[SimpleNamespace],
) -> None:
    """Revisión de #158: pgvector ordena por distancia sin umbral. Con la tabla
    entera trae 13 aunque ninguno sea del tema, así que "al menos 12" y
    "estos no son todos" eran falsos."""
    adapter, pedidos = _adapter(tabla_de_staging)

    out = await Sesiones().run({"texto": "ocupación de Airbnb"}, _ctx(adapter))

    [result] = out.results
    assert pedidos[0]["limit"] == 13
    assert result.metadata["tope_alcanzado"] is True
    assert not any("airbnb" in r["texto"].lower() for r in result.records)
    assert "total_records" not in result.metadata
    payload = json.loads(out.content)
    assert not isinstance(payload["filas_totales"], int)
    assert "al menos" not in out.content
    assert "no son todos" not in out.content
    assert "pueden ser de otros temas y no indican cuántos hay" in payload["aviso"]
    assert "si ninguno lo trata, decí que no se encontró nada" in payload["aviso"]


async def test_orador_sin_fragmentos_suyos_por_el_respaldo_local_real(
    tabla_de_staging: list[SimpleNamespace],
) -> None:
    """Revisión de #158: en staging `speaker` es NULL en las 1.030 filas. El
    filtro por orador vacía lo de pgvector y la búsqueda local (los archivos
    reales del repo) no filtra: baja el puntaje y trae fragmentos de nadie."""
    adapter, pedidos = _adapter(tabla_de_staging)

    out = await Sesiones().run({"texto": "jubilados", "orador": "Pichetto"}, _ctx(adapter))

    assert pedidos  # probó pgvector primero y respondió el respaldo local
    assert adapter._loaded  # noqa: SLF001
    [result] = out.results
    assert {r["orador"] for r in result.records} == {"No identificado"}
    assert result.metadata["orador_sin_atribuir"] is True
    assert "Ninguno trae a «Pichetto» como orador" in result.metadata["description"]
    payload = json.loads(out.content)
    assert "al menos" not in out.content
    assert "Ninguno está atribuido a «Pichetto»" in payload["aviso"]
    assert "no pudo filtrar por esa persona" in payload["aviso"]
    assert "ni digas cuántas veces habló" in payload["aviso"]
    assert payload["descripcion"] == result.metadata["description"]  # no se cortó


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
