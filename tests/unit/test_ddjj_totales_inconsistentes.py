"""H005: DDJJ cuyas cifras no cierran con la propia declaración.

La revisión del 05-oct encontró que el agente presentaba como "el caso más
llamativo" de enriquecimiento a un diputado cuyo total de bienes al cierre
($31.275,5 M) era 472 veces la suma de su detalle: una carga errónea del JSON
viejo, publicada con nombre y apellido. Desde el 08-oct las DDJJ salen de
`raw.cache_ddjj_*` y la marca se calcula al cargar (`ddjj_tasks`; las reglas se
prueban en `tests/integration/test_ddjj_oa_carga_db.py`, y las consultas en
`test_ddjj_adaptador_db.py`). Acá se prueba lo que pasa con una fila marcada:

- la fila lleva `inconsistente` y el motivo (que habla del registro, no de la
  persona) y pierde la variación patrimonial;
- al modelo le llega cuántas filas se excluyeron de un ranking, nunca el nombre;
- `ingresos_inconsistentes` sólo saca la fila del ranking por ingresos, y su
  motivo no la llama error de la persona;
- la fila marcada no genera tarjeta ni barra de gráfico; en un ranking, las
  tarjetas conservan su puesto;
- el aviso de exclusión llega aunque el ranking sea largo;
- la descripción de la herramienta prohíbe calificar variaciones.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest

from app.application.answers.engine import EngineRequest
from app.application.answers.tools.base import (
    MAX_CONTENT_CHARS,
    ToolContext,
    result_for_model,
    to_json,
)
from app.application.answers.tools.conectores import DeclaracionesJuradas
from app.application.pipeline import chart_builder
from app.application.pipeline.chart_builder import build_deterministic_charts
from app.application.pipeline.connectors.ddjj import execute_ddjj_step
from app.application.pipeline.nodes.finalize import _extract_documents
from app.domain.entities.connectors.data_result import DataResult, PlanStep
from app.infrastructure.adapters.connectors import ddjj_adapter as da

M = 1_000_000
_ADAPTADOR = da.DDJJAdapter(session_factory=MagicMock())


def _fila(
    nombre: str,
    *,
    bienes: float,
    deudas: float = 0,
    inicio: float | None = None,
    detalle: float | None = None,
    ingresos: float = 10 * M,
    gastos: float = 1 * M,
    inconsistente: bool = False,
    ingresos_inconsistentes: bool = False,
    fuente: str = da.FUENTE_OA,
) -> dict[str, Any]:
    """Una fila de `raw.cache_ddjj_declaraciones` (anual)."""
    inicio = bienes * 0.8 if inicio is None else inicio
    return {
        "fuente": fuente,
        "dj_id": abs(hash(nombre)) % 10**8,
        "cuit": f"20{abs(hash(nombre)) % 10**9:09d}",
        "nombre": nombre,
        "cargo": "Diputado Nacional",
        "organismo": "HONORABLE CAMARA DE DIPUTADOS DE LA NACION",
        "poder": "legislativo",
        "anio": 2024,
        "tipo": "Anual",
        "bienes_inicio": Decimal(str(inicio)),
        "deudas_inicio": Decimal(0),
        "bienes_cierre": Decimal(str(bienes)),
        "deudas_cierre": Decimal(str(deudas)),
        "bienes": Decimal(str(bienes)),
        "deudas": Decimal(str(deudas)),
        "patrimonio": Decimal(str(bienes - deudas)),
        "variacion_patrimonial": Decimal(str(bienes - deudas - inicio)),
        "ingresos_netos": Decimal(str(ingresos)),
        "gastos_personales": Decimal(str(gastos)),
        "detalle_bienes_cierre": None if detalle is None else Decimal(str(detalle)),
        "inconsistente": inconsistente,
        "ingresos_inconsistentes": ingresos_inconsistentes,
        "url_fuente": "https://datos.jus.gob.ar/x.csv",
    }


COHERENTE = _fila("PEREZ JUAN", bienes=60 * M, detalle=60 * M)
CON_HISTORIA = _fila("GOMEZ ANA", bienes=6_700 * M, inicio=1_540 * M, detalle=75 * M)
CARGA_ERRONEA = _fila(
    "LOPEZ CARLOS", bienes=31_000 * M, inicio=57 * M, detalle=62 * M, inconsistente=True
)
AHORRO_RARO = _fila(
    "RUIZ MARTA", bienes=5 * M, inicio=5 * M, ingresos=900 * M, ingresos_inconsistentes=True
)


def _busqueda(*filas: dict[str, Any]) -> DataResult:
    return _ADAPTADOR._resultado(
        "Búsqueda DDJJ", [da.registro(f, []) for f in filas], fuente=da.FUENTE_OA
    )


def _ranking(*filas: dict[str, Any], excluidas: list[str] | None = None) -> DataResult:
    resultado = _ADAPTADOR._resultado(
        "Ranking DDJJ",
        [da.registro(f, compacto=True) for f in filas],
        fuente=da.FUENTE_OA,
        descripcion="Ranking de funcionarios nacionales con mayor patrimonio declarado en 2024.",
        ranking=True,
    )
    if excluidas:
        resultado.metadata["excluidas_por_inconsistencia"] = len(excluidas)
        resultado.metadata["excluidas_por_inconsistencia_nombres"] = excluidas
        resultado.metadata["description"] += (
            " Se excluyó 1 declaración que habría entrado en este ranking: su registro "
            "publicado tiene cifras que no cierran con la propia DDJJ (probable error de "
            "carga), así que no es comparable (excluidas_por_inconsistencia)."
        )
    return resultado


def _names(records: list[dict]) -> list[str]:
    return [r["nombre"] for r in records]


# ── la fila ────────────────────────────────────────────────


def test_la_fila_inconsistente_lleva_la_marca_y_el_motivo() -> None:
    fila = da.registro(CARGA_ERRONEA, [])
    assert fila["inconsistente"] is True
    assert fila["variacion_patrimonial"] is None
    # El total declarado sigue visible: es lo que dice la DDJJ.
    assert fila["bienes_cierre"] == 31_000 * M
    motivo = fila["motivo_inconsistencia"]
    assert "500 veces la suma de los bienes del detalle" in motivo
    assert "probable error de carga" in motivo


def test_el_motivo_se_lo_atribuye_al_registro_no_a_la_persona() -> None:
    motivo = da.registro(CARGA_ERRONEA, [])["motivo_inconsistencia"]
    assert motivo.startswith("En el registro de la Oficina Anticorrupción")
    assert "LOPEZ" not in motivo
    for palabra in ("ocult", "enriquec", "sospech", "irregular"):
        assert palabra not in motivo.lower()


def test_la_fila_coherente_no_se_marca() -> None:
    fila = da.registro(COHERENTE, [])
    assert fila["inconsistente"] is False
    assert fila["ingresos_inconsistentes"] is False
    assert "motivo_inconsistencia" not in fila
    assert fila["variacion_patrimonial"] == 12 * M


def test_los_ingresos_que_no_cierran_no_se_llaman_error_de_la_persona() -> None:
    fila = da.registro(AHORRO_RARO, [])
    assert fila["ingresos_inconsistentes"] is True and fila["inconsistente"] is False
    motivo = fila["motivo_inconsistencia_ingresos"]
    assert "quedan fuera del ranking por ingresos" in motivo
    assert "siguen en los demás rankings" in motivo
    assert "error" not in motivo.lower()
    # Sus bienes cierran: la variación sigue.
    assert fila["variacion_patrimonial"] == 0


def test_caba_explica_que_no_hay_patrimonio_neto() -> None:
    fila = da.registro(
        {
            **COHERENTE,
            "fuente": da.FUENTE_CABA,
            "patrimonio": None,
            "deudas": None,
            "bienes_por_tipo": {"inmuebles": 50, "dinero_efectivo": 10},
        },
    )
    assert fila["fuente"] == "Ciudad de Buenos Aires"
    assert fila["patrimonio_cierre"] is None
    assert "sin deudas" in fila["nota"]
    assert fila["resumen_bienes"] == {"INMUEBLES": 50.0, "DINERO EFECTIVO": 10.0}


def test_caba_inconsistente_explica_la_mediana() -> None:
    motivo = da.motivo_inconsistencia({**CARGA_ERRONEA, "fuente": da.FUENTE_CABA})
    assert "1.000 veces la mediana" in motivo


def test_las_filas_del_ranking_llevan_la_marca_solo_si_vale_true() -> None:
    filas = {r["nombre"]: r for r in _ranking(COHERENTE, CON_HISTORIA, AHORRO_RARO).records}
    assert filas["RUIZ MARTA"]["ingresos_inconsistentes"] is True
    assert filas["RUIZ MARTA"]["motivo_inconsistencia_ingresos"]
    for nombre in ("PEREZ JUAN", "GOMEZ ANA"):
        assert "inconsistente" not in filas[nombre]
        assert "ingresos_inconsistentes" not in filas[nombre]
    # En `buscar` (filas completas) van las dos, también en false.
    [perez] = _busqueda(COHERENTE).records
    assert perez["inconsistente"] is False and perez["ingresos_inconsistentes"] is False


def test_el_detalle_va_con_titularidad_y_resumen() -> None:
    detalle = [
        {
            "tipo": "INMUEBLES EN EL PAIS",
            "descripcion": "Casa",
            "importe": Decimal(40 * M),
            "titularidad": Decimal("50.00"),
        },
        {
            "tipo": "AUTOMOTORES EN EL PAIS",
            "descripcion": "Auto",
            "importe": Decimal(20 * M),
            "titularidad": None,
        },
    ]
    fila = da.registro(COHERENTE, detalle)
    assert fila["cantidad_bienes"] == 2
    assert fila["bienes_detalle"][0] == {
        "tipo": "INMUEBLES EN EL PAIS",
        "descripcion": "Casa",
        "importe": 40 * M,
        "titularidad": "50%",
    }
    assert fila["resumen_bienes"] == {"INMUEBLES": 40 * M, "AUTOMOTORES": 20 * M}


# ── la herramienta del agente ──────────────────────────────


class _DDJJFalso:
    """Devuelve resultados armados con el formateador real y anota qué le pidieron."""

    def __init__(self, resultado: DataResult) -> None:
        self.resultado = resultado
        self.pedidos: list[tuple[str, tuple, dict]] = []

    async def search(self, *a: Any, **kw: Any) -> DataResult:
        self.pedidos.append(("search", a, kw))
        return self.resultado

    async def ranking(self, *a: Any, **kw: Any) -> DataResult:
        self.pedidos.append(("ranking", a, kw))
        return self.resultado

    async def stats(self, *a: Any, **kw: Any) -> DataResult:
        self.pedidos.append(("stats", a, kw))
        return self.resultado

    async def evolucion(self, *a: Any, **kw: Any) -> DataResult:
        self.pedidos.append(("evolucion", a, kw))
        return self.resultado


def _ctx(ddjj: Any) -> ToolContext:
    deps = MagicMock()
    deps.ddjj = ddjj
    return ToolContext(deps, EngineRequest("q", "u"))


async def test_herramienta_ranking_avisa_cuantas_excluyo_pero_no_quien() -> None:
    falso = _DDJJFalso(_ranking(COHERENTE, CON_HISTORIA, excluidas=["LOPEZ CARLOS"]))
    out = await DeclaracionesJuradas().run(
        {"accion": "ranking", "cargo": "diputado nacional", "anio": 2023}, _ctx(falso)
    )
    payload = json.loads(out.content)
    assert payload["excluidas_por_inconsistencia"] == 1
    assert "LOPEZ" not in out.content
    assert "probable error de carga" in payload["descripcion"]
    # Los filtros llegan al adaptador; la jurisdicción, por defecto nacional.
    [(_, args, kw)] = falso.pedidos
    assert args == ("patrimonio", 10, "desc")
    assert kw == {
        "anio": 2023,
        "jurisdiccion": "nacional",
        "poder": None,
        "organismo": None,
        "cargo": "diputado nacional",
    }


async def test_herramienta_estadisticas_trae_lo_que_promete_la_descripcion() -> None:
    """La descripción prometía «cuántos tienen patrimonio negativo» y la fila no lo
    traía: el 07-oct, ddjj_004 tomó el mínimo como conteo."""
    fila = {
        "total": 200,
        "anio": 2024,
        "patrimonio_promedio": 1.0,
        "patrimonio_mediano": 1.0,
        "cantidad_con_patrimonio_negativo": 2,
        "excluidas_por_inconsistencia": 1,
    }
    falso = _DDJJFalso(_ADAPTADOR._resultado("Estadísticas", [fila], fuente=da.FUENTE_OA))
    out = await DeclaracionesJuradas().run({"accion": "estadisticas"}, _ctx(falso))
    [vuelta] = json.loads(out.content)["filas"]
    assert "`cantidad_con_patrimonio_negativo` es cuántos tienen patrimonio negativo" in (
        DeclaracionesJuradas.spec.description
    )
    for campo in (
        "total",
        "patrimonio_promedio",
        "patrimonio_mediano",
        "anio",
        "cantidad_con_patrimonio_negativo",
        "excluidas_por_inconsistencia",
    ):
        assert campo in vuelta, campo


async def test_herramienta_buscar_devuelve_la_marca_y_no_genera_tarjeta() -> None:
    """El camino del agente: agent_engine arma las tarjetas con
    ``_extract_documents(evidence)``, y la evidencia es ``out.results``."""
    falso = _DDJJFalso(_busqueda(CARGA_ERRONEA))
    out = await DeclaracionesJuradas().run({"accion": "buscar", "nombre": "lopez"}, _ctx(falso))
    [fila] = json.loads(out.content)["filas"]
    assert fila["inconsistente"] is True and fila["variacion_patrimonial"] is None
    assert _extract_documents(out.results) is None


async def test_herramienta_sin_resultados_dice_que_cubren_las_fuentes() -> None:
    vacio = _ADAPTADOR._resultado("Búsqueda", [], fuente=None)
    vacio.metadata["cobertura"] = "Oficina Anticorrupción 2012–2024 (...)"
    out = await DeclaracionesJuradas().run(
        {"accion": "buscar", "nombre": "nadie"}, _ctx(_DDJJFalso(vacio))
    )
    payload = json.loads(out.content)
    assert payload["filas"] == [] and payload["cobertura"].startswith("Oficina Anticorrupción")


async def test_herramienta_evolucion_y_validaciones() -> None:
    falso = _DDJJFalso(_busqueda(COHERENTE))
    await DeclaracionesJuradas().run(
        {"accion": "evolucion", "nombre": "Perez", "jurisdiccion": "caba"}, _ctx(falso)
    )
    assert falso.pedidos == [("evolucion", ("Perez",), {"jurisdiccion": "caba"})]
    from app.application.answers.tools.base import ToolInputError

    with pytest.raises(ToolInputError):
        await DeclaracionesJuradas().run(
            {"accion": "ranking", "jurisdiccion": "cordoba"}, _ctx(falso)
        )
    with pytest.raises(ToolInputError):
        await DeclaracionesJuradas().run({"accion": "evolucion"}, _ctx(falso))


@pytest.mark.parametrize("cantidad", [20, 50])
async def test_herramienta_ranking_largo_entra_entero_y_con_el_aviso(cantidad: int) -> None:
    """Con 20 filas el JSON pasaba el tope de 12.000 caracteres: el modelo perdía la
    descripción y `excluidas_por_inconsistencia`, y leía la última fila cortada."""
    filas = [
        _fila(f"APELLIDO{i:02d} NOMBRE SEGUNDO", bienes=(500 - i) * M + 0.37, detalle=1)
        for i in range(cantidad)
    ]
    falso = _DDJJFalso(_ranking(*filas, excluidas=["LOPEZ CARLOS"]))
    out = await DeclaracionesJuradas().run({"accion": "ranking", "cantidad": cantidad}, _ctx(falso))
    assert len(out.content) <= MAX_CONTENT_CHARS
    payload = json.loads(out.content)  # JSON válido: no se cortó nada
    [result] = out.results
    assert payload["excluidas_por_inconsistencia"] == 1
    assert "LOPEZ" not in out.content
    assert out.content.index('"excluidas_por_inconsistencia"') < out.content.index('"filas"')
    assert payload["filas"] == result.records[: len(payload["filas"])]
    assert payload["filas_totales"] == cantidad


def test_la_descripcion_de_la_herramienta_explica_la_marca() -> None:
    description = DeclaracionesJuradas.spec.description
    assert "`inconsistente: true`" in description
    assert "`ingresos_inconsistentes: true`" in description
    assert "enriquecimiento" in description
    # El problema es del registro, no de la persona.
    assert "probable error de carga" in description
    assert "sin atribuírselo a la persona" in description
    assert "no nombres a la persona excluida salvo que pregunten por ella" in description
    assert "el total declarado no coincide con el detalle de sus bienes" not in description
    # El ahorro que no se refleja no se presenta como error de la persona.
    assert "no lo presentes como error de la persona ni como irregularidad" in description


def test_la_descripcion_de_la_herramienta_prohibe_calificar_variaciones() -> None:
    description = DeclaracionesJuradas.spec.description
    assert (
        "No califiques ninguna variación, patrimonio ni ingreso como sospechoso o llamativo "
        "ni lo atribuyas a nada."
    ) in description
    assert "La cifra de una persona va con su nombre y el año de la DDJJ." in description


def test_la_descripcion_dice_que_los_ingresos_solo_excluyen_del_ranking_por_ingresos() -> None:
    description = DeclaracionesJuradas.spec.description
    assert "el ranking y las estadísticas ya la excluyen" not in description
    assert "`ingresos_inconsistentes`, sólo del ranking por ingresos" in description
    assert "sigue en los demás rankings y en las estadísticas" in description


def test_la_descripcion_dice_que_cubre_y_que_no() -> None:
    description = DeclaracionesJuradas.spec.description
    assert "Oficina Anticorrupción (`jurisdiccion` nacional, 2012 en adelante" in description
    assert "Ciudad de Buenos Aires (`jurisdiccion` caba, 2023 en adelante" in description
    assert "No hay declaraciones de jueces en general ni de provincias." in description
    assert "no están ajustados por inflación" in description
    # «diputados» se filtra por cargo: el poder legislativo incluye a sus empleados.
    assert "Para «diputados» o «senadores», usá `cargo`" in description


# ── el pipeline viejo ──────────────────────────────────────


async def test_el_pipeline_viejo_busca_y_avisa_que_cubren_las_fuentes() -> None:
    falso = MagicMock()
    vacio = _ADAPTADOR._resultado("Búsqueda", [], fuente=None)

    async def _vacio(*a: Any, **kw: Any) -> DataResult:
        return vacio

    async def _cobertura() -> dict[str, tuple[int, int]]:
        return {da.FUENTE_OA: (2012, 2024), da.FUENTE_CABA: (2023, 2026)}

    falso.get_by_name = _vacio
    falso.cobertura = _cobertura
    falso.describir_cobertura = _ADAPTADOR.describir_cobertura
    step = PlanStep(id="s", action="query_ddjj", description="", params={"nombre": "Nadie"})
    [result] = await execute_ddjj_step(step, falso)
    nota = result.records[0]["nota"]
    assert "Oficina Anticorrupción 2012–2024" in nota
    assert "Ciudad de Buenos Aires 2023–2026" in nota
    assert "195" not in nota


# ── el gráfico ─────────────────────────────────────────────


def _bars(charts: list[dict]) -> list[str]:
    return [row["nombre"] for chart in charts for row in chart["data"]]


def test_el_grafico_de_una_busqueda_no_lleva_la_carga_erronea() -> None:
    result = _busqueda(CARGA_ERRONEA, COHERENTE, CON_HISTORIA)
    charts = build_deterministic_charts([result])
    assert charts
    assert "LOPEZ CARLOS" not in _bars(charts)
    assert len(_bars(charts)) == 2


def test_los_ingresos_que_no_cierran_salen_solo_de_un_grafico_de_ingresos() -> None:
    result = _ranking(COHERENTE, CON_HISTORIA, AHORRO_RARO)
    # Por patrimonio sus bienes cierran: sigue en el gráfico.
    charts = build_deterministic_charts([result])
    assert "RUIZ MARTA" in _bars(charts)
    # Con eje de ingresos, no.
    filas = [
        {"nombre": r["nombre"], "ingresos_trabajo_neto": r["ingresos_trabajo_neto"]}
        | ({"ingresos_inconsistentes": True} if r.get("ingresos_inconsistentes") else {})
        for r in result.records
    ]
    ingresos = _ADAPTADOR._resultado("Ranking", filas, fuente=da.FUENTE_OA)
    charts = build_deterministic_charts([ingresos])
    assert charts[0]["yKeys"] == ["ingresos_trabajo_neto"]
    assert "RUIZ MARTA" not in _bars(charts)
    assert len(_bars(charts)) == 2


def test_la_marca_solo_filtra_el_grafico_de_las_ddjj() -> None:
    """Una tabla cualquiera con una columna `inconsistente` no pierde filas."""
    filas = [
        {"nombre": "A", "total": 1, "inconsistente": True},
        {"nombre": "B", "total": 2, "inconsistente": False},
    ]
    result = _ADAPTADOR._resultado("Tabla", filas, fuente=da.FUENTE_OA)
    result.source = "sandbox:nl2sql"
    assert _bars(build_deterministic_charts([result])) == ["A", "B"]


def test_una_columna_booleana_no_es_una_serie_del_grafico() -> None:
    """``isinstance(False, int)`` es True: una marca entraba como serie de 0 y 1."""
    filas = [
        {"fecha": "2024-01-01", "valor": 10.0, "estimado": False},
        {"fecha": "2024-02-01", "valor": 12.0, "estimado": True},
    ]
    result = _ADAPTADOR._resultado("Tabla", filas, fuente=da.FUENTE_OA)
    result.source = "sandbox:nl2sql"
    [chart] = build_deterministic_charts([result])
    assert chart["yKeys"] == ["valor"]


@pytest.fixture
def eje_fecha(monkeypatch: pytest.MonkeyPatch) -> None:
    """Un gráfico de línea con varias series: el que salía cuando
    ``fecha_nacimiento`` contaba como fecha. Acá el eje es `fecha` a la fuerza,
    para probar que la marca no decida las series."""
    original = chart_builder.is_date_column
    monkeypatch.setattr(
        chart_builder, "is_date_column", lambda name: name == "fecha" or original(name)
    )
    monkeypatch.setattr(chart_builder, "es_fecha_de_atributo", lambda *_: False, raising=False)


@pytest.mark.usefixtures("eje_fecha")
def test_un_grafico_de_varias_series_no_toma_las_marcas_ni_pierde_series() -> None:
    result = _busqueda(CARGA_ERRONEA, COHERENTE, CON_HISTORIA)
    for i, r in enumerate(result.records):
        r["fecha"] = f"2024-0{i + 1}-01"
    [chart] = build_deterministic_charts([result])
    assert chart["xKey"] == "fecha"
    assert "inconsistente" not in chart["yKeys"]
    assert "ingresos_inconsistentes" not in chart["yKeys"]
    # La variación null de la carga errónea, primera fila, sacaba la serie para todos.
    assert "variacion_patrimonial" in chart["yKeys"]
    assert len(chart["data"]) == 2


# ── lo que lee el modelo ───────────────────────────────────


def _result(records: list[dict], **metadata: object) -> DataResult:
    return DataResult(
        source="x",
        portal_name="Fuente",
        portal_url="",
        dataset_title="Tabla",
        format="json",
        records=records,
        metadata=dict(metadata),
    )


def test_result_for_model_pone_las_filas_al_final_y_enteras() -> None:
    filas = [{"id": i, "texto": "x" * 1_000} for i in range(30)]
    payload = result_for_model(_result(filas, description="Aviso."), aviso_extra=1)
    assert list(payload)[-1] == "filas"
    content = to_json(payload)
    assert len(content) <= MAX_CONTENT_CHARS
    vuelta = json.loads(content)
    assert vuelta["descripcion"] == "Aviso."
    assert vuelta["aviso_extra"] == 1
    assert vuelta["filas"] == filas[: len(vuelta["filas"])]
    assert vuelta["nota"] == f"Se muestran {len(vuelta['filas'])} de 30 filas."


def test_result_for_model_sin_cambios_cuando_entra() -> None:
    filas = [{"id": i} for i in range(5)]
    payload = result_for_model(_result(filas, description="d", units="pesos"))
    assert payload == {
        "titulo": "Tabla",
        "fuente": "Fuente",
        "filas_totales": 5,
        "unidades": "pesos",
        "descripcion": "d",
        "filas": filas,
    }


def test_result_for_model_una_fila_que_sola_no_entra_igual_va() -> None:
    filas = [{"texto": "x" * 20_000}, {"texto": "y"}]
    payload = result_for_model(_result(filas, description="Aviso."))
    assert payload["filas"] == filas[:1]
    assert payload["nota"] == "Se muestran 1 de 2 filas."
    assert '"descripcion":"Aviso."' in to_json(payload)


# ── las tarjetas ───────────────────────────────────────────


def test_las_tarjetas_de_un_ranking_conservan_su_puesto() -> None:
    """El frontend numera las tarjetas por posición: si se salteaba la fila sin
    tarjeta, la tarjeta #3 mostraba a quien está 4.º. Se cortan antes de ella."""
    result = _ranking(CON_HISTORIA, COHERENTE, AHORRO_RARO, _fila("DIAZ PEDRO", bienes=1 * M))
    documents = _extract_documents([result]) or []
    assert _names(documents) == ["GOMEZ ANA", "PEREZ JUAN"]


def test_en_una_busqueda_la_fila_sin_tarjeta_no_corta_las_demas() -> None:
    """Una búsqueda no es un ranking: se saltea la fila y siguen las demás."""
    documents = _extract_documents([_busqueda(CARGA_ERRONEA, COHERENTE, AHORRO_RARO, CON_HISTORIA)])
    assert _names(documents or []) == ["PEREZ JUAN", "GOMEZ ANA"]


def test_una_fila_de_caba_sin_patrimonio_no_lleva_tarjeta() -> None:
    caba = {**COHERENTE, "fuente": da.FUENTE_CABA, "patrimonio": None}
    assert _extract_documents([_busqueda(caba)]) is None


# ── el filtro de cargo ─────────────────────────────────────


@pytest.mark.parametrize(
    ("cargo", "patrones"),
    [
        ("diputado nacional", ["DIPUTAD%", "%NACIONAL%"]),
        ("Senadora", ["SENADOR%"]),
        ("ministro", ["MINISTR%"]),
        ("juez", ["JUEZ%"]),
        ("", []),
    ],
)
def test_el_cargo_se_busca_sin_genero_y_desde_el_principio(cargo: str, patrones: list[str]) -> None:
    assert da._patrones_cargo(cargo) == patrones


def test_el_patron_de_cargo_trae_diputadas_y_no_candidatos() -> None:
    import fnmatch

    def coincide(valor: str) -> bool:
        normal = da._sin_tildes(valor).lstrip()
        return all(
            fnmatch.fnmatchcase(normal, p.replace("%", "*"))
            for p in da._patrones_cargo("diputado nacional")
        )

    assert coincide("DIPUTADA NACIONAL")
    assert coincide("Diputado Nacional por la Provincia de Córdoba")
    assert not coincide("CANDIDATO A DIPUTADO NACIONAL")
    assert not coincide("ASESOR DEL DIPUTADO NACIONAL")
