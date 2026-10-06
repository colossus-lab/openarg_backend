"""H005: DDJJ cuyo total de bienes no coincide con su propio detalle.

La revisión del 05-oct encontró que el agente presentaba como "el caso más
llamativo" de enriquecimiento a un diputado cuyo total de bienes al cierre
($31.275,5 M) es 472 veces la suma de sus 27 bienes listados ($66,2 M) y 550
veces el total al inicio ($56,8 M): una carga errónea publicada con nombre y
apellido. El adaptador no comparaba el total con nada.

Lo que se prueba:

- el total al cierre se valida contra la suma del detalle y el total al
  inicio, con una tolerancia de un orden de magnitud;
- la fila inconsistente se marca (``inconsistente`` + motivo) y pierde la
  variación patrimonial;
- el ranking y las estadísticas la excluyen y lo dicen;
- un detalle incompleto con historia (el total ya venía declarado al inicio)
  no se marca: la parte pública casi nunca lista todo;
- con el dataset real del repo, sólo esa DDJJ queda afuera.

Y lo que pidió la revisión de #150/#151:

- la fila inconsistente no genera tarjeta en el frontend (la tarjeta
  destacaba el patrimonio y mostraba la variación ``null`` como «+$ 0»);
- al modelo le llega cuántas filas se excluyeron, nunca el nombre, y sólo
  si la fila habría entrado en el recorte pedido;
- el motivo se lo atribuye al registro del dataset, no a la persona;
- los ingresos que no cierran con la propia declaración (lo que queda
  después de los gastos multiplica por cien los bienes) quedan fuera del
  ranking por ingresos;
- la descripción de la herramienta prohíbe calificar variaciones.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from app.application.answers.engine import EngineRequest
from app.application.answers.tools.base import ToolContext
from app.application.answers.tools.conectores import DeclaracionesJuradas
from app.application.pipeline.connectors.ddjj import execute_ddjj_step
from app.application.pipeline.nodes.finalize import _extract_documents
from app.domain.entities.connectors.data_result import PlanStep
from app.infrastructure.adapters.connectors.ddjj_adapter import DDJJAdapter

M = 1_000_000


def _ddjj(
    nombre: str,
    *,
    inicio: float,
    cierre: float,
    detalle: list[float],
    deudas: float = 0,
    ingresos: float = 10 * M,
    gastos: float = 1 * M,
) -> dict:
    return {
        "cuit": f"20-{len(nombre):08d}-0",
        "nombre": nombre,
        "anioDeclaracion": "2024",
        "tipoDeclaracion": "ANUAL",
        "bienesInicio": inicio,
        "deudasInicio": 0,
        "bienesCierre": cierre,
        "deudasCierre": deudas,
        "patrimonioCierre": cierre - deudas,
        "ingresosTrabajoNeto": ingresos,
        "gastosPersonales": gastos,
        "bienes": [{"tipo": "INMUEBLES EN EL PAIS", "importe": v} for v in detalle],
    }


# Un total coherente con su detalle.
COHERENTE = _ddjj("PEREZ JUAN", inicio=50 * M, cierre=60 * M, detalle=[40 * M, 20 * M])
# Detalle incompleto, pero el total ya venía declarado al inicio: no es error.
CON_HISTORIA = _ddjj("GOMEZ ANA", inicio=1_540 * M, cierre=6_700 * M, detalle=[50 * M, 25 * M])
# El total multiplica por 500 el detalle y el inicio: no lo respalda nada.
CARGA_ERRONEA = _ddjj(
    "LOPEZ CARLOS",
    inicio=57 * M,
    cierre=31_000 * M,
    detalle=[26 * M, 20 * M, 16 * M],
    ingresos=900 * M,
)


@pytest.fixture
def adapter() -> DDJJAdapter:
    a = DDJJAdapter()
    a._loaded = True
    a._dataset = [COHERENTE, CON_HISTORIA, CARGA_ERRONEA]
    return a


@pytest.fixture(scope="module")
def real() -> DDJJAdapter:
    """El dataset del repo, el mismo que sirve prod."""
    a = DDJJAdapter()
    assert a.record_count == 195
    return a


def _names(records: list[dict]) -> list[str]:
    return [r["nombre"] for r in records]


# ── la validación de cada fila ─────────────────────────────


def test_la_fila_inconsistente_se_marca_con_el_motivo(adapter: DDJJAdapter) -> None:
    [row] = adapter.search("lopez").records
    assert row["inconsistente"] is True
    motivo = row["motivo_inconsistencia"]
    assert "500 veces la suma de los 3 bienes del detalle" in motivo
    assert "$31.000,0 millones" in motivo
    assert "$62,0 millones" in motivo
    # La variación de un total que no cierra no es una variación.
    assert row["variacion_patrimonial"] is None


def test_la_fila_coherente_no_se_marca(adapter: DDJJAdapter) -> None:
    [row] = adapter.search("perez").records
    assert row["inconsistente"] is False
    assert "motivo_inconsistencia" not in row
    assert row["variacion_patrimonial"] == 10 * M


def test_detalle_incompleto_con_total_al_inicio_no_es_inconsistente(
    adapter: DDJJAdapter,
) -> None:
    """89 veces el detalle, pero 4,4 veces el inicio: el total tiene historia."""
    [row] = adapter.get_by_name("gomez").records
    assert row["inconsistente"] is False
    assert row["variacion_patrimonial"] == 5_160 * M


@pytest.mark.parametrize(
    ("cierre", "inconsistente"),
    [(599 * M, False), (600 * M, False), (601 * M, True)],
)
def test_la_tolerancia_es_un_orden_de_magnitud(cierre: float, inconsistente: bool) -> None:
    """La base es lo mayor entre el detalle (60 M) y el inicio (50 M)."""
    a = DDJJAdapter()
    a._loaded = True
    a._dataset = [_ddjj("X", inicio=50 * M, cierre=cierre, detalle=[40 * M, 20 * M])]
    [row] = a.search("x").records
    assert row["inconsistente"] is inconsistente


def test_un_total_muy_por_debajo_del_detalle_tambien_es_inconsistente() -> None:
    """Hacia abajo: el detalle no puede sumar diez veces el total."""
    a = DDJJAdapter()
    a._loaded = True
    a._dataset = [
        _ddjj("BAJO", inicio=40 * M, cierre=5 * M, detalle=[40 * M, 20 * M]),
        _ddjj("CERO", inicio=40 * M, cierre=0, detalle=[40 * M]),
        # El detalle puede superar al total sin que sea error (un monto que
        # aparece dos veces, un bien en condominio): en el dataset llega a 2×.
        _ddjj("DETALLE MAYOR", inicio=40 * M, cierre=30 * M, detalle=[60 * M]),
    ]
    rows = {r["nombre"]: r for r in a.search("", limit=10).records}
    assert rows["BAJO"]["inconsistente"] is True
    assert "12 veces el total de bienes al cierre" in rows["BAJO"]["motivo_inconsistencia"]
    assert rows["CERO"]["inconsistente"] is True
    assert rows["DETALLE MAYOR"]["inconsistente"] is False


def test_sin_detalle_no_hay_contra_que_validar() -> None:
    a = DDJJAdapter()
    a._loaded = True
    a._dataset = [_ddjj("SIN DETALLE", inicio=0, cierre=0, detalle=[])]
    [row] = a.search("sin detalle").records
    assert row["inconsistente"] is False


# ── ranking y estadísticas ─────────────────────────────────


@pytest.mark.parametrize("sort_by", ["patrimonio", "bienes", "ingresos"])
@pytest.mark.parametrize("order", ["desc", "asc"])
def test_el_ranking_excluye_la_inconsistente_y_lo_dice(
    adapter: DDJJAdapter, sort_by: str, order: str
) -> None:
    result = adapter.ranking(sort_by=sort_by, top=10, order=order)
    assert "LOPEZ CARLOS" not in _names(result.records)
    assert len(result.records) == 2
    assert all(r["inconsistente"] is False for r in result.records)
    # Al modelo le llega cuántas; el nombre queda sólo para auditoría.
    assert result.metadata["excluidas_por_inconsistencia"] == 1
    assert result.metadata["excluidas_por_inconsistencia_nombres"] == ["LOPEZ CARLOS"]
    assert "LOPEZ" not in result.metadata["description"]
    assert "Se excluyó 1 declaración" in result.metadata["description"]


def test_el_ranking_avisa_solo_si_la_excluida_entraba_en_el_recorte(
    adapter: DDJJAdapter,
) -> None:
    """Los de menor patrimonio no tienen por qué enterarse de la carga errónea."""
    asc = adapter.ranking(sort_by="patrimonio", top=1, order="asc")
    assert _names(asc.records) == ["PEREZ JUAN"]
    assert "excluidas_por_inconsistencia" not in asc.metadata
    assert "excluidas_por_inconsistencia_nombres" not in asc.metadata
    assert "excluy" not in asc.metadata["description"]

    desc = adapter.ranking(sort_by="patrimonio", top=1, order="desc")
    assert _names(desc.records) == ["GOMEZ ANA"]
    assert desc.metadata["excluidas_por_inconsistencia"] == 1


def test_el_ranking_sin_inconsistentes_no_agrega_aviso() -> None:
    a = DDJJAdapter()
    a._loaded = True
    a._dataset = [COHERENTE, CON_HISTORIA]
    result = a.ranking()
    assert "excluidas_por_inconsistencia" not in result.metadata
    assert "excluy" not in result.metadata["description"]


def test_las_estadisticas_se_calculan_sin_la_inconsistente(adapter: DDJJAdapter) -> None:
    result = adapter.stats()
    [stats] = result.records
    assert stats["total"] == 3
    assert stats["patrimonio_maximo_nombre"] == "GOMEZ ANA"
    assert stats["patrimonio_promedio"] == pytest.approx((60 * M + 6_700 * M) / 2)
    # La fila de estadísticas la lee el modelo: lleva la cantidad, no el nombre.
    assert stats["excluidas_por_inconsistencia"] == 1
    assert "LOPEZ" not in json.dumps(stats)
    assert result.metadata["excluidas_por_inconsistencia_nombres"] == ["LOPEZ CARLOS"]
    assert "sin 1 declaración" in result.metadata["description"]


def test_el_motivo_se_lo_atribuye_al_dataset_no_a_la_persona(adapter: DDJJAdapter) -> None:
    """No se verificó si el error viene de la Oficina Anticorrupción o de la conversión."""
    [row] = adapter.search("lopez").records
    motivo = row["motivo_inconsistencia"]
    assert motivo.startswith("En el registro del dataset")
    assert "probable error de carga" in motivo
    assert "no coincide con la propia declaración" not in motivo


# ── los ingresos contra la propia declaración ──────────────

# Lo que queda de los ingresos después de los gastos multiplica por 135 lo
# mayor entre bienes al inicio, al cierre y deudas al inicio: no está en
# ningún lado de la declaración.
INGRESO_ERRONEO = _ddjj(
    "RUIZ MARTA",
    inicio=31 * M,
    cierre=37 * M,
    detalle=[37 * M],
    ingresos=5_000 * M,
    gastos=0,
)
# Gana 44 M, gasta 42 M y tiene 3,3 M en bienes: 13 veces sus bienes, pero lo
# gastó. Es legítimo y no se marca.
GASTA_LO_QUE_GANA = _ddjj(
    "DIAZ PEDRO",
    inicio=1.6 * M,
    cierre=3.3 * M,
    detalle=[3.3 * M],
    ingresos=44 * M,
    gastos=42.4 * M,
)


@pytest.fixture
def con_ingresos() -> DDJJAdapter:
    a = DDJJAdapter()
    a._loaded = True
    a._dataset = [COHERENTE, CON_HISTORIA, INGRESO_ERRONEO, GASTA_LO_QUE_GANA]
    return a


def test_ingresos_que_no_cierran_con_la_declaracion_se_marcan(
    con_ingresos: DDJJAdapter,
) -> None:
    [row] = con_ingresos.search("ruiz").records
    assert row["ingresos_inconsistentes"] is True
    motivo = row["motivo_inconsistencia_ingresos"]
    assert motivo.startswith("En el registro del dataset")
    assert "$5.000,0 millones" in motivo
    assert "135 veces" in motivo
    assert "probable error de carga" in motivo
    # El patrimonio cierra: la fila sigue siendo comparable por patrimonio.
    assert row["inconsistente"] is False
    assert row["variacion_patrimonial"] == 6 * M


def test_gastar_lo_que_se_gana_no_es_inconsistente(con_ingresos: DDJJAdapter) -> None:
    [row] = con_ingresos.search("diaz").records
    assert row["ingresos_inconsistentes"] is False
    assert "motivo_inconsistencia_ingresos" not in row


@pytest.mark.parametrize(
    ("ingresos", "inconsistente"),
    [(599 * M, False), (601 * M, False), (602 * M, True)],
)
def test_la_tolerancia_de_ingresos_es_un_orden_de_magnitud(
    ingresos: float, inconsistente: bool
) -> None:
    """La base es lo mayor entre bienes al inicio (50 M) y al cierre (60 M); gastos 1 M."""
    a = DDJJAdapter()
    a._loaded = True
    a._dataset = [
        _ddjj("X", inicio=50 * M, cierre=60 * M, detalle=[40 * M, 20 * M], ingresos=ingresos)
    ]
    [row] = a.search("x").records
    assert row["ingresos_inconsistentes"] is inconsistente


def test_las_deudas_al_inicio_tambien_respaldan_los_ingresos() -> None:
    """Lo que se ahorró pudo ir a cancelar deudas: no es un error."""
    fila = _ddjj("PAGA DEUDAS", inicio=5 * M, cierre=5 * M, detalle=[5 * M], ingresos=100 * M)
    fila["deudasInicio"] = 95 * M
    a = DDJJAdapter()
    a._loaded = True
    a._dataset = [fila]
    [row] = a.search("paga").records
    assert row["ingresos_inconsistentes"] is False


def test_el_ranking_por_ingresos_excluye_los_ingresos_que_no_cierran(
    con_ingresos: DDJJAdapter,
) -> None:
    por_ingresos = con_ingresos.ranking(sort_by="ingresos", top=10)
    assert "RUIZ MARTA" not in _names(por_ingresos.records)
    assert por_ingresos.metadata["excluidas_por_inconsistencia"] == 1
    assert por_ingresos.metadata["excluidas_por_inconsistencia_nombres"] == ["RUIZ MARTA"]

    # Su patrimonio cierra: sigue en el ranking por patrimonio y en las
    # estadísticas, sin aviso.
    por_patrimonio = con_ingresos.ranking(sort_by="patrimonio", top=10)
    assert "RUIZ MARTA" in _names(por_patrimonio.records)
    assert "excluidas_por_inconsistencia" not in por_patrimonio.metadata
    [stats] = con_ingresos.stats().records
    assert "excluidas_por_inconsistencia" not in stats


# ── la tarjeta del frontend ────────────────────────────────


def test_solo_las_filas_que_cierran_generan_tarjeta(con_ingresos: DDJJAdapter) -> None:
    """La tarjeta destaca el patrimonio, los ingresos y la variación sin lugar
    para el motivo, y el frontend tipa la variación como número: un ``null``
    se veía como «+$ 0» en verde."""
    con_ingresos._dataset.append(CARGA_ERRONEA)
    result = con_ingresos.search("", limit=10)
    assert len(result.records) == 5
    documents = _extract_documents([result]) or []
    assert sorted(_names(documents)) == ["DIAZ PEDRO", "GOMEZ ANA", "PEREZ JUAN"]
    assert all(isinstance(d["variacion_patrimonial"], float | int) for d in documents)


# ── el dataset real del repo ───────────────────────────────


def test_real_la_carga_erronea_se_marca(real: DDJJAdapter) -> None:
    [row] = real.search("brugge").records
    assert row["bienes_cierre"] == pytest.approx(31_275_522_665.75)
    assert row["inconsistente"] is True
    assert "472 veces la suma de los 27 bienes del detalle" in row["motivo_inconsistencia"]
    assert "$66,2 millones" in row["motivo_inconsistencia"]
    assert row["variacion_patrimonial"] is None


def test_real_solo_esa_ddjj_queda_afuera(real: DDJJAdapter) -> None:
    """La tolerancia no tapa los 27 detalles incompletos con patrimonio real."""
    result = real.ranking(top=50)
    assert result.metadata["excluidas_por_inconsistencia"] == 1
    assert result.metadata["excluidas_por_inconsistencia_nombres"] == ["BRUGGE JUAN FERNANDO"]
    assert sum(r["inconsistente"] for r in real.search("", limit=500).records) == 1
    names = _names(result.records)
    assert names[0] == "KIRCHNER MAXIMO CARLOS"
    for alto_con_detalle_incompleto in (
        "RITONDO CRISTIAN ADRIAN",
        "CARRIZO ANA CARLA",
        "BENEDETTI ATILIO FRANCISCO S",
        "RANDAZZO ANIBAL FLORENCIO",
    ):
        assert alto_con_detalle_incompleto in names


def test_real_las_estadisticas_no_las_infla_la_carga_erronea(real: DDJJAdapter) -> None:
    [stats] = real.stats().records
    assert stats["total"] == 195
    assert stats["patrimonio_maximo_nombre"] == "KIRCHNER MAXIMO CARLOS"
    # Con la carga errónea el promedio daba 509 M.
    assert stats["patrimonio_promedio"] == pytest.approx(350_864_232, rel=1e-6)
    assert stats["excluidas_por_inconsistencia"] == 1
    assert "BRUGGE" not in json.dumps(stats)


def test_real_los_rankings_que_no_la_incluian_no_la_nombran(real: DDJJAdapter) -> None:
    """Brugge quedaba en el puesto 195 por menor patrimonio y en el 35 por ingresos."""
    asc = real.ranking(sort_by="patrimonio", top=3, order="asc")
    assert "excluidas_por_inconsistencia" not in asc.metadata
    assert "BRUGGE" not in json.dumps(asc.metadata) + json.dumps(asc.records)

    por_ingresos = real.ranking(sort_by="ingresos", top=20)
    assert "BRUGGE" not in json.dumps(por_ingresos.metadata) + json.dumps(por_ingresos.records)


def test_real_los_ingresos_de_osuna_no_cierran_y_son_los_unicos(real: DDJJAdapter) -> None:
    """5.016 M de ingresos sin gastos, con 36,9 M de bienes: 136 veces. El
    siguiente cociente legítimo del dataset es 5,1."""
    filas = real.search("", limit=500).records
    assert [f["nombre"] for f in filas if f["ingresos_inconsistentes"]] == ["OSUNA BLANCA INES"]
    [osuna] = real.search("osuna blanca").records
    assert osuna["inconsistente"] is False
    assert "136 veces" in osuna["motivo_inconsistencia_ingresos"]
    assert "$5.016,3 millones" in osuna["motivo_inconsistencia_ingresos"]
    # Quien gasta lo que gana (13,5 veces sus bienes, en bruto) no se marca.
    [arrua] = real.search("arrua pedro").records
    assert arrua["ingresos_inconsistentes"] is False


def test_real_el_ranking_por_ingresos_ya_no_lo_encabeza_osuna(real: DDJJAdapter) -> None:
    result = real.ranking(sort_by="ingresos", top=3)
    assert _names(result.records) == [
        "POLINI JUAN CARLOS",
        "RANDAZZO ANIBAL FLORENCIO",
        "RITONDO CRISTIAN ADRIAN",
    ]
    assert result.metadata["excluidas_por_inconsistencia"] == 1
    assert result.metadata["excluidas_por_inconsistencia_nombres"] == ["OSUNA BLANCA INES"]
    # Por patrimonio sigue entrando: sus bienes cierran.
    assert "OSUNA BLANCA INES" in _names(real.ranking(top=195).records)


def test_real_la_busqueda_de_brugge_no_genera_tarjeta(real: DDJJAdapter) -> None:
    assert _extract_documents([real.search("brugge")]) is None
    assert _extract_documents([real.search("osuna blanca")]) is None
    # Una DDJJ que cierra sigue teniendo su tarjeta.
    [doc] = _extract_documents([real.search("kirchner maximo")]) or []
    assert doc["doc_type"] == "ddjj"
    assert isinstance(doc["variacion_patrimonial"], float | int)


def test_real_el_pipeline_viejo_tampoco_lo_pone_primero(real: DDJJAdapter) -> None:
    """El camino legacy con «¿quién es el diputado más rico?» (position=1)."""
    step = PlanStep(
        id="s1",
        action="query_ddjj",
        description="más rico",
        params={"action": "ranking", "sortBy": "patrimonio", "top": 10, "position": 1},
    )
    [result] = execute_ddjj_step(step, real)
    assert _names(result.records) == ["KIRCHNER MAXIMO CARLOS"]


# ── la herramienta del agente ──────────────────────────────


def _ctx(ddjj: DDJJAdapter) -> ToolContext:
    deps = MagicMock()
    deps.ddjj = ddjj
    return ToolContext(deps, EngineRequest("q", "u"))


async def test_herramienta_ranking_avisa_que_excluyo(real: DDJJAdapter) -> None:
    out = await DeclaracionesJuradas().run({"accion": "ranking"}, _ctx(real))
    payload = json.loads(out.content)
    assert "BRUGGE JUAN FERNANDO" not in _names(payload["filas"])
    # Cuántas, no quién: el nombre no le llega al modelo en ningún campo.
    assert payload["excluidas_por_inconsistencia"] == 1
    assert "BRUGGE" not in out.content
    assert "no cierran con la propia DDJJ" in payload["descripcion"]
    assert "probable error de carga" in payload["descripcion"]


async def test_herramienta_estadisticas_no_le_pasa_el_nombre(real: DDJJAdapter) -> None:
    out = await DeclaracionesJuradas().run({"accion": "estadisticas"}, _ctx(real))
    [fila] = json.loads(out.content)["filas"]
    assert fila["excluidas_por_inconsistencia"] == 1
    assert "BRUGGE" not in out.content


async def test_herramienta_ranking_ascendente_sin_aviso(real: DDJJAdapter) -> None:
    """«Los 3 con menor patrimonio» no tiene nada que ver con la carga errónea."""
    out = await DeclaracionesJuradas().run(
        {"accion": "ranking", "ordenar_por": "patrimonio", "orden": "asc", "cantidad": 3},
        _ctx(real),
    )
    payload = json.loads(out.content)
    assert "excluidas_por_inconsistencia" not in payload
    assert "BRUGGE" not in out.content
    assert "excluy" not in payload.get("descripcion", "")


async def test_herramienta_buscar_devuelve_la_marca(real: DDJJAdapter) -> None:
    out = await DeclaracionesJuradas().run({"accion": "buscar", "nombre": "brugge"}, _ctx(real))
    [fila] = json.loads(out.content)["filas"]
    assert fila["inconsistente"] is True
    assert fila["variacion_patrimonial"] is None
    assert fila["motivo_inconsistencia"]


async def test_herramienta_buscar_brugge_no_genera_tarjeta(real: DDJJAdapter) -> None:
    """El camino del agente: agent_engine arma las tarjetas con
    ``_extract_documents(evidence)``, y la evidencia es ``out.results``."""
    out = await DeclaracionesJuradas().run({"accion": "buscar", "nombre": "brugge"}, _ctx(real))
    assert out.results
    assert _extract_documents(out.results) is None


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


def test_la_descripcion_de_la_herramienta_prohibe_calificar_variaciones() -> None:
    """main y staging no tienen en el prompt la regla de neutralidad de la ola 2:
    la regla de la fuente va en la descripción de su herramienta."""
    description = DeclaracionesJuradas.spec.description
    assert (
        "No califiques ninguna variación, patrimonio ni ingreso como sospechoso o llamativo "
        "ni lo atribuyas a nada: describí cifras con nombre y año."
    ) in description
