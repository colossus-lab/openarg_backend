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
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from app.application.answers.engine import EngineRequest
from app.application.answers.tools.base import ToolContext
from app.application.answers.tools.conectores import DeclaracionesJuradas
from app.application.pipeline.connectors.ddjj import execute_ddjj_step
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
        "gastosPersonales": 1 * M,
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
    assert result.metadata["excluidas_por_inconsistencia"] == ["LOPEZ CARLOS"]
    assert "Se excluyó 1 declaración" in result.metadata["description"]


def test_el_ranking_sin_inconsistentes_no_agrega_aviso() -> None:
    a = DDJJAdapter()
    a._loaded = True
    a._dataset = [COHERENTE, CON_HISTORIA]
    result = a.ranking()
    assert "excluidas_por_inconsistencia" not in result.metadata
    assert "excluy" not in result.metadata["description"]


def test_las_estadisticas_se_calculan_sin_la_inconsistente(adapter: DDJJAdapter) -> None:
    [stats] = adapter.stats().records
    assert stats["total"] == 3
    assert stats["patrimonio_maximo_nombre"] == "GOMEZ ANA"
    assert stats["patrimonio_promedio"] == pytest.approx((60 * M + 6_700 * M) / 2)
    assert stats["excluidas_por_inconsistencia"] == ["LOPEZ CARLOS"]
    assert "sin 1 declaración" in adapter.stats().metadata["description"]


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
    assert result.metadata["excluidas_por_inconsistencia"] == ["BRUGGE JUAN FERNANDO"]
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
    assert stats["excluidas_por_inconsistencia"] == ["BRUGGE JUAN FERNANDO"]


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
    assert payload["excluidas_por_inconsistencia"] == ["BRUGGE JUAN FERNANDO"]
    assert "no coincide con su propio detalle" in payload["descripcion"]


async def test_herramienta_buscar_devuelve_la_marca(real: DDJJAdapter) -> None:
    out = await DeclaracionesJuradas().run({"accion": "buscar", "nombre": "brugge"}, _ctx(real))
    [fila] = json.loads(out.content)["filas"]
    assert fila["inconsistente"] is True
    assert fila["variacion_patrimonial"] is None
    assert fila["motivo_inconsistencia"]


def test_la_descripcion_de_la_herramienta_explica_la_marca() -> None:
    description = DeclaracionesJuradas.spec.description
    assert "`inconsistente: true`" in description
    assert "enriquecimiento" in description
