"""`calcular`: la cuenta la arma nuestro código y cada rechazo vuelve al modelo.

El caso que lo motiva es Pinamar: 7.944 filas de una encuesta presentadas como
7.944 personas. Con `ponderar_por`, "cuántos" es la suma de los pesos.
"""

from __future__ import annotations

import pytest

from app.application.answers.aggregates import AggregateRequest, Filter, build_aggregate_query
from app.application.public_catalog import CatalogRequestError

ESTUDIO = "raw.datos_gob_ar__estudio_nacional_sobre_el_perfil_de__08ca74a9__v1"
TYPES = [
    ("pondera", "text"),
    ("dificultad_total", "text"),
    ("edad_agrupada", "text"),
    ("ingreso", "numeric"),
    ("_source_dataset_id", "text"),
]


def _req(**kw: object) -> AggregateRequest:
    base: dict[str, object] = {"table": ESTUDIO, "column_types": TYPES, "operacion": "conteo"}
    base.update(kw)
    return AggregateRequest(**base)  # type: ignore[arg-type]


def test_un_conteo_ponderado_suma_los_pesos_y_no_cuenta_filas() -> None:
    sql, cols = build_aggregate_query(
        _req(ponderar_por="pondera", filtros=[Filter("dificultad_total", "=", "1")])
    )
    assert 'sum((CASE WHEN "pondera"::text ~' in sql
    assert "count(*)" not in sql
    assert "\"dificultad_total\"::text = '1'" in sql
    assert cols == ["valor"]


def test_sin_ponderador_es_un_conteo_de_filas() -> None:
    sql, _ = build_aggregate_query(_req())
    assert "count(*) AS valor" in sql


def test_una_columna_numerica_no_se_convierte() -> None:
    sql, _ = build_aggregate_query(_req(operacion="promedio", columna="ingreso"))
    assert 'avg("ingreso")' in sql


def test_el_promedio_ponderado_divide_por_los_pesos_de_las_filas_con_valor() -> None:
    sql, _ = build_aggregate_query(
        _req(operacion="promedio", columna="ingreso", ponderar_por="pondera")
    )
    assert "NULLIF(sum(CASE WHEN" in sql


def test_agrupar_ordena_por_el_valor() -> None:
    sql, cols = build_aggregate_query(
        _req(ponderar_por="pondera", agrupar_por=["edad_agrupada"], orden="desc")
    )
    assert 'GROUP BY "edad_agrupada"' in sql
    assert "ORDER BY valor DESC NULLS LAST" in sql
    assert cols == ["edad_agrupada", "valor"]


def test_un_valor_con_comillas_no_rompe_la_consulta() -> None:
    sql, _ = build_aggregate_query(_req(filtros=[Filter("edad_agrupada", "=", "x' OR '1'='1")]))
    assert "'x'' OR ''1''=''1'" in sql


def test_contiene_escapa_los_comodines() -> None:
    sql, _ = build_aggregate_query(_req(filtros=[Filter("edad_agrupada", "contiene", "50%_")]))
    assert "ILIKE '%50\\%\\_%'" in sql


@pytest.mark.parametrize(
    ("kw", "mensaje"),
    [
        ({"operacion": "suma"}, "no es una columna"),
        ({"operacion": "suma", "columna": "no_existe"}, "no es una columna"),
        ({"operacion": "borrar"}, "operacion"),
        ({"ponderar_por": "_source_dataset_id"}, "no es una columna"),
        ({"agrupar_por": ["a", "b", "c", "d"]}, "agrupar_por"),
        ({"filtros": [Filter("ingreso", ">", "mucho")]}, "necesita un número"),
        ({"filtros": [Filter("ingreso", "LIKE", "1")]}, "Operador"),
        ({"desde": "2020-01"}, "columna de fecha"),
        ({"limite": 5000}, "limite"),
    ],
)
def test_los_pedidos_invalidos_vuelven_con_un_mensaje_para_el_modelo(
    kw: dict[str, object], mensaje: str
) -> None:
    with pytest.raises(CatalogRequestError, match=mensaje):
        build_aggregate_query(_req(**kw))


def test_las_columnas_internas_no_se_pueden_usar() -> None:
    """`_source_dataset_id` es del colector, no un dato."""
    with pytest.raises(CatalogRequestError):
        build_aggregate_query(_req(agrupar_por=["_source_dataset_id"]))
