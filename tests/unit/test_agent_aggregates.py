"""`calcular`: la cuenta la arma nuestro código y cada rechazo vuelve al modelo.

El caso que lo motiva es Pinamar: 7.944 filas de una encuesta presentadas como
7.944 personas. Con `ponderar_por`, "cuántos" es la suma de los pesos.

Y los de la auditoría del 04-oct: "12.500" leído como 12,5 (P0), un filtro
sin coincidencias que devolvía `valor = 0` como dato, y `> 1000000.5` roto
por el auto-fix del sandbox.
"""

from __future__ import annotations

import pytest

from app.application.answers.aggregates import (
    FILAS,
    FILAS_CON_VALOR,
    AggregateRequest,
    Filter,
    build_aggregate_query,
    numeric_columns,
)
from app.application.public_catalog import CatalogRequestError
from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import _validate_sql

ESTUDIO = "raw.datos_gob_ar__estudio_nacional_sobre_el_perfil_de__08ca74a9__v1"
TYPES = [
    ("pondera", "text"),
    ("dificultad_total", "text"),
    ("edad_agrupada", "text"),
    ("ingreso", "numeric"),
    ("anio", "bigint"),
    ("_source_dataset_id", "text"),
]


def _req(**kw: object) -> AggregateRequest:
    base: dict[str, object] = {"table": ESTUDIO, "column_types": TYPES, "operacion": "conteo"}
    base.update(kw)
    return AggregateRequest(**base)  # type: ignore[arg-type]


def test_un_conteo_ponderado_suma_los_pesos_y_no_cuenta_filas() -> None:
    q = build_aggregate_query(
        _req(ponderar_por="pondera", filtros=[Filter("dificultad_total", "=", "1")])
    )
    assert 'sum((CASE WHEN btrim("pondera"::text) ~' in q.sql
    assert "count(*) AS valor" not in q.sql
    assert q.params == {"p0": "1"}
    assert q.columns == ["valor", FILAS, FILAS_CON_VALOR]


def test_sin_ponderador_es_un_conteo_de_filas() -> None:
    q = build_aggregate_query(_req())
    assert "count(*) AS valor" in q.sql
    assert FILAS_CON_VALOR not in q.sql  # contar filas no necesita un número


def test_siempre_dice_sobre_cuantas_filas_calculo() -> None:
    """QW7: sin esto, un filtro sin coincidencias daba `valor = 0` como dato."""
    q = build_aggregate_query(_req(operacion="suma", columna="ingreso"))
    assert f"count(*) AS {FILAS}" in q.sql
    assert f'count("ingreso") AS {FILAS_CON_VALOR}' in q.sql


def test_una_columna_numerica_no_se_convierte() -> None:
    q = build_aggregate_query(_req(operacion="promedio", columna="ingreso"))
    assert 'avg("ingreso")' in q.sql


def test_el_promedio_ponderado_divide_por_los_pesos_de_las_filas_con_valor() -> None:
    q = build_aggregate_query(_req(operacion="promedio", columna="ingreso", ponderar_por="pondera"))
    assert "NULLIF(sum(CASE WHEN" in q.sql


def test_agrupar_ordena_por_el_valor() -> None:
    q = build_aggregate_query(
        _req(ponderar_por="pondera", agrupar_por=["edad_agrupada"], orden="desc")
    )
    assert 'GROUP BY "edad_agrupada"' in q.sql
    assert "ORDER BY valor DESC NULLS LAST" in q.sql
    assert q.columns == ["edad_agrupada", "valor", FILAS, FILAS_CON_VALOR]


def test_pide_una_fila_de_mas_para_saber_si_se_corto() -> None:
    q = build_aggregate_query(_req(agrupar_por=["edad_agrupada"], limite=10))
    assert q.sql.endswith("LIMIT 11") and q.limite == 10


def test_ordenar_por_una_columna_agrupada() -> None:
    q = build_aggregate_query(_req(agrupar_por=["anio"], ordenar_por="anio", orden="asc"))
    assert q.sql.split("ORDER BY", 1)[1].strip().startswith("(CASE")  # el año normalizado
    with pytest.raises(CatalogRequestError, match="ordenar_por"):
        build_aggregate_query(_req(agrupar_por=["anio"], ordenar_por="ingreso"))


class TestFormatoDeLosNumeros:
    """P0: con el patrón viejo, "12.500" era 12,5 y "1.234.567" NULL."""

    def test_columna_argentina(self) -> None:
        q = build_aggregate_query(
            _req(operacion="suma", columna="pondera", formatos={"pondera": "ar"})
        )
        assert "replace(btrim(\"pondera\"::text), '.', '')::numeric" in q.sql
        assert r"^\s*-?[0-9]+(\.[0-9]+)?\s*$" not in q.sql

    def test_formato_desconocido_no_lee_los_ambiguos(self) -> None:
        q = build_aggregate_query(_req(operacion="suma", columna="pondera"))
        assert "THEN NULL" in q.sql

    def test_columnas_que_se_leen_como_numero(self) -> None:
        assert numeric_columns(_req(operacion="suma", columna="pondera")) == ["pondera"]
        assert numeric_columns(_req(ponderar_por="pondera")) == ["pondera"]
        assert numeric_columns(_req()) == []


def test_un_valor_con_comillas_no_toca_el_sql() -> None:
    q = build_aggregate_query(_req(filtros=[Filter("edad_agrupada", "=", "x' OR '1'='1")]))
    assert "OR '1'" not in q.sql and "or '1'" not in q.sql
    assert q.params == {"p0": "x' or '1'='1"}


def test_contiene_escapa_los_comodines() -> None:
    q = build_aggregate_query(_req(filtros=[Filter("edad_agrupada", "contiene", "50%_")]))
    assert q.params == {"p0": "%50\\%\\_%"}


def test_mayor_que_con_decimales_va_como_parametro() -> None:
    """Con el valor en el SQL, el auto-fix lo dejaba en `> '1000000'.5`."""
    q = build_aggregate_query(_req(filtros=[Filter("ingreso", "mayor_que", "1000000.5")]))
    assert '"ingreso" > :p0' in q.sql and "1000000" not in q.sql
    assert _validate_sql(q.sql, built=True) is None


def test_periodo_sobre_una_columna_de_anio() -> None:
    q = build_aggregate_query(_req(desde="2020", hasta="2021"))
    assert q.fecha is not None and q.fecha.nombre == "anio"
    assert q.params == {"p0": "2020-01-01", "p1": "2021-12-31"}


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
        ({"column_types": [("valor", "numeric")], "desde": "2020-01"}, "No reconocí"),
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


def test_el_validador_del_sandbox_acepta_lo_que_armamos() -> None:
    q = build_aggregate_query(
        _req(
            operacion="suma",
            columna="pondera",
            agrupar_por=["edad_agrupada"],
            filtros=[
                Filter("dificultad_total", "en", ("1", "2")),
                Filter("edad_agrupada", "contiene", "Banco do Brasil"),
            ],
            desde="2019",
            formatos={"pondera": "ar"},
        )
    )
    assert _validate_sql(q.sql, built=True) is None
