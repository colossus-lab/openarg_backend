"""La gramática de filtros compartida (modo datos del MCP y agente).

Auditoría 3.2/3.3 y "lo que no vio" 15, verificados en staging el 04-oct:
igualdad byte a byte («Educacion y Cultura» → 0 filas), sin operador `en`, y
valores interpolados que el validador rechazaba por contener una palabra de
SQL («Banco do Brasil», «Call Center»: 867 valores en 313 tablas).
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.application.consultas.filtros import (
    Filter,
    describir_filtro,
    leer_filtros,
    sql_filtro,
    validar_filtros,
)
from app.application.consultas.sql import CatalogRequestError, Params
from app.application.consultas.texto import plegar
from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import _validate_sql

TIPOS = {"funcion_desc": "text", "credito_devengado": "text", "monto": "numeric", "fecha": "text"}


def _sql(f: Filter, **kw: object) -> tuple[str, dict]:
    params = Params()
    (valido,) = validar_filtros([f], TIPOS)
    return sql_filtro(valido, TIPOS, params, **kw), params.values  # type: ignore[arg-type]


class TestLectura:
    def test_el_dict_viejo_es_igualdad(self) -> None:
        assert leer_filtros({"funcion_desc": "Salud"}, 5) == [Filter("funcion_desc", "=", "Salud")]

    def test_la_lista_con_operador_y_alias(self) -> None:
        filtros = leer_filtros(
            [
                {"columna": "monto", "operador": "mayor_que", "valor": "10"},
                {"columna": "funcion_desc", "operador": "en", "valores": ["Salud", "Defensa"]},
            ],
            5,
        )
        assert filtros == [
            Filter("monto", ">", "10"),
            Filter("funcion_desc", "en", ("Salud", "Defensa")),
        ]

    def test_dict_y_lista_dan_la_misma_consulta(self) -> None:
        a = validar_filtros(leer_filtros({"funcion_desc": "Salud"}, 5), TIPOS)
        b = validar_filtros(
            leer_filtros([{"columna": "funcion_desc", "operador": "=", "valor": "Salud"}], 5), TIPOS
        )
        assert sql_filtro(a[0], TIPOS, Params()) == sql_filtro(b[0], TIPOS, Params())

    @pytest.mark.parametrize(
        ("raw", "mensaje"),
        [
            ("funcion_desc=Salud", "objeto"),
            ([{"operador": "="}], "columna"),
            ([{"columna": "monto", "operador": ">"}], "Falta el `valor`"),
            ([{"columna": "funcion_desc", "operador": "en", "valor": 3}], "lista"),
        ],
    )
    def test_formas_invalidas(self, raw: object, mensaje: str) -> None:
        with pytest.raises(CatalogRequestError, match=mensaje):
            leer_filtros(raw, 5)

    @pytest.mark.parametrize(
        ("f", "mensaje"),
        [
            (Filter("no_existe", "=", "x"), "no es una columna"),
            (Filter("monto", "LIKE", "x"), "Operador"),
            (Filter("funcion_desc", "=", "x" * 201), "demasiado largo"),
            (Filter("funcion_desc", "en", tuple(str(i) for i in range(51))), "Como mucho 50"),
        ],
    )
    def test_validacion(self, f: Filter, mensaje: str) -> None:
        with pytest.raises(CatalogRequestError, match=mensaje):
            validar_filtros([f], TIPOS)


class TestIgualdadTolerante:
    def test_no_distingue_mayusculas_ni_acentos(self) -> None:
        sql, params = _sql(Filter("funcion_desc", "=", "  educacion  y CULTURA "))
        assert sql.startswith('lower(translate(btrim("funcion_desc"::text), ')
        assert params == {"p0": "educacion y cultura"}
        # Mismo plegado de los dos lados.
        assert plegar("Educación y Cultura") == params["p0"]

    def test_la_enie_no_se_pliega(self) -> None:
        assert plegar("Peña") == "peña"

    def test_tabla_grande_usa_el_valor_real_de_pg_stats(self) -> None:
        f = Filter("funcion_desc", "=", "educacion y cultura", canonicos=("Educación y Cultura",))
        sql, params = _sql(f, tolerante=False)
        assert sql == '"funcion_desc"::text = ANY(:p0)'
        assert params == {"p0": ["Educación y Cultura"]}

    def test_tabla_grande_sin_valor_real_es_igualdad_exacta(self) -> None:
        sql, params = _sql(Filter("funcion_desc", "=", "Salud"), tolerante=False)
        assert sql == '"funcion_desc"::text = :p0' and params == {"p0": "Salud"}

    def test_en_es_una_lista(self) -> None:
        sql, params = _sql(Filter("funcion_desc", "en", ("Salud", "Defensa")))
        assert sql.endswith("= ANY(:p0)") and params == {"p0": ["salud", "defensa"]}

    def test_distinto_niega_la_igualdad(self) -> None:
        sql, _ = _sql(Filter("funcion_desc", "!=", "Salud"))
        assert sql.startswith("NOT (")

    def test_una_columna_numerica_compara_exacto(self) -> None:
        sql, params = _sql(Filter("monto", "=", "33.14"))
        assert sql == '"monto"::text = :p0' and params == {"p0": "33.14"}

    def test_se_cuenta_con_que_valor_se_filtro(self) -> None:
        f = Filter("funcion_desc", "=", "educacion y cultura", canonicos=("Educación y Cultura",))
        assert "«Educación y Cultura»" in (describir_filtro(f) or "")
        assert describir_filtro(Filter("funcion_desc", "=", "Salud", canonicos=("Salud",))) is None


class TestContiene:
    def test_escapa_los_comodines_y_pliega(self) -> None:
        sql, params = _sql(Filter("funcion_desc", "contiene", "50%_Educación"))
        assert sql.endswith("LIKE :p0")
        assert params == {"p0": "%50\\%\\_educacion%"}

    def test_tabla_grande_usa_ilike(self) -> None:
        sql, params = _sql(Filter("funcion_desc", "contiene", "educ"), tolerante=False)
        assert sql == '"funcion_desc"::text ILIKE :p0' and params == {"p0": "%educ%"}


class TestComparaciones:
    def test_decimales_van_como_parametro(self) -> None:
        sql, params = _sql(Filter("credito_devengado", ">", "1000000.5"), formatos={})
        assert sql.endswith("> :p0") and params == {"p0": Decimal("1000000.5")}
        assert "1000000" not in sql

    def test_usa_el_formato_de_la_columna(self) -> None:
        sql, _ = _sql(Filter("credito_devengado", ">=", "10"), formatos={"credito_devengado": "ar"})
        assert "replace(btrim(\"credito_devengado\"::text), '.', '')::numeric" in sql

    def test_una_fecha_se_filtra_con_desde_hasta(self) -> None:
        with pytest.raises(CatalogRequestError, match="desde"):
            _sql(Filter("fecha", ">", "2020-01"))


class TestNadaDelUsuarioLlegaAlSQL:
    """Los valores van ligados: ni comillas ni palabras de SQL tocan el texto."""

    @pytest.mark.parametrize(
        "valor",
        [
            "x' OR '1'='1",
            "Banco do Brasil",
            "Call Center",
            "a -- b",
            "/* c */",
            "x >= 2000 y",
            "%_\\",
        ],
    )
    def test_el_valor_nunca_aparece_en_el_sql(self, valor: str) -> None:
        for op in ("=", "contiene", "!="):
            sql, params = _sql(Filter("funcion_desc", op, valor))
            assert valor not in sql and plegar(valor) not in sql
            assert params  # viaja aparte
            built = f'SELECT "funcion_desc" FROM "raw"."cache_presupuesto_credito_2026" WHERE {sql}'
            assert _validate_sql(built, built=True) is None
