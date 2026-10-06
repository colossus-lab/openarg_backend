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
    notas_de_filtros,
    sql_filtro,
    validar_filtros,
)
from app.application.consultas.preparar import resolver_canonicos
from app.application.consultas.sql import CatalogRequestError, Params
from app.application.consultas.texto import plegar, plegar_sql
from app.domain.ports.sandbox.sql_sandbox import ColumnValueStats, TableValueStats
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
        # Los blancos de la columna se colapsan como los de lo pedido (H011).
        assert sql.startswith('lower(translate(btrim(regexp_replace("funcion_desc"::text, ')
        assert params == {"p0": "educacion y cultura"}
        # Mismo plegado de los dos lados.
        assert plegar("Educación y Cultura") == params["p0"]

    def test_la_enie_no_se_pliega(self) -> None:
        assert plegar("Peña") == "peña"

    def test_tabla_grande_suma_el_valor_real_de_pg_stats(self) -> None:
        f = Filter("funcion_desc", "=", "educacion y cultura", canonicos=("Educación y Cultura",))
        sql, params = _sql(f, tolerante=False)
        assert sql == '"funcion_desc"::text = ANY(:p0)'
        assert params == {"p0": ["Educación y Cultura", "educacion y cultura"]}

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

    def test_se_cuenta_con_que_valor_se_comparo(self) -> None:
        f = Filter("funcion_desc", "=", "educacion y cultura", canonicos=("Educación y Cultura",))
        nota = describir_filtro(f, tolerante=True) or ""
        assert "«educacion y cultura» coincide con «Educación y Cultura»" in nota
        exacto = Filter("funcion_desc", "=", "Salud", canonicos=("Salud",))
        assert describir_filtro(exacto, tolerante=True) is None
        assert describir_filtro(exacto, tolerante=False) is None


class TestBlancosIgualEnPythonYEnSQL:
    """H011 (revisión independiente del 05-oct): `plegar` colapsaba los espacios
    repetidos y los NBSP del valor pedido, pero `plegar_sql` sólo hacía `btrim`
    de la columna. Un valor real con dos espacios («Hosp. Zonal Gral. de Ag.
    Prof. Dr. R. Carrillo», 13 filas en staging) no se encontraba ni copiándolo
    exacto, y el diagnóstico de cero filas sugería el mismo valor: el reintento
    fallaba otra vez."""

    BLANCOS = [chr(c) for c in range(0x110000) if chr(c).isspace()]

    def test_el_sql_colapsa_los_mismos_blancos_que_python(self) -> None:
        sql = plegar_sql('"x"::text')
        assert 'btrim(regexp_replace("x"::text, ' in sql and "'g')" in sql
        # Cada blanco que `str.split()` colapsa está en la clase de la expresión.
        faltan = [hex(ord(c)) for c in self.BLANCOS if f"\\u{ord(c):04x}" not in sql]
        assert faltan == []

    def test_un_valor_con_espacios_dobles_o_nbsp_se_pliega_igual(self) -> None:
        pedido = "Hosp. Zonal Gral. de Ag.\u00a0 Prof. Dr. R. Carrillo"
        sql, params = _sql(Filter("funcion_desc", "=", pedido))
        assert params == {"p0": "hosp. zonal gral. de ag. prof. dr. r. carrillo"}
        assert "regexp_replace(" in sql

    def test_contiene_tambien_colapsa(self) -> None:
        sql, params = _sql(Filter("funcion_desc", "contiene", "Ag.  Prof"))
        assert params == {"p0": "%ag. prof%"}
        assert sql.startswith('lower(translate(btrim(regexp_replace("funcion_desc"::text, ')

    def test_el_validador_del_sandbox_acepta_la_expresion(self) -> None:
        sql, _ = _sql(Filter("funcion_desc", "=", "Hosp.  Carrillo"))
        built = f'SELECT "funcion_desc" FROM "raw"."cache_presupuesto_credito_2026" WHERE {sql}'
        assert _validate_sql(built, built=True) is None


def _stats(columna: str, mcv: list[str], filas: int = 1_426_810) -> TableValueStats:
    return TableValueStats(
        estimated_rows=filas,
        columns={columna: ColumnValueStats(columna, most_common_vals=mcv)},
    )


class TestTablaGrandeConValoresFrecuentesParciales:
    """Revisión del PR #133, verificado en staging el 05-oct (sólo lectura).

    En tablas de un millón de filas o más el filtro se REEMPLAZABA por los
    valores de `pg_stats.most_common_vals`, y lo pedido que no estaba entre
    los frecuentes desaparecía sin aviso: en el censo de hogares (1,4 M de
    filas) `en [Gnral.Pueyrredon, Gnral Viamonte]` contaba 110.822 en vez de
    113.255, y la nota decía «filtré por «Gnral.Pueyrredon»».
    """

    TIPOS = {"departamento": "text", "provincia": "text"}

    def _sql_grande(self, f: Filter, mcv: list[str]) -> tuple[str, dict, Filter]:
        (valido,) = validar_filtros([f], self.TIPOS)
        (resuelto,) = resolver_canonicos([valido], self.TIPOS, _stats(f.columna, mcv))
        params = Params()
        sql = sql_filtro(resuelto, self.TIPOS, params, tolerante=False)
        return sql, params.values, resuelto

    def test_en_no_pierde_el_valor_que_no_esta_entre_los_frecuentes(self) -> None:
        sql, params, _ = self._sql_grande(
            Filter("departamento", "en", ("Gnral.Pueyrredon", "Gnral Viamonte")),
            ["Gnral.Pueyrredon", "La Matanza"],
        )
        assert sql == '"departamento"::text = ANY(:p0)'
        assert set(params["p0"]) == {"Gnral.Pueyrredon", "Gnral Viamonte"}

    def test_igualdad_busca_lo_pedido_y_la_variante_frecuente(self) -> None:
        sql, params, _ = self._sql_grande(
            Filter("provincia", "=", "Córdoba"), ["Buenos Aires", "CORDOBA"]
        )
        assert set(params["p0"]) == {"CORDOBA", "Córdoba"}

    def test_distinto_excluye_lo_pedido_y_la_variante(self) -> None:
        sql, params, _ = self._sql_grande(
            Filter("provincia", "!=", "Córdoba"), ["Buenos Aires", "CORDOBA"]
        )
        assert sql == 'NOT ("provincia"::text = ANY(:p0))'
        assert set(params["p0"]) == {"CORDOBA", "Córdoba"}

    def test_la_nota_va_valor_por_valor_y_dice_que_se_busco_tal_cual(self) -> None:
        _, _, f = self._sql_grande(
            Filter("provincia", "en", ("córdoba", "Tierra del Fuego", "Buenos Aires")),
            ["Buenos Aires", "CORDOBA"],
        )
        nota = describir_filtro(f, tolerante=False) or ""
        assert "«córdoba» tal cual y como «CORDOBA»" in nota
        assert "«Tierra del Fuego» tal cual" in nota
        assert "Buenos Aires" not in nota  # está tal cual en la tabla: nada que decir
        assert "filtré por" not in nota  # nunca la lista parcial como el filtro entero

    def test_en_tabla_chica_la_nota_no_presenta_una_lista_parcial(self) -> None:
        """Con `en [salud, defensa]` y sólo «Salud» en pg_stats se filtra por los dos."""
        f = Filter("funcion_desc", "en", ("salud", "defensa"), canonicos=("Salud",))
        nota = describir_filtro(f, tolerante=True) or ""
        assert "«salud» coincide con «Salud»" in nota
        assert "filtré por" not in nota and "defensa" not in nota
        assert notas_de_filtros([f], TIPOS, tolerante=True) == [nota]

    def test_una_columna_no_de_texto_no_lleva_nota(self) -> None:
        f = Filter("monto", "=", "33.14")
        assert notas_de_filtros([f], TIPOS, tolerante=False) == []


def test_los_numeros_de_un_json_son_texto_sin_punto_cero() -> None:
    """Un cliente del MCP manda `en [2020, 2021.0]` sobre una columna de año."""
    (f,) = leer_filtros([{"columna": "anio", "operador": "en", "valores": [2020, 2021.0]}], 5)
    assert f.valor == ("2020", "2021")
    assert leer_filtros({"anio": 2020.0}, 5) == [Filter("anio", "=", "2020")]


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
