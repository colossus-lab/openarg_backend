"""Números guardados como texto: el formato se decide por columna.

El P0 de la auditoría verificada el 04-oct: `calcular` leía "12.500" como
12,5 (Pauta CABA: 119.968 en vez de 242.635.557). Y el arreglo obvio, el CASE
por celda del legacy, mete el error inverso: "-59.796" (una longitud en
formato inglés) pasaba a -59796.

Y los de la revisión independiente del 05-oct (H001, H009, H041, H042): el
desempate por enteros cortos leía mil veces más altos los minutos de la ENUT,
un solo valor de la muestra decidía la columna, la evidencia sin ambiguos no
se usaba y Python limpiaba espacios que el SQL dejaba.
"""

from __future__ import annotations

import re
from decimal import Decimal
from typing import Any

import pytest

from app.application.consultas.numeros import (
    ESPACIOS,
    RE_AMBIGUO,
    RE_AMBIGUO_COMA,
    RE_AMBIGUO_PUNTO,
    RE_AR,
    RE_EN,
    RE_ENTERO,
    clase_valor,
    expresion_ambiguo,
    expresion_numero,
    leer_numero,
    numero_de_filtro,
    perfil_columna,
)
from app.application.consultas.preparar import TOLERANTE_MAX_FILAS, preparar
from app.application.consultas.sql import CatalogRequestError
from app.domain.ports.sandbox.sql_sandbox import ColumnValueStats, SandboxResult, TableValueStats

# Cómo queda una columna de texto limpia en el SQL: los mismos caracteres que
# saca Python (`ESPACIOS`), no sólo el espacio de `btrim(x)`.
LIMPIO = 'btrim("{}"::text, chr(32) || chr(9) || chr(10) || chr(13) || chr(160))'

# Cinco valores que sólo se pueden leer de una forma, de cada formato: la
# evidencia mínima para decidir una columna.
EVIDENCIA_AR = ["1.234,5", "7,25", "1.000.000", "3,5", "12.345,67"]
EVIDENCIA_EN = ["1,234.5", "7.25", "1,000,000", "3.5", "12,345.67"]


@pytest.mark.parametrize(
    ("valor", "clase"),
    [
        ("1234", "entero"),
        ("-7", "entero"),
        ("12.500", "ambiguo_punto"),
        ("-59.796", "ambiguo_punto"),
        ("1,250", "ambiguo_coma"),
        ("1.234.567", "ar"),
        ("1.234,56", "ar"),
        ("1234,5", "ar"),
        ("0,250", "ar"),  # un cero adelante no es un grupo de miles
        ("1,234,567", "en"),
        ("1,234.5", "en"),
        ("12.5", "en"),
        ("0.125", "en"),
        ("1234.567", "en"),  # cuatro cifras antes del punto: no es miles argentino
        ("s/d", None),
        ("", None),
        ("1.234.567.89", None),
        # H042: los espacios que limpia el SQL, ni más ni menos.
        ("\xa012.500\xa0", "ambiguo_punto"),
        ("12.500\t", "ambiguo_punto"),
        ("0,82\r\r\n", "ar"),
        (" 1,234.5 ", "en"),
        ("12\u2007", None),  # otro espacio que `btrim` no saca: tampoco acá
    ],
)
def test_cada_valor_se_clasifica_por_lo_que_puede_ser(valor: str, clase: str | None) -> None:
    assert clase_valor(valor) == clase


class TestPerfilDeColumna:
    def test_pauta_caba_los_miles_de_varios_grupos_son_argentinos(self) -> None:
        # Valores reales de IMPORTE en caba__pauta_publicitaria (prod y staging
        # `ecd09a9a`, 94 de 1.312 con dos grupos de miles). Antes alcanzaba con
        # uno solo («1.218.600»), que es lo que H009 pide no hacer.
        perfil = perfil_columna(
            "IMPORTE",
            ["27.830", "7.000", "15.000", "1.218.600", "1.344.663", "1.446.434", "1.456.840"]
            + ["1.271.613"],
        )
        assert perfil.formato == "ar" and perfil.problema is None
        assert not perfil_columna("IMPORTE", ["27.830", "7.000", "15.000", "1.218.600"]).formato

    def test_decimales_ingleses_de_tres_cifras_son_ingleses(self) -> None:
        perfil = perfil_columna(
            "lon",
            ["-59.796", "-58.381", "-64.183", "-60.0", "0.125", "-34.6", "-31.42", "-27.5"],
        )
        assert perfil.formato == "en"

    def test_sin_valores_ambiguos_ni_evidencia_suficiente_no_hace_falta_decidir(self) -> None:
        perfil = perfil_columna("monto", ["1.234,5", "12", "3,75"])
        assert perfil.formato is None and perfil.problema is None

    def test_enteros_cortos_y_miles_con_punto_no_deciden(self) -> None:
        """H001: el desempate "el separador aparece desde mil" no es evidencia.

        "500" y "12.500" son también una columna inglesa con tres decimales.
        Antes este test daba por buena la lectura argentina.
        """
        perfil = perfil_columna("cantidad", ["500", "999", "12.500", "3.000"])
        assert perfil.formato is None
        assert perfil.problema is not None and "ningún valor" in perfil.problema

    def test_enteros_cortos_y_miles_con_coma_no_deciden(self) -> None:
        """El espejo de H001: "1,250" también es uno coma veinticinco."""
        perfil = perfil_columna("cantidad", ["500", "999", "12,500"])
        assert perfil.formato is None and perfil.problema is not None

    def test_enut_minutos_con_tres_decimales_ingleses_no_se_leen_como_miles(self) -> None:
        """H001: TSS_ACT_DEPORTE de la ENUT (staging) salía 190,88 minutos por
        día contra 16,85, con un máximo de 291.665 minutos en un día de 1.440."""
        muestra = ["0"] * 150 + ["60", "120", "30", "90", "58.333", "116.666", "29.166"]
        perfil = perfil_columna("TSS_ACT_DEPORTE", muestra)
        assert perfil.formato is None
        assert perfil.problema is not None and "58.333" in perfil.problema

    def test_un_solo_valor_no_decide_la_columna(self) -> None:
        """H009: RETENCION_DIAS_U de SADE (CABA) quedaba argentina por un único
        «1,22» del histograma en una tabla e inglesa por desempate en la otra:
        promedios de 7,567 y 8,820 para los mismos 500.000 valores."""
        base = ["365", "730", "1,039", "1,043", "1,058"]
        for muestra in (base, [*base, "1,22"]):
            perfil = perfil_columna("RETENCION_DIAS_U", muestra)
            assert perfil.formato is None and perfil.problema is not None

    def test_hacen_falta_cinco_valores_de_evidencia(self) -> None:
        cuatro = perfil_columna("x", [*EVIDENCIA_AR[:4], "12.500"])
        assert cuatro.formato is None
        assert cuatro.problema is not None and "sólo 4 valores distintos" in cuatro.problema
        assert perfil_columna("x", [*EVIDENCIA_AR, "12.500"]).formato == "ar"
        assert perfil_columna("x", [*EVIDENCIA_EN, "12.500"]).formato == "en"

    def test_un_solo_valor_del_otro_formato_es_mezcla(self) -> None:
        """Revisión del PR #148: con una mayoría del 90 %, 9 argentinos y 1
        inglés decidían «argentino», y este test lo daba por bueno. La muestra
        (pg_stats y las primeras 200 filas) no representa a la columna: ante
        cualquier evidencia contraria se rechaza, como antes de la mayoría."""
        nueve_y_uno = [*EVIDENCIA_AR, *EVIDENCIA_AR[:4], "12.5", "12.500"]
        perfil = perfil_columna("x", nueve_y_uno)
        assert perfil.formato is None
        assert perfil.problema is not None and "mezcla" in perfil.problema

    def test_subte_mayoria_argentina_en_la_muestra_inglesa_en_la_columna(self) -> None:
        """fr1_km de caba__subte_trenes_despachados (staging `b0b65caa`, 500.000
        filas). La muestra trae 209 argentinos, 8 ingleses y 27 ambiguos con
        punto; la columna entera, 50.494 argentinos y 159.571 ingleses. Leída
        como argentina (lo que hacía la mayoría del 90 %), «9.721» km eran
        9.721 y el promedio daba 3.248 km por tren en vez de 280."""
        muestra = (
            ["11,77", "10,29", "4,29", "9,72"] * 52
            + ["8,04"]
            + ["11.77", "10.64", "10.29", "14.8", "0.0", "7.22", "9.5", "4.3"]
            + ["4.294", "9.721", "8.044"] * 9
            + ["1,234"] * 4
            + ["10", "11"]
        )
        perfil = perfil_columna("fr1_km", muestra)
        assert perfil.formato is None
        assert perfil.problema is not None and "mezcla" in perfil.problema
        assert leer_numero("9.721", perfil.formato) is None

    def test_mezcla_sin_ambiguos_en_la_muestra_queda_sin_formato(self) -> None:
        """Revisión del PR #148: sin ambiguos en la muestra, una mezcla quedaba
        decidida por mayoría y los ambiguos de fuera de la muestra se leían con
        ese formato sin aviso (72 columnas en staging). Sin formato quedan en
        NULL y el cálculo los cuenta aparte."""
        perfil = perfil_columna("CAMBIO", [*EVIDENCIA_EN, *EVIDENCIA_EN, "56,16"])
        assert perfil.formato is None and perfil.problema is None
        assert leer_numero("12.500", perfil.formato) is None

    def test_un_valor_repetido_cuenta_una_sola_vez(self) -> None:
        """Revisión del PR #148: la evidencia se contaba por apariciones. Un
        «1,22» repetido cinco veces en la muestra decidía la columna, que es H009
        por otro camino (los verificadores leen RETENCION_DIAS_U como inglesa)."""
        base = ["365", "730", "1,039", "1,043", "1,058"]
        perfil = perfil_columna("RETENCION_DIAS_U", [*base, *["1,22"] * 5])
        assert perfil.formato is None
        assert perfil.problema is not None and "sólo 1 valor distinto" in perfil.problema

    def test_pg_stats_y_las_filas_no_cuentan_dos_veces_el_mismo_valor(self) -> None:
        """La muestra junta most_common_vals, histogram_bounds y las primeras
        200 filas: en una tabla chica cada valor llega dos veces. En
        caba__casos_penales (staging `d1fd5984`, 6 filas) tres valores
        argentinos sumaban seis apariciones y alcanzaban el mínimo."""
        muestra = ["100,0", "12,4", "87,6"] * 2 + ["69.572"]
        perfil = perfil_columna("col_69_572", muestra)
        assert perfil.formato is None
        assert perfil.problema is not None and "sólo 3 valores distintos" in perfil.problema

    def test_con_evidencia_clara_decide_aunque_la_muestra_no_tenga_ambiguos(self) -> None:
        """H041: presupuesto APN (credito_pagado, staging) traía 384 valores
        argentinos y ningún ambiguo en la muestra; el formato quedaba sin
        decidir y los «6,555» del resto de la tabla, en NULL."""
        perfil = perfil_columna("credito_pagado", ["1.234.567,89", "2.345,10", "0,5", "37,08", "8"])
        assert perfil.formato is None  # cuatro valores: no alcanza
        perfil = perfil_columna("credito_pagado", [*EVIDENCIA_AR, "17"])
        assert perfil.formato == "ar" and perfil.problema is None
        assert leer_numero("6,555", perfil.formato) == Decimal("6.555")
        assert leer_numero("12.500", perfil.formato) == Decimal("12500")

    def test_los_espacios_raros_no_cuentan_como_evidencia_distinta(self) -> None:
        """H042: «0,82\\r\\r\\n» es un número argentino también para el perfil."""
        perfil = perfil_columna("v_max_med", [f"{v}\r\r\n" for v in EVIDENCIA_AR] + ["\xa012.500"])
        assert perfil.formato == "ar"

    def test_solo_ambiguos_se_rechaza_con_un_mensaje_claro(self) -> None:
        perfil = perfil_columna("IMPORTE", ["27.830", "7.000", "15.000"])
        assert perfil.formato is None
        assert perfil.problema is not None
        assert "27.830" in perfil.problema and "mil veces" in perfil.problema

    def test_formatos_mezclados_con_ambiguos_se_rechaza(self) -> None:
        perfil = perfil_columna("x", ["1.234,5", "1,234.5", "12.500"])
        assert perfil.problema is not None and "mezcla" in perfil.problema

    def test_enteros_largos_no_alcanzan_para_decidir(self) -> None:
        # "1500" sin separador y "1.500" con: incoherente en los dos formatos.
        assert perfil_columna("x", ["1500", "1.500"]).problema is not None


class TestLecturaEnPython:
    """`leer_numero` es el espejo en Python del CASE de SQL."""

    @pytest.mark.parametrize(
        ("valor", "formato", "esperado"),
        [
            ("12.500", "ar", Decimal("12500")),
            ("12.500", "en", Decimal("12.500")),
            ("12.500", None, None),  # ambiguo sin formato: NULL, nunca mal leído
            ("1,250", "ar", Decimal("1.250")),
            ("1,250", "en", Decimal("1250")),
            ("1.234.567,89", None, Decimal("1234567.89")),
            ("1,234,567.89", None, Decimal("1234567.89")),
            ("-59.796", "en", Decimal("-59.796")),
            ("s/d", "ar", None),
            # H042: nbsp, tab, CR y LF alrededor se limpian igual que en SQL.
            ("\xa012.500\xa0", "ar", Decimal("12500")),
            ("12.500\t", "en", Decimal("12.500")),
            ("0,82\r\r\n", None, Decimal("0.82")),
            ("12\u2007", None, None),
        ],
    )
    def test_lee_segun_el_formato(self, valor: str, formato: str | None, esperado: Decimal) -> None:
        assert leer_numero(valor, formato) == esperado


class TestLosCincoFormatosDelPedido:
    """Los cinco valores del pedido original, en una columna argentina, en una
    inglesa y en una sin formato (sin evidencia en la muestra)."""

    # valor: (columna argentina, columna inglesa, columna sin formato).
    # None: no se calcula (el valor es ambiguo y la columna no dice el formato).
    CASOS = {
        "12.500": (Decimal("12500"), Decimal("12.500"), None),
        "12,5": (Decimal("12.5"), Decimal("12.5"), Decimal("12.5")),
        "1.234,56": (Decimal("1234.56"), Decimal("1234.56"), Decimal("1234.56")),
        "1,234.56": (Decimal("1234.56"), Decimal("1234.56"), Decimal("1234.56")),
        "12.5": (Decimal("12.5"), Decimal("12.5"), Decimal("12.5")),
    }

    @pytest.mark.parametrize("valor", CASOS)
    @pytest.mark.parametrize(
        ("columna", "i"),
        [(EVIDENCIA_AR, 0), (EVIDENCIA_EN, 1), (["500", "999", "7"], 2)],
        ids=["argentina", "inglesa", "sin_formato"],
    )
    def test_en_la_muestra(self, valor: str, columna: list[str], i: int) -> None:
        perfil = perfil_columna("c", [*columna, valor])
        esperado = self.CASOS[valor][i]
        if esperado is None:
            assert perfil.problema is not None and perfil.formato is None
        else:
            assert perfil.problema is None
            assert leer_numero(valor, perfil.formato) == esperado

    @pytest.mark.parametrize("valor", CASOS)
    @pytest.mark.parametrize(("formato", "i"), [("ar", 0), ("en", 1), (None, 2)])
    def test_fuera_de_la_muestra(self, valor: str, formato: str | None, i: int) -> None:
        """Un valor que la muestra no vio se lee con el formato de la columna;
        en una sin formato, el ambiguo queda en NULL (nunca mal leído)."""
        assert leer_numero(valor, formato) == self.CASOS[valor][i]


class TestExpresionSQL:
    def test_una_columna_numerica_no_se_convierte(self) -> None:
        assert expresion_numero("monto", "numeric", None) == '"monto"'
        assert expresion_numero("monto", "double precision", "ar") == '"monto"'

    def test_el_viejo_patron_no_vuelve(self) -> None:
        """El patrón que leía "12.500" como 12,5 aceptaba cualquier punto decimal."""
        sql = expresion_numero("IMPORTE", "text", "ar")
        assert r"^\s*-?[0-9]+(\.[0-9]+)?\s*$" not in sql

    def test_columna_argentina_quita_el_punto_de_los_ambiguos(self) -> None:
        sql = expresion_numero("IMPORTE", "text", "ar")
        x = LIMPIO.format("IMPORTE")
        assert f"WHEN {x} ~ '{RE_AMBIGUO_PUNTO}' THEN replace({x}, '.', '')::numeric" in sql

    def test_columna_inglesa_lee_el_punto_como_decimal(self) -> None:
        sql = expresion_numero("lon", "text", "en")
        x = LIMPIO.format("lon")
        assert f"WHEN {x} ~ '{RE_AMBIGUO_PUNTO}' THEN {x}::numeric" in sql

    def test_sql_limpia_los_mismos_espacios_que_python(self) -> None:
        """H042: `btrim(x)` sólo saca el espacio; `str.strip()` sacaba además
        nbsp, tab, CR y LF. «0,82\\r\\r\\n» (RNI de CABA) contaba como número
        en la muestra y quedaba en NULL en la suma."""
        assert set(ESPACIOS) == {" ", "\t", "\n", "\r", "\xa0"}
        sql = expresion_numero("v", "text", "ar")
        assert 'btrim("v"::text)' not in sql
        assert sql.count(LIMPIO.format("v")) == sql.count("btrim(")
        assert {chr(int(c)) for c in re.findall(r"chr\((\d+)\)", LIMPIO)} == set(ESPACIOS)

    def test_condicion_de_ambiguo(self) -> None:
        """H041: el cálculo cuenta aparte las filas con un número ambiguo. Con
        un solo `btrim` y un solo regex por fila (revisión del PR #148)."""
        x = LIMPIO.format("monto")
        assert expresion_ambiguo("monto", "text") == f"({x} ~ '{RE_AMBIGUO}')"
        assert expresion_ambiguo("monto", "numeric") is None

    @pytest.mark.parametrize(
        "valor",
        ["12.500", "-59.796", "1,250", "+999,000", "0.500", "1234.567", "12.5", "1.234.567", "x"],
    )
    def test_el_regex_de_ambiguo_es_la_union_de_los_dos(self, valor: str) -> None:
        dos = bool(re.match(RE_AMBIGUO_PUNTO, valor) or re.match(RE_AMBIGUO_COMA, valor))
        assert bool(re.match(RE_AMBIGUO, valor)) == dos

    def test_formato_desconocido_deja_los_ambiguos_en_null(self) -> None:
        sql = expresion_numero("x", "text", None)
        assert f"~ '{RE_AMBIGUO_PUNTO}' THEN NULL" in sql
        assert f"~ '{RE_AMBIGUO_COMA}' THEN NULL" in sql

    def test_los_ambiguos_se_prueban_antes_que_los_patrones_generales(self) -> None:
        sql = expresion_numero("x", "text", "ar")
        orden = [sql.index(p) for p in (RE_ENTERO, RE_AMBIGUO_PUNTO, RE_AMBIGUO_COMA, RE_AR, RE_EN)]
        assert orden == sorted(orden)


class TestNumeroDeFiltro:
    def test_decimal_con_punto(self) -> None:
        assert numero_de_filtro("1000000.5", "c", ">") == Decimal("1000000.5")

    def test_miles_argentinos_de_varios_grupos(self) -> None:
        assert numero_de_filtro("1.000.000", "c", ">") == Decimal("1000000")

    def test_ambiguo_pide_escribirlo_sin_separador(self) -> None:
        with pytest.raises(CatalogRequestError, match="dos formas"):
            numero_de_filtro("1.500", "c", ">")

    def test_texto_no_es_un_numero(self) -> None:
        with pytest.raises(CatalogRequestError, match="necesita un número"):
            numero_de_filtro("mucho", "c", ">")


# ── Tablas chicas: el formato de la muestra se confirma en la columna entera ──

# consultas_medicas de buenos_aires_prov__rendimiento_de_establecimiento
# (staging `a7ce7a82`, 2.371 filas), tal como la lee `perfiles_numericos`:
# pg_stats y las primeras 200 filas no nulas. Trae 40 enteros, 275 ambiguos
# con punto y 25 valores distintos que sólo pueden ser ingleses («1.07»,
# «24.87»). La columna entera tiene 8 que sólo pueden ser argentinos
# («1.005.915»), ninguno en la muestra.
CONSULTAS_MEDICAS_PG_STATS = """
0 1.036 10.417 1.12 1.146 12.431 12.484 13.868 14.282 151 15.185 15.285 15.681 17.093 18.298
1.834 19.246 2.178 2.349 265 266 27.284 2.979 3.017 3.439 3.604 3.989 4.412 44.696 4.754 5.105
530 6.034 7.607 7.626 768 8.128 9.642 994 1.001 10.229 10.593 10.849 11.073 1.134 11.626 119
121.636 12.374 12.576 12.838 13.093 133.797 13.627 13.921 14.132 144.429 14.731 15.177 15.515
15.992 163.478 170.346 17.476 17.995 18.318 188.361 19.373 197.821 20.182 20.587 20.963 21.527
22.207 22.692 232.762 23.786 244 24.938 25.597 26.202 26.805 27.41 2.805 28.614 294 299.379
3.048 31.201 324 33.171 33.59 34.32 3.528 36.409 372.438 38.199 39.287 40.003 40.83 41.743
42.866 4.39 44.915 45.909 46.829 48.07 49.525 5.068 51.524 5.281 54.115 55.387 56.908 57.956
5.882 6.015 6.161 6.296 64.395 65.93 67.532 6.91 7.05 7.239 73.695 7.518 76.26 7.784 796 8.103
82.627 84 85.527 87.095 8.985 9.208 951 97.574 9.998
""".split()
CONSULTAS_MEDICAS_FILAS = """
120.813 7.339 411 1.349 119 38.128 7.156 5.202 19.536 39.891 8.673 38.572 3.396 7.583 238 1.993
3.811 3.528 2.83 165.452 4.441 8.266 0 0 5.682 0 7.7 8.045 16.018 8.168 7.567 30.565 0 5.719
7.589 7.765 0 2.178 11.133 4.217 16.192 7.596 1.789 33.76 7.692 6.779 5.405 7.029 0 13.572 13
7.676 6.561 6.015 8.261 0 0 7.566 5.235 6.517 10.399 9.718 6.2 9.998 9.367 22.798 6.224 1.31
6.034 11.084 13.577 7.097 7.997 90.116 5.491 14.223 24.87 8.421 3.752 1.07 284 8.344 2.465 1.724
3.624 92.665 2.293 1.322 4.239 3.017 9.208 1.177 8.555 4.64 660 1.315 97.155 5.068 44.221 1.203
685 5.105 3.372 6.623 36.377 1.396 1.45 2.158 867 1.252 682 70 87.95 1.336 22.256 6.072 2.796
12.837 7.775 12.159 5.852 5.628 6.019 2.438 5.698 98.173 25.449 4.861 768 425 8.393 110.586
14.465 13.094 14.349 13.819 13.998 12.706 6.275 21.281 85.052 18.338 3.655 968 2.404 17.641
101.008 12.357 3.953 10.959 8.07 3.361 2.177 2.637 57.253 8.98 1.55 2.593 2.002 19.246 2.145
14.053 45.665 93.419 13.799 12.564 14.077 16.777 14.155 14.052 7.557 7.768 10.794 197.821 10.954
11.177 406 3.604 12.423 10.618 6.635 5.844 3.78 8.802 9.658 11.134 10.833 6.056 48.973 26.538
27.025 290 58 62 5.544 3.252 443 7.113 12.642 8.779
""".split()


class _Tabla:
    """Un sandbox con una sola columna de texto: devuelve la muestra (pg_stats y
    las primeras filas) y cuenta en la columna entera con las condiciones del
    SQL que recibe (sus ``~ '…'`` y ``!~ '…'``), como lo haría Postgres."""

    def __init__(
        self,
        pg_stats: list[str],
        filas: list[str],
        resto: list[str],
        *,
        estimadas: int | None = None,
        error: str | None = None,
    ) -> None:
        self.pg_stats, self.filas = pg_stats, filas
        self.columna = [*filas, *resto]
        self.estimadas = len(self.columna) if estimadas is None else estimadas
        self.error = error
        self.consultas: list[tuple[str, int]] = []

    async def get_value_stats(self, tabla: str, columnas: list[str]) -> TableValueStats:
        return TableValueStats(
            self.estimadas,
            {c: ColumnValueStats(c, histogram_bounds=self.pg_stats) for c in columnas},
        )

    async def execute_readonly(
        self, sql: str, timeout_seconds: int = 10, *, params: Any = None
    ) -> SandboxResult:
        self.consultas.append((sql, timeout_seconds))
        cuenta = re.search(r"count\(\*\) AS (\w+)", sql)
        if cuenta is None:
            return SandboxResult(["v"], [{"v": v} for v in self.filas], len(self.filas), False)
        if self.error:
            return SandboxResult([], [], 0, False, error=self.error, error_kind="timeout")
        condiciones = re.findall(r"(!?~) '([^']*)'", sql)
        assert condiciones, sql
        n = sum(
            all(
                (re.search(patron, v.strip(ESPACIOS)) is not None) == (op == "~")
                for op, patron in condiciones
            )
            for v in self.columna
        )
        return SandboxResult([cuenta.group(1)], [{cuenta.group(1): n}], 1, False)


async def _formato(tabla: _Tabla, columna: str = "consultas_medicas") -> str | None:
    prep = await preparar(tabla, "raw.t", {columna: "text"}, [], numericas=[columna])
    return prep.formatos[columna]


class TestColumnaEnteraEnTablasChicas:
    """Segunda revisión del PR #148: la regla «sin mezcla» sólo miraba la
    muestra. En consultas_medicas la muestra dice «inglés» y la columna entera
    es argentina: «120.813» consultas (Adolfo Alsina, 2024) salían 120,813."""

    async def test_consultas_medicas_un_argentino_fuera_de_la_muestra_es_mezcla(self) -> None:
        muestra = [*CONSULTAS_MEDICAS_PG_STATS, *CONSULTAS_MEDICAS_FILAS]
        assert perfil_columna("consultas_medicas", muestra).formato == "en"  # la muestra sola
        tabla = _Tabla(CONSULTAS_MEDICAS_PG_STATS, CONSULTAS_MEDICAS_FILAS, ["1.005.915"])
        with pytest.raises(CatalogRequestError, match="mil veces") as exc:
            await _formato(tabla)
        assert "columna entera" in str(exc.value) and "argentino" in str(exc.value)
        assert not any("AS valor" in sql for sql, _ in tabla.consultas)

    async def test_sin_valores_del_otro_formato_vale_lo_que_dice_la_muestra(self) -> None:
        tabla = _Tabla(CONSULTAS_MEDICAS_PG_STATS, CONSULTAS_MEDICAS_FILAS, ["1.234", "5"])
        assert await _formato(tabla) == "en"
        [(_, timeout)] = [(sql, t) for sql, t in tabla.consultas if "count(*)" in sql]
        assert timeout < 10  # más corto que el del cálculo, que viene después

    async def test_mezcla_fuera_de_la_muestra_sin_ambiguos_queda_sin_formato(self) -> None:
        """Como en la muestra: sin ambiguos no hace falta rechazar. Quedan en
        NULL los de fuera de la muestra y el cálculo los cuenta aparte."""
        tabla = _Tabla([], [*EVIDENCIA_EN, "500"], ["1.234,5", "12.500"])
        assert await _formato(tabla, "monto") is None

    async def test_si_la_cuenta_no_termina_no_se_decide(self) -> None:
        tabla = _Tabla(
            CONSULTAS_MEDICAS_PG_STATS,
            CONSULTAS_MEDICAS_FILAS,
            [],
            error="canceling statement due to statement timeout",
        )
        assert await _formato(tabla) is None
        assert leer_numero("120.813", None) is None

    async def test_en_la_muestra_argentina_se_buscan_ingleses(self) -> None:
        tabla = _Tabla([], [*EVIDENCIA_AR, "12.500"], ["1,234,567"])
        with pytest.raises(CatalogRequestError, match="columna entera"):
            await _formato(tabla, "IMPORTE")

    async def test_en_una_tabla_grande_no_se_recorre_la_columna(self) -> None:
        """Desde ``TOLERANTE_MAX_FILAS`` la cuenta no se hace (recorrer la
        columna es lo que en esas tablas pasa el timeout): decide la muestra
        sola, como antes de este arreglo."""
        tabla = _Tabla(
            CONSULTAS_MEDICAS_PG_STATS,
            CONSULTAS_MEDICAS_FILAS,
            ["1.005.915"],
            estimadas=TOLERANTE_MAX_FILAS,
        )
        await _formato(tabla)
        assert not any("count(*)" in sql for sql, _ in tabla.consultas)
