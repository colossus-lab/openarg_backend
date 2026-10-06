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

import pytest

from app.application.consultas.numeros import (
    ESPACIOS,
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
from app.application.consultas.sql import CatalogRequestError

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
        assert cuatro.problema is not None and "sólo 4" in cuatro.problema
        assert perfil_columna("x", [*EVIDENCIA_AR, "12.500"]).formato == "ar"
        assert perfil_columna("x", [*EVIDENCIA_EN, "12.500"]).formato == "en"

    def test_hace_falta_el_noventa_por_ciento_de_un_formato(self) -> None:
        nueve_y_uno = [*EVIDENCIA_AR, *EVIDENCIA_AR[:4], "12.5", "12.500"]
        assert perfil_columna("x", nueve_y_uno).formato == "ar"  # 9 de 10
        ocho_y_uno = [*EVIDENCIA_AR, *EVIDENCIA_AR[:3], "12.5", "12.500"]
        perfil = perfil_columna("x", ocho_y_uno)  # 8 de 9: 88 %
        assert perfil.formato is None
        assert perfil.problema is not None and "mezcla" in perfil.problema

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
        """H041: el cálculo cuenta aparte las filas con un número ambiguo."""
        x = LIMPIO.format("monto")
        assert expresion_ambiguo("monto", "text") == (
            f"({x} ~ '{RE_AMBIGUO_PUNTO}' OR {x} ~ '{RE_AMBIGUO_COMA}')"
        )
        assert expresion_ambiguo("monto", "numeric") is None

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
