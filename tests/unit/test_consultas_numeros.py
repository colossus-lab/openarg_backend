"""Números guardados como texto: el formato se decide por columna.

El P0 de la auditoría verificada el 04-oct: `calcular` leía "12.500" como
12,5 (Pauta CABA: 119.968 en vez de 242.635.557). Y el arreglo obvio, el CASE
por celda del legacy, mete el error inverso: "-59.796" (una longitud en
formato inglés) pasaba a -59796.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from app.application.consultas.numeros import (
    RE_AMBIGUO_COMA,
    RE_AMBIGUO_PUNTO,
    RE_AR,
    RE_EN,
    RE_ENTERO,
    clase_valor,
    expresion_numero,
    leer_numero,
    numero_de_filtro,
    perfil_columna,
)
from app.application.consultas.sql import CatalogRequestError


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
    ],
)
def test_cada_valor_se_clasifica_por_lo_que_puede_ser(valor: str, clase: str | None) -> None:
    assert clase_valor(valor) == clase


class TestPerfilDeColumna:
    def test_pauta_caba_los_miles_con_un_valor_de_varios_grupos_son_argentinos(self) -> None:
        # Los valores de la tabla de prod (IMPORTE de caba__pauta_publicitaria).
        perfil = perfil_columna("IMPORTE", ["27.830", "7.000", "15.000", "1.218.600"])
        assert perfil.formato == "ar" and perfil.problema is None

    def test_decimales_ingleses_de_tres_cifras_son_ingleses(self) -> None:
        perfil = perfil_columna("lon", ["-59.796", "-58.381", "-64.183", "-60.0", "0.125"])
        assert perfil.formato == "en"

    def test_sin_valores_ambiguos_no_hace_falta_decidir(self) -> None:
        perfil = perfil_columna("monto", ["1.234,5", "12", "3,75"])
        assert perfil.formato is None and perfil.problema is None

    def test_enteros_cortos_y_miles_con_punto_es_argentino(self) -> None:
        # "500" sin separador y "12.500" con: el separador aparece desde mil.
        assert perfil_columna("cantidad", ["500", "999", "12.500", "3.000"]).formato == "ar"

    def test_enteros_cortos_y_miles_con_coma_es_ingles(self) -> None:
        assert perfil_columna("cantidad", ["500", "999", "12,500"]).formato == "en"

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
        ],
    )
    def test_lee_segun_el_formato(self, valor: str, formato: str | None, esperado: Decimal) -> None:
        assert leer_numero(valor, formato) == esperado


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
        assert (
            f"WHEN btrim(\"IMPORTE\"::text) ~ '{RE_AMBIGUO_PUNTO}' "
            "THEN replace(btrim(\"IMPORTE\"::text), '.', '')::numeric" in sql
        )

    def test_columna_inglesa_lee_el_punto_como_decimal(self) -> None:
        sql = expresion_numero("lon", "text", "en")
        assert (
            f'WHEN btrim("lon"::text) ~ \'{RE_AMBIGUO_PUNTO}\' THEN btrim("lon"::text)::numeric'
            in sql
        )

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
