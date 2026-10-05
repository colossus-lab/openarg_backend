"""Fechas: qué columna ordena la tabla y cómo se filtra por período.

Auditoría 4.1 / ok.1, verificada en prod el 04-oct: la columna de fecha se
elegía sólo por el nombre (``PUBLICACION_FECHA`` y ``anio`` no; ``updated_at``
sí) y el filtro era lexicográfico sobre el texto (0 filas sin aviso con
"1/10/2017", "201801" o "Junio de 2026").
"""

from __future__ import annotations

from datetime import date, datetime

import pytest

from app.application.consultas.fechas import (
    ColumnaFecha,
    condiciones_periodo,
    es_nombre_de_fecha,
    expresion_fecha,
    fecha_iso,
    formato_uniforme,
    orden_fecha,
    rama_fecha,
    resolver_columna_fecha,
    sin_columna_fecha,
    validar_fecha,
)
from app.application.consultas.sql import CatalogRequestError, Params


class TestQueColumnaEsLaFecha:
    def test_publicacion_fecha_se_reconoce_por_la_palabra(self) -> None:
        """Diputados, proyectos parlamentarios: daba 'no tiene columna de fecha'."""
        col = resolver_columna_fecha(["PROYECTO_ID", "TITULO", "PUBLICACION_FECHA"])
        assert col is not None and col.nombre == "PUBLICACION_FECHA"

    def test_una_columna_de_anio_es_un_periodo(self) -> None:
        """SNIC provincial (`anio` bigint) y presupuesto (`ejercicio_presupuestario`)."""
        col = resolver_columna_fecha([("provincia_id", "bigint"), ("anio", "bigint")])
        assert col == ColumnaFecha("anio", "bigint", "anio")
        assert resolver_columna_fecha(["ejercicio_presupuestario", "credito"]).nombre == (  # type: ignore[union-attr]
            "ejercicio_presupuestario"
        )

    @pytest.mark.parametrize(
        "metadato", ["updated_at", "updated_ts", "ultima_actualizacion_fecha", "fecha_carga"]
    )
    def test_las_fechas_de_carga_no_son_la_fecha_del_dato(self, metadato: str) -> None:
        assert resolver_columna_fecha([metadato, "valor"]) is None
        assert resolver_columna_fecha([metadato, "fecha", "valor"]).nombre == "fecha"  # type: ignore[union-attr]

    def test_date_tiene_que_ser_una_palabra_no_una_subcadena(self) -> None:
        assert not es_nombre_de_fecha("candidate")
        assert not es_nombre_de_fecha("updated")
        assert es_nombre_de_fecha("start_date")
        assert es_nombre_de_fecha("startDate")

    def test_el_tipo_date_cuenta_aunque_el_nombre_no_diga_fecha(self) -> None:
        col = resolver_columna_fecha([("dia", "date"), ("valor", "numeric")])
        assert col is not None and col.nombre == "dia"

    def test_el_nombre_exacto_gana_sobre_el_resto(self) -> None:
        col = resolver_columna_fecha([("fecha_inicio", "text"), ("indice_tiempo", "text")])
        assert col is not None and col.nombre == "indice_tiempo"

    def test_la_columna_elegida_por_el_usuario_se_respeta(self) -> None:
        col = resolver_columna_fecha(["fecha_inicio", "fecha_fin"], "fecha_fin")
        assert col is not None and col.nombre == "fecha_fin"
        with pytest.raises(CatalogRequestError, match="columna_fecha"):
            resolver_columna_fecha(["fecha"], "no_existe")


class TestMensajeSinFecha:
    def test_nombra_las_columnas_que_podrian_ser_periodo(self) -> None:
        mensaje = sin_columna_fecha(["provincia", "mes", "trimestre", "valor"])
        assert "mes, trimestre" in mensaje
        assert "No reconocí" in mensaje

    def test_explica_por_que_no_usa_las_de_carga(self) -> None:
        mensaje = sin_columna_fecha(["provincia", "updated_at"])
        assert "updated_at" in mensaje and "columna_fecha" in mensaje


class TestNormalizacion:
    """`fecha_iso` es el espejo en Python de `expresion_fecha` (el test de
    integración compara los dos contra Postgres)."""

    @pytest.mark.parametrize(
        ("valor", "iso"),
        [
            ("2026-03-05T00:00:00", "2026-03-05"),  # PUBLICACION_FECHA
            ("2018-12-19", "2018-12-19"),
            ("2024-03", "2024-03"),
            ("2024/3/5", "2024-03-05"),
            ("2024/03", "2024-03"),
            ("1/10/2017", "2017-10-01"),  # IPIM: d/m/aaaa
            ("05/06/2017", "2017-06-05"),  # CABA terrenos
            ("1/4/2025 00:00", "2025-04-01"),
            ("01-10-2017", "2017-10-01"),
            ("201801", "2018-01"),  # obras sociales
            ("20180115", "2018-01-15"),
            ("2020", "2020"),
            ("2020.0", "2020"),
            (2020, "2020"),
            (2020.0, "2020"),
            ("Junio de 2026", "2026-06"),  # afiliaciones
            ("setiembre 2025", "2025-09"),
            ("Mar-2024", "2024-03"),
            ("2018 SEPTIEMBRE", "2018-09"),
            (date(2024, 3, 5), "2024-03-05"),
            (datetime(2024, 3, 5, 10, 0), "2024-03-05"),
        ],
    )
    def test_formatos_conocidos(self, valor: object, iso: str) -> None:
        assert fecha_iso(valor) == iso

    @pytest.mark.parametrize("valor", ["Nocturno", "13/13/2020", "2024-T1", "", None, "3/25/2024"])
    def test_lo_que_no_se_reconoce_es_none(self, valor: object) -> None:
        assert fecha_iso(valor) is None

    def test_inicio_y_fin_completan_el_periodo(self) -> None:
        assert (fecha_iso("2020", "inicio"), fecha_iso("2020", "fin")) == (
            "2020-01-01",
            "2020-12-31",
        )
        assert (fecha_iso("201806", "inicio"), fecha_iso("201806", "fin")) == (
            "2018-06-01",
            "2018-06-31",
        )

    def test_la_expresion_sql_no_usa_to_date(self) -> None:
        """`to_date('31/02/2020', ...)` lanza un error y aborta la consulta entera."""
        sql = expresion_fecha("FECHA", "text")
        assert "to_date" not in sql and "to_char" not in sql
        assert sql.endswith("END)")

    def test_una_columna_date_no_pasa_por_las_expresiones(self) -> None:
        assert expresion_fecha("dia", "date") == 'left("dia"::text, 10)'
        assert expresion_fecha("ts", "timestamp without time zone") == 'left("ts"::text, 10)'


class TestCaminoRapido:
    """En tablas de 6-8 M de filas el CASE pasaba el timeout del sandbox (staging,
    04-oct): con una forma única en la muestra de pg_stats se usa una expresión
    directa."""

    def test_una_sola_forma_en_la_muestra(self) -> None:
        assert formato_uniforme(["2019-05-28", "2014-02-17", "2016-01-01T00:00:00"]) == "iso_dia"
        assert formato_uniforme(["1/10/2017", "15/1/2018", "01-02-2019"]) == "dmy"

    def test_formas_mezcladas_usan_el_case(self) -> None:
        assert formato_uniforme(["2019-05-28", "1/10/2017", "2020"]) is None
        assert formato_uniforme(["Junio de 2026", "Mayo de 2026", "Abril de 2026"]) is None
        assert formato_uniforme(["2019-05-28"]) is None  # muestra muy chica

    def test_tabla_grande_con_una_forma_dominante_usa_la_guarda(self) -> None:
        """Molinetes del subte: 200 valores d/m/aaaa y un "8/20/2025" (m/d)."""
        muestra = ["1/4/2025"] * 200 + ["8/20/2025"]
        assert formato_uniforme(muestra, filas=8_427_643) == "dmy*"
        assert formato_uniforme(muestra, filas=5_000) is None

    def test_la_expresion_directa_no_tiene_el_case(self) -> None:
        sql = expresion_fecha("fecha_de_inicio", "text", "fin", "iso_dia")
        assert sql == "left(NULLIF(btrim(\"fecha_de_inicio\"::text), ''), 10)"
        assert "CASE" not in expresion_fecha("x", "text", "inicio", "dmy")

    def test_la_guardada_deja_en_null_lo_que_no_es_de_su_forma(self) -> None:
        sql = expresion_fecha("FECHA", "text", "inicio", "dmy*")
        assert sql.startswith("(CASE WHEN NULLIF(btrim(\"FECHA\"::text), '') ~ '")
        assert sql.count(" WHEN ") == 1

    def test_el_orden_usa_la_columna_cruda_si_ya_ordena_bien(self) -> None:
        assert orden_fecha(ColumnaFecha("fecha", "text", formato="iso_dia")) == '"fecha"'
        assert orden_fecha(ColumnaFecha("dia", "date")) == '"dia"'
        assert "split_part" in orden_fecha(ColumnaFecha("FECHA", "text", formato="dmy"))
        assert "CASE" in orden_fecha(ColumnaFecha("FECHA", "text"))

    def test_rama_de_cada_valor(self) -> None:
        assert rama_fecha("2026-03-05T00:00:00") == "iso_dia"
        assert rama_fecha("Junio de 2026") == "mes_nombre"
        assert rama_fecha(2020) == "anio"
        assert rama_fecha("Primitiva") is None


class TestPeriodo:
    def test_desde_y_hasta_son_parametros_y_se_comparan_por_solapamiento(self) -> None:
        params = Params()
        cond = condiciones_periodo(ColumnaFecha("anio", "bigint"), "2020-06", "2021", params)
        assert params.values == {"p0": "2020-06-01", "p1": "2021-12-31"}
        assert cond[0].endswith(">= :p0") and "'-12-31'" in cond[0]  # fin del período del valor
        assert cond[1].endswith("<= :p1") and "'-01-01'" in cond[1]  # inicio
        assert "2020" not in " ".join(cond)  # el valor no se interpola

    @pytest.mark.parametrize("valor", ["2025-01-01' OR '1'='1", "ayer", "2025/01/01", "20250101"])
    def test_desde_y_hasta_tienen_forma_iso(self, valor: str) -> None:
        with pytest.raises(CatalogRequestError, match="fecha"):
            validar_fecha(valor, "desde")
