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
    aviso_formato_guardado,
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

    @pytest.mark.parametrize("metadato", ["update_date", "last_update", "fecha_update"])
    def test_la_fecha_de_la_ultima_edicion_no_es_la_del_dato(self, metadato: str) -> None:
        """Revisión del PR #133: 'update' no estaba en la lista, sólo 'updated'."""
        assert resolver_columna_fecha([("candidate", "text"), (metadato, "text")]) is None
        assert not es_nombre_de_fecha(metadato)

    def test_una_marca_de_tiempo_sola_es_la_fecha_del_dato(self) -> None:
        """Sensores, viajes: `timestamp` es la única marca temporal de la fila."""
        col = resolver_columna_fecha([("sensor", "text"), ("timestamp", "timestamp")])
        assert col is not None and col.nombre == "timestamp"
        col = resolver_columna_fecha([("sensor", "text"), ("event_ts", "timestamp with time zone")])
        assert col is not None and col.nombre == "event_ts"
        col = resolver_columna_fecha([("sensor", "text"), ("fecha_timestamp", "text")])
        assert col is not None and col.nombre == "fecha_timestamp"

    def test_una_marca_de_tiempo_no_le_gana_a_otra_candidata(self) -> None:
        col = resolver_columna_fecha([("timestamp", "timestamp"), ("anio", "bigint")])
        assert col is not None and col.nombre == "anio"
        # `ingest_ts` de texto, sin otra señal: no se adivina.
        assert resolver_columna_fecha([("ingest_ts", "text"), ("valor", "text")]) is None
        # `created_ts`: metadato por `created`, aunque sea de tipo timestamp.
        assert resolver_columna_fecha([("created_ts", "timestamp"), ("valor", "text")]) is None


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
        """Molinetes del subte: d/m/aaaa y un "8/20/2025" (m/d).

        La muestra real (staging, 05-oct, 8,4 M de filas) tiene 114 valores que
        sólo pueden ser d/m y uno que sólo puede ser m/d. La de antes (200
        «1/4/2025» y el «8/20/2025») no tenía ninguno que sólo pudiera ser d/m:
        por la regla de la revisión independiente (H003) esa columna es m/d.
        """
        muestra = ["17/7/2025", "18/8/2025", "25/7/2025", "13/6/2025", "5/6/2025"] * 40
        muestra.append("8/20/2025")
        assert formato_uniforme(muestra, filas=8_427_643) == "dmy*"
        assert formato_uniforme(["1/4/2025"] * 200 + ["8/20/2025"], filas=8_427_643) == "mdy"

    def test_la_expresion_directa_no_tiene_el_case(self) -> None:
        sql = expresion_fecha("fecha_de_inicio", "text", "fin", "iso_dia")
        assert sql == "left(NULLIF(btrim(\"fecha_de_inicio\"::text), ''), 10)"
        assert "CASE" not in expresion_fecha("x", "text", "inicio", "dmy")

    def test_la_guardada_deja_en_null_lo_que_no_es_de_su_forma(self) -> None:
        sql = expresion_fecha("FECHA", "text", "inicio", "dmy*")
        assert sql.startswith("(CASE WHEN NULLIF(btrim(\"FECHA\"::text), '') ~ '")
        assert sql.count(" WHEN ") == 1

    def test_la_guardada_avisa_que_filas_quedan_afuera(self) -> None:
        """Revisión del PR #133: con la guarda, un cálculo con período excluía en
        silencio hasta el 10 % de las filas (las de otra forma de fecha)."""
        aviso = aviso_formato_guardado(ColumnaFecha("FECHA", "text", formato="dmy*")) or ""
        assert "«FECHA»" in aviso and "forma dominante (d/m/aaaa)" in aviso
        assert aviso_formato_guardado(ColumnaFecha("FECHA", "text", formato="dmy")) is None
        assert aviso_formato_guardado(ColumnaFecha("FECHA", "text")) is None
        assert aviso_formato_guardado(None) is None

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


# ── revisión independiente del 05-oct (H002, H003, H010, H044) ─────────────


class TestAnioYMesSeparados:
    """H002 y H010: en una tabla con `anio` y `mes`, la fecha era el año solo.

    Un pedido de junio de 2019 se solapaba con todo 2019 y devolvía los doce
    meses (staging, capturas_maritimas 2db47cbb: 384.639,77 en vez de
    33.071,94, sin aviso), y `orden=desc` traía enero como "lo último".
    """

    def test_la_columna_de_anio_recuerda_la_de_mes(self) -> None:
        col = resolver_columna_fecha([("anio", "bigint"), ("mes", "bigint"), ("captura", "text")])
        assert col is not None
        assert (col.nombre, col.clase, col.mes, col.tipo_mes) == ("anio", "anio", "mes", "bigint")

    @pytest.mark.parametrize(
        ("anio", "mes"),
        [
            ("reclamo_ano", "reclamo_mes_nro"),
            ("impacto_presupuestario_anio", "impacto_presupuestario_mes"),
            ("año", "n° mes"),
            ("Año", "MES"),
            ("year", "month"),
            ("anio", "nombre_mes"),
        ],
    )
    def test_reconoce_la_columna_de_mes(self, anio: str, mes: str) -> None:
        col = resolver_columna_fecha([(anio, "bigint"), (mes, "text"), ("valor", "text")])
        assert col is not None and col.nombre == anio and col.mes == mes

    def test_no_toma_cualquier_columna_que_diga_mes(self) -> None:
        """EPH: `pp04b3_mes` es el mes en que empezó un trabajo, no el del dato."""
        col = resolver_columna_fecha(
            [
                ("anio", "bigint"),
                ("pp04b3_mes", "text"),
                ("mes_transferencia", "text"),
                ("meses_de_atraso", "bigint"),
                ("mes_carga", "text"),
            ]
        )
        assert col is not None and col.mes is None

    def test_prefiere_el_numero_del_mes_al_nombre(self) -> None:
        col = resolver_columna_fecha(
            [
                ("reclamo_ano", "bigint"),
                ("reclamo_mes_nombre", "text"),
                ("reclamo_mes_nro", "bigint"),
            ]
        )
        assert col is not None and col.mes == "reclamo_mes_nro"

    def test_un_pedido_mensual_usa_el_anio_y_el_mes(self) -> None:
        col = resolver_columna_fecha([("anio", "bigint"), ("mes", "bigint")])
        assert col is not None
        params = Params()
        cond = condiciones_periodo(col, "2019-06", "2019-06", params)
        assert params.values == {"p0": "2019-06-01", "p1": "2019-06-31"}
        assert len(cond) == 2 and all('btrim("mes"::text)' in c for c in cond)

    @pytest.mark.parametrize(
        ("desde", "hasta"),
        [("2019", "2019"), ("2019-01", "2019-12"), ("2019-01-01", "2020-12-31")],
    )
    def test_un_pedido_de_anios_enteros_sigue_usando_el_anio(self, desde: str, hasta: str) -> None:
        col = resolver_columna_fecha([("anio", "bigint"), ("mes", "bigint")])
        assert col is not None
        cond = condiciones_periodo(col, desde, hasta, Params())
        assert not any('"mes"' in c for c in cond)

    def test_sin_columna_de_mes_un_pedido_mensual_se_rechaza(self) -> None:
        col = resolver_columna_fecha([("anio", "bigint"), ("valor", "text")])
        assert col is not None
        with pytest.raises(CatalogRequestError, match="años enteros"):
            condiciones_periodo(col, "2019-06", "2019-06", Params())
        # El año entero escrito con meses o con días se acepta.
        assert condiciones_periodo(col, "2019-01", "2019-12", Params())
        assert condiciones_periodo(col, "2019-01-01", "2019-12-31", Params())

    def test_el_rango_de_la_tabla_va_por_el_anio_y_el_mes(self) -> None:
        """Revisión de ola 3 (#144 × #154): con el año solo, una tabla de 2026 con
        meses hasta marzo daba de 2026 a 2026, y describir_tabla la declaraba una
        foto vigente al día de lectura. El mes irreconocible cuenta por el año."""
        from app.application.consultas.fechas import consulta_rango, expresion_mes

        col = resolver_columna_fecha([("anio", "bigint"), ("mes", "bigint")])
        assert col is not None
        sql = consulta_rango('"raw"."t"', col)
        rango = sql.split(" AS f,")[0]
        assert expresion_mes("mes") in rango
        assert rango.count("COALESCE(") == 1
        assert expresion_fecha("anio", "bigint", "iso") in rango
        # La clave del rango es AAAA-MM, sin día.
        assert "'-01'" not in rango and "'-31'" not in rango
        # Sin columna de mes, como siempre.
        solo = consulta_rango('"raw"."t"', ColumnaFecha("anio", "bigint", "anio"))
        assert "COALESCE(" not in solo and '"mes"' not in solo

    def test_una_columna_de_anio_con_meses_adentro_no_se_rechaza(self) -> None:
        """`ano_mes` con "2016-05" (mercado inmobiliario de CABA): el valor ya trae el mes."""
        col = ColumnaFecha("ano_mes", "text", "anio", formato="iso_mes")
        assert condiciones_periodo(col, "2016-06", "2016-06", Params())

    def test_una_columna_de_anio_de_texto_se_decide_con_la_muestra(self) -> None:
        from app.application.consultas.preparar import con_formato
        from app.domain.ports.sandbox.sql_sandbox import ColumnValueStats, TableValueStats

        def stats(valores: list[str]) -> TableValueStats:
            return TableValueStats(
                estimated_rows=500,
                columns={"anio": ColumnValueStats("anio", most_common_vals=valores)},
            )

        col = ColumnaFecha("anio", "text", "anio")
        # Sin la muestra (la primera validación, que no toca la base) no se decide.
        assert condiciones_periodo(col, "2019-06", "2019-06", Params())
        # Años, aunque haya una fila «Total» o no haya estadísticas: se rechaza.
        for anual in (con_formato(col, stats(["2019", "2020", "Total"])), con_formato(col, None)):
            with pytest.raises(CatalogRequestError, match="años enteros"):
                condiciones_periodo(anual, "2019-06", "2019-06", Params())
        # Meses adentro: se filtra por el valor.
        mensual = con_formato(col, stats(["2016-05", "2016-06", "2016-07"]))
        assert condiciones_periodo(mensual, "2016-06", "2016-06", Params())

    @pytest.mark.parametrize(
        ("valor", "mes"),
        [
            (6, "06"),
            ("6", "06"),
            ("06", "06"),
            ("6.0", "06"),
            (6.0, "06"),
            ("Junio", "06"),
            ("SEPTIEMBRE", "09"),
            ("Setiembre", "09"),
            ("dic.", "12"),
            ("Diciembre*", "12"),
            ("ene-17", "01"),
            ("10/2020", "10"),
            ("2024-07-01 00:00:00", "07"),
        ],
    )
    def test_el_mes_se_lee_en_sus_formas(self, valor: object, mes: str) -> None:
        from app.application.consultas.fechas import mes_de

        assert mes_de(valor) == mes

    @pytest.mark.parametrize("valor", ["Total", "13", "0", "99", "", None, "Marcas", "Mayor"])
    def test_lo_que_no_es_un_mes_es_none(self, valor: object) -> None:
        from app.application.consultas.fechas import mes_de

        assert mes_de(valor) is None


class TestDiaMesOMesDia:
    """H003: RE_DMY sólo entendía d/m. En una columna m/d (Pauta publicitaria de
    CABA, c7fd6b9c: 2.719 de 3.655 filas con el segundo campo > 12 y ninguna con
    el primero) «3/4/2025» quedaba en abril y «5/31/2021» en NULL: mayo de 2021
    daba 53 filas de otros meses en vez de 292, sin aviso."""

    # Valores de la muestra de pg_stats de esa columna en staging.
    PAUTA = [
        "3/30/2021",
        "8/31/2021",
        "6/15/2021",
        "5/18/2021",
        "12/27/2021",
        "4/20/2021",
        "10/6/2021",
        "11/4/2021",
        "1/19/2021",
    ]
    # mart.pauta_oficial: une fuentes d/m («15/01/2020») y m/d («10/31/2022»).
    MIXTA = ["15/01/2020", "01/06/2014", "10/31/2022", "12/21/2021", "2018-10-01", "jun-18"]
    MDY = "(0?[1-9]|1[0-2])[/-](0?[1-9]|[12][0-9]|3[01])[/-]"
    DMY = "(0?[1-9]|[12][0-9]|3[01])[/-](0?[1-9]|1[0-2])[/-]"

    def test_una_columna_mes_dia_se_lee_mes_dia(self) -> None:
        formato = formato_uniforme(self.PAUTA)
        assert formato == "mdy"
        sql = expresion_fecha("FECHA", "text", "inicio", formato)
        assert "CASE" not in sql  # camino rápido, como d/m
        # El mes es el primer campo y el día el segundo.
        assert sql.index("'/', 1)") < sql.index("'/', 2)")

    def test_la_lectura_mes_dia_en_python(self) -> None:
        assert fecha_iso("3/4/2025", lectura="mdy") == "2025-03-04"
        assert fecha_iso("5/31/2021", lectura="mdy") == "2021-05-31"
        assert fecha_iso("31/5/2021", lectura="mdy") is None
        assert fecha_iso("3/4/2025") == "2025-04-03"  # por defecto, d/m como siempre

    def test_la_lectura_se_decide_por_columna(self) -> None:
        from app.application.consultas.fechas import lectura_dia_mes

        assert lectura_dia_mes(self.PAUTA) == "mdy"
        assert lectura_dia_mes(["15/1/2018", "1/10/2017", "3/4/2025"]) == "dmy"
        assert lectura_dia_mes(["3/4/2025", "5/6/2025"]) == "dmy"  # nada lo decide
        assert lectura_dia_mes(self.MIXTA) == "mixta"
        assert lectura_dia_mes(["2024-03-05", "Junio de 2026"]) == "dmy"

    def test_formas_mezcladas_con_mes_dia_leen_mes_dia(self) -> None:
        formato = formato_uniforme([*self.PAUTA, "2021-05-31", "Mayo de 2021"])
        assert formato is not None  # antes: None, el CASE que sólo entiende d/m
        col = ColumnaFecha("FECHA", "text", formato=formato)
        cond = condiciones_periodo(col, "2021-05", None, Params())
        # «5/31/2021» entra en el CASE como mes/día: «5» es el mes.
        assert self.MDY in cond[0] and self.DMY not in cond[0]

    def test_evidencia_mixta_sólo_filtra_años_enteros(self) -> None:
        col = ColumnaFecha("periodo", "text", formato=formato_uniforme(self.MIXTA))
        with pytest.raises(CatalogRequestError, match="mezcla"):
            condiciones_periodo(col, "2021-05", "2021-05", Params())
        # El año de cada fila sí es seguro: lee las dos formas (antes «10/31/2022»
        # quedaba en NULL y salía de 2022).
        cond = condiciones_periodo(col, "2022", "2022", Params())
        assert self.MDY in cond[0] and self.DMY in cond[0]
        assert fecha_iso("10/31/2022", lectura="mixta") == "2022-10-31"
        assert fecha_iso("01/06/2014", lectura="mixta") == "2014-06-01"

    def test_evidencia_mixta_avisa(self) -> None:
        from app.application.consultas.fechas import aviso_lectura_fecha

        col = ColumnaFecha("periodo", "text", formato=formato_uniforme(self.MIXTA))
        aviso = aviso_lectura_fecha(col) or ""
        assert "«periodo»" in aviso and "mezcla" in aviso
        assert aviso_lectura_fecha(ColumnaFecha("FECHA", "text", formato="mdy")) is None

    def test_tabla_grande_mixta_sin_forma_dominante_no_usa_la_guarda(self) -> None:
        muestra = ["15/1/2020", "1/31/2020"] * 50
        assert formato_uniforme(muestra, filas=8_000_000) == formato_uniforme(self.MIXTA)


class TestFechasDeAtributo:
    """H044: cualquier nombre con "fecha" ganaba. En 85 tablas de prod la fecha
    elegida era una de nacimiento, y `desde`/`hasta` filtraban por el año de
    nacimiento (consultas a los centros de acceso a la justicia:
    `consultante_fecha_nacimiento` con `consulta_fecha_carga` al lado)."""

    def test_una_fecha_de_nacimiento_no_le_gana_a_otra_candidata(self) -> None:
        col = resolver_columna_fecha(
            [("consultante_fecha_nacimiento", "text"), ("consulta_fecha", "text")]
        )
        assert col is not None and col.nombre == "consulta_fecha"
        col = resolver_columna_fecha(
            [("fecha_nacimiento", "text"), ("fecha_incio_mandato", "text")]
        )
        assert col is not None and col.nombre == "fecha_incio_mandato"
        col = resolver_columna_fecha([("fecha_nacimiento", "date"), ("anio", "bigint")])
        assert col is not None and col.nombre == "anio"
        col = resolver_columna_fecha([("fecha_vencimiento", "date"), ("fecha_emision", "text")])
        assert col is not None and col.nombre == "fecha_emision"

    def test_si_es_la_unica_se_usa_y_se_avisa(self) -> None:
        from app.application.consultas.fechas import aviso_lectura_fecha

        col = resolver_columna_fecha(
            [("consultante_fecha_nacimiento", "text"), ("consulta_fecha_carga", "text")]
        )
        assert col is not None and col.nombre == "consultante_fecha_nacimiento"
        aviso = aviso_lectura_fecha(col) or ""
        assert "«consultante_fecha_nacimiento»" in aviso and "columna_fecha" in aviso

    def test_la_elegida_por_el_usuario_no_lleva_aviso(self) -> None:
        from app.application.consultas.fechas import aviso_lectura_fecha

        col = resolver_columna_fecha(
            [("fecha_nacimiento", "text"), ("fecha", "text")], "fecha_nacimiento"
        )
        assert col is not None and col.nombre == "fecha_nacimiento"
        assert aviso_lectura_fecha(col) is None


# ── revisión del PR #154 ────────────────────────────────────


class TestSeriesMensualesConBarras:
    """H003 en las series mensuales m/d escritas «M/1/AAAA»: ningún valor tiene
    el día mayor que 12, así que no había evidencia y se leían d/m. Cada mes
    caía en enero («10/1/2017» era el 10 de enero): en biodiésel y bioetanol
    26bc8483 (staging) un pedido de enero de 2017 sumaba el año entero
    (2.871.435 t, 415 filas; real 189.763,4 t, 30 filas) y uno de octubre daba
    0 filas (real 293.096,3 t, 35 filas), sin aviso."""

    # La muestra de pg_stats de `fecha` en esa tabla (staging, 06-oct).
    BIODIESEL = [
        "10/1/2014",
        "10/1/2017",
        "11/1/2017",
        "1/1/2018",
        "12/1/2017",
        "2/1/2017",
        "2/1/2018",
        "3/1/2017",
        "3/1/2018",
        "4/1/2017",
        "5/1/2017",
        "6/1/2017",
        "7/1/2017",
        "8/1/2017",
        "9/1/2017",
        "10/1/2015",
        "11/1/2014",
        "11/1/2015",
        "1/1/2015",
        "1/1/2016",
        "12/1/2014",
        "12/1/2015",
        "7/1/2014",
        "8/1/2014",
        "9/1/2014",
        "11/1/2013",
        "1/1/2014",
        "12/1/2013",
        "2/1/2016",
        "4/1/2015",
    ]

    def test_una_serie_mensual_mes_dia_se_lee_mes_dia(self) -> None:
        from app.application.consultas.fechas import lectura_dia_mes

        assert lectura_dia_mes(self.BIODIESEL) == "mdy"
        assert formato_uniforme(self.BIODIESEL) == "mdy"

    def test_octubre_es_octubre_y_enero_no_es_el_anio_entero(self) -> None:
        from app.application.consultas.fechas import lectura_de

        lectura = lectura_de(formato_uniforme(self.BIODIESEL))
        filas = [(f"{m}/1/2017", 10 * m) for m in range(1, 13)]

        def suma(mes: str) -> int:
            return sum(v for f, v in filas if (fecha_iso(f, lectura=lectura) or "")[:7] == mes)

        assert suma("2017-10") == 100
        assert suma("2017-01") == 10
        # En SQL, el mes es el primer campo.
        sql = expresion_fecha("fecha", "text", "inicio", formato_uniforme(self.BIODIESEL))
        assert sql.index("'/', 1)") < sql.index("'/', 2)")

    def test_una_serie_mensual_dia_mes_sigue_dia_mes(self) -> None:
        from app.application.consultas.fechas import lectura_dia_mes

        assert lectura_dia_mes(["1/10/2017", "1/11/2017", "1/12/2017", "1/1/2018"]) == "dmy"

    def test_la_estructura_contra_un_valor_inequivoco_es_mixta(self) -> None:
        """Si la forma de los ambiguos choca con un valor que sólo se lee de la
        otra manera (en otro mes), no se da vuelta la columna: sólo años enteros,
        con aviso."""
        from app.application.consultas.fechas import lectura_dia_mes

        assert lectura_dia_mes([*self.BIODIESEL, "31/5/2021"]) == "mixta"
        dm_mensual = [f"1/{m}/2017" for m in range(7, 13)]
        assert lectura_dia_mes(dm_mensual) == "dmy"
        assert lectura_dia_mes([*dm_mensual, "5/31/2021"]) == "mixta"

    def test_una_tabla_diaria_de_enero_sigue_dia_mes(self) -> None:
        """Molinetes del subte 91ca9141 (staging, 500.000 filas, enero de 2020):
        los ambiguos tienen todos el segundo campo en 1, como una serie mensual
        m/d, pero «17/01/2020» dice que es enero leído d/m. Con la regla de la
        revisión (forma contra inequívocos = mixta) esta tabla y otras seis
        (radares y seguridad vial de AUSA, terrenos, residuos de Mendoza) dejaban
        de filtrar meses."""
        from app.application.consultas.fechas import lectura_dia_mes

        molinetes = [f"{d:02d}/01/2020" for d in range(1, 29)]
        assert lectura_dia_mes(molinetes) == "dmy"
        assert formato_uniforme(molinetes, filas=500_000) == "dmy"

    def test_con_un_valor_inequivoco_que_coincide_sigue_mes_dia(self) -> None:
        """Las tablas hermanas de 26bc8483 ya se leían m/d por tres «1/18/2018»."""
        from app.application.consultas.fechas import lectura_dia_mes

        assert lectura_dia_mes([*self.BIODIESEL, "1/18/2018", "2/18/2018"]) == "mdy"

    def test_pocos_meses_no_alcanzan_para_decidir(self) -> None:
        """Centro de transferencia de residuos de Corrientes (staging, toda la
        muestra): ¿1 de junio, julio y agosto, o 6, 7 y 8 de enero? Queda d/m."""
        from app.application.consultas.fechas import lectura_dia_mes

        assert lectura_dia_mes(["07/01/2020", "08/01/2020", "06/01/2020"]) == "dmy"
        # Días y meses variando: nada que decida, d/m como siempre.
        assert lectura_dia_mes(["3/4/2025", "5/6/2025", "7/1/2025", "2/9/2025"]) == "dmy"


class TestFechaConAniosSinLlamarseAnio:
    """H002 en columnas de años que no se llaman `anio`: `periodo`,
    `indice_tiempo` o `fecha` con años (consultas_medicas_ambulatorias a29f6e30
    en staging: «2013»…«2021») aceptaban desde=hasta=2019-06 y devolvían el año
    entero, sin rechazo ni nota. La misma tabla con `anio` ya se rechazaba."""

    @pytest.mark.parametrize("nombre", ["periodo", "indice_tiempo", "fecha"])
    @pytest.mark.parametrize("formato", ["anio", "anio*"])
    def test_un_pedido_mensual_se_rechaza(self, nombre: str, formato: str) -> None:
        col = resolver_columna_fecha([(nombre, "text"), ("valor", "text")])
        assert col is not None and col.clase == "fecha"
        anual = ColumnaFecha(col.nombre, col.tipo, formato=formato)
        with pytest.raises(CatalogRequestError, match="años enteros"):
            condiciones_periodo(anual, "2019-06", "2019-06", Params())
        # El año entero, escrito como sea, sí.
        assert condiciones_periodo(anual, "2019", "2019", Params())
        assert condiciones_periodo(anual, "2019-01", "2019-12", Params())

    def test_por_agregar_datos_tambien(self) -> None:
        from app.application.answers.aggregates import AggregateRequest, build_aggregate_query

        req = AggregateRequest(
            table="raw.salud__consultas_medicas_ambulatorias__a29f6e30__v1",
            column_types=[("periodo", "text"), ("valor", "text")],
            operacion="conteo",
            desde="2019-06",
            hasta="2019-06",
            formato_fecha="anio",
        )
        with pytest.raises(CatalogRequestError, match="años enteros"):
            build_aggregate_query(req)

    def test_la_muestra_decide_aunque_tenga_pocos_valores_o_un_total(self) -> None:
        """Con uno o dos años en la muestra no hay forma única (`formato_uniforme`
        pide tres); con un «Total», tampoco. Igual son años."""
        from app.application.consultas.preparar import con_formato
        from app.domain.ports.sandbox.sql_sandbox import ColumnValueStats, TableValueStats

        def stats(valores: list[str]) -> TableValueStats:
            return TableValueStats(
                estimated_rows=500,
                columns={"periodo": ColumnValueStats("periodo", most_common_vals=valores)},
            )

        col = ColumnaFecha("periodo", "text")
        for muestra in (["2019"], ["2019", "2020"], ["2019", "2020", "Total"]):
            with pytest.raises(CatalogRequestError, match="años enteros"):
                condiciones_periodo(con_formato(col, stats(muestra)), "2019-06", None, Params())
        # Sin muestra no se decide (una fecha no es un año por el nombre).
        assert condiciones_periodo(con_formato(col, None), "2019-06", None, Params())
        assert condiciones_periodo(con_formato(col, stats([])), "2019-06", None, Params())
        # Con meses, se filtra por el valor.
        mensual = con_formato(col, stats(["2019-05", "2019-06"]))
        assert condiciones_periodo(mensual, "2019-06", None, Params())


class TestFechasDeAtributoEnSuTabla:
    """H044: en las tablas de defunciones y de nacimientos, `fecha_defuncion` y
    `hijo_fecha_nacimiento` SON la fecha del dato, pero el aviso decía «es una
    fecha de defunción, no la del dato» (25 de las 100 tablas con el aviso en
    staging). La revisión pedía tratarlas como atributo «fuera de las tablas de
    nacimientos y defunciones»."""

    DEFUNCIONES = "raw.caba__defunciones__91003a9e__v1"
    COLS_DEFUNCIONES = [
        ("FECHA_DEFUNCION", "text"),
        ("GENERO", "text"),
        ("DESCRIPCION_SUBTIPO", "text"),
    ]
    NACIMIENTOS = "raw.caba__nacimientos__1c5b3921__v2"
    COLS_NACIMIENTOS = [
        ("hijo_fecha_nacimiento", "text"),
        ("hijo_genero", "text"),
        ("madre_nacionalidad", "text"),
        ("padre_nacionalidad", "text"),
    ]

    def test_en_su_tabla_es_la_fecha_del_dato_y_no_se_avisa(self) -> None:
        from app.application.consultas.fechas import aviso_lectura_fecha

        for tabla, cols, nombre in (
            (self.DEFUNCIONES, self.COLS_DEFUNCIONES, "FECHA_DEFUNCION"),
            (self.NACIMIENTOS, self.COLS_NACIMIENTOS, "hijo_fecha_nacimiento"),
            ("raw.caba__nacimientos__d209c8fa__v3", [("fecha_nacimiento", "text")], None),
        ):
            col = resolver_columna_fecha(cols, tabla=tabla)
            assert col is not None and col.nombre == (nombre or "fecha_nacimiento")
            assert not col.atributo and aviso_lectura_fecha(col) is None

    def test_en_su_tabla_le_gana_a_otra_fecha_de_atributo(self) -> None:
        col = resolver_columna_fecha(
            [("fecha_nacimiento", "text"), ("fecha_defuncion", "text")], tabla=self.DEFUNCIONES
        )
        assert col is not None and col.nombre == "fecha_defuncion" and not col.atributo

    def test_no_cambia_la_eleccion_entre_las_del_dato(self) -> None:
        """defunciones_generales_mensuales: `anio_def` y `mes_def` siguen siendo la fecha."""
        col = resolver_columna_fecha(
            [
                ("region", "text"),
                ("mes_anio_defuncion", "text"),
                ("mes_def", "text"),
                ("anio_def", "text"),
                ("cantidad", "text"),
            ],
            tabla="raw.datos_gob_ar__defunciones_generales_mensuales_ocu__2d1590db__v1",
        )
        assert col is not None and (col.nombre, col.mes) == ("anio_def", "mes_def")

    def test_fuera_de_su_tabla_el_aviso_no_afirma_que_no_es_la_del_dato(self) -> None:
        from app.application.consultas.fechas import aviso_lectura_fecha

        col = resolver_columna_fecha(
            [("consultante_fecha_nacimiento", "text"), ("consulta_fecha_carga", "text")],
            tabla="raw.datos_gob_ar__consultas_efectuadas_en_los_centros__279f7728__v1",
        )
        assert col is not None and col.atributo
        aviso = aviso_lectura_fecha(col) or ""
        assert "«consultante_fecha_nacimiento»" in aviso and "columna_fecha" in aviso
        assert "no la del dato" not in aviso
        assert "nacimientos" in aviso  # dice cuándo sí sería la del dato

    def test_por_los_constructores_de_consultas(self) -> None:
        from app.application.answers.aggregates import AggregateRequest, build_aggregate_query
        from app.application.public_catalog import (
            DataRequest,
            build_data_query,
            resolve_date_column,
        )

        nombres = [c for c, _ in self.COLS_DEFUNCIONES]
        q = build_data_query(
            DataRequest(
                table=self.DEFUNCIONES,
                available_columns=nombres,
                column_types=self.COLS_DEFUNCIONES,
                desde="2020",
            )
        )
        assert q.fecha is not None and not q.fecha.atributo
        a = build_aggregate_query(
            AggregateRequest(
                table=self.DEFUNCIONES,
                column_types=self.COLS_DEFUNCIONES,
                operacion="conteo",
                desde="2020",
            )
        )
        assert a.fecha is not None and not a.fecha.atributo
        fecha = resolve_date_column(self.COLS_DEFUNCIONES, tabla=self.DEFUNCIONES)
        assert fecha is not None and not fecha.atributo


class TestCostoDelMes:
    """En mart.estadistica_mediaciones (3,5 M de filas, `anio` y `mes` de texto)
    el mes agregaba un CASE de cuatro expresiones regulares por fila al ORDER BY
    de todo obtener_datos y dos CASE anidados al WHERE: medido en staging por la
    revisión, obtener_datos desc pasó de 0,6-1,9 s a 4-6 s y un mes, de 1,8-3,2 s
    a ~7 s (el timeout del sandbox es de 10 s)."""

    def test_el_mes_escrito_como_numero_no_pasa_por_expresiones_regulares(self) -> None:
        from app.application.consultas.fechas import expresion_mes

        sql = expresion_mes("mes")
        # Las formas de número («6», «06») se resuelven antes que cualquier
        # regex, y sin btrim (las que tienen blancos siguen por las regex).
        assert "\"mes\"::text IN ('1', '2'" in sql and "'01'" in sql
        assert sql.index(" IN (") < sql.index(" ~")

    def test_un_anio_con_forma_conocida_se_filtra_antes_del_mes(self) -> None:
        from app.application.consultas.fechas import RE_ANIO

        col = ColumnaFecha("anio", "text", "anio", formato="anio", mes="mes")
        params = Params()
        cond = condiciones_periodo(col, "2023-06", "2023-06", params)
        assert not any(RE_ANIO in c for c in cond)
        # Primero el año, que es barato; después la clave con el mes.
        assert len(cond) == 4 and not any('"mes"' in c for c in cond[:2])
        assert params.values == {
            "p0": "2023",
            "p1": "2023",
            "p2": "2023-06-01",
            "p3": "2023-06-31",
        }
        # Sin forma conocida (un «Total» entre los años), la guarda sigue.
        col = ColumnaFecha("anio", "text", "anio", formato="case_anio", mes="mes")
        cond = condiciones_periodo(col, "2023-06", "2023-06", Params())
        assert len(cond) == 2 and all(RE_ANIO in c for c in cond)
