"""Modo datos de la API pública: armado de consultas sin SQL del usuario.

Cada identificador sale del schema real de la tabla y cada valor viaja como
parámetro ligado. Estos tests cubren sobre todo lo que tiene que rechazarse y
los bugs de la auditoría del 04-oct (fechas, igualdad tolerante, operadores).
"""

from __future__ import annotations

import pytest

from app.application.public_catalog import (
    MAX_LIMIT,
    CatalogRequestError,
    DataRequest,
    build_data_query,
    build_date_range_query,
    build_sample_query,
    date_column,
    resolve_table,
)
from app.domain.ports.sandbox.sql_sandbox import CachedTableInfo
from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import _validate_sql

_T = "raw.datos_gob_ar__principales_tasas_de_interes__6335b6d1__v1"
_COLS = ["indice_tiempo", "tasas_interes_call", "tasas_interes_badlar", "_source_url"]
_PRESUPUESTO = "raw.cache_presupuesto_credito_2026"


def _req(**kw) -> DataRequest:
    return DataRequest(table=_T, available_columns=_COLS, **kw)


class TestResolveTable:
    tables = [
        CachedTableInfo(table_name=_T, dataset_id="d1", row_count=8569, columns=[]),
        CachedTableInfo(table_name="cache_legacy_x", dataset_id="d2", row_count=5, columns=[]),
    ]

    def test_qualified_or_bare_name_resolves_to_the_listed_table(self) -> None:
        assert resolve_table(_T, self.tables).table_name == _T
        assert resolve_table(_T.split(".")[1], self.tables).table_name == _T

    def test_upper_case_names_resolve_too(self) -> None:
        assert resolve_table(_T.upper(), self.tables).table_name == _T

    def test_names_outside_the_catalog_are_not_tables(self) -> None:
        assert resolve_table("api_keys", self.tables) is None
        assert resolve_table("public.users", self.tables) is None
        assert resolve_table("", self.tables) is None


class TestBuildDataQuery:
    def test_default_is_every_visible_column_ordered_by_date(self) -> None:
        q = build_data_query(_req())
        assert q.columns == ["indice_tiempo", "tasas_interes_call", "tasas_interes_badlar"]
        assert '"_source_url"' not in q.sql
        assert q.sql.startswith(
            'SELECT "indice_tiempo", "tasas_interes_call", "tasas_interes_badlar" '
            'FROM "raw"."datos_gob_ar__principales_tasas_de_interes__6335b6d1__v1"'
        )
        # Desempate por posición física: filas con la misma fecha, siempre igual.
        assert q.sql.endswith("ASC NULLS LAST, ctid LIMIT 100")
        assert q.params == {}

    def test_period_filter_overlaps_and_values_are_bound(self) -> None:
        q = build_data_query(_req(desde="2025-12", hasta="2026-06-18", orden="desc", limite=50))
        assert q.params == {"p0": "2025-12-01", "p1": "2026-06-18"}
        assert ">= :p0" in q.sql and "<= :p1" in q.sql
        assert "2025-12" not in q.sql
        assert q.sql.endswith("DESC NULLS LAST, ctid LIMIT 50")

    def test_the_period_is_not_a_lexicographic_left(self) -> None:
        """`left(col::text, 7) >= '2025-12'` daba 0 filas con "1/10/2017"."""
        q = build_data_query(_req(desde="2017"))
        assert "left(\"indice_tiempo\"::text, 4) >= '2017'" not in q.sql
        assert "split_part" in q.sql  # entiende d/m/aaaa

    def test_equality_filter_is_tolerant_and_bound(self) -> None:
        cols = ["provincia", "valor"]
        q = build_data_query(
            DataRequest(table=_T, available_columns=cols, filtros={"provincia": "O'Higgins"})
        )
        assert "O'Higgins" not in q.sql and "o'higgins" not in q.sql
        assert q.params == {"p0": "o'higgins"}
        assert 'lower(translate(btrim("provincia"::text)' in q.sql

    def test_exact_equality_when_the_table_is_too_big_to_fold(self) -> None:
        q = build_data_query(
            DataRequest(
                table=_T,
                available_columns=["provincia"],
                filtros={"provincia": "Córdoba"},
                tolerante=False,
            )
        )
        assert '"provincia"::text = :p0' in q.sql and q.params == {"p0": "Córdoba"}

    def test_operators_in_list_form(self) -> None:
        q = build_data_query(
            DataRequest(
                table=_PRESUPUESTO,
                available_columns=["funcion_desc", "credito_devengado"],
                column_types=[("funcion_desc", "text"), ("credito_devengado", "double precision")],
                filtros=[
                    {"columna": "funcion_desc", "operador": "en", "valores": ["Salud", "Defensa"]},
                    {"columna": "credito_devengado", "operador": "mayor_que", "valor": "1000000.5"},
                ],
            )
        )
        assert '"credito_devengado" > :p1' in q.sql
        assert str(q.params["p1"]) == "1000000.5"
        assert q.params["p0"] == ["salud", "defensa"]
        assert _validate_sql(q.sql, built=True) is None

    @pytest.mark.parametrize(
        "columnas",
        [
            ['indice_tiempo" FROM api_keys --'],
            ["*"],
            ["_source_url"],  # interna: no se sirve
            ["no_existe"],
        ],
    )
    def test_columns_must_exist_in_the_table(self, columnas: list[str]) -> None:
        with pytest.raises(CatalogRequestError, match="no existen"):
            build_data_query(_req(columns=columnas))

    @pytest.mark.parametrize("value", ["2025-01-01' OR '1'='1", "ayer", "2025/01/01", "20250101"])
    def test_dates_must_be_iso(self, value: str) -> None:
        with pytest.raises(CatalogRequestError, match="fecha"):
            build_data_query(_req(desde=value))

    def test_filter_column_must_exist(self) -> None:
        with pytest.raises(CatalogRequestError, match="filtrar"):
            build_data_query(_req(filtros={"1=1; --": "x"}))

    def test_too_many_filters(self) -> None:
        cols = [f"c{i}" for i in range(10)]
        with pytest.raises(CatalogRequestError, match="filtros"):
            build_data_query(
                DataRequest(table=_T, available_columns=cols, filtros={c: "x" for c in cols[:6]})
            )

    @pytest.mark.parametrize("limite", [0, MAX_LIMIT + 1])
    def test_limit_bounds(self, limite: int) -> None:
        with pytest.raises(CatalogRequestError, match="limite"):
            build_data_query(_req(limite=limite))

    def test_order_is_asc_or_desc(self) -> None:
        with pytest.raises(CatalogRequestError, match="orden"):
            build_data_query(_req(orden="asc; DROP TABLE x"))

    def test_period_on_a_table_without_dates_names_the_candidates(self) -> None:
        with pytest.raises(CatalogRequestError, match="No reconocí") as exc:
            build_data_query(
                DataRequest(table=_T, available_columns=["provincia", "mes", "valor"], desde="2025")
            )
        assert "mes" in str(exc.value)

    def test_quoted_identifier_with_double_quote_is_escaped(self) -> None:
        # Un nombre de columna real con comillas (existe en el schema) queda citado bien.
        q = build_data_query(
            DataRequest(table=_T, available_columns=['raro"nombre'], columns=['raro"nombre'])
        )
        assert '"raro""nombre"' in q.sql


class TestTruncadoOrdenYOffset:
    """Auditoría ok.3 / QW12: `truncado = cantidad >= limite` daba falsos
    positivos, sin fecha no había ORDER BY y no había forma de pedir la página
    siguiente."""

    def test_pide_una_fila_de_mas_para_saber_si_hay_mas(self) -> None:
        q = build_data_query(_req(limite=100, una_de_mas=True))
        assert q.sql.endswith("LIMIT 101") and q.limite == 100

    def test_offset_va_despues_del_limite(self) -> None:
        q = build_data_query(_req(limite=50, offset=200, una_de_mas=True))
        assert q.sql.endswith("LIMIT 51 OFFSET 200")

    @pytest.mark.parametrize("offset", [-1, 10_001])
    def test_offset_acotado(self, offset: int) -> None:
        with pytest.raises(CatalogRequestError, match="offset"):
            build_data_query(_req(offset=offset))

    def test_sin_fecha_y_chica_ordena_por_posicion_fisica(self) -> None:
        q = build_data_query(
            DataRequest(table=_PRESUPUESTO, available_columns=["a", "b"], orden_fisico=True)
        )
        assert q.sql.endswith("ORDER BY ctid LIMIT 100") and q.orden == "fisico"

    def test_sin_fecha_y_grande_no_ordena(self) -> None:
        """Ordenar una tabla sin índices la recorre entera (11 s en 300.000 filas)."""
        q = build_data_query(DataRequest(table=_PRESUPUESTO, available_columns=["a", "b"]))
        assert "ORDER BY" not in q.sql and q.orden is None

    def test_un_mart_no_desempata_por_ctid(self) -> None:
        q = build_data_query(
            DataRequest(table="mart.inflacion", available_columns=["fecha", "v"], orden_fisico=True)
        )
        assert "ctid" not in q.sql and q.orden == "fecha"

    def test_el_validador_acepta_ctid_y_offset(self) -> None:
        q = build_data_query(_req(offset=100, una_de_mas=True))
        assert _validate_sql(q.sql, built=True) is None
        q = build_data_query(
            DataRequest(table=_PRESUPUESTO, available_columns=["a"], orden_fisico=True, offset=5)
        )
        assert _validate_sql(q.sql, built=True) is None


class TestFechasQueAntesNoSeReconocian:
    """Casos de la auditoría 4.1 (tablas de staging y prod)."""

    def test_proyectos_parlamentarios_publicacion_fecha(self) -> None:
        q = build_data_query(
            DataRequest(
                table="raw.diputados__proyectos_parlamentarios__ecffe8be__v1",
                available_columns=["PROYECTO_ID", "TITULO", "PUBLICACION_FECHA"],
                desde="2026-01",
            )
        )
        assert q.fecha is not None and q.fecha.nombre == "PUBLICACION_FECHA"
        assert '"PUBLICACION_FECHA"' in q.sql.split("WHERE", 1)[1]

    def test_snic_anio_bigint(self) -> None:
        q = build_data_query(
            DataRequest(
                table="cache_datos_gob_ar_snic_provincial_estad_sticas_cri_r1893ad7984",
                available_columns=["provincia_nombre", "anio", "cantidad_hechos"],
                column_types=[
                    ("provincia_nombre", "text"),
                    ("anio", "bigint"),
                    ("cantidad_hechos", "bigint"),
                ],
                desde="2020",
                hasta="2020",
            )
        )
        assert q.fecha is not None and q.fecha.nombre == "anio"
        assert q.params == {"p0": "2020-01-01", "p1": "2020-12-31"}

    def test_updated_at_no_es_la_fecha_de_la_serie(self) -> None:
        assert date_column(["updated_at", "fecha", "valor"]) == "fecha"
        assert date_column(["updated_ts", "valor"]) is None

    def test_columna_fecha_elegida(self) -> None:
        q = build_data_query(
            DataRequest(
                table=_T,
                available_columns=["fecha_inicio", "fecha_fin"],
                desde="2020",
                columna_fecha="fecha_fin",
            )
        )
        assert q.fecha is not None and q.fecha.nombre == "fecha_fin"


class TestTheSandboxValidatorAcceptsWhatWeBuild:
    """Las consultas pasan igual por el validador del sandbox (segunda barrera).

    Si rechazara la forma que armamos (expresiones de fecha, `translate`,
    marcadores `:p0`), el modo datos respondería error en producción aunque
    todo lo demás ande.
    """

    def test_data_query(self) -> None:
        q = build_data_query(_req(desde="2025-12", hasta="2026-06", orden="desc", limite=500))
        assert _validate_sql(q.sql, built=True) is None

    def test_filtered_query(self) -> None:
        q = build_data_query(
            DataRequest(
                table=_T, available_columns=["provincia", "valor"], filtros={"provincia": "Córdoba"}
            )
        )
        assert _validate_sql(q.sql, built=True) is None

    def test_a_filter_value_with_sql_words_is_not_in_the_text(self) -> None:
        """«Banco do Brasil» y «Call Center» los rechazaba el validador (867 valores)."""
        for value in ("Banco do Brasil", "Call Center", "ADT SECURITY SERVICE SA"):
            q = build_data_query(
                DataRequest(table=_T, available_columns=["entidad"], filtros={"entidad": value})
            )
            assert _validate_sql(q.sql, built=True) is None

    def test_a_column_named_with_a_sql_word(self) -> None:
        """'Set.' rompía describir_tabla en 13 tablas de prod."""
        q = build_data_query(DataRequest(table=_T, available_columns=["Set.", "valor"]))
        assert _validate_sql(q.sql, built=True) is None
        assert _validate_sql(build_sample_query(_T, ["Set.", "valor"]), built=True) is None

    def test_aux_queries(self) -> None:
        assert _validate_sql(build_sample_query(_T, _COLS), built=True) is None
        assert _validate_sql(build_date_range_query(_T, "indice_tiempo"), built=True) is None

    def test_legacy_cache_table(self) -> None:
        q = build_data_query(DataRequest(table="cache_ipc", available_columns=["fecha", "v"]))
        assert _validate_sql(q.sql, built=True) is None


class TestAuxQueries:
    def test_sample_skips_internal_columns(self) -> None:
        sql = build_sample_query(_T, _COLS)
        assert '"_source_url"' not in sql and sql.endswith("LIMIT 5")

    def test_date_range_counts_recognized_values(self) -> None:
        sql = build_date_range_query(_T, "indice_tiempo")
        assert "min(f) AS desde" in sql and "max(f) AS hasta" in sql
        assert "count(f) AS reconocidas" in sql and "count(c) AS con_valor" in sql
        # La expresión se calcula una vez por fila, no una por agregado.
        assert sql.count("(CASE WHEN") == 1 and "OFFSET 0" in sql
