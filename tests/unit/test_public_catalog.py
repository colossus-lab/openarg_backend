"""Modo datos de la API pública: armado de consultas sin SQL del usuario.

Cada identificador sale del schema real de la tabla y cada valor pasa por una
forma validada. Estos tests cubren sobre todo lo que tiene que rechazarse.
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
    resolve_table,
)
from app.domain.ports.sandbox.sql_sandbox import CachedTableInfo

_T = "raw.datos_gob_ar__principales_tasas_de_interes__6335b6d1__v1"
_COLS = ["indice_tiempo", "tasas_interes_call", "tasas_interes_badlar", "_source_url"]


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

    def test_names_outside_the_catalog_are_not_tables(self) -> None:
        assert resolve_table("api_keys", self.tables) is None
        assert resolve_table("public.users", self.tables) is None
        assert resolve_table("", self.tables) is None


class TestBuildDataQuery:
    def test_default_is_every_visible_column_ordered_by_date(self) -> None:
        sql, cols = build_data_query(_req())
        assert cols == ["indice_tiempo", "tasas_interes_call", "tasas_interes_badlar"]
        assert '"_source_url"' not in sql
        assert sql.startswith(
            'SELECT "indice_tiempo", "tasas_interes_call", "tasas_interes_badlar" '
            'FROM "raw"."datos_gob_ar__principales_tasas_de_interes__6335b6d1__v1"'
        )
        assert sql.endswith('ORDER BY "indice_tiempo" ASC LIMIT 100')

    def test_period_filter_uses_the_date_column(self) -> None:
        sql, _ = build_data_query(
            _req(desde="2025-12", hasta="2026-06-18", orden="desc", limite=50)
        )
        assert "left(\"indice_tiempo\"::text, 7) >= '2025-12'" in sql
        assert "left(\"indice_tiempo\"::text, 10) <= '2026-06-18'" in sql
        assert sql.endswith("DESC LIMIT 50")

    def test_equality_filter_value_is_escaped(self) -> None:
        cols = ["provincia", "valor"]
        sql, _ = build_data_query(
            DataRequest(table=_T, available_columns=cols, filtros={"provincia": "O'Higgins"})
        )
        assert "\"provincia\"::text = 'O''Higgins'" in sql

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

    def test_period_on_a_table_without_dates(self) -> None:
        with pytest.raises(CatalogRequestError, match="columna de fecha"):
            build_data_query(
                DataRequest(table=_T, available_columns=["provincia", "valor"], desde="2025")
            )

    def test_quoted_identifier_with_double_quote_is_escaped(self) -> None:
        # Un nombre de columna real con comillas (existe en el schema) queda citado bien.
        sql, _ = build_data_query(
            DataRequest(table=_T, available_columns=['raro"nombre'], columns=['raro"nombre'])
        )
        assert '"raro""nombre"' in sql


class TestTheSandboxValidatorAcceptsWhatWeBuild:
    """Las consultas pasan igual por el validador del sandbox (segunda barrera).

    Si rechazara la forma que armamos (`left(...)::text`, `~`, comillas), el
    modo datos respondería error en producción aunque todo lo demás ande.
    """

    def test_data_query(self) -> None:
        from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import _validate_sql

        sql, _ = build_data_query(_req(desde="2025-12", hasta="2026-06", orden="desc", limite=500))
        assert _validate_sql(sql) is None

    def test_filtered_query(self) -> None:
        from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import _validate_sql

        sql, _ = build_data_query(
            DataRequest(
                table=_T, available_columns=["provincia", "valor"], filtros={"provincia": "Córdoba"}
            )
        )
        assert _validate_sql(sql) is None

    def test_aux_queries(self) -> None:
        from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import _validate_sql

        assert _validate_sql(build_sample_query(_T, _COLS)) is None
        assert _validate_sql(build_date_range_query(_T, "indice_tiempo")) is None

    def test_legacy_cache_table(self) -> None:
        from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import _validate_sql

        sql, _ = build_data_query(DataRequest(table="cache_ipc", available_columns=["fecha", "v"]))
        assert _validate_sql(sql) is None


class TestAuxQueries:
    def test_sample_skips_internal_columns(self) -> None:
        sql = build_sample_query(_T, _COLS)
        assert '"_source_url"' not in sql and sql.endswith("LIMIT 5")

    def test_date_range(self) -> None:
        sql = build_date_range_query(_T, "indice_tiempo")
        assert 'min(left("indice_tiempo"::text, 10))' in sql
        assert "'^[0-9]{4}'" in sql
