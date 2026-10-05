"""El sandbox con SQL armado por nuestro código: parámetros ligados y sin auto-fix.

Auditoría, "lo que no vio" 15 (verificado en staging el 04-oct):

- el validador rechazaba valores de filtro con palabras de SQL («Banco do
  Brasil», «Call Center»: 867 valores en 313 tablas) porque los valores iban
  interpolados y la regex recorre todo el texto;
- el auto-fix de enteros reescribía `> 1000000.5` como `> '1000000'.5`
  (error de sintaxis) y también tocaba los literales.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from app.infrastructure.adapters.sandbox import pg_sandbox_adapter as adapter_module
from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import (
    PgSandboxAdapter,
    _autofix_bare_integers,
    _validate_sql,
)


class TestAutoFixDeEnteros:
    def test_sigue_citando_un_entero_desnudo(self) -> None:
        assert _autofix_bare_integers("SELECT * FROM cache_x WHERE anio = 2024") == (
            "SELECT * FROM cache_x WHERE anio = '2024'"
        )

    def test_no_rompe_los_decimales(self) -> None:
        sql = "SELECT * FROM cache_x WHERE monto > 1000000.5"
        assert _autofix_bare_integers(sql) == sql

    def test_no_toca_los_literales(self) -> None:
        sql = "SELECT * FROM cache_x WHERE nota = 'x >= 2000 y' AND anio >= 2020"
        assert _autofix_bare_integers(sql) == (
            "SELECT * FROM cache_x WHERE nota = 'x >= 2000 y' AND anio >= '2020'"
        )

    def test_no_toca_los_nombres_citados(self) -> None:
        sql = 'SELECT "a>=2000" FROM cache_x'
        assert _autofix_bare_integers(sql) == sql


class TestValidadorConSQLArmado:
    def test_el_sql_de_un_modelo_sigue_con_el_escaneo_completo(self) -> None:
        assert _validate_sql("SELECT a FROM cache_x WHERE b = 'Banco do Brasil'") is not None

    def test_el_sql_armado_no_escanea_literales_ni_nombres_citados(self) -> None:
        assert (
            _validate_sql(
                'SELECT "Set." FROM raw.x WHERE "canal"::text = \'Call Center\'', built=True
            )
            is None
        )

    @pytest.mark.parametrize(
        "sql",
        [
            "SELECT a FROM raw.x; DELETE FROM raw.x",
            "SELECT a FROM raw.x WHERE a = :p0 UNION SELECT key_hash FROM api_keys",
            "SELECT pg_read_file('/etc/passwd') FROM raw.x",
            "SELECT * FROM raw.cached_datasets",
            "SELECT a FROM secret.x",
            "DELETE FROM raw.x",
        ],
    )
    def test_las_demas_barreras_siguen(self, sql: str) -> None:
        assert _validate_sql(sql, built=True) is not None


def _adapter_with(conn: MagicMock) -> PgSandboxAdapter:
    adapter = PgSandboxAdapter()
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn
    adapter._engine = engine
    return adapter


class _Result:
    def keys(self):
        return ["n"]

    def fetchmany(self, n):
        return [(16,)]


class TestEjecucion:
    def _run(self, sql: str, params: dict | None) -> MagicMock:
        conn = MagicMock()
        conn.execute.side_effect = [None, None, _Result()]
        adapter = _adapter_with(conn)
        with (
            patch.object(adapter_module, "_blocked_mart_error", return_value=None),
            patch.object(adapter_module, "_findings_blocked_error", return_value=None),
        ):
            result = adapter._execute_sync(sql, 10, params)
        assert result.error is None, result.error
        return conn

    def test_con_parametros_el_texto_no_se_toca_y_los_valores_van_aparte(self) -> None:
        sql = 'SELECT count(*) AS n FROM raw.x WHERE "monto" > 1000000.5 AND "c"::text = :p0'
        conn = self._run(sql, {"p0": "Banco do Brasil"})
        statement, params = conn.execute.call_args_list[-1].args
        assert str(statement) == sql
        assert params == {"p0": "Banco do Brasil"}

    def test_sin_parametros_es_sql_de_un_modelo_y_pasa_por_el_auto_fix(self) -> None:
        conn = self._run("SELECT count(*) AS n FROM cache_x WHERE anio = 2024", None)
        (statement,) = conn.execute.call_args_list[-1].args
        assert str(statement).endswith("anio = '2024'")

    def test_un_rechazo_del_validador_dice_que_clase_de_error_es(self) -> None:
        result = PgSandboxAdapter()._execute_sync("DELETE FROM raw.x", 10, {})
        assert result.error_kind == "validation"


class TestBusquedaDeTablas:
    def test_un_nombre_en_mayusculas_se_busca_tambien_en_minusculas(self) -> None:
        """QW13 / ok.2: 'RAW.CACHE_SERIES_TIPO_CAMBIO' daba 404."""
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = []
        _adapter_with(conn)._find_tables_sync([], ["RAW.CACHE_SERIES_TIPO_CAMBIO"])
        params = conn.execute.call_args.args[1]
        assert "cache_series_tipo_cambio" in params["names"]


class TestEstadisticas:
    def test_no_lee_estadisticas_de_tablas_internas(self) -> None:
        conn = MagicMock()
        adapter = _adapter_with(conn)
        assert adapter._get_value_stats_sync("raw.cached_datasets", ["table_name"]) is None
        assert adapter._get_value_stats_sync("public.users", ["email"]) is None
        conn.execute.assert_not_called()

    def test_lee_pg_stats_de_una_tabla_del_catalogo(self) -> None:
        conn = MagicMock()
        rel = MagicMock(schema_name="raw", table_name="cache_x", reltuples=4794.0)
        stat = MagicMock(
            attname="funcion_desc",
            null_frac=0.0,
            n_distinct=29.0,
            mcv=["Educación y Cultura", "Salud"],
            mcf=[0.1, 0.05],
            hist=None,
        )
        conn.execute.side_effect = [
            None,
            None,
            MagicMock(first=MagicMock(return_value=rel)),
            MagicMock(fetchall=MagicMock(return_value=[stat])),
        ]
        stats = _adapter_with(conn)._get_value_stats_sync("raw.cache_x", ["funcion_desc"])
        assert stats is not None and stats.estimated_rows == 4794
        assert stats.columns["funcion_desc"].most_common_vals == ["Educación y Cultura", "Salud"]
