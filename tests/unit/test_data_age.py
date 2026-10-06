"""The reader has to be able to tell how old the answer is.

These tests are mostly about what the module refuses to say. A freshness label
that is confidently wrong is worse than none, because it converts "I don't know"
into a promise.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace

import pytest

from app.application.quality.data_age import (
    STALE_AFTER_DAYS,
    DataAge,
    data_age_for,
    staleness_warning,
    table_freshness,
)


class _Conn:
    """Answers each lookup by what it reads: the collector's last read
    (`cached_datasets`), the registry, the matview's sources (`pg_depend`) or
    the mart definition."""

    def __init__(
        self,
        registry=None,
        mart=None,
        raises=False,
        cached=None,
        mart_sources=None,
        mart_undated=0,
    ):
        self.registry, self.mart, self.raises = registry, mart, raises
        self.cached, self.mart_sources = cached, mart_sources
        self.mart_undated = mart_undated
        self.queried: list[str] = []

    def execute(self, stmt, params=None):
        if self.raises:
            raise RuntimeError("pg is down")
        sql = str(stmt)
        self.queried.append(sql)
        if "pg_depend" in sql:
            row = SimpleNamespace(as_of=self.mart_sources, undated=self.mart_undated)
            return SimpleNamespace(fetchone=lambda: row)
        if "cached_datasets" in sql:
            value = self.cached
        elif "raw_table_versions" in sql:
            value = self.registry
        else:
            value = self.mart
        return SimpleNamespace(fetchone=lambda: SimpleNamespace(as_of=value))

    def rollback(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _Engine:
    def __init__(self, conn):
        self._conn = conn

    def connect(self):
        return self._conn


def _ago(days):
    return datetime.now(UTC) - timedelta(days=days)


def test_fresh_data_earns_no_line():
    """A notice on every answer becomes furniture the reader stops seeing."""
    e = _Engine(_Conn(registry=_ago(3)))
    assert staleness_warning(e, "raw.cache_x") is None


def test_stale_data_says_when_it_was_read():
    e = _Engine(_Conn(registry=datetime(2026, 5, 6, tzinfo=UTC)))
    line = staleness_warning(e, "raw.cache_indec_pobreza")
    assert line is not None
    assert "mayo" in line and "2026" in line


def test_a_lookup_failure_costs_no_answer():
    """Worst case the line is absent — never an exception into the response."""
    assert staleness_warning(_Engine(_Conn(raises=True)), "t") is None


def test_unknown_table_says_nothing_rather_than_guessing():
    assert data_age_for(_Engine(_Conn()), "never_heard_of_it") is None
    assert data_age_for(_Engine(_Conn()), None) is None
    assert data_age_for(_Engine(_Conn()), "  ") is None


def test_mart_falls_back_to_its_source_dates_not_its_rebuild_time():
    """The distinction the whole module exists for.

    A mart rebuilt this morning over sources last read in May holds May's data.
    With no source the collector knows, the lookup ends at
    `mart_definitions.source_data_oldest` — recorded from the tables the
    macros resolved to, never from `last_refreshed_at`.
    """
    conn = _Conn(registry=None, mart=datetime(2026, 5, 6, tzinfo=UTC))
    age = data_age_for(_Engine(conn), "mart.pobreza_indec_aglomerados")
    assert age is not None
    assert age.source == "mart_definition"
    assert age.is_stale
    # It asked the matview's own sources first and only then the definition.
    assert "pg_depend" in conn.queried[0]
    assert "mart_definitions" in conn.queried[1]
    # And it never consulted the rebuild time.
    assert not any("last_refreshed_at" in q for q in conn.queried)


def test_a_table_read_this_morning_is_not_reported_as_may():
    """Staging, 04-oct: `cache_bcra_cotizaciones` is re-read every day by its
    vía-B writer, but the registry's `created_at` stayed at 2026-05-05 (the
    upsert never touches it), so the notice said "mayo de 2026". The last read
    is `cached_datasets.updated_at`."""
    conn = _Conn(cached=_ago(0), registry=datetime(2026, 5, 5, tzinfo=UTC))
    assert staleness_warning(_Engine(conn), "raw.cache_bcra_cotizaciones") is None
    age = data_age_for(_Engine(conn), "raw.cache_bcra_cotizaciones")
    assert age is not None and age.source == "cached"


def test_a_table_the_collector_does_not_know_falls_back_to_the_registry():
    conn = _Conn(cached=None, registry=datetime(2026, 5, 6, tzinfo=UTC))
    age = data_age_for(_Engine(conn), "raw.cache_x")
    assert age is not None and age.source == "registry"
    assert "cached_datasets" in conn.queried[0]


def test_a_mart_is_as_old_as_its_oldest_source_read():
    """A mart rebuilt on 04-oct over sources read in September said "mayo"
    because `source_data_oldest` froze at build time. With every source dated,
    its age comes from the tables the matview reads today."""
    conn = _Conn(mart_sources=_ago(29), mart=datetime(2026, 5, 5, tzinfo=UTC))
    age = data_age_for(_Engine(conn), "mart.presupuesto_consolidado")
    assert age is not None
    assert age.source == "mart"
    assert age.days == 29
    assert not age.is_stale


def test_a_mart_source_we_cannot_date_does_not_make_the_mart_fresher():
    """Staging, 05-oct: `presupuesto_consolidado` read 10 of its 11 sources
    that morning and the 11th has no ready row; ignoring it said "read today".
    With a source we cannot date, the date recorded at build time is taken if
    it is older."""
    conn = _Conn(mart_sources=_ago(0), mart_undated=1, mart=datetime(2026, 5, 8, tzinfo=UTC))
    age = data_age_for(_Engine(conn), "mart.bcra_principales_indicadores")
    assert age is not None
    assert age.source == "mart_definition"
    assert age.is_stale
    assert "mart_definitions" in conn.queried[1]


def test_an_undated_source_never_makes_the_mart_younger_either():
    """The recorded date only wins when it is older than the sources we can date."""
    conn = _Conn(mart_sources=datetime(2026, 5, 6, tzinfo=UTC), mart_undated=1, mart=_ago(1))
    age = data_age_for(_Engine(conn), "mart.x")
    assert age is not None and age.source == "mart" and age.is_stale
    # Every source undated: the recorded date, as before.
    conn = _Conn(mart_sources=None, mart_undated=3, mart=datetime(2026, 5, 9, tzinfo=UTC))
    age = data_age_for(_Engine(conn), "mart.inflacion_argentina")
    assert age is not None and age.source == "mart_definition"


def test_the_mart_lookup_dates_each_source_by_indexed_keys():
    """Each source is dated as if served directly (ready row, else registry),
    and the registry is joined by (schema_name, table_name), its unique index:
    by `table_name` alone it seq-scanned and took 10 s on a 287-source mart in
    staging (05-oct), 49 ms with the index."""
    from app.application.quality.data_age import _MART_SOURCES_SQL

    sql = " ".join(str(_MART_SOURCES_SQL).split())
    assert "coalesce(cd.updated_at, rtv.created_at)" in sql
    assert "rtv.schema_name = src.schema_name AND rtv.table_name = src.table_name" in sql
    assert "AS undated" in sql


def test_the_stale_threshold_matches_the_collector_backstop():
    """If chat and refresh disagree about 'stale', one of them is lying."""
    from app.application.collection.freshness import backstop_age

    assert STALE_AFTER_DAYS == backstop_age().days


def test_schema_qualified_names_are_accepted():
    e = _Engine(_Conn(registry=_ago(200)))
    assert staleness_warning(e, 'raw."cache_x"') is not None


def test_days_never_goes_negative_on_a_future_timestamp():
    age = DataAge(as_of=datetime.now(UTC) + timedelta(days=2), days=0, source="registry")
    assert age.days == 0
    assert not age.is_stale


# ── table_freshness: lo que describir_tabla dice de una tabla (3.4) ──────


def _leida(year: int, month: int, day: int, days: int) -> DataAge:
    return DataAge(as_of=datetime(year, month, day, 3, 0, tzinfo=UTC), days=days, source="cached")


def test_una_serie_da_su_ultimo_dato_y_cuando_se_leyo():
    """Reservas en staging: leída el 09-may, último dato abril de 2023."""
    f = table_freshness(
        _leida(2026, 5, 9, 149),
        columna_fecha="indice_tiempo",
        desde="1940-01-01",
        hasta="2023-04-01",
    )
    assert f.serie and f.ultimo_dato == "2023-04-01" and f.fecha_corte is None
    assert f.actualizada is not None and f.actualizada.isoformat() == "2026-05-09"
    assert f.nota is not None and "hace 149 días" in f.nota


def test_un_solo_periodo_es_una_foto_con_su_fecha_de_corte():
    """El crédito presupuestario 2026: ejercicio 2026 en todas las filas."""
    f = table_freshness(
        _leida(2026, 10, 5, 0), columna_fecha="ejercicio_presupuestario", desde="2026", hasta="2026"
    )
    assert not f.serie and f.fecha_corte is not None
    assert f.fecha_corte.isoformat() == "2026-10-05" and f.ultimo_dato == "2026"
    assert f.nota is not None and "foto" in f.nota and "hace" not in f.nota


def test_sin_columna_de_fecha_no_hay_ultimo_dato_ni_fecha_de_corte():
    """Revisión independiente del 05-oct (H022): antes una tabla sin columna de
    fecha salía con `fecha_corte` = día de lectura, "vigente al" ese día. La
    fecha de lectura no dice de cuándo son los datos."""
    f = table_freshness(_leida(2026, 10, 4, 1), columna_fecha=None, desde=None, hasta=None)
    assert f.ultimo_dato is None and f.fecha_corte is None and not f.serie
    assert f.nota is not None
    assert "los vigentes al" not in f.nota and "foto" not in f.nota
    assert "no tiene columna de fecha" in f.nota and "4 de octubre de 2026" in f.nota
    # La fecha de lectura se informa igual: es cierta.
    assert f.actualizada is not None and f.actualizada.isoformat() == "2026-10-04"


def test_una_tabla_de_2001_sin_fecha_no_se_declara_vigente_hoy():
    """H022: 3.331 tablas de prod sin columna de fecha (390 con un año ≤2023
    en el título), releídas a diario, salían "vigentes al 5 de octubre de
    2026". El año está en el título; la tabla no lo tiene en ninguna columna."""
    f = table_freshness(_leida(2026, 10, 5, 0), columna_fecha=None, desde=None, hasta=None)
    assert f.fecha_corte is None and f.serie is False
    assert f.nota is not None and "vigentes al" not in f.nota
    assert "título del dataset" in f.nota


def test_sin_columna_de_fecha_y_sin_fecha_de_lectura_tampoco_afirma_nada():
    f = table_freshness(None, columna_fecha=None, desde=None, hasta=None)
    assert f.fecha_corte is None and f.actualizada is None
    assert f.nota is not None and "los vigentes al" not in f.nota


def test_un_solo_periodo_pasado_dice_de_cuando_son_los_datos():
    """H022, la reproducción de la revisión: ejercicio 2019 releído el 04-oct-2026
    salía «Es una foto… los datos son los vigentes al 4 de octubre de 2026»."""
    f = table_freshness(
        _leida(2026, 10, 4, 1), columna_fecha="ejercicio", desde="2019", hasta="2019"
    )
    assert f.serie is False and f.ultimo_dato == "2019"
    assert f.fecha_corte is None
    assert f.nota is not None
    assert "los vigentes al" not in f.nota and "foto" not in f.nota
    assert "los datos son de 2019" in f.nota
    assert f.actualizada is not None and f.actualizada.isoformat() == "2026-10-04"


@pytest.mark.parametrize(
    ("periodo", "leida", "foto"),
    [
        # Ejercicio: el en curso o el anterior (el de 2025 se cierra en 2026).
        ("2026", (2026, 10, 5), True),
        ("2025", (2026, 10, 5), True),
        ("2024", (2026, 10, 5), False),
        ("2001", (2026, 10, 5), False),
        # Un ejercicio futuro (un proyecto de presupuesto) tampoco es vigente.
        ("2027", (2026, 10, 5), False),
        # Mes: el corriente o el anterior.
        ("2026-10", (2026, 10, 5), True),
        ("2026-09", (2026, 10, 5), True),
        ("2025-12", (2026, 1, 3), True),
        ("2026-07", (2026, 10, 5), False),
        # Día: el de la lectura o el anterior.
        ("2026-10-05", (2026, 10, 5), True),
        ("2026-10-04", (2026, 10, 5), True),
        ("2026-09-30", (2026, 10, 5), False),
        # Un valor que no se reconoce: sin saber de cuándo es, no es vigente.
        ("2do semestre", (2026, 10, 5), False),
    ],
)
def test_un_solo_periodo_es_una_foto_solo_si_es_el_actual_o_el_anterior(
    periodo: str, leida: tuple[int, int, int], foto: bool
) -> None:
    f = table_freshness(_leida(*leida, 0), columna_fecha="periodo", desde=periodo, hasta=periodo)
    assert f.serie is False and f.ultimo_dato == periodo
    if foto:
        assert f.fecha_corte is not None and f.fecha_corte == date(*leida)
        assert f.nota is not None and "vigentes al" in f.nota
    else:
        assert f.fecha_corte is None
        assert f.nota is not None and "los vigentes al" not in f.nota and "foto" not in f.nota


@pytest.mark.parametrize(
    ("periodo", "texto"),
    [("2026-03", "de marzo de 2026"), ("2026-09-30", "del 30 de septiembre de 2026")],
)
def test_el_periodo_pasado_se_dice_como_una_persona(periodo: str, texto: str) -> None:
    f = table_freshness(_leida(2026, 10, 5, 0), columna_fecha="fecha", desde=periodo, hasta=periodo)
    assert f.nota is not None and f"los datos son {texto}." in f.nota


def test_un_solo_periodo_sin_fecha_de_lectura_dice_igual_de_cuando_es():
    f = table_freshness(None, columna_fecha="ejercicio", desde="2019", hasta="2019")
    assert f.fecha_corte is None and f.ultimo_dato == "2019"
    assert f.nota is not None and "los datos son de 2019" in f.nota


def test_sin_fecha_de_lectura_no_inventa_una():
    f = table_freshness(None, columna_fecha="fecha", desde="2020-01-01", hasta="2026-08-01")
    assert f.actualizada is None and f.dias_desde_actualizacion is None and f.nota is None
    assert f.ultimo_dato == "2026-08-01"


def test_con_columna_de_fecha_y_sin_rango_no_es_una_foto():
    """Revisión del PR #139: fechas en un formato que no se reconoce, o una tabla
    enorme sin fechas en la muestra. Antes salía como foto «vigente al» día de
    lectura, y podía ser una serie que termina en 2019."""
    f = table_freshness(
        _leida(2026, 10, 2, 3), columna_fecha="indice_tiempo", desde=None, hasta=None
    )
    assert f.serie is None
    assert f.fecha_corte is None and f.ultimo_dato is None
    assert f.nota is not None
    assert "foto" not in f.nota and "vigentes" not in f.nota
    assert "no se pudo determinar" in f.nota and "indice_tiempo" in f.nota
    # La fecha de lectura se informa igual: es cierta.
    assert f.actualizada is not None and f.actualizada.isoformat() == "2026-10-02"


def test_un_periodo_sacado_de_la_muestra_es_aproximado():
    f = table_freshness(
        _leida(2026, 10, 4, 1),
        columna_fecha="fecha",
        desde="2019-01-01",
        hasta="2025-11-01",
        aproximado=True,
    )
    assert f.serie and f.aproximado and f.ultimo_dato == "2025-11-01"
    assert f.nota is not None and "aproximado" in f.nota and "2025-11-01" in f.nota


def test_aproximado_sin_rango_no_marca_nada():
    f = table_freshness(None, columna_fecha=None, desde=None, hasta=None, aproximado=True)
    assert not f.aproximado
