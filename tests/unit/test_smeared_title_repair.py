"""Recovering a header pandas smeared across the columns.

116 servable tables carry this in production, holding 291,436 rows, and they are
not obscure: `caba__acceso_de_mujeres_a_la_salud`,
`caba__casos_penales_contravencionales_violencia`,
`caba__educacion_sexual_integral`. A person asking about maternal mortality gets
a table whose every column is the same sentence.
"""

from __future__ import annotations

from app.application.pipeline.parsers.header_recovery import (
    _looks_like_year_header_row,
)
from app.application.repair.parse_repair import (
    _year_or_identifier,
    propose_smeared_title_rename,
)

SMEARED = [
    "DEFUNCIONES MATERNAS (ocurridas durante el embarazo o dentro de",
    "DEFUNCIONES MATERNAS (ocurridas durante el embarazo o dentro _2",
    "DEFUNCIONES MATERNAS (ocurridas durante el embarazo o dentro _3",
    "DEFUNCIONES MATERNAS (ocurridas durante el embarazo o dentro _4",
    "DEFUNCIONES MATERNAS (ocurridas durante el embarazo o dentro _5",
]
HEADER_ROW = [("DEFUNCIONES MATERNAS", "2017", "2018.0", "2019.0", "2020.0")]


def test_it_recovers_the_real_header_from_row_zero():
    cols, renamed, why = propose_smeared_title_rename(SMEARED, HEADER_ROW)
    assert why == "ok"
    assert cols == ["defunciones_maternas", "anio_2017", "anio_2018",
                    "anio_2019", "anio_2020"]
    assert renamed == 5


def test_a_year_column_read_as_float_is_still_a_year():
    """pandas reads a year column as float, so the header arrives as `2018.0`
    and `int()` refuses it. A whole family of statistical tables turns on
    that `.0`."""
    assert _looks_like_year_header_row(["ETIQUETA", "2017", "2018.0", "2019.0"])
    assert _looks_like_year_header_row(["ETIQUETA", "2017", "2018", "2019"])


def test_a_row_of_observations_is_not_a_header():
    """`['TOTAL NOTIFICADAS', '8', '8.0', '10.0']` is data. Promoting it would
    name the columns after one observation and lose that row."""
    assert not _looks_like_year_header_row(["TOTAL NOTIFICADAS", "8", "8.0", "10.0"])
    _, _, why = propose_smeared_title_rename(
        SMEARED, [("TOTAL NOTIFICADAS AL DMI", "8", "8.0", "10.0", "10.0")]
    )
    assert why == "row0_not_header_like"


def test_three_columns_is_enough():
    """The May heuristic requires thirty, on the reasoning that a wide table is
    where this happens. These are eight wide and just as unusable; the width was
    never what made the defect."""
    old = ["Superficie sembrada por departamento y campaña agrícola",
           "Superficie sembrada por departamento y campaña agrícola_2",
           "Superficie sembrada por departamento y campaña agrícola_3"]
    cols, _, why = propose_smeared_title_rename(old, [("Departamento", "2020", "2021")])
    assert why == "ok"
    assert cols == ["departamento", "anio_2020", "anio_2021"]


def test_real_column_names_are_left_alone():
    """A shared prefix only condemns when it is a phrase: `fecha_inicio` and
    `fecha_fin` share seven characters and are both real."""
    old = ["fecha_inicio", "fecha_fin", "fecha_alta"]
    _, _, why = propose_smeared_title_rename(old, [("a", "b", "c")])
    assert why == "no_common_prefix"


def test_collector_lineage_columns_keep_their_names():
    old = ["Muy largo titulo repetido en cada columna aca",
           "Muy largo titulo repetido en cada columna _2",
           "Muy largo titulo repetido en cada columna _3", "_source_dataset_id"]
    cols, _, why = propose_smeared_title_rename(old, [("Depto", "2020", "2021", "x")])
    assert why == "ok"
    assert "_source_dataset_id" in cols


def test_a_sparse_row_is_not_promoted():
    """Half the cells empty is a separator or a stray line, not a header."""
    _, _, why = propose_smeared_title_rename(SMEARED, [("Depto", None, None, None, None)])
    assert why in ("row0_too_sparse", "row0_not_header_like")


def test_years_get_a_name_a_person_would_choose():
    assert _year_or_identifier("2018.0") == "anio_2018"
    assert _year_or_identifier("2017") == "anio_2017"
    assert _year_or_identifier(None) == ""
    # Not a year: falls back to the ordinary normaliser.
    assert _year_or_identifier("Departamento") == "departamento"
    assert not _year_or_identifier("1850").startswith("anio_")


def test_numbers_that_are_not_years_are_not_a_header():
    """The narrow-table fallback has to be stricter, not looser: with only two
    numeric cells there is less evidence, so *every* one must be a year.

    `['Buenos Aires', '1500', '1600']` is a row of observations and naming the
    columns after it would lose that row.
    """
    old = ["Superficie sembrada por departamento y campaña agrícola",
           "Superficie sembrada por departamento y campaña agrícola_2",
           "Superficie sembrada por departamento y campaña agrícola_3"]
    _, _, why = propose_smeared_title_rename(old, [("Buenos Aires", "1500", "1600")])
    assert why == "row0_not_header_like"


def test_the_sweep_orders_by_width_not_by_name():
    """The SQL can only narrow to "several long names ending in `_N`" — 1,064
    tables — and roughly a tenth carry the defect.

    A first production run of 300 refused all 300 while the table the repair was
    written for sorted outside the alphabetical window. Column count is the one
    signal available in SQL that correlates with the shape.
    """
    from app.infrastructure.celery.tasks.parse_repair_tasks import (
        _SMEARED_CANDIDATES_SQL,
    )

    sql = str(_SMEARED_CANDIDATES_SQL)
    assert "ORDER BY count(*) DESC" in sql
    assert "ORDER BY table_name\n" not in sql
