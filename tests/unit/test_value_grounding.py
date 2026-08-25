"""Tests for checking a proposed column name against the column's values.

The error this exists for is the *semantically adjacent* one — `latitud` on a
longitude column, `fecha_fin` on a start date. Those are valid identifiers,
distinct, correctly typed, and completely wrong, so they pass every other check
we have. What matters as much as catching them is staying silent about the many
names nothing can verify: a check that refuses what it does not understand is a
check that gets switched off.
"""

from __future__ import annotations

from app.application.repair.value_grounding import (
    ground_name,
    is_valid_cuit,
    looks_like_provincia,
    looks_like_year,
    reject_contradicted,
)

# CUITs reales por construcción (dígito verificador calculado).
_CUITS = ["20-12345678-6", "27-22222222-8", "30-71234567-1", "23-45678901-3"]
_FECHAS = ["2024-01-15", "2024-02-20", "2023-11-03", "2024-07-09"]
_MONTOS = ["1250000.50", "890000.00", "2340500.75", "15000.00"]


# ── dígito verificador ─────────────────────────────────────────


def test_a_well_formed_cuit_passes():
    assert is_valid_cuit("20-12345678-6")


def test_a_cuit_with_one_digit_changed_fails():
    # El punto entero: esto es aritmética, no criterio.
    assert not is_valid_cuit("20-12345678-9")


def test_something_that_is_not_a_cuit_fails():
    assert not is_valid_cuit("1234")


# ── el veredicto ───────────────────────────────────────────────


def test_a_name_the_values_support_is_ok():
    assert ground_name("cuit", _CUITS).verdict == "ok"


def test_a_name_the_values_contradict_is_refused():
    g = ground_name("cuit", _FECHAS)
    assert g.contradicted
    assert "dígito verificador" in g.detail


def test_a_date_column_named_as_an_amount_is_refused():
    assert ground_name("monto", ["hola", "chau", "que tal", "nada"]).contradicted


def test_a_name_nothing_can_verify_says_nothing():
    # La mayoría de los nombres son de esta clase, y refutarlos por las dudas
    # volvería el chequeo inútil.
    assert ground_name("observaciones", ["x", "y", "z", "w"]).verdict == "unknown"


def test_a_sample_too_small_says_nothing():
    assert ground_name("cuit", ["2024-01-15"]).verdict == "unknown"


def test_a_few_bad_rows_do_not_condemn_a_good_name():
    # Las columnas reales traen blancos, centinelas y alguna fila mala.
    assert ground_name("fecha", [*_FECHAS, "s/d", "n/a"]).verdict == "ok"


def test_the_first_matching_fragment_decides():
    # `fecha_cuit` se lee como fecha, no como identificador fiscal.
    assert ground_name("fecha_cuit", _FECHAS).verdict == "ok"


def test_a_province_column_is_checked_by_membership():
    assert ground_name("provincia", ["Buenos Aires", "Córdoba", "SALTA", "Tucumán"]).verdict == "ok"
    assert ground_name("provincia", ["rojo", "verde", "azul", "gris"]).contradicted


def test_province_matching_tolerates_accents_and_prefixes():
    assert looks_like_provincia("Provincia de Córdoba")
    assert looks_like_provincia("CORDOBA")


def test_a_year_outside_a_plausible_range_is_not_a_year():
    assert looks_like_year("2024")
    assert not looks_like_year("1200")
    assert not looks_like_year("99")


# ── el rechazo por columna ─────────────────────────────────────


def _rows(*cols):
    return [list(t) for t in zip(*cols, strict=True)]


def test_only_the_contradicted_rename_reverts():
    filas = _rows(_CUITS, _FECHAS)
    aplicar, rechazos = reject_contradicted(["col_1", "col_2"], ["fecha", "fecha_alta"], filas)
    # `col_1` tiene CUITs y se propuso `fecha`: contradicho, vuelve atrás.
    # `col_2` tiene fechas y se propuso `fecha_alta`: correcto, se aplica.
    assert aplicar == ["col_1", "fecha_alta"]
    assert len(rechazos) == 1


def test_a_clean_proposal_passes_untouched():
    filas = _rows(_CUITS, _MONTOS)
    aplicar, rechazos = reject_contradicted(["col_1", "col_2"], ["cuit", "monto"], filas)
    assert aplicar == ["cuit", "monto"]
    assert rechazos == []


def test_an_unchanged_column_is_never_questioned():
    aplicar, rechazos = reject_contradicted(["fecha"], ["fecha"], _rows(_CUITS))
    assert aplicar == ["fecha"] and rechazos == []


def test_a_length_mismatch_is_left_alone():
    aplicar, rechazos = reject_contradicted(["a"], ["b", "c"], [])
    assert aplicar == ["b", "c"] and rechazos == []


def test_no_sample_rows_means_no_opinion():
    aplicar, rechazos = reject_contradicted(["col_1"], ["cuit"], [])
    assert aplicar == ["cuit"] and rechazos == []
