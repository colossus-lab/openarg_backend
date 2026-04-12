"""Unit tests for the analyst output scrubbers.

FR-025e / FIX-011: ``_scrub_internal_identifiers`` removes internal
sandbox/infrastructure identifiers that the analyst LLM sometimes
cites verbatim (``cache_*``, ``dataset_chunks``, ``pgvector``, ...).

FR-025f / FIX-012: ``_drop_apologetic_preface`` removes a leading
"No tengo X pero..." sentence when real data follows, so the user
sees the data first instead of a disclaimer. Honest "no data"
answers are preserved.
"""

from __future__ import annotations

import pytest

from app.application.pipeline.nodes.analyst import (
    _drop_apologetic_preface,
    _scrub_internal_identifiers,
)

# ── _scrub_internal_identifiers ───────────────────────────────────


def test_scrub_removes_bare_cache_identifier() -> None:
    answer = "Se sancionaron 661 leyes. Fuente: cache_leyes_sancionadas."
    scrubbed = _scrub_internal_identifiers(answer)
    assert "cache_leyes_sancionadas" not in scrubbed
    assert "661 leyes" in scrubbed


def test_scrub_removes_parenthetical_citation() -> None:
    answer = (
        "Los empleados públicos totalizan 3.2 millones (Fuente: cache_empleados_publicos_nacion)."
    )
    scrubbed = _scrub_internal_identifiers(answer)
    assert "cache_empleados_publicos_nacion" not in scrubbed
    assert "fuente:" not in scrubbed.lower()  # aside removed, not dangling
    assert "3.2 millones" in scrubbed


def test_scrub_removes_bracket_citation() -> None:
    answer = "Presupuesto 2025: $10B [src: cache_presupuesto_2025]"
    scrubbed = _scrub_internal_identifiers(answer)
    assert "cache_presupuesto_2025" not in scrubbed
    assert "src:" not in scrubbed.lower()
    assert "$10B" in scrubbed


def test_scrub_removes_multiple_leaks_in_same_answer() -> None:
    answer = (
        "Según cache_leyes_sancionadas hay 661 leyes, y los datos de "
        "cache_congreso_staff muestran 3657 empleados. Ver también "
        "dataset_chunks y pgvector."
    )
    scrubbed = _scrub_internal_identifiers(answer)
    for leak in ("cache_leyes_sancionadas", "cache_congreso_staff", "dataset_chunks", "pgvector"):
        assert leak not in scrubbed
    assert "661 leyes" in scrubbed
    assert "3657 empleados" in scrubbed


def test_scrub_is_case_insensitive() -> None:
    answer = "Datos de CACHE_TRANSPORTE_FERROCARRIL y Query_Cache."
    scrubbed = _scrub_internal_identifiers(answer)
    assert "CACHE_TRANSPORTE_FERROCARRIL" not in scrubbed
    assert "Query_Cache" not in scrubbed


def test_scrub_preserves_paragraph_breaks() -> None:
    answer = "Línea uno.\n\nLínea dos con cache_leyes.\n\nLínea tres."
    scrubbed = _scrub_internal_identifiers(answer)
    assert "cache_leyes" not in scrubbed
    # Paragraph structure survives even though a token was removed.
    assert "Línea uno." in scrubbed
    assert "Línea dos" in scrubbed
    assert "Línea tres." in scrubbed


def test_scrub_is_a_noop_on_clean_text() -> None:
    clean = "Se sancionaron 661 leyes en la Cámara de Diputados en 2024."
    assert _scrub_internal_identifiers(clean) == clean


def test_scrub_handles_empty_string() -> None:
    assert _scrub_internal_identifiers("") == ""


# ── _drop_apologetic_preface ──────────────────────────────────────


def test_drop_preface_strips_leading_no_tengo_when_data_follows() -> None:
    answer = (
        "No tengo datos específicos sobre sueldos en hoteles. "
        "Sin embargo, el Índice de Salarios creció 7.869% desde 2016."
    )
    result = _drop_apologetic_preface(answer)
    assert not result.lower().startswith("no tengo")
    assert "Índice de Salarios creció 7.869%" in result


def test_drop_preface_strips_aunque_no_pude_accedere() -> None:
    answer = (
        "Aunque no pude acceder a datos en tiempo real, los salarios crecieron 95% desde mayo 2024."
    )
    result = _drop_apologetic_preface(answer)
    assert not result.lower().startswith("aunque no pude")
    assert "95%" in result


def test_drop_preface_strips_si_bien_no_tengo() -> None:
    answer = (
        "Si bien no tengo el dato exacto para febrero 2026, "
        "la inflación en enero 2026 fue de 2.88%."
    )
    result = _drop_apologetic_preface(answer)
    assert not result.lower().startswith("si bien")
    assert "2.88%" in result


def test_drop_preface_preserves_genuinely_empty_responses() -> None:
    """When there's NO real data after the disclaimer, keep the honest answer."""
    answer = (
        "No tengo información sobre el RENABAP en el dataset actual. "
        "Esta consulta está fuera del alcance de nuestros datos."
    )
    result = _drop_apologetic_preface(answer)
    # No numeric data → the disclaimer is the entire honest answer, keep it.
    assert result == answer


def test_drop_preface_keeps_text_that_does_not_start_with_apology() -> None:
    answer = "El Índice de Salarios creció 7.869% entre 2016 y 2025."
    assert _drop_apologetic_preface(answer) == answer


def test_drop_preface_handles_leading_whitespace() -> None:
    answer = (
        "\n\n  No tengo acceso directo al dataset de X. "
        "Los datos disponibles muestran 123 registros."
    )
    result = _drop_apologetic_preface(answer)
    assert "No tengo acceso" not in result
    assert "123 registros" in result


def test_drop_preface_is_idempotent() -> None:
    answer = "No encontré datos específicos. Los últimos valores disponibles son 456 y 789."
    once = _drop_apologetic_preface(answer)
    twice = _drop_apologetic_preface(once)
    assert once == twice


def test_drop_preface_handles_empty_string() -> None:
    assert _drop_apologetic_preface("") == ""


def test_drop_preface_strips_mid_sentence_pero_no_tengo() -> None:
    """The LLM writes ', pero no tengo X' as a sub-clause — we should
    remove the clause while keeping the first half of the sentence and
    the continuation afterwards. This was the failure mode of
    ``test_dv02_sueldo_empleado_hotel`` in the 2026-04-11 re-run."""
    answer = (
        "OpenArg cubre datos de salarios agregados a nivel nacional, "
        "pero no tengo información específica sobre sueldos de "
        "empleados de hotel por categoría laboral. Lo que sí puedo "
        "mostrarte es la evolución del 7.869% entre 2016 y 2025."
    )
    result = _drop_apologetic_preface(answer)
    assert "no tengo" not in result.lower()
    assert "salarios agregados a nivel nacional" in result
    assert "7.869%" in result
    # Sub-clause removed, sentence continues naturally from the first half.
    assert result.startswith("OpenArg cubre datos de salarios agregados")


def test_drop_preface_strips_first_sentence_with_heading_and_negation() -> None:
    """The LLM writes '# Heading\\n\\nEl problema es claro: no tengo X.
    Resto con datos.' — we should drop the first sentence (including
    the bold marker) while keeping the heading and the data sentence.
    This was the failure mode of ``test_inflacion_real_vs_sueldo``."""
    answer = (
        "# Comparación: Inflación vs Salarios de CONICET\n\n"
        "**El problema es claro: no tengo datos específicos de CPA "
        "de CONICET en los datasets disponibles.** "
        "Sin embargo, la inflación acumulada 2024–2025 fue del 56%."
    )
    result = _drop_apologetic_preface(answer)
    assert "no tengo" not in result.lower()
    assert "# Comparación" in result
    assert "56%" in result


def test_drop_preface_preserves_heading_only_empty_answer() -> None:
    """If the whole answer is a heading + apologetic statement with no
    numeric data below, keep it — it is an honest empty response."""
    answer = (
        "# Consulta RENABAP\n\n"
        "No tengo información sobre el porcentaje de terrenos privados "
        "del RENABAP en los datasets actuales."
    )
    result = _drop_apologetic_preface(answer)
    # No numeric data → honest empty answer → untouched.
    assert result == answer


@pytest.mark.parametrize(
    "preface_opener",
    [
        "No tengo",
        "No cuento con",
        "No encontré",
        "No pude acceder",
        "No pude obtener",
        "No dispongo",
        "Aunque no tengo",
        "Aunque no pude",
        "Si bien no tengo",
        "Lamentablemente no",
    ],
)
def test_drop_preface_covers_all_apology_openers(preface_opener: str) -> None:
    answer = f"{preface_opener} el dato específico. Pero el valor es 42.5%."
    result = _drop_apologetic_preface(answer)
    assert not result.lower().startswith(preface_opener.lower())
    assert "42.5%" in result
