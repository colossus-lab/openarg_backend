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
from tests.evaluation.quality_checks import leaked_identifiers

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


# ── M7 (round v46): expanded internal-name coverage ──────────────


@pytest.mark.parametrize(
    "leaked_name",
    [
        # Pre-fix coverage (kept passing).
        "cache_leyes_sancionadas",
        "dataset_chunks",
        "pgvector",
        "query_cache",
        "cached_datasets",
        # M7 additions — the analyst could mention any of these and the
        # pre-fix scrubber left them in the visible answer.
        "catalog_resources",
        "raw_table_versions",
        "mart_definitions",
        "mart_sample_queries",
        "query_analytics",
        "table_catalog",
        "parse_repair_audit",
        "successful_queries",
        "user_queries",
        "query_dataset_links",
        "agent_tasks",
        "api_keys",
        "api_usage",
        "sesion_chunks",
    ],
)
def test_scrub_strips_full_internal_table_set(leaked_name: str) -> None:
    answer = f"Dato extraído de {leaked_name} muestra 42 registros."
    scrubbed = _scrub_internal_identifiers(answer)
    assert leaked_name not in scrubbed
    assert "42 registros" in scrubbed


def test_scrub_strips_parenthetical_for_new_names() -> None:
    """The whole `(Fuente: successful_queries)` aside disappears, not
    just the identifier — so the user doesn't see "(Fuente: )" dangling."""
    answer = "Hay 7 ejemplos similares (Fuente: successful_queries)."
    scrubbed = _scrub_internal_identifiers(answer)
    assert "successful_queries" not in scrubbed
    assert "fuente:" not in scrubbed.lower()
    assert "7 ejemplos" in scrubbed


def test_scrub_does_not_overmatch_partial_substring() -> None:
    """`apicultor` (or any word that contains a substring of a banned
    name as a non-word boundary) must NOT be scrubbed."""
    answer = "El apicultor produjo miel. Y los api_eros también."
    scrubbed = _scrub_internal_identifiers(answer)
    assert "apicultor" in scrubbed
    assert "api_eros" in scrubbed


# ── Nombres de la capa raw: <portal>__<título>__<hash>__v<N> ──────

# Los dos que publicó ckan_003 en la batería de staging del 02-nov.
EGRESOS = "raw.buenos_aires_prov__egresos_hospitalarios__dbcccf6f__v1"
RED_RECORTADA = "raw.buenos_aires_prov__red_de_establecimientos_que_ga__..."


def test_scrub_borra_los_dos_nombres_de_ckan_003_sin_romper_el_titulo() -> None:
    answer = (
        f"**3. Red de establecimientos IVE/ILE** (`{RED_RECORTADA}`)\n"
        "Hospitales y centros de la Provincia. (~617 registros)\n\n"
        f"**4. Egresos hospitalarios 2021–2022** (`{EGRESOS}`)\n"
        "Egresos por año y región. (~388.000 registros)\n"
    )
    assert _scrub_internal_identifiers(answer) == (
        "**3. Red de establecimientos IVE/ILE**\n"
        "Hospitales y centros de la Provincia. (~617 registros)\n\n"
        "**4. Egresos hospitalarios 2021–2022**\n"
        "Egresos por año y región. (~388.000 registros)\n"
    )


@pytest.mark.parametrize(
    ("citado", "leido"),
    [
        (f"`{EGRESOS}`", "egresos hospitalarios"),
        (EGRESOS, "egresos hospitalarios"),
        ("buenos_aires_prov__egresos_hospitalarios__dbcccf6f__v1", "egresos hospitalarios"),
        ("`buenos_aires_prov__egresos_hospitalarios__dbcccf6f__v12`", "egresos hospitalarios"),
        ('raw."buenos_aires_prov__egresos_hospitalarios__dbcccf6f__v1"', "egresos hospitalarios"),
        ('"raw"."buenos_aires_prov__egresos_hospitalarios__dbcccf6f__v1"', "egresos hospitalarios"),
        ("`raw.buenos_aires_prov__egresos_hospitalarios__...`", "egresos hospitalarios"),
        ("raw.buenos_aires_prov__egresos_hospitalarios__…", "egresos hospitalarios"),
        ("`raw.buenos_aires_prov__egresos_hospitalarios__*`", "egresos hospitalarios"),
        ("`raw.buenos_aires_prov__egresos_hospitalarios__`", "egresos hospitalarios"),
        # Cortado por el modelo en medio de una palabra.
        ("`raw.buenos_aires_prov__egresos_hospita...`", "egresos"),
        # Cortado por el namer: los 63 caracteres no alcanzan para el título.
        (f"`{RED_RECORTADA}`", "red de establecimientos"),
        ("raw.datos_gob_ar__estudio_nacional_sobre_el_perfil_de__08ca74a9__v1", None),
        # Un título que empieza con un número lleva `t_`.
        ("`raw.caba__t_2023_viajes_en_subte__4e140173__v10`", "2023 viajes en subte"),
        ("staging.presupuesto__presupuesto_abierto", "presupuesto abierto"),
    ],
)
def test_scrub_lee_el_titulo_del_nombre_en_medio_de_la_frase(
    citado: str, leido: str | None
) -> None:
    leido = leido or "estudio nacional sobre el perfil"
    scrubbed = _scrub_internal_identifiers(f"Según {citado}, hay 42 registros.")
    assert scrubbed == f"Según {leido}, hay 42 registros."


@pytest.mark.parametrize(
    ("antes", "despues"),
    [
        (f"Egresos (`{EGRESOS}`), con 42 registros.", "Egresos, con 42 registros."),
        (f"Egresos [{EGRESOS}].", "Egresos."),
        (f"Egresos ( {EGRESOS} ).", "Egresos."),
        # Con algo más adentro se conserva lo que dice el paréntesis.
        (f"Egresos (`{EGRESOS}`, 42 filas).", "Egresos (egresos hospitalarios, 42 filas)."),
        (f"Egresos (Fuente: {EGRESOS}).", "Egresos (Fuente: egresos hospitalarios)."),
        # El texto de un enlace no es una cita: el enlace sigue funcionando.
        (f"Ver [{EGRESOS}](https://x.org).", "Ver [egresos hospitalarios](https://x.org)."),
    ],
)
def test_scrub_borra_la_cita_sola_entre_parentesis(antes: str, despues: str) -> None:
    assert _scrub_internal_identifiers(antes) == despues


@pytest.mark.parametrize(
    "texto",
    [
        "https://datos.gob.ar/dataset/foo__bar_baz",
        "descargá reporte__2024.csv desde el portal",
        "la ruta /datos/foo__bar",
        "el campo __filas_total",
        "una palabra_con_guiones y `tasa_call`",
    ],
)
def test_scrub_no_toca_lo_que_no_es_un_nombre_de_tabla(texto: str) -> None:
    assert _scrub_internal_identifiers(texto) == texto


@pytest.mark.parametrize(
    "citado",
    [
        EGRESOS,
        RED_RECORTADA,
        f"`{EGRESOS}`",
        "buenos_aires_prov__red_de_establecimientos_que_ga__...",
        "buenos_aires_prov__egresos_hospitalarios__dbcccf6f",
        'raw."buenos_aires_prov__egresos_hospitalarios__dbcccf6f__v1"',
    ],
)
def test_lo_que_detecta_la_bateria_no_sobrevive_a_la_limpieza(citado: str) -> None:
    for answer in (f"Según {citado}, hay 42.", f"Egresos ({citado}), con 42."):
        assert leaked_identifiers(answer), "el detector de la batería tiene que verlo"
        assert leaked_identifiers(_scrub_internal_identifiers(answer)) == []


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
