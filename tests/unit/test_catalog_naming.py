"""Tests for WS4 deterministic naming."""

from __future__ import annotations

from app.application.catalog import (
    MAX_TABLE_NAME_LENGTH,
    PhysicalNamer,
    TitleExtractor,
    TitleSource,
    extract_title,
    physical_table_name,
)


# ---------- physical naming ----------


def test_physical_name_under_pg_limit():
    name = physical_table_name(
        "datos_gob_ar", "indicadores_de_evolucion_del_sector_externo_full_long"
    )
    assert len(name) <= MAX_TABLE_NAME_LENGTH


def test_physical_name_is_idempotent():
    a = physical_table_name("datos_gob_ar", "indicador-1")
    b = physical_table_name("datos_gob_ar", "indicador-1")
    assert a == b


def test_three_collision_cases_from_prod_get_distinct_names():
    """The three real cases from collector_plan WS4."""
    a = physical_table_name(
        "datos_gob_ar", "indicadores_de_evolucion_del_sector_externo_long_a"
    )
    b = physical_table_name(
        "datos_gob_ar", "inase_establecimientos_vigentes_inscripto_largo_b"
    )
    c = physical_table_name(
        "datos_gob_ar", "lacteos_estimacion_de_la_distribucion_largo_caso_c"
    )
    assert a != b != c
    # And they all fit
    assert max(len(x) for x in (a, b, c)) <= MAX_TABLE_NAME_LENGTH


def test_same_resource_different_words_have_different_discriminators():
    a = physical_table_name("portal_x", "resource_alpha")
    b = physical_table_name("portal_x", "resource_alpha_2")
    assert a != b


def test_table_name_charset_is_safe():
    name = physical_table_name(
        "datos_córdoba", "Áéíóú niños & números — 2024  ¿? !"
    )
    # Must be only [a-z0-9_]
    assert all(c.isalnum() or c == "_" for c in name), name
    assert name.islower() or name.replace("_", "").isalnum()


def test_table_name_does_not_start_with_digit():
    name = physical_table_name("123portal", "456resource")
    assert not name[0].isdigit()


def test_physical_namer_validates_discriminator_length():
    import pytest

    with pytest.raises(ValueError):
        PhysicalNamer(discriminator_length=2)


# ---------- canonical title extraction ----------


def test_index_title_wins_over_caratula():
    res = extract_title(
        {
            "index_title": "Tasa de actividad por trimestre",
            "caratula_title": "Cuadro 1.7",
            "dataset_title": "EPH",
        }
    )
    assert res.title_source == TitleSource.INDEX
    assert res.title_confidence >= 0.9
    assert "actividad" in res.canonical_title.lower()


def test_caratula_used_when_index_missing():
    res = extract_title({"caratula_title": "Cuadro 4: Población económicamente activa"})
    assert res.title_source == TitleSource.CARATULA


def test_metadata_combination_when_no_titles():
    res = extract_title(
        {
            "dataset_title": "Encuesta Permanente de Hogares",
            "sheet_name": "T1 2024",
            "cuadro_numero": "1.7",
            "provincia": "Buenos Aires",
        }
    )
    assert res.title_source == TitleSource.METADATA
    assert "Cuadro 1.7" in res.canonical_title
    assert "Buenos Aires" in res.canonical_title


def test_fallback_to_filename():
    res = extract_title({"filename": "padron_oficial_2024.xlsx"})
    assert res.title_source == TitleSource.FALLBACK
    assert res.title_confidence < 0.5


def test_indec_template_renders_full_display():
    extractor = TitleExtractor()
    res = extractor.extract(
        {
            "portal": "indec",
            "index_title": "Tasa de desempleo trimestral",
            "provincia": "Mendoza",
            "cuadro_numero": "1.4",
        }
    )
    assert res.display_name.startswith("INDEC — Mendoza — Cuadro 1.4")


def test_optional_placeholders_dropped_when_empty():
    extractor = TitleExtractor()
    res = extractor.extract(
        {
            "portal": "indec",
            "index_title": "Población urbana 2010",
            # provincia + cuadro absent — placeholders are optional
        }
    )
    # Should not contain "— —" or trailing dashes
    assert "— —" not in res.display_name
    assert not res.display_name.rstrip().endswith("—")


def test_titles_are_normalized_unicode():
    res = extract_title({"index_title": "Población   total   "})
    assert "  " not in res.canonical_title
    assert res.canonical_title == res.canonical_title.strip()


def test_titles_are_deterministic():
    ctx = {"index_title": "Foo", "portal": "indec", "provincia": "X", "cuadro_numero": "1"}
    a = extract_title(ctx)
    b = extract_title(ctx)
    assert a == b
