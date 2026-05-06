"""Unit tests for the Phase 1 raw-schema naming helpers.

Per MASTERPLAN Fase 1, every materialized table lives in schema `raw` with the
shape `<portal>__<slug>__<discriminator>__v<N>`. The naming MUST be:
  - identity-stable across versions (same `(portal, source_id)` ⇒ same `stem`).
  - within Postgres' 63-char identifier limit.
  - SQL-safe (charset `[a-z0-9_]`).
  - deterministic (no random suffixes).
"""

from __future__ import annotations

import pytest

from app.application.catalog.physical_namer import (
    MAX_TABLE_NAME_LENGTH,
    RawPhysicalNamer,
    raw_table_name,
    raw_table_qualified,
)


def test_basic_format() -> None:
    namer = RawPhysicalNamer()
    r = namer.build("entre_rios", "padron-2024", version=1, slug_hint="Padrón 2024")
    assert r.bare_name.startswith("entre_rios__")
    assert r.bare_name.endswith("__v1")
    assert r.version == 1
    assert len(r.bare_name) <= MAX_TABLE_NAME_LENGTH
    assert all(c.islower() or c.isdigit() or c == "_" for c in r.bare_name)


def test_version_suffix_changes_only_version() -> None:
    namer = RawPhysicalNamer()
    v1 = namer.build("caba", "subte-viajes-molinetes", version=1)
    v2 = namer.build("caba", "subte-viajes-molinetes", version=2)
    assert v1.stem == v2.stem
    assert v1.discriminator == v2.discriminator
    assert v1.bare_name.replace("__v1", "__v2") == v2.bare_name


def test_identity_stable_across_calls() -> None:
    namer = RawPhysicalNamer()
    a = namer.build("portal_x", "source-y", version=1, slug_hint="Title Z")
    b = namer.build("portal_x", "source-y", version=1, slug_hint="Title Z")
    assert a.bare_name == b.bare_name
    assert a.discriminator == b.discriminator


def test_long_inputs_get_truncated_within_limit() -> None:
    namer = RawPhysicalNamer()
    long_slug = "x" * 200
    r = namer.build("very_long_portal_name_for_test", "src", version=99, slug_hint=long_slug)
    assert len(r.bare_name) <= MAX_TABLE_NAME_LENGTH
    assert r.truncated is True
    assert r.bare_name.endswith("__v99")


def test_leading_digits_in_slug_get_prefixed() -> None:
    namer = RawPhysicalNamer()
    r = namer.build("caba", "123-comuna", version=1)
    # Postgres identifiers cannot start with a digit; the namer must avoid
    # "123_comuna" landing as the slug body. The portal prefix protects the
    # overall identifier, but the slug itself should not start with a digit.
    slug_part = r.bare_name.replace("caba__", "", 1).rsplit("__", 2)[0]
    assert not slug_part[0].isdigit()


def test_qualified_name_includes_schema() -> None:
    qualified = raw_table_qualified("p", "s", version=1)
    assert qualified.startswith('raw."')
    assert qualified.endswith('"')


def test_helper_returns_bare_name() -> None:
    name = raw_table_name("portal", "source", version=1)
    assert "raw." not in name  # bare name has no schema prefix
    assert name.endswith("__v1")


def test_special_characters_get_slugified() -> None:
    namer = RawPhysicalNamer()
    r = namer.build("España!", "fuente-con-ñ-y-acentos", version=1)
    assert all(c.islower() or c.isdigit() or c == "_" for c in r.bare_name)
    # Slugify drops accents; ensure the shape still starts with portal
    assert r.bare_name.startswith("espana__") or r.bare_name.startswith("esp__")


def test_invalid_version_raises() -> None:
    namer = RawPhysicalNamer()
    with pytest.raises(ValueError):
        namer.build("p", "s", version=0)
    with pytest.raises(ValueError):
        namer.build("p", "s", version=1000)
