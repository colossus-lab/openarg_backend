"""La huella del contenido (`table_fingerprints`) contra Postgres de verdad.

Lo que un test con dobles no ve: que el SQL armado corra (``to_regclass`` con
el nombre citado, ``ROW(...)::text``, el paso de md5 a ``bit(64)``), que la
huella no dependa del orden físico de las filas ni de las columnas ``_*`` del
colector, y que distinga un valor nulo de un texto vacío. Verificado también
contra el Postgres de staging (sólo lectura, rol del sandbox) el 07-oct: los
cuadros 3.1 y 4.1 del ISAC dan huellas distintas y los 1, 6.1 y 6.3, la misma.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator

import pytest
from sqlalchemy import create_engine, text


def _engine():
    url = os.getenv("DATABASE_URL", "")
    if not url:
        pytest.skip("DATABASE_URL not set — este test necesita una DB real")
    engine = create_engine(url, pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:  # pragma: no cover — environmental
        pytest.skip(f"DB unreachable: {exc}")
    return engine


@pytest.fixture
def tablas() -> Iterator[dict[str, str]]:
    engine = _engine()
    tag = uuid.uuid4().hex[:8]
    filas = {
        # La misma tabla que "a", con las filas en otro orden y otra columna _*.
        "a": "(1, 'Asfalto', '102,2', 'ds-a'), (2, 'Cales', '98,1', 'ds-a')",
        "b": "(2, 'Cales', '98,1', 'ds-b'), (1, 'Asfalto', '102,2', 'ds-b')",
        # Misma forma, otro valor (la desestacionalizada).
        "c": "(1, 'Asfalto', '86,6', 'ds-c'), (2, 'Cales', '98,1', 'ds-c')",
        # Nulo contra texto vacío.
        "d": "(1, 'Asfalto', NULL, 'ds-d'), (2, 'Cales', '98,1', 'ds-d')",
        "e": "(1, 'Asfalto', '', 'ds-e'), (2, 'Cales', '98,1', 'ds-e')",
    }
    names = {k: f"cache_test_huella_{tag}_{k}" for k in filas}
    with engine.begin() as conn:
        for k, name in names.items():
            conn.execute(
                text(
                    f'CREATE TABLE public."{name}" '
                    '(id integer, "Insumo" text, "valor" text, "_source_dataset_id" text)'
                )
            )
            conn.execute(text(f'INSERT INTO public."{name}" VALUES {filas[k]}'))
    try:
        yield names
    finally:
        with engine.begin() as conn:
            for name in names.values():
                conn.execute(text(f'DROP TABLE IF EXISTS public."{name}"'))


def test_same_rows_same_fingerprint_whatever_the_order(tablas: dict[str, str]) -> None:
    from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import PgSandboxAdapter

    out = PgSandboxAdapter()._table_fingerprints_sync(
        [*tablas.values(), "cache_test_huella_no_existe", "datasets"]
    )

    assert set(out) == set(tablas.values())
    huella = {k: out[name] for k, name in tablas.items()}
    assert huella["a"] == huella["b"]
    assert huella["a"] != huella["c"]
    assert huella["d"] != huella["e"]
    assert huella["a"].startswith("2:")


def test_qualified_names_resolve_like_find_tables_returns_them(tablas: dict[str, str]) -> None:
    from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import PgSandboxAdapter

    sandbox = PgSandboxAdapter()
    bare = sandbox._table_fingerprints_sync([tablas["a"]])
    qualified = sandbox._table_fingerprints_sync([f"public.{tablas['a']}"])
    assert bare == qualified == {tablas["a"]: bare[tablas["a"]]}
