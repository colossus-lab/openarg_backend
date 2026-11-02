"""El link de la fuente de un mart o una tabla, contra Postgres.

Un mart salía sin URL y el frontend mandaba a la portada de datos.gob.ar. Y en
CABA `datasets.url` guarda una lista de contactos en JSON: medido en staging el
02-nov-2026, 2.882 de 3.094 tablas listas citaban eso como link.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator

import pytest
from sqlalchemy import create_engine, text

from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import PgSandboxAdapter

_ARCHIVO = "https://www.indec.gob.ar/ftp/cuadros/sociedad/zz_09_26.xls"
_OTRO = "https://www.indec.gob.ar/ftp/cuadros/sociedad/zz_otro.xls"
_CONTACTOS = '["https://encontacto.buenosaires.gob.ar/index.php/983552"]'
_PAGINA = "https://datos.gob.ar/dataset/zz"


def _engine():
    url = os.getenv("DATABASE_URL", "")
    if not url:
        pytest.skip("DATABASE_URL not set — este test necesita una DB real")
    engine = create_engine(url.replace("+asyncpg", "+psycopg"), pool_pre_ping=True)
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:  # pragma: no cover — environmental
        pytest.skip(f"DB unreachable: {exc}")
    return engine


def _tabla(conn, tag: str, nombre: str, *, url: str, descarga: str | None) -> str:
    """Una tabla en raw, con su dataset si `descarga` no es None."""
    tabla = f"zz_{tag}_{nombre}"
    conn.execute(text(f'CREATE TABLE raw."{tabla}" (valor int)'))
    if descarga is None:
        return tabla
    dataset_id = conn.execute(
        text(
            "INSERT INTO datasets (source_id, title, organization, portal, url, download_url, "
            "format, columns, tags, last_updated_at, is_cached, row_count) "
            "VALUES (:sid, :title, 'INDEC', 'zz', :url, :dl, 'xls', '[]', '', now(), true, 1) "
            "RETURNING CAST(id AS text)"
        ),
        {"sid": f"zz-{tabla}", "title": f"Título {nombre}", "url": url, "dl": descarga},
    ).scalar()
    conn.execute(
        text(
            "INSERT INTO raw.cached_datasets (dataset_id, table_name, status, row_count) "
            "VALUES (CAST(:d AS uuid), :t, 'ready', 1)"
        ),
        {"d": dataset_id, "t": tabla},
    )
    return tabla


@pytest.fixture
def marts() -> Iterator[dict[str, str]]:
    engine = _engine()
    tag = uuid.uuid4().hex[:8]
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS mart"))
        if not conn.execute(text("SELECT to_regclass('raw.cached_datasets') IS NOT NULL")).scalar():
            conn.execute(
                text(
                    "CREATE TABLE raw.cached_datasets (LIKE public.cached_datasets "
                    "INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES)"
                )
            )
        hoja_1 = _tabla(conn, tag, "hoja_1", url=_ARCHIVO, descarga=_ARCHIVO)
        hoja_2 = _tabla(conn, tag, "hoja_2", url=_ARCHIVO, descarga=_ARCHIVO)
        otro = _tabla(conn, tag, "otro", url=_OTRO, descarga=_OTRO)
        sin_dataset = _tabla(conn, tag, "suelta", url="", descarga=None)
        contactos = _tabla(conn, tag, "caba", url=_CONTACTOS, descarga=_ARCHIVO)
        pagina = _tabla(conn, tag, "pagina", url=_PAGINA, descarga=_ARCHIVO)
        sin_link = _tabla(conn, tag, "sin_link", url=_CONTACTOS, descarga="")
        vistas = {
            "una_hoja": [hoja_1],
            "dos_hojas": [hoja_1, hoja_2],
            "dos_archivos": [hoja_1, otro],
            "tabla_suelta": [hoja_1, sin_dataset],
            "sin_descarga": [sin_link],
        }
        nombres = {clave: f"zz_{tag}_{clave}" for clave in vistas}
        for clave, tablas in vistas.items():
            union = " UNION ALL ".join(f'SELECT valor FROM raw."{t}"' for t in tablas)
            conn.execute(text(f'CREATE MATERIALIZED VIEW mart."{nombres[clave]}" AS {union}'))
    yield {
        **nombres,
        "tag": tag,
        "contactos": contactos,
        "pagina": pagina,
        "sin_link": sin_link,
    }
    with engine.begin() as conn:
        for nombre in nombres.values():
            conn.execute(text(f'DROP MATERIALIZED VIEW IF EXISTS mart."{nombre}"'))
        tablas = list(
            conn.execute(
                text(
                    "SELECT tablename FROM pg_tables WHERE schemaname = 'raw' AND tablename LIKE :p"
                ),
                {"p": f"zz_{tag}_%"},
            ).scalars()
        )
        for tabla in tablas:
            conn.execute(text(f'DROP TABLE raw."{tabla}"'))
        conn.execute(
            text("DELETE FROM raw.cached_datasets WHERE table_name LIKE :p"),
            {"p": f"zz_{tag}_%"},
        )
        conn.execute(text("DELETE FROM datasets WHERE source_id LIKE :p"), {"p": f"zz-zz_{tag}_%"})


def _urls(marts: dict[str, str]) -> dict[str, str]:
    claves = ("una_hoja", "dos_hojas", "dos_archivos", "tabla_suelta", "sin_descarga")
    adapter = PgSandboxAdapter()
    with adapter._get_engine().connect() as conn:
        return adapter._mart_source_urls(conn, [marts[c] for c in claves])


def test_un_mart_de_un_solo_archivo_enlaza_ese_archivo(marts):
    urls = _urls(marts)
    assert urls[marts["una_hoja"]] == _ARCHIVO
    # Varias hojas del mismo archivo siguen siendo un archivo.
    assert urls[marts["dos_hojas"]] == _ARCHIVO


@pytest.mark.parametrize("clave", ["dos_archivos", "tabla_suelta", "sin_descarga"])
def test_sin_un_unico_archivo_conocido_no_hay_link(marts, clave):
    assert marts[clave] not in _urls(marts)


def test_url_que_no_es_un_link_cae_en_la_descarga(marts):
    fuentes = PgSandboxAdapter()._get_table_sources_sync(
        [marts["contactos"], marts["pagina"], marts["sin_link"]]
    )
    assert fuentes[marts["contactos"]].url == _ARCHIVO
    # La página del dataset sigue ganándole a la descarga.
    assert fuentes[marts["pagina"]].url == _PAGINA
    assert fuentes[marts["sin_link"]].url == ""
