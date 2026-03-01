"""
Scraper Worker — Agente de indexación del catálogo.

Recorre los portales de datos abiertos, extrae metadata de cada dataset
y la guarda en PostgreSQL. Luego dispara el embedding de cada uno.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import UTC, datetime

from sqlalchemy import create_engine, text

from app.infrastructure.celery.app import celery_app

logger = logging.getLogger(__name__)


def _get_sync_engine():
    url = os.getenv("DATABASE_URL", "postgresql+psycopg://postgres:postgres@localhost:5432/openarg_db")
    return create_engine(url)


@celery_app.task(name="openarg.scrape_catalog", bind=True, max_retries=3)
def scrape_catalog(self, portal: str = "datos_gob_ar", batch_size: int = 100):
    """
    Scrapea el catálogo completo de un portal de datos.
    Guarda metadata en tabla datasets. Dispara embedding para cada uno.
    """
    import httpx

    logger.info(f"Starting catalog scrape for portal: {portal}")

    portal_urls = {
        # Nacionales
        "datos_gob_ar": "https://datos.gob.ar/api/3/action",
        "diputados": "https://datos.hcdn.gob.ar/api/3/action",
        "justicia": "https://datos.jus.gob.ar/api/3/action",
        # CABA
        "caba": "https://data.buenosaires.gob.ar/api/3/action",
        # Provincias
        "buenos_aires_prov": "https://catalogo.datos.gba.gob.ar/api/3/action",
        "cordoba_prov": "https://datosgestionabierta.cba.gov.ar/api/3/action",
        "santa_fe": "https://datos.santafe.gob.ar/api/3/action",
        "mendoza": "https://datosabiertos.mendoza.gov.ar/api/3/action",
        "entre_rios": "https://datos.entrerios.gov.ar/api/3/action",
        "neuquen_legislatura": "https://datos.legislaturaneuquen.gob.ar/api/3/action",
    }
    base_url = portal_urls.get(portal)
    if not base_url:
        logger.error(f"Unknown portal: {portal}")
        return {"error": f"Unknown portal: {portal}"}

    engine = _get_sync_engine()
    client = httpx.Client(timeout=60.0)

    try:
        # Get total count
        count_resp = client.get(f"{base_url}/package_search", params={"rows": 0})
        count_resp.raise_for_status()
        total = count_resp.json().get("result", {}).get("count", 0)
        logger.info(f"Portal {portal} has {total} packages")

        indexed = 0
        offset = 0

        while offset < total:
            resp = client.get(
                f"{base_url}/package_search",
                params={"rows": batch_size, "start": offset},
            )
            resp.raise_for_status()
            packages = resp.json().get("result", {}).get("results", [])

            for pkg in packages:
                for resource in pkg.get("resources", []):
                    fmt = resource.get("format", "").lower()
                    if fmt not in ("csv", "json", "xlsx", "xls"):
                        continue

                    source_id = resource.get("id", "")
                    title = pkg.get("title", "")
                    description = pkg.get("notes", "")
                    organization = pkg.get("organization", {}).get("title", "")
                    url = pkg.get("url", "")
                    download_url = resource.get("url", "")
                    tags = ",".join(t.get("name", "") for t in pkg.get("tags", []))

                    columns_list = []
                    if resource.get("attributesDescription"):
                        try:
                            attrs = json.loads(resource["attributesDescription"])
                            columns_list = list(attrs.keys()) if isinstance(attrs, dict) else []
                        except (json.JSONDecodeError, TypeError):
                            pass

                    columns_json = json.dumps(columns_list)

                    with engine.begin() as conn:
                        # Upsert
                        existing = conn.execute(
                            text(
                                "SELECT id FROM datasets WHERE source_id = :sid AND portal = :p"
                            ),
                            {"sid": source_id, "p": portal},
                        ).fetchone()

                        if existing:
                            conn.execute(
                                text("""
                                    UPDATE datasets SET
                                        title = :title, description = :desc,
                                        organization = :org, url = :url,
                                        download_url = :dl, format = :fmt,
                                        columns = :cols, tags = :tags,
                                        updated_at = :now
                                    WHERE id = :id
                                """),
                                {
                                    "title": title, "desc": description,
                                    "org": organization, "url": url,
                                    "dl": download_url, "fmt": fmt,
                                    "cols": columns_json, "tags": tags,
                                    "now": datetime.now(UTC), "id": existing[0],
                                },
                            )
                            dataset_id = str(existing[0])
                        else:
                            result = conn.execute(
                                text("""
                                    INSERT INTO datasets
                                        (source_id, title, description, organization,
                                         portal, url, download_url, format, columns, tags)
                                    VALUES
                                        (:sid, :title, :desc, :org, :portal, :url, :dl, :fmt, :cols, :tags)
                                    RETURNING id
                                """),
                                {
                                    "sid": source_id, "title": title,
                                    "desc": description, "org": organization,
                                    "portal": portal, "url": url,
                                    "dl": download_url, "fmt": fmt,
                                    "cols": columns_json, "tags": tags,
                                },
                            )
                            dataset_id = str(result.fetchone()[0])

                    # Dispatch embedding task
                    index_dataset_embedding.delay(dataset_id)
                    indexed += 1

            offset += batch_size
            logger.info(f"Scraped {offset}/{total} packages from {portal}")

        return {"portal": portal, "total_packages": total, "datasets_indexed": indexed}

    except Exception as exc:
        logger.exception(f"Scraper failed for {portal}")
        raise self.retry(exc=exc, countdown=60)
    finally:
        client.close()
        engine.dispose()


@celery_app.task(name="openarg.scrape_all_portals")
def scrape_all_portals():
    """Dispara scrape_catalog para todos los portales registrados."""
    portals = [
        "datos_gob_ar", "diputados", "justicia", "caba",
        "buenos_aires_prov", "cordoba_prov", "santa_fe",
        "mendoza", "entre_rios", "neuquen_legislatura",
    ]
    for portal in portals:
        scrape_catalog.delay(portal)
    logger.info(f"Dispatched scrape for {len(portals)} portals")
    return {"dispatched": portals}


def _portal_display_name(portal: str) -> str:
    """Nombre legible del portal para chunks."""
    names = {
        "datos_gob_ar": "Datos Abiertos Argentina (Nacional)",
        "diputados": "Cámara de Diputados de la Nación",
        "justicia": "Datos Abiertos de Justicia (incl. Oficina Anticorrupción)",
        "caba": "Buenos Aires Ciudad (CABA)",
        "buenos_aires_prov": "Buenos Aires Provincia",
        "cordoba_prov": "Córdoba Provincia",
        "santa_fe": "Santa Fe",
        "mendoza": "Mendoza",
        "entre_rios": "Entre Ríos",
        "neuquen_legislatura": "Legislatura de Neuquén",
    }
    return names.get(portal, portal)


def _get_data_statistics(engine, dataset_id: str) -> str | None:
    """
    Extrae estadísticas reales de los datos cacheados en la tabla cache_*.
    Retorna un texto resumen o None si no hay datos cacheados.
    """
    try:
        with engine.begin() as conn:
            cache_row = conn.execute(
                text(
                    "SELECT table_name, row_count, columns_json "
                    "FROM cached_datasets "
                    "WHERE dataset_id = CAST(:did AS uuid) AND status = 'ready'"
                ),
                {"did": dataset_id},
            ).fetchone()

        if not cache_row or not cache_row.table_name:
            return None

        table_name = cache_row.table_name
        # Validate table name to prevent injection
        import re
        if not re.match(r"^cache_[a-z0-9_]{1,100}$", table_name):
            return None

        cols_json = cache_row.columns_json or "[]"
        try:
            columns = json.loads(cols_json) if isinstance(cols_json, str) else cols_json
        except (json.JSONDecodeError, TypeError):
            columns = []

        if not columns:
            return None

        stats_parts = [
            f"Estadísticas de datos reales ({cache_row.row_count or '?'} registros):",
        ]

        with engine.begin() as conn:
            # Sample up to 5 columns for statistics
            sample_cols = columns[:5]
            for col_name in sample_cols:
                # Sanitize column name
                safe_col = col_name.replace('"', '""')
                try:
                    # Get column type and basic stats
                    info = conn.execute(
                        text(f'SELECT pg_typeof("{safe_col}") as dtype FROM "{table_name}" LIMIT 1'),
                    ).fetchone()
                    dtype = str(info[0]) if info else "unknown"

                    if dtype in ("integer", "bigint", "numeric", "double precision", "real"):
                        agg = conn.execute(
                            text(
                                f'SELECT MIN("{safe_col}"), MAX("{safe_col}"), '
                                f'ROUND(AVG("{safe_col}")::numeric, 2) '
                                f'FROM "{table_name}" WHERE "{safe_col}" IS NOT NULL'
                            ),
                        ).fetchone()
                        if agg and agg[0] is not None:
                            stats_parts.append(
                                f"- {col_name} (numérico): min={agg[0]}, max={agg[1]}, promedio={agg[2]}"
                            )
                    elif dtype in ("date", "timestamp without time zone", "timestamp with time zone"):
                        agg = conn.execute(
                            text(
                                f'SELECT MIN("{safe_col}"), MAX("{safe_col}") '
                                f'FROM "{table_name}" WHERE "{safe_col}" IS NOT NULL'
                            ),
                        ).fetchone()
                        if agg and agg[0] is not None:
                            stats_parts.append(
                                f"- {col_name} (fecha): desde {agg[0]} hasta {agg[1]}"
                            )
                    else:
                        # Text/categorical: count distinct + sample values
                        agg = conn.execute(
                            text(
                                f'SELECT COUNT(DISTINCT "{safe_col}") '
                                f'FROM "{table_name}" WHERE "{safe_col}" IS NOT NULL'
                            ),
                        ).fetchone()
                        n_distinct = agg[0] if agg else 0
                        samples = conn.execute(
                            text(
                                f'SELECT DISTINCT "{safe_col}" FROM "{table_name}" '
                                f'WHERE "{safe_col}" IS NOT NULL LIMIT 5'
                            ),
                        ).fetchall()
                        sample_vals = [str(s[0])[:50] for s in samples]
                        stats_parts.append(
                            f"- {col_name} (texto, {n_distinct} valores únicos): "
                            f"ej. {', '.join(sample_vals)}"
                        )
                except Exception:
                    continue

            # Show remaining column names if any
            if len(columns) > 5:
                remaining = columns[5:]
                stats_parts.append(f"Otras columnas: {', '.join(str(c) for c in remaining)}")

        return "\n".join(stats_parts) if len(stats_parts) > 1 else None

    except Exception:
        logger.debug("Could not extract data statistics for %s", dataset_id, exc_info=True)
        return None


@celery_app.task(name="openarg.index_dataset", bind=True, max_retries=3)
def index_dataset_embedding(self, dataset_id: str):
    """
    Genera múltiples chunks de embedding por dataset:
    1. Chunk principal: título + descripción + organización + tags + jurisdicción
    2. Chunk de columnas: lista de columnas con contexto semántico
    3. Chunk de contexto: uso potencial, preguntas que responde
    4. Chunk de estadísticas: datos reales (rangos, promedios, valores de ejemplo)

    Esto mejora la búsqueda vectorial al tener más puntos de entrada semánticos.
    """
    import google.generativeai as genai

    logger.info(f"Generating embeddings for dataset: {dataset_id}")
    engine = _get_sync_engine()

    try:
        with engine.begin() as conn:
            row = conn.execute(
                text(
                    "SELECT title, description, columns, tags, organization, portal, "
                    "format, download_url, is_cached, row_count "
                    "FROM datasets WHERE id = CAST(:id AS uuid)"
                ),
                {"id": dataset_id},
            ).fetchone()

        if not row:
            logger.warning(f"Dataset {dataset_id} not found")
            return

        portal_name = _portal_display_name(row.portal)

        # Build multiple chunks for richer semantic search
        chunks = []

        # Chunk 1: Main metadata (catches general queries)
        main_parts = [
            f"Dataset: {row.title}",
            f"Descripción: {row.description or 'Sin descripción'}",
            f"Organización: {row.organization or 'No especificada'}",
            f"Portal: {portal_name} ({row.portal})",
            f"Formato: {row.format or 'desconocido'}",
        ]
        if row.tags:
            main_parts.append(f"Temas: {row.tags}")
        if row.row_count:
            main_parts.append(f"Cantidad de registros: {row.row_count}")
        if row.download_url:
            main_parts.append(f"URL de descarga: {row.download_url}")
        chunks.append("\n".join(main_parts))

        # Chunk 2: Columns focus (catches column-specific queries)
        columns_str = row.columns or "[]"
        try:
            cols = json.loads(columns_str) if isinstance(columns_str, str) else columns_str
        except (json.JSONDecodeError, TypeError):
            cols = []

        if cols and len(cols) > 0:
            col_parts = [
                f"Dataset '{row.title}' de {portal_name} contiene las siguientes columnas/variables:",
                ", ".join(str(c) for c in cols),
                f"Publicado por: {row.organization or 'No especificada'}",
                f"Este dataset permite analizar: {row.description or row.title}",
            ]
            if row.row_count:
                col_parts.append(f"Total de registros disponibles: {row.row_count}")
            chunks.append("\n".join(col_parts))

        # Chunk 3: Contextual/use-case chunk (catches "how to" queries)
        context_parts = [
            f"Para consultar datos sobre {row.title.lower()} en Argentina,",
            f"existe el dataset '{row.title}' publicado por {row.organization or 'el gobierno'}.",
            f"Disponible en formato {row.format or 'CSV'} desde {portal_name}.",
        ]
        if row.description and len(row.description) > 20:
            context_parts.append(f"Detalle: {row.description[:500]}")
        if row.is_cached:
            context_parts.append(
                f"Los datos están cacheados localmente ({row.row_count or '?'} filas) "
                f"para consulta SQL directa."
            )
        if row.tags:
            context_parts.append(f"Palabras clave: {row.tags}")
        chunks.append("\n".join(context_parts))

        # Chunk 4: Data statistics (catches queries about specific values/ranges)
        data_stats = _get_data_statistics(engine, dataset_id)
        if data_stats:
            stats_chunk_parts = [
                f"Datos reales del dataset '{row.title}' ({portal_name}):",
                data_stats,
            ]
            if cols:
                stats_chunk_parts.append(f"Columnas: {', '.join(str(c) for c in cols[:10])}")
            chunks.append("\n".join(stats_chunk_parts))

        # Generate embeddings in batch
        genai.configure(api_key=os.getenv("GEMINI_API_KEY", ""))
        resp = genai.embed_content(
            model="models/gemini-embedding-001",
            content=chunks,
            output_dimensionality=768,
        )
        embeddings = resp["embedding"]

        with engine.begin() as conn:
            # Delete old chunks
            conn.execute(
                text("DELETE FROM dataset_chunks WHERE dataset_id = :did"),
                {"did": dataset_id},
            )
            # Insert all chunks
            for i, emb in enumerate(embeddings):
                embedding_str = "[" + ",".join(str(v) for v in emb) + "]"
                conn.execute(
                    text("""
                        INSERT INTO dataset_chunks (dataset_id, content, embedding)
                        VALUES (:did, :content, CAST(:embedding AS vector))
                    """),
                    {"did": dataset_id, "content": chunks[i], "embedding": embedding_str},
                )

        logger.info(f"Indexed {len(chunks)} chunks for dataset: {dataset_id}")
        return {"dataset_id": dataset_id, "chunks_created": len(chunks)}

    except Exception as exc:
        logger.exception(f"Embedding failed for dataset {dataset_id}")
        raise self.retry(exc=exc, countdown=30)
    finally:
        engine.dispose()
