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
        "datos_gob_ar": "https://datos.gob.ar/api/3/action",
        "caba": "https://data.buenosaires.gob.ar/api/3/action",
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


@celery_app.task(name="openarg.index_dataset", bind=True, max_retries=3)
def index_dataset_embedding(self, dataset_id: str):
    """
    Genera múltiples chunks de embedding por dataset:
    1. Chunk principal: título + descripción + organización + tags
    2. Chunk de columnas: lista de columnas con contexto semántico
    3. Chunk de contexto: uso potencial, preguntas que responde

    Esto mejora la búsqueda vectorial al tener más puntos de entrada semánticos.
    """
    import openai

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

        # Build multiple chunks for richer semantic search
        chunks = []

        # Chunk 1: Main metadata (catches general queries)
        main_parts = [
            f"Dataset: {row.title}",
            f"Descripción: {row.description or 'Sin descripción'}",
            f"Organización: {row.organization or 'No especificada'}",
            f"Portal: {row.portal}",
            f"Formato: {row.format or 'desconocido'}",
        ]
        if row.tags:
            main_parts.append(f"Temas: {row.tags}")
        if row.row_count:
            main_parts.append(f"Cantidad de registros: {row.row_count}")
        chunks.append("\n".join(main_parts))

        # Chunk 2: Columns focus (catches column-specific queries)
        columns_str = row.columns or "[]"
        try:
            cols = json.loads(columns_str) if isinstance(columns_str, str) else columns_str
        except (json.JSONDecodeError, TypeError):
            cols = []

        if cols and len(cols) > 0:
            col_parts = [
                f"Dataset '{row.title}' contiene las siguientes columnas/variables:",
                ", ".join(str(c) for c in cols),
                f"Organización: {row.organization or 'No especificada'}",
                f"Este dataset permite analizar: {row.description or row.title}",
            ]
            chunks.append("\n".join(col_parts))

        # Chunk 3: Contextual/use-case chunk (catches "how to" queries)
        context_parts = [
            f"Para consultar datos sobre {row.title.lower()} en Argentina,",
            f"existe el dataset '{row.title}' publicado por {row.organization or 'el gobierno'}.",
            f"Disponible en formato {row.format or 'CSV'} desde el portal {row.portal}.",
        ]
        if row.description and len(row.description) > 20:
            context_parts.append(f"Detalle: {row.description[:300]}")
        if row.is_cached:
            context_parts.append(f"Los datos están cacheados localmente ({row.row_count or '?'} filas) para consulta SQL directa.")
        chunks.append("\n".join(context_parts))

        # Generate embeddings in batch
        api_key = os.getenv("OPENAI_API_KEY", "")
        client = openai.OpenAI(api_key=api_key)
        resp = client.embeddings.create(
            input=chunks,
            model="text-embedding-3-small",
            dimensions=1536,
        )

        with engine.begin() as conn:
            # Delete old chunks
            conn.execute(
                text("DELETE FROM dataset_chunks WHERE dataset_id = :did"),
                {"did": dataset_id},
            )
            # Insert all chunks
            for i, emb_data in enumerate(resp.data):
                embedding_str = "[" + ",".join(str(v) for v in emb_data.embedding) + "]"
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
