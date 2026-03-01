"""
Embedding Worker — Re-indexación masiva.

Permite re-generar embeddings de todos los datasets o de un portal específico.
También indexa chunks de sesiones parlamentarias en pgvector.
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path

from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine
from app.infrastructure.celery.tasks.scraper_tasks import index_dataset_embedding

logger = logging.getLogger(__name__)

_CHUNKS_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "chunks"


@celery_app.task(name="openarg.reindex_all_embeddings", bind=True)
def reindex_all_embeddings(self, portal: str | None = None):
    """Re-genera embeddings para todos los datasets (o filtrado por portal)."""
    engine = get_sync_engine()

    try:
        with engine.begin() as conn:
            if portal:
                rows = conn.execute(
                    text("SELECT CAST(id AS text) FROM datasets WHERE portal = :portal LIMIT 10000"),
                    {"portal": portal},
                ).fetchall()
            else:
                rows = conn.execute(text("SELECT CAST(id AS text) FROM datasets LIMIT 10000")).fetchall()

        dataset_ids = [row[0] for row in rows]
        logger.info(f"Reindexing {len(dataset_ids)} datasets")

        for did in dataset_ids:
            index_dataset_embedding.delay(did)

        return {"dispatched": len(dataset_ids), "portal": portal or "all"}

    finally:
        engine.dispose()


@celery_app.task(name="openarg.index_sesiones", bind=True, max_retries=2, soft_time_limit=1800, time_limit=2100)
def index_sesiones_chunks(self, batch_size: int = 50):
    """
    Load congressional session chunks from JSON files, generate embeddings,
    and index them in the sesion_chunks pgvector table.

    Processes in batches to avoid overwhelming the embedding API.
    """
    import google.generativeai as genai

    engine = get_sync_engine()
    gemini_key = os.getenv("GEMINI_API_KEY", "")
    if not gemini_key:
        logger.error("GEMINI_API_KEY not set, cannot index sesiones")
        return {"error": "GEMINI_API_KEY not set"}

    genai.configure(api_key=gemini_key)

    try:
        # Check if already indexed
        with engine.begin() as conn:
            count = conn.execute(text("SELECT COUNT(*) FROM sesion_chunks")).scalar()
            if count and count > 0:
                logger.info("sesion_chunks already has %d rows, skipping indexing", count)
                return {"status": "already_indexed", "count": count}

        # Load all chunks from JSON files
        all_chunks: list[dict] = []
        if not _CHUNKS_DIR.exists():
            logger.warning("Chunks directory not found: %s", _CHUNKS_DIR)
            return {"error": "chunks directory not found"}

        for f in sorted(_CHUNKS_DIR.glob("*.json")):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                if isinstance(data, list):
                    all_chunks.extend(data)
            except Exception:
                logger.warning("Bad chunk file: %s", f)

        logger.info("Loaded %d sesiones chunks from %s", len(all_chunks), _CHUNKS_DIR)
        if not all_chunks:
            return {"status": "no_chunks"}

        indexed = 0
        for i in range(0, len(all_chunks), batch_size):
            batch = all_chunks[i : i + batch_size]

            # Build text for embedding (speaker context + text)
            texts = []
            for chunk in batch:
                speaker = chunk.get("speaker") or ""
                chunk_text = chunk.get("text", "")
                # Include speaker in embedding text for better semantic matching
                if speaker:
                    texts.append(f"Orador: {speaker}\n{chunk_text[:2000]}")
                else:
                    texts.append(chunk_text[:2000])

            # Generate embeddings in batch
            resp = genai.embed_content(
                model="models/gemini-embedding-001",
                content=texts,
                output_dimensionality=768,
            )
            embeddings = resp["embedding"]

            # Insert into DB
            with engine.begin() as conn:
                for j, emb in enumerate(embeddings):
                    chunk = batch[j]
                    embedding_str = "[" + ",".join(str(v) for v in emb) + "]"
                    conn.execute(
                        text("""
                            INSERT INTO sesion_chunks
                                (periodo, reunion, fecha, tipo_sesion, pdf_url,
                                 total_pages, speaker, chunk_index, content, embedding)
                            VALUES
                                (:periodo, :reunion, :fecha, :tipo_sesion, :pdf_url,
                                 :total_pages, :speaker, :chunk_index, :content,
                                 CAST(:embedding AS vector))
                        """),
                        {
                            "periodo": chunk.get("periodo"),
                            "reunion": chunk.get("reunion"),
                            "fecha": chunk.get("fecha", ""),
                            "tipo_sesion": chunk.get("tipoSesion", ""),
                            "pdf_url": chunk.get("pdfUrl", ""),
                            "total_pages": chunk.get("totalPages"),
                            "speaker": chunk.get("speaker"),
                            "chunk_index": chunk.get("chunkIndex"),
                            "content": chunk.get("text", ""),
                            "embedding": embedding_str,
                        },
                    )

            indexed += len(batch)
            logger.info("Indexed %d/%d sesiones chunks", indexed, len(all_chunks))

        return {"status": "indexed", "total_chunks": indexed}

    except SoftTimeLimitExceeded:
        logger.error("Sesiones indexing timed out")
        raise
    except Exception as exc:
        logger.exception("Sesiones indexing failed")
        raise self.retry(exc=exc, countdown=60)
    finally:
        engine.dispose()
