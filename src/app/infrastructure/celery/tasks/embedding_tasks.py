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
from typing import Any

from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import text

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine
from app.infrastructure.celery.tasks.scraper_tasks import index_dataset_embedding

logger = logging.getLogger(__name__)

_CHUNKS_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "chunks"


def _sesion_chunk_key(chunk: dict[str, Any]) -> tuple[str, int]:
    """Build a stable identity for a session chunk."""
    return (
        str(chunk.get("pdfUrl") or ""),
        int(chunk.get("chunkIndex") or 0),
    )


@celery_app.task(name="openarg.reindex_all_embeddings", bind=True)  # type: ignore[misc]
def reindex_all_embeddings(self: Any, portal: str | None = None) -> dict[str, Any]:
    """Re-genera embeddings para todos los datasets (o filtrado por portal)."""
    engine = get_sync_engine()

    try:
        with engine.begin() as conn:
            if portal:
                rows = conn.execute(
                    text(
                        "SELECT CAST(id AS text) FROM datasets WHERE portal = :portal LIMIT 10000"
                    ),
                    {"portal": portal},
                ).fetchall()
            else:
                rows = conn.execute(
                    text("SELECT CAST(id AS text) FROM datasets LIMIT 10000")
                ).fetchall()

        dataset_ids = [row[0] for row in rows]
        logger.info(f"Reindexing {len(dataset_ids)} datasets")

        for did in dataset_ids:
            index_dataset_embedding.delay(did)

        return {"dispatched": len(dataset_ids), "portal": portal or "all"}

    finally:
        engine.dispose()


@celery_app.task(
    name="openarg.index_sesiones", bind=True, max_retries=2, soft_time_limit=1800, time_limit=2100
)  # type: ignore[misc]
def index_sesiones_chunks(self: Any, batch_size: int = 50) -> dict[str, Any]:
    """
    Load congressional session chunks from JSON files, generate embeddings,
    and index them in the sesion_chunks pgvector table.

    Processes in batches to avoid overwhelming the embedding API.
    """
    import json as _json

    engine = get_sync_engine()

    try:
        # Load existing chunk identities for incremental indexing
        with engine.begin() as conn:
            existing_rows = conn.execute(
                text("SELECT COALESCE(pdf_url, ''), COALESCE(chunk_index, 0) FROM sesion_chunks")
            ).fetchall()
            existing_keys = {(str(row[0]), int(row[1])) for row in existing_rows}

        # Load all chunks from JSON files
        all_chunks: list[dict[str, Any]] = []
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

        pending_chunks = [chunk for chunk in all_chunks if _sesion_chunk_key(chunk) not in existing_keys]
        skipped = len(all_chunks) - len(pending_chunks)
        if not pending_chunks:
            logger.info(
                "sesion_chunks already up to date (%d existing rows, %d source chunks)",
                len(existing_keys),
                len(all_chunks),
            )
            return {"status": "already_indexed", "count": len(existing_keys), "skipped": skipped}

        import boto3

        bedrock = boto3.client(
            "bedrock-runtime",
            region_name=os.getenv("AWS_REGION", "us-east-1"),
        )

        indexed = 0
        for i in range(0, len(pending_chunks), batch_size):
            batch = pending_chunks[i : i + batch_size]

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

            # Generate embeddings in batch via Bedrock Cohere
            _resp = bedrock.invoke_model(
                modelId=os.getenv("BEDROCK_EMBEDDING_MODEL", "cohere.embed-multilingual-v3"),
                body=_json.dumps(
                    {"texts": texts, "input_type": "search_document", "truncate": "END"}
                ),
            )
            _result = _json.loads(_resp["body"].read())
            embeddings: list[list[float]] = _result["embeddings"]

            # Insert into DB
            with engine.begin() as conn:
                rows = []
                for chunk, emb in zip(batch, embeddings, strict=False):
                    rows.append(
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
                            "embedding": "[" + ",".join(str(v) for v in emb) + "]",
                        }
                    )
                if rows:
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
                        rows,
                    )

            indexed += len(batch)
            logger.info("Indexed %d/%d new sesiones chunks", indexed, len(pending_chunks))

        return {
            "status": "indexed",
            "total_chunks": indexed,
            "skipped": skipped,
            "existing_before": len(existing_keys),
        }

    except SoftTimeLimitExceeded:
        logger.error("Sesiones indexing timed out")
        raise
    except Exception as exc:
        logger.exception("Sesiones indexing failed")
        raise self.retry(exc=exc, countdown=60)
    finally:
        engine.dispose()
