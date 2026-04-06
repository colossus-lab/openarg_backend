from __future__ import annotations

import heapq
import json
import logging
import re
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.entities.connectors.data_result import DataResult  # type: ignore[import-not-found]
from app.domain.exceptions.connector_errors import ConnectorError  # type: ignore[import-not-found]
from app.domain.exceptions.error_codes import ErrorCode  # type: ignore[import-not-found]
from app.domain.ports.connectors.sesiones import (
    ISesionesConnector,  # type: ignore[import-not-found]
)

logger = logging.getLogger(__name__)

_CHUNKS_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "chunks"
_TERM_RE = re.compile(r"\w+")


def _extract_terms(text: str) -> list[str]:
    return [term for term in _TERM_RE.findall(text.lower()) if len(term) > 2]


class SesionesAdapter(ISesionesConnector):  # type: ignore[misc]
    """Congressional session search with pgvector (primary) + local keyword fallback."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gemini_api_key: str = "",
    ) -> None:
        self._session_factory = session_factory
        self._gemini_api_key = gemini_api_key
        self._chunks: list[dict[str, Any]] = []
        self._chunk_term_counts: list[Counter[str]] = []
        self._speaker_lowers: list[str] = []
        self._term_index: dict[str, set[int]] = defaultdict(set)
        self._period_index: dict[int, list[int]] = defaultdict(list)
        self._loaded = False

    async def _generate_embedding(self, text_input: str) -> list[float] | None:
        """Generate query embedding using Bedrock Cohere."""
        try:
            import asyncio
            import json as _json
            import os

            import boto3

            def _call_bedrock() -> list[float]:
                bedrock = boto3.client(
                    "bedrock-runtime",
                    region_name=os.getenv("AWS_REGION", "us-east-1"),
                )
                resp = bedrock.invoke_model(
                    modelId=os.getenv("BEDROCK_EMBEDDING_MODEL", "cohere.embed-multilingual-v3"),
                    body=_json.dumps(
                        {
                            "texts": [text_input[:2000]],
                            "input_type": "search_query",
                            "truncate": "END",
                        }
                    ),
                )
                result = _json.loads(resp["body"].read())
                return result["embeddings"][0]

            return await asyncio.to_thread(_call_bedrock)
        except Exception:
            logger.error(
                "Failed to generate Bedrock embedding for sesiones — falling back to keyword search",
                exc_info=True,
            )
            return None

    async def _search_pgvector(
        self,
        query: str,
        periodo: int | None = None,
        orador: str | None = None,
        limit: int = 15,
    ) -> list[dict[str, Any]] | None:
        """Search sesion_chunks using pgvector cosine similarity."""
        embedding = await self._generate_embedding(query)
        if not embedding:
            return None

        try:
            embedding_str = "[" + ",".join(str(v) for v in embedding) + "]"

            params: dict[str, Any] = {"embedding": embedding_str, "limit": limit}
            if periodo is not None:
                params["periodo"] = periodo
                sql = text(
                    "SELECT periodo, reunion, fecha, tipo_sesion, pdf_url,"
                    " total_pages, speaker, content,"
                    " 1 - (embedding <=> CAST(:embedding AS vector)) AS score"
                    " FROM sesion_chunks"
                    " WHERE periodo = :periodo"
                    " ORDER BY embedding <=> CAST(:embedding AS vector)"
                    " LIMIT :limit"
                )
            else:
                sql = text(
                    "SELECT periodo, reunion, fecha, tipo_sesion, pdf_url,"
                    " total_pages, speaker, content,"
                    " 1 - (embedding <=> CAST(:embedding AS vector)) AS score"
                    " FROM sesion_chunks"
                    " ORDER BY embedding <=> CAST(:embedding AS vector)"
                    " LIMIT :limit"
                )

            async with self._session_factory() as session:
                result = await session.execute(sql, params)
                rows = result.fetchall()

            if not rows:
                return None

            chunks = []
            for row in rows:
                chunks.append(
                    {
                        "periodo": row.periodo,
                        "reunion": row.reunion,
                        "fecha": row.fecha or "",
                        "tipoSesion": row.tipo_sesion or "",
                        "pdfUrl": row.pdf_url or "",
                        "totalPages": row.total_pages or 0,
                        "speaker": row.speaker,
                        "text": row.content or "",
                        "score": float(row.score),
                    }
                )

            # Post-filter by orador if specified
            if orador:
                orador_lower = orador.lower()
                chunks = [
                    c for c in chunks if c.get("speaker") and orador_lower in c["speaker"].lower()
                ]

            logger.info("pgvector sesiones returned %d results for '%s'", len(chunks), query[:60])
            return chunks if chunks else None
        except Exception:
            logger.error(
                "pgvector sesiones search failed — falling back to local keyword search",
                exc_info=True,
            )
            return None

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        try:
            if not _CHUNKS_DIR.exists():
                logger.error("Sesiones chunks directory not found: %s", _CHUNKS_DIR)
                # Don't set _loaded = True — allow retry if the directory appears later
                return

            files = sorted(_CHUNKS_DIR.glob("*.json"))
            logger.info("Loading %d sesiones chunk files from %s", len(files), _CHUNKS_DIR)
            bad_files = 0
            for f in files:
                try:
                    data = json.loads(f.read_text(encoding="utf-8"))
                    if isinstance(data, list):
                        for chunk in data:
                            if not isinstance(chunk, dict):
                                continue
                            idx = len(self._chunks)
                            self._chunks.append(chunk)

                            text_lower = str(chunk.get("text", "")).lower()
                            speaker_lower = str(chunk.get("speaker") or "").lower()
                            self._speaker_lowers.append(speaker_lower)

                            term_counts = Counter(_extract_terms(text_lower))
                            self._chunk_term_counts.append(term_counts)
                            for term in term_counts:
                                self._term_index[term].add(idx)

                            periodo = chunk.get("periodo")
                            if isinstance(periodo, int):
                                self._period_index[periodo].append(idx)
                except Exception:
                    bad_files += 1
                    logger.warning("Bad sesiones chunk file: %s", f)

            if bad_files:
                logger.warning("Skipped %d bad chunk files out of %d", bad_files, len(files))
            logger.info("Loaded %d sesiones chunks", len(self._chunks))
            self._loaded = True
        except Exception:
            logger.error("Failed to load sesiones chunks", exc_info=True)
            # Don't set _loaded = True — allow retry on next call

    def _search_local(
        self,
        query: str,
        periodo: int | None = None,
        orador: str | None = None,
        limit: int = 15,
    ) -> list[dict[str, Any]] | None:
        """Local keyword search fallback."""
        self._ensure_loaded()
        if not self._chunks:
            return None

        query_terms = _extract_terms(query)
        if not query_terms:
            return None

        candidate_ids: set[int] = set()
        for term in query_terms:
            candidate_ids.update(self._term_index.get(term, ()))

        if not candidate_ids:
            return None

        if periodo is not None:
            candidate_ids.intersection_update(self._period_index.get(periodo, ()))
            if not candidate_ids:
                return None

        orador_lower = orador.lower() if orador else None

        scored: list[tuple[float, int]] = []
        for idx in candidate_ids:
            speaker_lower = self._speaker_lowers[idx]
            term_counts = self._chunk_term_counts[idx]

            score = 0.0
            for term in query_terms:
                score += term_counts.get(term, 0) * 2
                if term in speaker_lower:
                    score += 10

            if orador_lower:
                if orador_lower in speaker_lower:
                    score += 20
                else:
                    score *= 0.1

            if score > 0:
                scored.append((score, idx))

        top_chunks = [self._chunks[idx] for _, idx in heapq.nlargest(limit, scored)]
        return top_chunks if top_chunks else None

    def _chunks_to_data_result(self, query: str, chunks: list[dict[str, Any]]) -> DataResult:
        """Convert chunks to DataResult."""
        records = [
            {
                "periodo": c.get("periodo"),
                "reunion": c.get("reunion"),
                "fecha": c.get("fecha", ""),
                "tipo_sesion": c.get("tipoSesion", ""),
                "orador": c.get("speaker") or "No identificado",
                "texto": c.get("text", ""),
                "paginas_totales": c.get("totalPages", 0),
                "pdf": c.get("pdfUrl", ""),
            }
            for c in chunks
        ]

        unique_sessions = {f"P{c.get('periodo')}-R{c.get('reunion')}" for c in chunks}
        unique_speakers = {c.get("speaker") for c in chunks if c.get("speaker")}

        return DataResult(
            source="sesiones:diputados",
            portal_name="Diario de Sesiones \u2014 C\u00e1mara de Diputados",
            portal_url="https://www.diputados.gov.ar/sesiones/",
            dataset_title=f'Transcripciones parlamentarias: "{query}"',
            format="json",
            records=records,
            metadata={
                "total_records": len(records),
                "fetched_at": datetime.now(UTC).isoformat(),
                "description": (
                    f"Se encontraron {len(records)} fragmentos relevantes en "
                    f"{len(unique_sessions)} sesi\u00f3n(es), con intervenciones de "
                    f"{len(unique_speakers)} orador(es)."
                ),
            },
        )

    async def search(
        self,
        query: str,
        periodo: int | None = None,
        orador: str | None = None,
        limit: int = 15,
    ) -> DataResult | None:
        try:
            # Try pgvector search first
            chunks = await self._search_pgvector(query, periodo, orador, limit)
            if chunks:
                return self._chunks_to_data_result(query, chunks)
            logger.info("pgvector returned no results, falling back to local keyword search")

            # Fallback to local keyword search
            chunks = self._search_local(query, periodo, orador, limit)
            if chunks:
                return self._chunks_to_data_result(query, chunks)

            return None
        except ConnectorError:
            raise
        except Exception as exc:
            raise ConnectorError(
                error_code=ErrorCode.CN_SESIONES_NO_RESULTS,
                details={"query": query[:100], "reason": str(exc)},
            ) from exc
