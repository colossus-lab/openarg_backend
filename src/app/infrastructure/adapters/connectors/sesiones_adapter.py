from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.entities.connectors.data_result import DataResult
from app.domain.exceptions.connector_errors import ConnectorError
from app.domain.exceptions.error_codes import ErrorCode
from app.domain.ports.connectors.sesiones import ISesionesConnector

logger = logging.getLogger(__name__)

_CHUNKS_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "chunks"


class SesionesAdapter(ISesionesConnector):
    """Congressional session search with pgvector (primary) + local keyword fallback."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gemini_api_key: str = "",
    ) -> None:
        self._session_factory = session_factory
        self._gemini_api_key = gemini_api_key
        self._chunks: list[dict] = []
        self._loaded = False

    async def _generate_embedding(self, text_input: str) -> list[float] | None:
        """Generate query embedding using Gemini text-embedding-004."""
        if not self._gemini_api_key:
            return None
        try:
            import asyncio

            import google.generativeai as genai

            genai.configure(api_key=self._gemini_api_key)
            result = await asyncio.to_thread(
                genai.embed_content,
                model="models/gemini-embedding-001",
                content=text_input[:2000],
                output_dimensionality=768,
            )
            return result["embedding"]
        except Exception:
            logger.warning("Failed to generate Gemini embedding for sesiones", exc_info=True)
            return None

    async def _search_pgvector(
        self,
        query: str,
        periodo: int | None = None,
        orador: str | None = None,
        limit: int = 15,
    ) -> list[dict] | None:
        """Search sesion_chunks using pgvector cosine similarity."""
        embedding = await self._generate_embedding(query)
        if not embedding:
            return None

        try:
            embedding_str = "[" + ",".join(str(v) for v in embedding) + "]"

            periodo_clause = ""
            params: dict = {"embedding": embedding_str, "limit": limit}
            if periodo is not None:
                periodo_clause = "AND periodo = :periodo"
                params["periodo"] = periodo

            sql = text(f"""
                SELECT
                    periodo, reunion, fecha, tipo_sesion, pdf_url,
                    total_pages, speaker, content,
                    1 - (embedding <=> CAST(:embedding AS vector)) AS score
                FROM sesion_chunks
                WHERE 1=1 {periodo_clause}
                ORDER BY embedding <=> CAST(:embedding AS vector)
                LIMIT :limit
            """)

            async with self._session_factory() as session:
                result = await session.execute(sql, params)
                rows = result.fetchall()

            if not rows:
                return None

            chunks = []
            for row in rows:
                chunks.append({
                    "periodo": row.periodo,
                    "reunion": row.reunion,
                    "fecha": row.fecha or "",
                    "tipoSesion": row.tipo_sesion or "",
                    "pdfUrl": row.pdf_url or "",
                    "totalPages": row.total_pages or 0,
                    "speaker": row.speaker,
                    "text": row.content or "",
                    "score": float(row.score),
                })

            # Post-filter by orador if specified
            if orador:
                orador_lower = orador.lower()
                chunks = [c for c in chunks if c.get("speaker") and orador_lower in c["speaker"].lower()]

            logger.info("pgvector sesiones returned %d results for '%s'", len(chunks), query[:60])
            return chunks if chunks else None
        except Exception:
            logger.warning("pgvector sesiones search failed", exc_info=True)
            return None

    def _ensure_loaded(self) -> None:
        if self._loaded:
            return
        try:
            if not _CHUNKS_DIR.exists():
                logger.warning("Sesiones chunks directory not found: %s", _CHUNKS_DIR)
                self._loaded = True
                return

            files = sorted(_CHUNKS_DIR.glob("*.json"))
            logger.info("Loading %d sesiones chunk files from %s", len(files), _CHUNKS_DIR)
            for f in files:
                try:
                    data = json.loads(f.read_text(encoding="utf-8"))
                    if isinstance(data, list):
                        self._chunks.extend(data)
                except Exception:
                    logger.debug("Bad chunk file: %s", f)

            logger.info("Loaded %d sesiones chunks", len(self._chunks))
        except Exception:
            logger.warning("Failed to load sesiones chunks", exc_info=True)
        self._loaded = True

    def _search_local(
        self,
        query: str,
        periodo: int | None = None,
        orador: str | None = None,
        limit: int = 15,
    ) -> list[dict] | None:
        """Local keyword search fallback."""
        self._ensure_loaded()
        if not self._chunks:
            return None

        query_terms = [t for t in query.lower().split() if len(t) > 2]
        if not query_terms:
            return None

        filtered = self._chunks
        if periodo is not None:
            filtered = [c for c in filtered if c.get("periodo") == periodo]

        scored: list[tuple[dict, float]] = []
        for chunk in filtered:
            text_lower = chunk.get("text", "").lower()
            speaker_lower = (chunk.get("speaker") or "").lower()

            score = 0.0
            for term in query_terms:
                count = len(re.findall(re.escape(term), text_lower))
                score += count * 2
                if term in speaker_lower:
                    score += 10

            if orador:
                orador_lower = orador.lower()
                if orador_lower in speaker_lower:
                    score += 20
                else:
                    score *= 0.1

            if score > 0:
                scored.append((chunk, score))

        scored.sort(key=lambda x: x[1], reverse=True)
        top_chunks = [c for c, _ in scored[:limit]]
        return top_chunks if top_chunks else None

    def _chunks_to_data_result(self, query: str, chunks: list[dict]) -> DataResult:
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
