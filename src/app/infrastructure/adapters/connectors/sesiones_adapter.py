from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime
from pathlib import Path

from app.domain.entities.connectors.data_result import DataResult
from app.domain.ports.connectors.sesiones import ISesionesConnector

logger = logging.getLogger(__name__)

_CHUNKS_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "chunks"


class SesionesAdapter(ISesionesConnector):
    """Local keyword-based search over congressional session chunks."""

    def __init__(self) -> None:
        self._chunks: list[dict] = []
        self._loaded = False

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

    async def search(
        self,
        query: str,
        periodo: int | None = None,
        orador: str | None = None,
        limit: int = 15,
    ) -> DataResult | None:
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

        if not top_chunks:
            return None

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
            for c in top_chunks
        ]

        unique_sessions = {f"P{c.get('periodo')}-R{c.get('reunion')}" for c in top_chunks}
        unique_speakers = {c.get("speaker") for c in top_chunks if c.get("speaker")}

        return DataResult(
            source="sesiones:diputados",
            portal_name="Diario de Sesiones — Cámara de Diputados",
            portal_url="https://www.diputados.gov.ar/sesiones/",
            dataset_title=f'Transcripciones parlamentarias: "{query}"',
            format="json",
            records=records,
            metadata={
                "total_records": len(records),
                "fetched_at": datetime.now(UTC).isoformat(),
                "description": (
                    f"Se encontraron {len(records)} fragmentos relevantes en "
                    f"{len(unique_sessions)} sesión(es), con intervenciones de "
                    f"{len(unique_speakers)} orador(es)."
                ),
            },
        )
