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
    """Congressional session search with Firestore vector search (primary) + local keyword fallback."""

    def __init__(
        self,
        gemini_api_key: str = "",
        firebase_project_id: str = "",
        firebase_database_id: str = "(default)",
        firebase_client_email: str = "",
        firebase_private_key: str = "",
    ) -> None:
        self._chunks: list[dict] = []
        self._loaded = False
        self._gemini_api_key = gemini_api_key
        self._firebase_project_id = firebase_project_id
        self._firebase_database_id = firebase_database_id
        self._firebase_client_email = firebase_client_email
        self._firebase_private_key = firebase_private_key
        self._firestore_db = None
        self._firestore_initialized = False

    @property
    def _firestore_available(self) -> bool:
        return bool(
            self._firebase_project_id
            and self._firebase_client_email
            and self._firebase_private_key
            and self._gemini_api_key
        )

    def _get_firestore_db(self):  # noqa: ANN202
        """Lazy-init Firestore client (singleton)."""
        if self._firestore_initialized:
            return self._firestore_db
        self._firestore_initialized = True

        if not self._firestore_available:
            return None

        try:
            import firebase_admin
            from firebase_admin import credentials, firestore

            # Check if already initialized
            try:
                app = firebase_admin.get_app("openarg_sesiones")
            except ValueError:
                cred = credentials.Certificate({
                    "type": "service_account",
                    "project_id": self._firebase_project_id,
                    "client_email": self._firebase_client_email,
                    "private_key": self._firebase_private_key,
                    "token_uri": "https://oauth2.googleapis.com/token",
                })
                app = firebase_admin.initialize_app(cred, name="openarg_sesiones")

            self._firestore_db = firestore.client(app, database_id=self._firebase_database_id)
            logger.info("Firestore initialized for sesiones (project=%s)", self._firebase_project_id)
            return self._firestore_db
        except Exception:
            logger.warning("Failed to initialize Firestore for sesiones", exc_info=True)
            return None

    async def _generate_embedding(self, text: str) -> list[float] | None:
        """Generate query embedding using Gemini embedding model."""
        try:
            import google.generativeai as genai

            genai.configure(api_key=self._gemini_api_key)
            result = genai.embed_content(
                model="models/gemini-embedding-001",
                content=text[:2000],
            )
            # Truncate to 2048 dims to match stored Firestore vectors
            return result["embedding"][:2048]
        except Exception:
            logger.warning("Failed to generate Gemini embedding for sesiones", exc_info=True)
            return None

    async def _search_firestore(
        self,
        query: str,
        periodo: int | None = None,
        orador: str | None = None,
        limit: int = 15,
    ) -> list[dict] | None:
        """Search Firestore using vector similarity (cosine)."""
        db = self._get_firestore_db()
        if not db:
            return None

        embedding = await self._generate_embedding(query)
        if not embedding:
            return None

        try:
            from google.cloud.firestore_v1.vector import Vector
            from google.cloud.firestore_v1.base_vector_query import DistanceMeasure

            collection = db.collection("sesiones_chunks")

            # Apply periodo pre-filter if specified
            base_query = collection
            if periodo is not None:
                base_query = base_query.where("periodo", "==", periodo)

            vector_query = base_query.find_nearest(
                vector_field="embedding",
                query_vector=Vector(embedding),
                distance_measure=DistanceMeasure.COSINE,
                limit=limit,
            )

            docs = vector_query.get()

            chunks = []
            for doc in docs:
                data = doc.to_dict()
                chunks.append({
                    "periodo": data.get("periodo"),
                    "reunion": data.get("reunion"),
                    "fecha": data.get("fecha", ""),
                    "tipoSesion": data.get("tipoSesion", ""),
                    "pdfUrl": data.get("pdfUrl", ""),
                    "totalPages": data.get("totalPages", 0),
                    "speaker": data.get("speaker"),
                    "text": data.get("text", ""),
                })

            if not chunks:
                return None

            # Post-filter by orador
            if orador:
                orador_lower = orador.lower()
                chunks = [c for c in chunks if c.get("speaker") and orador_lower in c["speaker"].lower()]

            logger.info("Firestore sesiones returned %d results for '%s'", len(chunks), query[:60])
            return chunks if chunks else None
        except Exception:
            logger.warning("Firestore sesiones search failed", exc_info=True)
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

    async def search(
        self,
        query: str,
        periodo: int | None = None,
        orador: str | None = None,
        limit: int = 15,
    ) -> DataResult | None:
        # Try Firestore vector search first
        if self._firestore_available:
            chunks = await self._search_firestore(query, periodo, orador, limit)
            if chunks:
                return self._chunks_to_data_result(query, chunks)
            logger.info("Firestore returned no results, falling back to local search")

        # Fallback to local keyword search
        chunks = self._search_local(query, periodo, orador, limit)
        if chunks:
            return self._chunks_to_data_result(query, chunks)

        return None
