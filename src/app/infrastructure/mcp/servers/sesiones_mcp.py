"""Standalone MCP Server for congressional session transcripts."""
from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path

from app.infrastructure.mcp.base_server import MCPServer

logger = logging.getLogger(__name__)

_CHUNKS_DIR = Path(os.getenv(
    "SESIONES_CHUNKS_DIR",
    str(Path(__file__).resolve().parent.parent.parent / "data" / "chunks"),
))

_chunks: list[dict] = []
_loaded = False


def _ensure_loaded() -> None:
    global _chunks, _loaded
    if _loaded:
        return
    try:
        if not _CHUNKS_DIR.exists():
            logger.warning("Sesiones chunks directory not found: %s", _CHUNKS_DIR)
            _loaded = True
            return
        files = sorted(_CHUNKS_DIR.glob("*.json"))
        logger.info("Loading %d sesiones chunk files from %s", len(files), _CHUNKS_DIR)
        for f in files:
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
                if isinstance(data, list):
                    _chunks.extend(data)
            except Exception:
                logger.debug("Bad chunk file: %s", f)
        logger.info("Loaded %d sesiones chunks", len(_chunks))
    except Exception:
        logger.warning("Failed to load sesiones chunks", exc_info=True)
    _loaded = True


async def search_sesiones(
    query: str,
    periodo: str | None = None,
    orador: str | None = None,
    limit: int = 15,
) -> dict:
    _ensure_loaded()
    if not _chunks:
        return {"results": [], "message": "No session data loaded"}

    query_terms = [t for t in query.lower().split() if len(t) > 2]
    if not query_terms:
        return {"results": []}

    filtered = _chunks
    if periodo is not None:
        try:
            periodo_int = int(periodo)
            filtered = [c for c in filtered if c.get("periodo") == periodo_int]
        except ValueError:
            pass

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
    top = scored[:limit]

    results = [
        {
            "periodo": c.get("periodo"),
            "reunion": c.get("reunion"),
            "fecha": c.get("fecha", ""),
            "tipo_sesion": c.get("tipoSesion", ""),
            "orador": c.get("speaker") or "No identificado",
            "texto": c.get("text", "")[:500],
            "pdf": c.get("pdfUrl", ""),
            "score": round(s, 2),
        }
        for c, s in top
    ]

    return {"results": results, "total_results": len(results)}


def create_server() -> MCPServer:
    server = MCPServer(
        server_name="sesiones",
        description="Congressional Sessions — Search Argentine parliamentary transcripts",
        port=int(os.getenv("MCP_PORT", "8094")),
    )

    server.register_tool(
        name="search_sesiones",
        description="Search Argentine congressional session transcripts",
        parameters={
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Search query"},
                "periodo": {"type": "string", "description": "Legislative period number"},
                "orador": {"type": "string", "description": "Speaker name filter"},
                "limit": {"type": "integer", "default": 15},
            },
            "required": ["query"],
        },
        handler=search_sesiones,
    )

    return server


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    _ensure_loaded()
    srv = create_server()
    srv.run()
