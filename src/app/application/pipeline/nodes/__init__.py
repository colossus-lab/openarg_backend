"""LangGraph pipeline nodes — shared dependency injection."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class PipelineDeps:
    """All dependencies needed by pipeline nodes."""

    llm: Any  # ILLMProvider
    embedding: Any  # IEmbeddingProvider
    vector_search: Any  # IVectorSearch
    cache: Any  # ICacheService
    series: Any  # ISeriesTiempoConnector
    arg_datos: Any  # IArgentinaDatosConnector
    georef: Any  # IGeorefConnector
    ckan: Any  # ICKANSearchConnector
    sesiones: Any  # ISesionesConnector
    ddjj: Any  # DDJJAdapter
    staff: Any  # IStaffConnector | None
    bcra: Any  # BCRAAdapter | None
    sandbox: Any  # ISQLSandbox | None
    semantic_cache: Any  # SemanticCache
    chat_repo: Any  # IChatRepository | None
    metrics: Any  # MetricsCollector


# Set by graph builder before first invocation
_deps: PipelineDeps | None = None
