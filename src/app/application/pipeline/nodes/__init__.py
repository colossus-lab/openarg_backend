"""LangGraph pipeline nodes — shared dependency injection.

Dependencies are isolated per-request using ``contextvars.ContextVar``
to prevent race conditions when concurrent requests update deps.
"""

from __future__ import annotations

from contextvars import ContextVar
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


# Per-request dependency isolation using ContextVar (thread/coroutine-safe)
_deps_var: ContextVar[PipelineDeps | None] = ContextVar("pipeline_deps", default=None)


@property  # type: ignore[misc]
def _deps_compat() -> PipelineDeps | None:
    """Backwards-compatible access: nodes read ``nodes_pkg._deps``."""
    return _deps_var.get()


# Module-level _deps — used by all node files via ``import nodes; nodes._deps``
# This is a property-like accessor backed by ContextVar for thread safety.
# For setting: use set_deps(). For reading: access _deps directly.
_deps: PipelineDeps | None = None  # Overwritten by __getattr__


def set_deps(deps: PipelineDeps) -> None:
    """Set pipeline dependencies for the current request context."""
    _deps_var.set(deps)


def get_deps() -> PipelineDeps:
    """Get pipeline dependencies for the current request context."""
    deps = _deps_var.get()
    if deps is None:
        msg = "PipelineDeps not initialised — call set_deps() before invoking the graph"
        raise RuntimeError(msg)
    return deps


def __getattr__(name: str) -> Any:
    """Module-level __getattr__ so ``nodes_pkg._deps`` reads from ContextVar."""
    if name == "_deps":
        return _deps_var.get()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
