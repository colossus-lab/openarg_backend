"""Serving Port — stable interface between LangGraph pipeline and data layers.

Per MASTERPLAN.md, the pipeline imports `IServingPort` and never touches
`cache_*`, `staging_*`, `mart_*` table names directly. The adapter behind
this port evolves with the medallion migration; the interface does not.
"""

from app.domain.ports.serving.serving_port import IServingPort

__all__ = ["IServingPort"]
