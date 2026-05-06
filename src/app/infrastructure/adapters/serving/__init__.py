"""Serving Port adapters.

The `legacy_serving_adapter` reads from the current `cache_*` + `table_catalog` +
`catalog_resources` infrastructure so the pipeline keeps working unchanged
during Phases 0–1 of the MASTERPLAN.

Future adapters (staging-aware, mart-aware) will replace it incrementally.
"""

from app.infrastructure.adapters.serving.legacy_serving_adapter import LegacyServingAdapter

__all__ = ["LegacyServingAdapter"]
