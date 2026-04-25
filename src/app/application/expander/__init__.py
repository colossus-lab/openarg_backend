"""Multi-file expansion (WS5 subplan).

Treat ZIPs as containers, not as resources. Each tabular entry becomes its
own logical resource in `catalog_resources` with `parent_resource_id`
pointing to the ZIP, and the per-entry pipeline reuses WS0 detectors.
"""

from app.application.expander.multi_file_expander import (
    EntryClassification,
    EntryDecision,
    ExpandedEntry,
    MultiFileExpander,
    expand_zip,
)

__all__ = [
    "EntryClassification",
    "EntryDecision",
    "ExpandedEntry",
    "MultiFileExpander",
    "expand_zip",
]
