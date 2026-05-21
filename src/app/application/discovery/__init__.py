"""WS3 — hybrid discovery layer.

Reads from `catalog_resources` (the new logical catalog) so the planner can
see resources that exist conceptually even when their physical table is not
yet materialized.

`cache_*` keeps serving for already-materialized queries; this module is
purely additive during the transition.
"""

from app.application.discovery.catalog_discovery import (
    CatalogDiscovery,
    DiscoveredResource,
    catalog_discovery,
    catalog_only_mode,
    discovery_enabled,
)

__all__ = [
    "CatalogDiscovery",
    "DiscoveredResource",
    "catalog_discovery",
    "catalog_only_mode",
    "discovery_enabled",
]
