from __future__ import annotations

from app.infrastructure.persistence_sqla.mappings.agent_mappings import map_agent_tables
from app.infrastructure.persistence_sqla.mappings.dataset_mappings import map_dataset_tables
from app.infrastructure.persistence_sqla.mappings.query_mappings import map_query_tables


def map_tables() -> None:
    map_dataset_tables()
    map_query_tables()
    map_agent_tables()
