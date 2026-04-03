from __future__ import annotations

from app.infrastructure.persistence_sqla.mappings.agent_mappings import map_agent_tables
from app.infrastructure.persistence_sqla.mappings.api_key_mappings import map_api_key_tables
from app.infrastructure.persistence_sqla.mappings.chat_mappings import map_chat_tables
from app.infrastructure.persistence_sqla.mappings.dataset_mappings import map_dataset_tables
from app.infrastructure.persistence_sqla.mappings.query_mappings import map_query_tables
from app.infrastructure.persistence_sqla.mappings.staff_mappings import map_staff_tables
from app.infrastructure.persistence_sqla.mappings.user_mappings import map_user_tables


def map_tables() -> None:
    map_dataset_tables()
    map_query_tables()
    map_agent_tables()
    map_user_tables()
    map_chat_tables()
    map_staff_tables()
    map_api_key_tables()
