"""Naming + catalog utilities (WS4).

Two ortogonal concerns kept apart:
  - `title_extractor` produces the `canonical_title` (human-readable).
  - `physical_namer` produces the `materialized_table_name` (SQL-safe, with
    discriminator).
"""

from app.application.catalog.physical_namer import (
    MAX_TABLE_NAME_LENGTH,
    PhysicalNamer,
    physical_table_name,
)
from app.application.catalog.title_extractor import (
    TitleExtraction,
    TitleExtractor,
    TitleSource,
    extract_title,
)

__all__ = [
    "MAX_TABLE_NAME_LENGTH",
    "PhysicalNamer",
    "TitleExtraction",
    "TitleExtractor",
    "TitleSource",
    "extract_title",
    "physical_table_name",
]
