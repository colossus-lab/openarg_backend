from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from app.domain.entities.base import BaseEntity, _utcnow


@dataclass
class Dataset(BaseEntity):
    """Metadata de un dataset público indexado."""

    source_id: str = ""
    title: str = ""
    description: str = ""
    organization: str = ""
    portal: str = ""  # datos_gob_ar, caba, cordoba, santa_fe, mendoza, buenos_aires
    url: str = ""
    download_url: str = ""
    format: str = ""  # csv, json, xlsx, etc.
    columns: str = ""  # JSON string con nombres de columnas
    sample_rows: str = ""  # JSON string con 3-5 filas de ejemplo
    tags: str = ""  # comma-separated tags
    last_updated_at: datetime = field(default_factory=_utcnow)
    is_cached: bool = False
    row_count: int | None = None


@dataclass
class DatasetChunk(BaseEntity):
    """Chunk embebido de la metadata de un dataset para búsqueda vectorial."""

    dataset_id: str = ""
    content: str = ""  # texto combinado: title + description + columns + sample
    embedding: list[float] = field(default_factory=list)
