from __future__ import annotations

from dataclasses import dataclass

from app.domain.entities.base import BaseEntity


@dataclass
class CachedDataset(BaseEntity):
    """Cache de datos reales descargados de un dataset."""

    dataset_id: str = ""
    table_name: str = ""  # nombre de la tabla SQL dinámica
    row_count: int = 0
    columns_json: str = ""  # JSON con schema de columnas
    size_bytes: int = 0
    status: str = "pending"  # pending, downloading, ready, error
    error_message: str = ""
    s3_key: str = ""  # key del archivo crudo en S3
