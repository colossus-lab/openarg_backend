from __future__ import annotations

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    String,
    Table,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from app.domain.entities.dataset.cached_data import CachedDataset
from app.domain.entities.dataset.dataset import Dataset, DatasetChunk
from app.infrastructure.persistence_sqla.registry import mapping_registry

_mapped = False


def map_dataset_tables() -> None:
    global _mapped
    if _mapped:
        return

    datasets_table = Table(
        "datasets",
        mapping_registry.metadata,
        Column(
            "id", PG_UUID(as_uuid=True), primary_key=True,
            server_default=text("gen_random_uuid()"),
        ),
        Column("source_id", String(500), nullable=False),
        Column("title", String(1000), nullable=False),
        Column("description", Text, nullable=True),
        Column("organization", String(500), nullable=True),
        Column("portal", String(100), nullable=False, index=True),
        Column("url", Text, nullable=True),
        Column("download_url", Text, nullable=True),
        Column("format", String(50), nullable=True),
        Column("columns", Text, nullable=True),
        Column("sample_rows", Text, nullable=True),
        Column("tags", Text, nullable=True),
        Column("last_updated_at", DateTime(timezone=True), nullable=True),
        Column("is_cached", Boolean, server_default="false"),
        Column("row_count", Integer, nullable=True),
        Column("created_at", DateTime(timezone=True), server_default=func.now()),
        Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
    )

    dataset_chunks_table = Table(
        "dataset_chunks",
        mapping_registry.metadata,
        Column(
            "id", PG_UUID(as_uuid=True), primary_key=True,
            server_default=text("gen_random_uuid()"),
        ),
        Column("dataset_id", PG_UUID(as_uuid=True), nullable=False, index=True),
        Column("content", Text, nullable=False),
        # embedding column added via raw SQL in migration (pgvector type)
        Column("created_at", DateTime(timezone=True), server_default=func.now()),
        Column("updated_at", DateTime(timezone=True), server_default=func.now()),
    )

    cached_datasets_table = Table(
        "cached_datasets",
        mapping_registry.metadata,
        Column(
            "id", PG_UUID(as_uuid=True), primary_key=True,
            server_default=text("gen_random_uuid()"),
        ),
        Column("dataset_id", PG_UUID(as_uuid=True), nullable=False, index=True),
        Column("table_name", String(255), nullable=False, unique=True),
        Column("row_count", Integer, server_default="0"),
        Column("columns_json", Text, nullable=True),
        Column("size_bytes", Integer, server_default="0"),
        Column("status", String(50), server_default="pending"),
        Column("error_message", Text, nullable=True),
        Column("s3_key", String(500), nullable=True),
        Column("created_at", DateTime(timezone=True), server_default=func.now()),
        Column("updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()),
    )

    mapping_registry.map_imperatively(Dataset, datasets_table)
    mapping_registry.map_imperatively(DatasetChunk, dataset_chunks_table)
    mapping_registry.map_imperatively(CachedDataset, cached_datasets_table)

    _mapped = True
