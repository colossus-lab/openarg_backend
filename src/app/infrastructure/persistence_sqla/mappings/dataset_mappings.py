from __future__ import annotations

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Table,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

from app.domain.entities.dataset.cached_data import CachedDataset
from app.domain.entities.dataset.catalog_resource import CatalogResource
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
            "id",
            PG_UUID(as_uuid=True),
            primary_key=True,
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
        Column(
            "updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
        ),
    )

    dataset_chunks_table = Table(
        "dataset_chunks",
        mapping_registry.metadata,
        Column(
            "id",
            PG_UUID(as_uuid=True),
            primary_key=True,
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
            "id",
            PG_UUID(as_uuid=True),
            primary_key=True,
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
        Column(
            "updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
        ),
    )

    catalog_resources_table = Table(
        "catalog_resources",
        mapping_registry.metadata,
        Column(
            "id",
            PG_UUID(as_uuid=True),
            primary_key=True,
            server_default=text("gen_random_uuid()"),
        ),
        Column("resource_identity", String(255), nullable=False, unique=True),
        Column("dataset_id", PG_UUID(as_uuid=True), nullable=True, index=True),
        Column(
            "parent_resource_id",
            PG_UUID(as_uuid=True),
            ForeignKey("catalog_resources.id", ondelete="CASCADE"),
            nullable=True,
        ),
        Column("portal", String(100), server_default=""),
        Column("source_id", String(500), server_default=""),
        Column("s3_key", String(500), nullable=True),
        Column("filename", String(500), nullable=True),
        Column("sub_path", String(500), nullable=True),
        Column("sheet_name", String(255), nullable=True),
        Column("cuadro_numero", String(50), nullable=True),
        Column("provincia", String(100), nullable=True),
        Column("raw_title", Text, server_default=""),
        Column("canonical_title", Text, server_default=""),
        Column("display_name", Text, server_default=""),
        Column("title_source", String(30), server_default="fallback"),
        Column("title_confidence", Float, server_default=text("0.0")),
        Column("domain", String(100), nullable=True),
        Column("subdomain", String(100), nullable=True),
        Column("taxonomy_key", String(255), nullable=True),
        Column("resource_kind", String(30), server_default="file"),
        Column("materialization_status", String(40), server_default="pending"),
        Column("materialized_table_name", String(255), nullable=True),
        Column("parser_version", String(20), nullable=True),
        Column("normalization_version", String(20), nullable=True),
        Column("created_at", DateTime(timezone=True), server_default=func.now()),
        Column(
            "updated_at", DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
        ),
        # `embedding vector(1024)` is added via raw SQL in migration 0035
        # because the pgvector type isn't a stock SA type.
    )

    mapping_registry.map_imperatively(Dataset, datasets_table)
    mapping_registry.map_imperatively(DatasetChunk, dataset_chunks_table)
    mapping_registry.map_imperatively(CachedDataset, cached_datasets_table)
    mapping_registry.map_imperatively(CatalogResource, catalog_resources_table)

    _mapped = True
