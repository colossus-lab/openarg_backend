"""Catalog resource — entidad lógica del catálogo nuevo (WS2).

Una fila por recurso lógico consultable. Puede representar dataset, hoja,
cuadro o recorte definido por el modelo. Independiente de si la tabla
física fue materializada — un recurso puede existir sin tabla.
"""

from __future__ import annotations

from dataclasses import dataclass

from app.domain.entities.base import BaseEntity

# materialization_status enum values (validated by DB CHECK constraint).
MATERIALIZATION_PENDING = "pending"
MATERIALIZATION_READY = "ready"
MATERIALIZATION_LIVE_API = "live_api"
MATERIALIZATION_NON_TABULAR = "non_tabular"
MATERIALIZATION_CORRUPTED = "materialization_corrupted"
MATERIALIZATION_FAILED = "failed"

ALL_MATERIALIZATION_STATUSES = (
    MATERIALIZATION_PENDING,
    MATERIALIZATION_READY,
    MATERIALIZATION_LIVE_API,
    MATERIALIZATION_NON_TABULAR,
    MATERIALIZATION_CORRUPTED,
    MATERIALIZATION_FAILED,
)

# resource_kind enum values.
RESOURCE_KIND_FILE = "file"
RESOURCE_KIND_SHEET = "sheet"
RESOURCE_KIND_CUADRO = "cuadro"
RESOURCE_KIND_ZIP_MEMBER = "zip_member"
RESOURCE_KIND_DOCUMENT_BUNDLE = "document_bundle"
RESOURCE_KIND_CONNECTOR_ENDPOINT = "connector_endpoint"

ALL_RESOURCE_KINDS = (
    RESOURCE_KIND_FILE,
    RESOURCE_KIND_SHEET,
    RESOURCE_KIND_CUADRO,
    RESOURCE_KIND_ZIP_MEMBER,
    RESOURCE_KIND_DOCUMENT_BUNDLE,
    RESOURCE_KIND_CONNECTOR_ENDPOINT,
)


@dataclass
class CatalogResource(BaseEntity):
    """Recurso lógico canónico (independiente de tabla física)."""

    # Stable key. Algorithmic, deterministic — same inputs always yield same key.
    resource_identity: str = ""

    # Source linkage (optional — connector endpoints have no datasets row).
    dataset_id: str | None = None
    parent_resource_id: str | None = None

    # Provenance.
    portal: str = ""
    source_id: str = ""
    s3_key: str | None = None
    filename: str | None = None
    sub_path: str | None = None  # e.g. ZIP entry name
    sheet_name: str | None = None
    cuadro_numero: str | None = None
    provincia: str | None = None

    # Naming.
    raw_title: str = ""
    canonical_title: str = ""
    display_name: str = ""
    title_source: str = ""  # 'index' | 'caratula' | 'metadata' | 'fallback' | 'manual'
    title_confidence: float = 0.0  # 0.0 - 1.0

    # Taxonomy.
    domain: str | None = None
    subdomain: str | None = None
    taxonomy_key: str | None = None

    # Materialization.
    resource_kind: str = RESOURCE_KIND_FILE
    materialization_status: str = MATERIALIZATION_PENDING
    materialized_table_name: str | None = None
    layout_profile: str | None = None
    header_quality: str | None = None
    parser_version: str | None = None
    normalization_version: str | None = None
