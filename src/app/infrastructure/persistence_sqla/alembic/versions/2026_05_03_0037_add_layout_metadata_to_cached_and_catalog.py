"""WS4 — persist layout/header metadata in cached_datasets and catalog_resources.

Revision ID: 2026_05_03_0037
Revises: 2026_04_25_0036
Create Date: 2026-05-03
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0037"
down_revision = "0036"
branch_labels = None
depends_on = None


_LAYOUT_PROFILE_VALUES = (
    "simple_tabular",
    "presentation_sheet",
    "header_multiline",
    "header_sparse",
    "wide_csv",
)
_HEADER_QUALITY_VALUES = ("good", "degraded", "invalid")


def _layout_check_clause() -> str:
    quoted = ", ".join(f"'{v}'" for v in _LAYOUT_PROFILE_VALUES)
    return f"layout_profile IS NULL OR layout_profile IN ({quoted})"


def _header_check_clause() -> str:
    quoted = ", ".join(f"'{v}'" for v in _HEADER_QUALITY_VALUES)
    return f"header_quality IS NULL OR header_quality IN ({quoted})"


def upgrade() -> None:
    op.add_column("cached_datasets", sa.Column("layout_profile", sa.String(length=40), nullable=True))
    op.add_column("cached_datasets", sa.Column("header_quality", sa.String(length=20), nullable=True))

    op.add_column("catalog_resources", sa.Column("layout_profile", sa.String(length=40), nullable=True))
    op.add_column("catalog_resources", sa.Column("header_quality", sa.String(length=20), nullable=True))

    op.create_check_constraint(
        "ck_cached_datasets_layout_profile",
        "cached_datasets",
        _layout_check_clause(),
    )
    op.create_check_constraint(
        "ck_cached_datasets_header_quality",
        "cached_datasets",
        _header_check_clause(),
    )
    op.create_check_constraint(
        "ck_catalog_resources_layout_profile",
        "catalog_resources",
        _layout_check_clause(),
    )
    op.create_check_constraint(
        "ck_catalog_resources_header_quality",
        "catalog_resources",
        _header_check_clause(),
    )


def downgrade() -> None:
    op.drop_constraint("ck_catalog_resources_header_quality", "catalog_resources", type_="check")
    op.drop_constraint("ck_catalog_resources_layout_profile", "catalog_resources", type_="check")
    op.drop_constraint("ck_cached_datasets_header_quality", "cached_datasets", type_="check")
    op.drop_constraint("ck_cached_datasets_layout_profile", "cached_datasets", type_="check")

    op.drop_column("catalog_resources", "header_quality")
    op.drop_column("catalog_resources", "layout_profile")
    op.drop_column("cached_datasets", "header_quality")
    op.drop_column("cached_datasets", "layout_profile")
