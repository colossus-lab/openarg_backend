"""Marts (semantic views) for the LangGraph serving layer.

Per MASTERPLAN Fase 3, marts are the layer the query pipeline consults.
Each mart is declared in `config/marts/<mart_id>.yaml`, joining one or more
staging tables into a canonical, semantically-tagged materialized view.

Public surface:
    Mart                  — parsed mart DTO
    MartCanonicalColumn   — one canonical column declaration
    load_mart             — read a single YAML
    load_all_marts        — read every YAML in a directory
    build_create_view_sql — DDL for `CREATE OR REPLACE MATERIALIZED VIEW`
    build_comment_sql     — `COMMENT ON COLUMN` statements for semantics
"""

from app.application.marts.builder import build_comment_sql, build_create_view_sql
from app.application.marts.mart import (
    Mart,
    MartCanonicalColumn,
    load_all_marts,
    load_mart,
)

__all__ = [
    "Mart",
    "MartCanonicalColumn",
    "build_comment_sql",
    "build_create_view_sql",
    "load_all_marts",
    "load_mart",
]
