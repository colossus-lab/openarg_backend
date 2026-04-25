"""Specialized table parsers for the collector (WS5)."""

from app.application.pipeline.parsers.hierarchical_headers import (
    NORMALIZATION_VERSION,
    PARSER_VERSION,
    HierarchicalHeaderParser,
    parse_hierarchical_headers,
)

__all__ = [
    "NORMALIZATION_VERSION",
    "PARSER_VERSION",
    "HierarchicalHeaderParser",
    "parse_hierarchical_headers",
]
