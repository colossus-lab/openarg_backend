"""Specialized table parsers for the collector (WS5)."""

from app.application.pipeline.parsers.column_normalization import (
    PG_NAME_LIMIT_BYTES,
    dedupe_column_names,
    garbage_column_ratio,
    is_garbage_column,
    is_placeholder_column,
    is_title_row_column,
    is_url_column,
    truncate_utf8_bytes,
)
from app.application.pipeline.parsers.header_recovery import (
    find_data_start_row,
    promote_buried_headers,
)
from app.application.pipeline.parsers.hierarchical_headers import (
    NORMALIZATION_VERSION,
    PARSER_VERSION,
    HierarchicalHeaderParser,
    parse_hierarchical_headers,
)
from app.application.pipeline.parsers.time_pivot import (
    is_time_column,
    time_column_ratio,
    unpivot_if_time_pivoted,
)

__all__ = [
    "NORMALIZATION_VERSION",
    "PARSER_VERSION",
    "PG_NAME_LIMIT_BYTES",
    "HierarchicalHeaderParser",
    "dedupe_column_names",
    "find_data_start_row",
    "garbage_column_ratio",
    "is_garbage_column",
    "is_placeholder_column",
    "is_time_column",
    "is_title_row_column",
    "is_url_column",
    "parse_hierarchical_headers",
    "promote_buried_headers",
    "time_column_ratio",
    "truncate_utf8_bytes",
    "unpivot_if_time_pivoted",
]
