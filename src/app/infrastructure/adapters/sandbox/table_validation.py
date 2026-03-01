from __future__ import annotations

import re

_TABLE_NAME_RE = re.compile(r"^cache_[a-z0-9_]{1,100}$")


def validate_table_name(name: str) -> bool:
    """Return True if the table name matches the expected cached-dataset pattern."""
    return bool(_TABLE_NAME_RE.match(name))


def safe_table_query(table_name: str, template: str) -> str | None:
    """Validate table_name and return a formatted query, or None if invalid.

    ``template`` must contain exactly one ``{}`` placeholder for the table name.
    Example: ``'SELECT * FROM "{}" LIMIT 10'``
    """
    if not validate_table_name(table_name):
        return None
    return template.format(table_name)
