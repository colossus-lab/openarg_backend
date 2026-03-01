"""Shared sync engine for Celery tasks."""
from __future__ import annotations

import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

_DEFAULT_DB_URL = "postgresql+psycopg://postgres:postgres@localhost:5432/openarg_db"


def get_sync_engine() -> Engine:
    url = os.getenv("DATABASE_URL", _DEFAULT_DB_URL)
    return create_engine(url)
