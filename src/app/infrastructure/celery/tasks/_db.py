"""Shared sync engine for Celery tasks."""
from __future__ import annotations

import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

_DEFAULT_DB_URL = "postgresql+psycopg://postgres:postgres@localhost:5432/openarg_db"

_engine: Engine | None = None


def get_sync_engine() -> Engine:
    global _engine  # noqa: PLW0603
    if _engine is None:
        url = os.getenv("DATABASE_URL", _DEFAULT_DB_URL)
        _engine = create_engine(
            url,
            pool_pre_ping=True,
            pool_recycle=300,
            pool_size=5,
            max_overflow=3,
        )
    return _engine
