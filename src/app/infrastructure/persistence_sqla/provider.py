from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import NewType, cast

from sqlalchemy import event
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.infrastructure.persistence_sqla.config import SqlaEngineConfig

logger = logging.getLogger(__name__)

MainAsyncSession = NewType("MainAsyncSession", AsyncSession)


async def get_async_engine(config: SqlaEngineConfig) -> AsyncEngine:
    engine = create_async_engine(
        str(config.dsn),
        echo=config.echo,
        echo_pool=config.echo_pool,
        pool_size=config.pool_size,
        max_overflow=config.max_overflow,
        pool_pre_ping=True,
        pool_recycle=300,
    )

    # Belt-and-suspenders search_path setup. The database-level
    # `ALTER DATABASE openarg_staging SET search_path = raw, public,
    # "$user"` is unreliable under SQLAlchemy pool checkout (DISCARD
    # ALL or similar can wipe session settings). Apply it on every
    # new sync DBAPI connection that the async engine wraps so the
    # 120+ unprefixed `cached_datasets` references resolve correctly
    # regardless of pool / pgbouncer / RDS routing.
    # Order `public, raw` is canonical: tables that exist in BOTH
    # schemas (mart_sample_queries, langgraph checkpoints, portals)
    # resolve to public.
    @event.listens_for(engine.sync_engine, "connect")
    def _set_search_path(dbapi_conn, _connection_record):
        cursor = dbapi_conn.cursor()
        try:
            cursor.execute("SET search_path = public, raw")
        except Exception:
            logger.warning(
                "Failed to set search_path on async engine connection",
                exc_info=True,
            )
        finally:
            cursor.close()

    return engine


def get_async_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )


async def get_main_async_session(
    factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[MainAsyncSession]:
    async with factory() as session:
        yield cast("MainAsyncSession", session)
