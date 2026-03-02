from __future__ import annotations

from collections.abc import AsyncIterator
from typing import NewType, cast

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.infrastructure.persistence_sqla.config import SqlaEngineConfig

MainAsyncSession = NewType("MainAsyncSession", AsyncSession)


async def get_async_engine(config: SqlaEngineConfig) -> AsyncEngine:
    return create_async_engine(
        str(config.dsn),
        echo=config.echo,
        echo_pool=config.echo_pool,
        pool_size=config.pool_size,
        max_overflow=config.max_overflow,
    )


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
