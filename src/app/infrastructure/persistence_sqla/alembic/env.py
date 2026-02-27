from __future__ import annotations

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy.ext.asyncio import async_engine_from_config

from app.infrastructure.persistence_sqla.mappings.all import map_tables
from app.infrastructure.persistence_sqla.registry import mapping_registry
from app.setup.config.settings import load_settings

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

map_tables()
target_metadata = mapping_registry.metadata


def _get_dsn() -> str:
    import os

    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return db_url
    settings = load_settings()
    return settings.postgres.dsn


def run_migrations_offline() -> None:
    url = _get_dsn()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection) -> None:  # type: ignore[no-untyped-def]
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations() -> None:
    cfg = config.get_section(config.config_ini_section, {})
    cfg["sqlalchemy.url"] = _get_dsn()

    engine = async_engine_from_config(
        cfg,
        prefix="sqlalchemy.",
        future=True,
    )

    async with engine.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await engine.dispose()


def run_migrations_online() -> None:
    asyncio.run(run_async_migrations())


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
