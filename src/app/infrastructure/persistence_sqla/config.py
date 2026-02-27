from __future__ import annotations

from dataclasses import dataclass

from app.setup.config.settings import PostgresSettings, SqlaEngineSettings


@dataclass(frozen=True)
class PostgresDsn:
    url: str

    def __str__(self) -> str:
        return self.url


@dataclass(frozen=True)
class SqlaEngineConfig:
    dsn: PostgresDsn
    echo: bool = False
    echo_pool: bool = False
    pool_size: int = 20
    max_overflow: int = 10

    @classmethod
    def from_settings(
        cls,
        pg: PostgresSettings,
        sqla: SqlaEngineSettings,
    ) -> SqlaEngineConfig:
        dsn = PostgresDsn(url=pg.dsn)
        return cls(
            dsn=dsn,
            echo=sqla.ECHO,
            echo_pool=sqla.ECHO_POOL,
            pool_size=sqla.POOL_SIZE,
            max_overflow=sqla.MAX_OVERFLOW,
        )
