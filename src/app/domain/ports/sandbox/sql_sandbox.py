from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class CachedTableInfo:
    table_name: str
    dataset_id: str
    row_count: int | None
    columns: list[str]


@dataclass
class SandboxResult:
    columns: list[str]
    rows: list[dict]
    row_count: int
    truncated: bool
    error: str | None = None


class ISQLSandbox(ABC):
    @abstractmethod
    async def execute_readonly(
        self, sql: str, timeout_seconds: int = 10
    ) -> SandboxResult: ...

    @abstractmethod
    async def list_cached_tables(self) -> list[CachedTableInfo]: ...
