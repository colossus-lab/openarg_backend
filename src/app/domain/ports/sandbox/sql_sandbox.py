from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass
class CachedTableInfo:
    """Una tabla consultable del sandbox.

    ``table_name`` viene **calificado** (``raw.cache_x``) para las tablas de
    la capa raw y **pelado** (``cache_x``) para las legacy de ``public``. Las
    dos formas nombran la misma tabla al ejecutar, porque el engine del
    sandbox corre con ``search_path = public,raw``; la diferencia importa
    sólo al *comparar* nombres.

    Para comparar, usar los helpers de
    ``app.domain.value_objects.table_reference`` — nunca ``==`` ni
    ``fnmatch`` contra ``table_name`` pelado. Hasta 2026-09 este contrato
    vivía en un docstring del adapter y los consumidores lo ignoraban: el
    ruteo del planner quedó roto cinco meses por eso.
    """

    table_name: str
    dataset_id: str
    row_count: int | None
    columns: list[str]


@dataclass
class TableSource:
    """El dataset publicado del que sale una tabla del sandbox.

    Es lo que se le muestra al usuario como fuente: sin esto, una respuesta
    armada con NL2SQL citaba "Consulta SQL: <la pregunta>" en vez del dataset
    oficial, y no había forma de ir a verificar el dato.
    """

    title: str
    portal: str
    url: str


@dataclass
class SandboxResult:
    columns: list[str]
    rows: list[dict]
    row_count: int
    truncated: bool
    error: str | None = None


class ISQLSandbox(ABC):
    @abstractmethod
    async def execute_readonly(self, sql: str, timeout_seconds: int = 10) -> SandboxResult: ...

    @abstractmethod
    async def list_cached_tables(self) -> list[CachedTableInfo]: ...

    @abstractmethod
    async def get_column_types(
        self,
        table_names: list[str],
    ) -> dict[str, list[tuple[str, str]]]:
        """Return {table_name: [(column_name, data_type), ...]} for given tables."""
        ...

    async def get_table_sources(self, table_names: list[str]) -> dict[str, TableSource]:
        """Return {bare_table_name: TableSource} for the tables that map to a dataset.

        No es abstracto a propósito: una implementación que no sepa resolverlo
        (un fake de test, un sandbox sin catálogo) devuelve vacío y el llamador
        se queda con la etiqueta genérica, como antes.
        """
        return {}
