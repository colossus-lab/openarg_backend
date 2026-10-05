from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


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
class TableProfile:
    """Lo que hace falta para elegir entre copias de un mismo archivo.

    El catálogo tiene el mismo archivo varias veces: gemelos de la migración de
    datos.gob.ar (IDs regenerados, misma URL), espejos entre portales y el CSV
    y el JSON de un mismo recurso. Elegir cuál mostrar pide datos que
    ``CachedTableInfo`` no trae:

    - ``rows``: las filas que registró el colector al materializar la versión
      viva (``raw_table_versions.row_count``). ``cached_datasets.row_count``
      no sirve para esto: en staging vale 0 en 18.134 de 31.221 tablas listas
      y en prod difiere de la versión viva en 1.672 (Proyectos Parlamentarios
      en CSV anuncia 111.091 y tiene 11.089).
    - ``truncated``: la versión quedó cortada en ``MAX_TABLE_ROWS``.
    - ``dataset_created_at``: separa la era vieja de datos.gob.ar de la nueva.
    - ``columns``: los nombres reales de las primeras columnas, para notar un
      encabezado que en realidad es una fila de datos.
    """

    table_name: str  # pelado, como ``get_table_sources``
    rows: int | None = None
    truncated: bool = False
    loaded_at: datetime | None = None
    dataset_created_at: datetime | None = None
    format: str | None = None
    columns: list[str] = field(default_factory=list)


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
class MartInfo:
    """Un mart: una tabla curada que junta y normaliza varios datasets.

    ``columns`` trae la descripción de cada columna tal como la escribió el
    autor del mart (``canonical_columns`` del YAML): es lo único que dice en
    qué unidad está un valor o qué nivel geográfico cubre una fila.
    """

    table_name: str  # calificado: "mart.<vista>"
    mart_id: str
    description: str
    domain: str
    row_count: int | None
    score: float = 0.0
    columns: list[dict] | None = None


@dataclass
class SandboxResult:
    columns: list[str]
    rows: list[dict]
    row_count: int
    truncated: bool
    error: str | None = None
    # Qué clase de error, para que el llamador no tenga que adivinarlo por el
    # texto: "timeout", "validation" (el validador rechazó el SQL), "blocked"
    # (tabla con un hallazgo de calidad abierto o mart retirado),
    # "missing_column" / "missing_table" (la tabla cambió o se reemplazó entre
    # que se leyó su esquema y se consultó) o "execution". None si no hubo
    # error.
    error_kind: str | None = None


@dataclass
class ColumnValueStats:
    """Lo que ``pg_stats`` sabe de los valores de una columna.

    Sale del ANALYZE de Postgres (una muestra al azar de la tabla), así que
    cuesta milisegundos aun en tablas de millones de filas, donde un DISTINCT
    pasa el timeout del sandbox (medido en staging: 30-142 ms contra más de
    10 s en 11,6 M de filas).
    """

    column: str
    null_frac: float | None = None
    # Negativo = fracción de las filas (convención de Postgres).
    n_distinct: float | None = None
    most_common_vals: list[str] = field(default_factory=list)
    most_common_freqs: list[float] = field(default_factory=list)
    histogram_bounds: list[str] = field(default_factory=list)


@dataclass
class TableValueStats:
    # `pg_class.reltuples`; None si la tabla nunca se analizó.
    estimated_rows: int | None
    columns: dict[str, ColumnValueStats] = field(default_factory=dict)


class ISQLSandbox(ABC):
    @abstractmethod
    async def execute_readonly(
        self,
        sql: str,
        timeout_seconds: int = 10,
        *,
        params: Mapping[str, Any] | None = None,
    ) -> SandboxResult:
        """Ejecuta un SELECT en una transacción de sólo lectura.

        ``params`` distingue los dos orígenes del SQL:

        - ``None``: SQL escrito por un modelo (NL2SQL del pipeline viejo, el
          SQL crudo de /sandbox). Pasa por el arreglo automático de enteros y
          por el validador completo.
        - un dict (aunque esté vacío): SQL armado por nuestro código, con los
          valores del usuario como parámetros ligados (``:p0``). No se toca el
          texto, y el validador no busca palabras prohibidas dentro de los
          literales y los nombres citados, que vienen del esquema real.
        """
        ...

    async def get_value_stats(self, table_name: str, columns: list[str]) -> TableValueStats | None:
        """Estadísticas de valores (``pg_stats``) de algunas columnas de una tabla.

        No es abstracto: un sandbox que no las tenga (un fake de test) devuelve
        None y quien llama sigue sin ellas.
        """
        return None

    @abstractmethod
    async def list_cached_tables(self) -> list[CachedTableInfo]: ...

    @abstractmethod
    async def get_column_types(
        self,
        table_names: list[str],
    ) -> dict[str, list[tuple[str, str]]]:
        """Return {table_name: [(column_name, data_type), ...]} for given tables."""
        ...

    async def find_tables(
        self,
        *,
        dataset_ids: list[str] | None = None,
        table_names: list[str] | None = None,
    ) -> list[CachedTableInfo]:
        """Las tablas listas de ciertos datasets o con ciertos nombres.

        Mismo contrato que `list_cached_tables()` pero acotado: el listado
        completo son ~32.000 filas y, con el sandbox en scope de request, su
        caché no sobrevive entre pedidos (4 s cada vez en staging). La versión
        por defecto filtra el listado completo; los adapters reales la
        reemplazan por una consulta puntual.
        """
        wanted_ids = {str(i) for i in dataset_ids or []}
        wanted_names = {n.split(".")[-1].strip('"').lower() for n in table_names or []}
        return [
            t
            for t in await self.list_cached_tables()
            if (t.dataset_id and str(t.dataset_id) in wanted_ids)
            or t.table_name.split(".")[-1].strip('"').lower() in wanted_names
        ]

    async def find_marts(self, query_embedding: list[float], limit: int = 5) -> list[MartInfo]:
        """Los marts más parecidos a una pregunta, sin los retirados del serving.

        No es abstracto: un sandbox sin marts (un fake de test) devuelve vacío.
        """
        return []

    async def describe_marts(self, table_names: list[str]) -> dict[str, MartInfo]:
        """``{"mart.<vista>": MartInfo}`` de los marts pedidos que se pueden servir."""
        return {}

    async def table_profiles(self, table_names: list[str]) -> dict[str, TableProfile]:
        """``{nombre_pelado: TableProfile}`` de las tablas listas pedidas.

        No es abstracto: un sandbox que no sepa resolverlo (un fake de test)
        devuelve vacío, y la búsqueda elige entre copias con lo que trae
        ``find_tables``.
        """
        return {}

    async def get_table_sources(self, table_names: list[str]) -> dict[str, TableSource]:
        """Return {bare_table_name: TableSource} for the tables that map to a dataset.

        No es abstracto a propósito: una implementación que no sepa resolverlo
        (un fake de test, un sandbox sin catálogo) devuelve vacío y el llamador
        se queda con la etiqueta genérica, como antes.
        """
        return {}
