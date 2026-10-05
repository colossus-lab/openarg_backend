"""Piezas mínimas de SQL: identificadores citados y parámetros ligados."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class CatalogRequestError(ValueError):
    """Pedido inválido; el mensaje se le puede mostrar tal cual al usuario."""


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


@dataclass
class Params:
    """Los valores de una consulta, que nunca se interpolan en el SQL.

    Cada ``bind`` devuelve el marcador (``:p0``, ``:p1``…) que va en el texto;
    el sandbox le pasa los valores al driver por separado
    (``execute_readonly(sql, params=...)``). Antes los valores iban como
    literales escapados y el validador del sandbox, que mira todo el texto,
    rechazaba filtros legítimos como «Banco do Brasil» o «Call Center» (867
    valores en 313 tablas de staging) por contener una palabra de SQL.
    """

    values: dict[str, Any] = field(default_factory=dict)

    def bind(self, value: Any) -> str:
        name = f"p{len(self.values)}"
        self.values[name] = value
        return f":{name}"


@dataclass(frozen=True)
class BuiltQuery:
    """Una consulta lista para ``execute_readonly(sql, params=params)``."""

    sql: str
    params: dict[str, Any]
    columns: list[str]
