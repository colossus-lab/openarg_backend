"""Encabezados hechos de datos: la tabla física tiene filas donde iban los nombres.

Medido en prod el 04-oct-2026: unas 414 tablas listas tienen como nombres de
columna valores de una fila de datos (fechas ISO, UUID, URL, códigos de
expediente) mientras `cached_datasets.columns_json` conserva el encabezado
bueno del archivo. No era el archivo: era el colector, que volvía a inferir el
encabezado en cada escritura y en cada chunk y promovía filas de datos a
encabezado (`leyes_sancionadas` CSV con `'2025-12-26T00:00:00'` como columna,
`proyectos_parlamentarios` con 11.089 filas de 111.091). De paso renombraba las
columnas de linaje `_source_*` a su valor, un UUID o una URL.

Ningún detector lo veía: `placeholder_headers` busca `Unnamed: N`, y el gate
post-parse recibía las columnas que el parser *dijo* haber escrito, no las de
la tabla.

Dos reglas, de más fuerte a más débil:

1. **Renombrada contra lo declarado.** Los nombres físicos difieren de
   `columns_json`, el encabezado declarado era limpio, y entre los nombres
   nuevos hay alguno que no puede ser un encabezado (UUID, URL, fecha con hora
   `T`, código `HCDN123456`, expediente `0011-PE-2024`) o al menos dos números
   con formato de dato. Es la huella exacta del bug.
2. **Datos en el encabezado.** Sin `columns_json` para comparar (en staging el
   85 % de las filas listas lo tiene vacío) o igual a lo físico: dos nombres que
   no pueden ser encabezado, o nombres numéricos que son la mitad o más de las
   columnas (`['CANON DE EXPLORACIÓN', 'Primer Período', '0.46', '29.15']`).

**Pivots legítimos.** Unas 50 tablas traen datos en los nombres *desde el
origen*: cuadros anchos con años, meses o fechas como columnas (`2019`,
`2015-11-01 00:00:00`, `Enero 2024`, `2016.1` que es el `2016` repetido que
pandas desambigua). Esos nombres se clasifican como "período" y nunca cuentan
para disparar: ésa es la lista blanca, por forma del nombre y no por nombre de
tabla, porque las tablas cambian de nombre en cada versión (`__vN`). Un número
suelto de un decimal (`1.1`, `4.2`) tampoco cuenta: en las encuestas del INDEC
es un código de pregunta.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from app.application.pipeline.parsers.header_tokens import (
    KIND_DATA,
    KIND_NUMBER,
    KIND_PERIOD,
    classify_column_name,
    normalize_name,
    strip_dedup_suffix,
)
from app.application.validation.detector import (
    Detector,
    Finding,
    Mode,
    ResourceContext,
    Severity,
)

__all__ = [
    "KIND_DATA",
    "KIND_NUMBER",
    "KIND_PERIOD",
    "HeaderFromDataDetector",
    "HeaderVerdict",
    "classify_column_name",
    "evaluate_header",
    "is_internal_column",
    "parse_columns_json",
]


def is_internal_column(name: object) -> bool:
    """Columnas de bookkeeping del colector (`_source_url`, `_overflow_json`…)."""
    return str(name or "").startswith("_")


def _is_placeholder(name: str) -> bool:
    lowered = name.strip().lower()
    return (
        not lowered
        or lowered.startswith("unnamed:")
        or re.fullmatch(r"col_[0-9]+", lowered) is not None
        or re.fullmatch(r"[0-9]{1,3}", lowered) is not None
    )


def parse_columns_json(raw: object) -> list[str]:
    """`columns_json` tolerante: texto JSON, lista ya decodificada o basura."""
    if raw is None:
        return []
    if isinstance(raw, list | tuple):
        return [str(c) for c in raw]
    try:
        value = json.loads(str(raw))
    except (TypeError, ValueError):
        return []
    if not isinstance(value, list):
        return []
    return [str(c) for c in value]


@dataclass(frozen=True)
class HeaderVerdict:
    """Qué vio el detector en un par (columnas físicas, columnas declaradas)."""

    rule: str  # "renamed_from_declared" | "data_in_header"
    data_names: list[str]
    renamed: list[str]
    physical_count: int
    declared_count: int


def evaluate_header(
    physical_columns: Sequence[object],
    declared_columns: Iterable[object] | None = None,
) -> HeaderVerdict | None:
    """Función pura detrás del detector y del listado de candidatas al backfill.

    `physical_columns` son las columnas de la tabla tal cual están en Postgres;
    `declared_columns`, las que el parser dijo haber leído (`columns_json`).
    Las columnas internas (prefijo `_`) no cuentan de ninguno de los dos lados.
    """
    physical = [normalize_name(c) for c in physical_columns if not is_internal_column(c)]
    physical = [c for c in physical if c]
    if not physical:
        return None
    declared = [normalize_name(c) for c in (declared_columns or []) if not is_internal_column(c)]
    declared = [c for c in declared if c]

    kinds = {c: classify_column_name(c) for c in physical}
    strong = [c for c in physical if kinds[c] == KIND_DATA]
    numeric = [c for c in physical if kinds[c] == KIND_NUMBER]

    renamed: list[str] = []
    if declared:
        declared_set = set(declared)
        renamed = [
            c
            for c in physical
            if c not in declared_set and strip_dedup_suffix(c) not in declared_set
        ]
        declared_kinds = [classify_column_name(c) for c in declared]
        declared_clean = not any(_is_placeholder(c) for c in declared) and not any(
            k in (KIND_DATA, KIND_NUMBER) for k in declared_kinds
        )
        renamed_strong = [c for c in renamed if kinds[c] == KIND_DATA]
        renamed_numeric = [c for c in renamed if kinds[c] == KIND_NUMBER]
        if (
            declared_clean
            and len(renamed) >= max(2, round(0.25 * len(physical)))
            and (renamed_strong or len(renamed_numeric) >= 2)
        ):
            return HeaderVerdict(
                rule="renamed_from_declared",
                data_names=renamed_strong + renamed_numeric,
                renamed=renamed,
                physical_count=len(physical),
                declared_count=len(declared),
            )

    dataish = strong + numeric
    if len(strong) >= 2 or (len(dataish) >= 2 and len(dataish) * 2 >= len(physical)):
        return HeaderVerdict(
            rule="data_in_header",
            data_names=[c for c in physical if c in set(dataish)],
            renamed=renamed,
            physical_count=len(physical),
            declared_count=len(declared),
        )
    return None


class HeaderFromDataDetector(Detector):
    """Los nombres de columna de la tabla física son valores de una fila de datos.

    CRITICAL: una tabla así responde con columnas que no existen en la fuente y,
    cuando venía de un CSV por chunks, con una fracción de las filas. El sandbox
    ya se niega a consultar tablas con un hallazgo crítico abierto.

    En post-parse lee las columnas físicas de `ctx.metadata["physical_columns"]`
    (las pone `_finalize_cached_dataset` leyendo la tabla recién escrita); en el
    barrido retrospectivo `materialized_columns` ya son las físicas.
    """

    name = "header_from_data"
    version = "1"
    severity = Severity.CRITICAL

    def __init__(self, severity: Severity | None = None) -> None:
        if severity is not None:
            self.severity = severity

    @staticmethod
    def _physical(ctx: ResourceContext) -> list[str]:
        meta = ctx.metadata.get("physical_columns") if ctx.metadata else None
        if meta:
            return [str(c) for c in meta]
        return [str(c) for c in (ctx.materialized_columns or [])]

    def applicable_to(self, ctx: ResourceContext) -> bool:
        return bool(self._physical(ctx))

    def run(self, ctx: ResourceContext, mode: Mode) -> Finding | None:
        physical = self._physical(ctx)
        verdict = evaluate_header(physical, parse_columns_json(ctx.columns_json))
        if verdict is None:
            return None
        sample = ", ".join(verdict.data_names[:3])
        if verdict.rule == "renamed_from_declared":
            message = (
                f"{len(verdict.renamed)} de {verdict.physical_count} columnas de la tabla no "
                f"están en el encabezado declarado y sus nombres son datos ({sample}): "
                "el colector promovió filas de datos a encabezado"
            )
        else:
            message = (
                f"{len(verdict.data_names)} de {verdict.physical_count} nombres de columna "
                f"son valores de datos ({sample})"
            )
        return self._finding(
            mode=mode,
            payload={
                "rule": verdict.rule,
                "data_names": [n[:80] for n in verdict.data_names[:8]],
                "renamed_count": len(verdict.renamed),
                "physical_count": verdict.physical_count,
                "declared_count": verdict.declared_count,
                "sample_physical": [str(c)[:80] for c in physical[:10]],
            },
            message=message,
            # Re-descargar no cambia nada: el archivo está bien, lo leímos mal.
            should_redownload=False,
        )
