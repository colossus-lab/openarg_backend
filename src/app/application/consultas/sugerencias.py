"""Cuando un pedido da cero filas: por qué, y qué valores existen.

El pipeline viejo tenía esto (``discover_values_node`` y
``_is_effectively_empty`` en ``pipeline/subgraphs/nl2sql.py``): una consulta
sin filas se re-apuntaba a los valores reales de la columna, acotados por los
demás filtros. El agente que atiende en prod desde el 03-oct lo perdió: un
filtro sin coincidencias en ``calcular`` devolvía ``[{'valor': 0}]`` o
``None`` como dato citable, y ``obtener_datos`` (MCP y agente) "La consulta no
devolvió filas" sin ninguna pista.

Lo que se hace acá, sin LLM:

- si había período, se mira si la columna de fecha tiene fechas reconocibles
  (si no, es un error explícito, no "0 filas") y qué rango cubre;
- por cada filtro de texto se buscan valores parecidos: un ``GROUP BY``
  acotado (LIMIT, timeout corto) que respeta los DEMÁS filtros —como el
  legacy—, en tablas de menos de un millón de filas; en las grandes, los
  valores frecuentes de ``pg_stats``, que cuestan milisegundos.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Any

from app.application.consultas.fechas import (
    ColumnaFecha,
    condiciones_periodo,
    consulta_rango,
)
from app.application.consultas.filtros import Filter, es_columna_de_texto, sql_filtro
from app.application.consultas.preparar import (
    TOLERANTE_MAX_FILAS,
    ejecutar,
    estadisticas,
)
from app.application.consultas.sql import CatalogRequestError, Params, quote_ident
from app.application.consultas.texto import plegar_suelto
from app.domain.ports.sandbox.sql_sandbox import TableValueStats
from app.domain.value_objects.table_reference import quote_qualified

logger = logging.getLogger(__name__)

MAX_SUGERENCIAS = 5
_MAX_CANDIDATOS = 200
_TIMEOUT_SONDEO_S = 5
# Con 0,6 «Comunicaciones» salía como parecido a «Educación» (staging, 04-oct).
_PARECIDO_MINIMO = 0.7


@dataclass
class Diagnostico:
    """Lo que se le dice al modelo o al usuario en vez de "0 filas"."""

    aviso: str
    # columna → [{"valor": ..., "filas": n}] (filas puede ser None si sale de pg_stats).
    sugerencias: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    rango: dict[str, Any] | None = None


def ordenar_parecidos(
    pedidos: Iterable[str], candidatos: list[tuple[str, int | None]], limite: int = MAX_SUGERENCIAS
) -> tuple[list[tuple[str, int | None]], bool]:
    """Los candidatos más parecidos a lo pedido; si ninguno se parece, los más frecuentes.

    Devuelve ``(lista, se_parecen)``. "Parecido": igual sin acentos ni
    puntuación, uno contenido en el otro («Educación» en «Educación y
    Cultura»), palabras en común o una distancia de edición chica.
    """
    buscados = [b for b in (plegar_suelto(p) for p in pedidos) if b]
    puntuados: list[tuple[float, int, str, int | None]] = []
    for valor, filas in candidatos:
        c = plegar_suelto(valor)
        if not c:
            continue
        mejor = 0.0
        for q in buscados:
            if c == q:
                puntaje = 1.0
            elif len(q) >= 3 and (q in c or c in q):
                puntaje = 0.9
            else:
                qp, cp = set(q.split()), set(c.split())
                jaccard = len(qp & cp) / len(qp | cp) if qp | cp else 0.0
                puntaje = max(jaccard * 0.85, SequenceMatcher(None, q, c).ratio())
            mejor = max(mejor, puntaje)
        puntuados.append((mejor, filas or 0, valor, filas))
    parecidos = [p for p in puntuados if p[0] >= _PARECIDO_MINIMO]
    if parecidos:
        parecidos.sort(key=lambda p: (-p[0], -p[1]))
        return [(p[2], p[3]) for p in parecidos[:limite]], True
    puntuados.sort(key=lambda p: -p[1])
    return [(p[2], p[3]) for p in puntuados[:limite]], False


def _lista(valores: list[tuple[str, int | None]]) -> str:
    partes = []
    for valor, filas in valores:
        texto = f"«{valor[:120]}»"
        if filas:
            texto += f" ({filas} filas)" if filas != 1 else " (1 fila)"
        partes.append(texto)
    return ", ".join(partes)


async def _candidatos(
    sandbox: Any,
    *,
    tabla: str,
    filtro: Filter,
    otras: list[str],
    params: Params,
    stats: TableValueStats | None,
    grande: bool,
) -> tuple[list[tuple[str, int | None]], bool | None]:
    """Valores existentes de la columna del filtro y si sin él hay filas.

    Devuelve ``(candidatos, hay_filas_sin_el_filtro)``; el segundo es None si
    no se pudo saber (tabla grande o sondeo fallido).
    """
    if not grande:
        ident = quote_ident(filtro.columna)
        where = " AND ".join([*otras, f"{ident} IS NOT NULL"])
        result = await ejecutar(
            sandbox,
            f"SELECT {ident}::text AS valor, count(*) AS filas FROM {quote_qualified(tabla)} "
            f"WHERE {where} GROUP BY 1 ORDER BY 2 DESC LIMIT {_MAX_CANDIDATOS}",
            params.values,
            timeout_seconds=_TIMEOUT_SONDEO_S,
        )
        if not result.error:
            filas: list[tuple[str, int | None]] = [
                (str(r["valor"]), int(r["filas"]))
                for r in result.rows
                if r.get("valor") is not None
            ]
            return filas, bool(filas)
        logger.info("consultas: sondeo de valores falló en %s: %s", tabla, result.error)
    columna = stats.columns.get(filtro.columna) if stats else None
    if columna is None:
        return [], None
    total = stats.estimated_rows if stats else None
    frecuentes: list[tuple[str, int | None]] = []
    for i, valor in enumerate(columna.most_common_vals):
        freq = columna.most_common_freqs[i] if i < len(columna.most_common_freqs) else None
        frecuentes.append((valor, round(freq * total) if freq and total else None))
    return frecuentes, None


async def diagnosticar_vacio(
    sandbox: Any,
    *,
    tabla: str,
    tipos: Mapping[str, str],
    filtros: list[Filter],
    fecha: ColumnaFecha | None = None,
    desde: str | None = None,
    hasta: str | None = None,
    tolerante: bool = True,
    formatos: Mapping[str, str | None] | None = None,
    stats: TableValueStats | None = None,
    filas_estimadas: int | None = None,
) -> Diagnostico:
    """Explica un resultado vacío. Lanza ``CatalogRequestError`` si el período no se puede filtrar.

    ``filtros`` son los mismos (ya preparados) con que se armó la consulta.
    """
    partes: list[str] = []
    rango: dict[str, Any] | None = None
    if fecha is not None and (desde or hasta):
        result = await ejecutar(sandbox, consulta_rango(quote_qualified(tabla), fecha), {})
        if not result.error and result.rows:
            fila = result.rows[0]
            if fila.get("con_valor") and not fila.get("reconocidas"):
                raise CatalogRequestError(await _sin_fechas(sandbox, tabla, fecha))
            if fila.get("reconocidas"):
                rango = {"columna": fecha.nombre, "desde": fila["desde"], "hasta": fila["hasta"]}
                partes.append(
                    f"La columna de fecha «{fecha.nombre}» va de {fila['desde']} a "
                    f"{fila['hasta']}; revisá el período pedido."
                )
            elif not fila.get("con_valor"):
                partes.append(f"La columna de fecha «{fecha.nombre}» está vacía.")

    de_texto = [
        f
        for f in filtros
        if f.operador in ("=", "en", "contiene") and es_columna_de_texto(tipos.get(f.columna, ""))
    ]
    grande = bool(filas_estimadas and filas_estimadas >= TOLERANTE_MAX_FILAS)
    if de_texto and stats is None:
        stats = await estadisticas(sandbox, tabla, [f.columna for f in de_texto])
    sugerencias: dict[str, list[dict[str, Any]]] = {}
    for filtro in de_texto:
        params = Params()
        otras = [
            sql_filtro(o, tipos, params, tolerante=tolerante, formatos=formatos)
            for o in filtros
            if o is not filtro
        ]
        if fecha is not None:
            otras.extend(condiciones_periodo(fecha, desde, hasta, params))
        candidatos, hay_sin_el = await _candidatos(
            sandbox,
            tabla=tabla,
            filtro=filtro,
            otras=otras,
            params=params,
            stats=stats,
            grande=grande,
        )
        if hay_sin_el is False and otras:
            partes.append(
                f"Aun sin el filtro sobre «{filtro.columna}» no hay filas: el problema está en "
                "los otros filtros o en el período."
            )
            continue
        if not candidatos:
            continue
        elegidos, parecidos = ordenar_parecidos(filtro.valores, candidatos)
        pedido = " / ".join(f"«{v}»" for v in filtro.valores)
        verbo = "con" if filtro.operador == "contiene" else "igual a"
        if parecidos:
            partes.append(
                f"En «{filtro.columna}» no hay valores {verbo} {pedido}. "
                f"Valores parecidos: {_lista(elegidos)}."
            )
        else:
            partes.append(
                f"En «{filtro.columna}» no hay valores {verbo} {pedido} ni parecidos. "
                f"Algunos de los que hay: {_lista(elegidos)}."
            )
        sugerencias[filtro.columna] = [{"valor": v, "filas": n} for v, n in elegidos]

    if not filtros and not (desde or hasta):
        aviso = "La tabla no tiene filas."
    else:
        aviso = " ".join(["Ninguna fila cumple los filtros pedidos.", *partes])
    return Diagnostico(aviso=aviso, sugerencias=sugerencias, rango=rango)


async def _sin_fechas(sandbox: Any, tabla: str, fecha: ColumnaFecha) -> str:
    ident = quote_ident(fecha.nombre)
    result = await ejecutar(
        sandbox,
        f"SELECT {ident}::text AS v FROM {quote_qualified(tabla)} WHERE {ident} IS NOT NULL LIMIT 3",
        {},
    )
    ejemplos = (
        ", ".join(f"«{r['v']}»" for r in result.rows if r.get("v")) if not result.error else ""
    )
    detalle = f" (por ejemplo {ejemplos})" if ejemplos else ""
    return (
        f"La columna «{fecha.nombre}» no tiene fechas en un formato que reconozca{detalle}, "
        "así que no puedo filtrar por período. Pedí las filas sin `desde`/`hasta`, filtrá "
        "con `filtros`, o elegí otra columna con `columna_fecha`."
    )
