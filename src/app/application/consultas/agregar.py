"""Un agregado de punta a punta: validar, preparar, consultar y explicar el resultado.

Lo usan la herramienta ``calcular`` del agente y ``/catalogo/agregar`` (la
herramienta ``agregar_datos`` del MCP). Antes el modo datos no tenía agregados
(auditoría 3.1): para un total, el modelo del cliente traía hasta 500 filas en
CSV y las sumaba a mano, y el "Educación y Cultura" de 2026 (493 filas) ya
quedaba al borde del tope.

La consulta la arma ``aggregates.build_aggregate_query`` (identificadores del
esquema real, valores ligados, números por formato de columna). Este módulo
pone alrededor lo que cada consumidor repetía:

- leer y validar los filtros, mirar ``pg_stats`` y el formato de los números
  (``preparar``) y volver a armar la consulta con eso;
- sobre cuántas filas se calculó, de todos los grupos (no sólo los mostrados);
- si hay más grupos que ``limite`` (se piden ``limite + 1``);
- cero filas no es un valor: se explica con ``diagnosticar_vacio`` (qué
  valores existen) en vez de devolver ``valor = 0`` o ``None``.

Cada consumidor decide cómo correr el SQL (``ejecutar``): el agente convierte
un error en ``ToolInputError``, el router en un 400 con su mensaje.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field, replace
from typing import Any

from app.application.answers.aggregates import (
    COLUMNAS_DE_CONTROL,
    FILAS,
    FILAS_AMBIGUAS,
    FILAS_CON_VALOR,
    FILAS_CON_VALOR_TOTAL,
    FILAS_TOTAL,
    MAX_FILTERS,
    AggregateQuery,
    AggregateRequest,
    build_aggregate_query,
    numeric_columns,
)
from app.application.consultas.fechas import aviso_lectura_fecha
from app.application.consultas.filtros import leer_filtros, notas_de_filtros, validar_filtros
from app.application.consultas.preparar import preparar
from app.application.consultas.sugerencias import diagnosticar_vacio
from app.application.public_catalog import visible_types

__all__ = [
    "ALIAS_OPERACION",
    "EjecutarSQL",
    "PedidoAgregado",
    "ResultadoAgregado",
    "agregar",
    "normalizar_operacion",
]

logger = logging.getLogger(__name__)

# Corre SQL armado por nuestro código con sus valores ligados; levanta la
# excepción que el consumidor quiera mostrar si el sandbox lo rechaza.
EjecutarSQL = Callable[[str, Mapping[str, Any]], Awaitable[list[dict[str, Any]]]]

# Lo que un modelo cliente escribe en vez del nombre de la operación.
ALIAS_OPERACION = {
    "sum": "suma",
    "total": "suma",
    "avg": "promedio",
    "mean": "promedio",
    "media": "promedio",
    "count": "conteo",
    "contar": "conteo",
    "cantidad": "conteo",
    "min": "minimo",
    "mínimo": "minimo",
    "max": "maximo",
    "máximo": "maximo",
}


def normalizar_operacion(operacion: str | None) -> str:
    op = (operacion or "").strip().lower()
    return ALIAS_OPERACION.get(op, op)


@dataclass(frozen=True)
class PedidoAgregado:
    tabla: str  # calificada, tal como la lista el sandbox
    # [(columna, tipo)] de `get_column_types`.
    tipos: list[tuple[str, str]]
    operacion: str
    columna: str | None = None
    ponderar_por: str | None = None
    agrupar_por: list[str] = field(default_factory=list)
    # Crudos: {columna: valor} o [{columna, operador, valor|valores}].
    filtros: Any = None
    desde: str | None = None
    hasta: str | None = None
    orden: str = "desc"
    limite: int = 50
    columna_fecha: str | None = None
    ordenar_por: str | None = None
    # Filas según el catálogo (para decidir si se pliegan acentos fila por fila).
    filas_tabla: int | None = None


@dataclass
class ResultadoAgregado:
    req: AggregateRequest
    query: AggregateQuery
    # "suma de credito_devengado ponderado por pondera"
    calculo: str
    # Los grupos sin las columnas de control: las agrupadas y "valor".
    grupos: list[dict[str, Any]]
    # Filas de la tabla en cada grupo (None si el sandbox no las devolvió).
    filas_por_grupo: list[int | None]
    # Sobre cuántas filas se calculó: de todos los grupos, aunque `limite`
    # deje algunos afuera. None si el sandbox no devolvió las de control.
    filas_usadas: int | None
    filas_con_valor: int | None
    # Hay más grupos que `limite`.
    truncado: bool
    # Truncado y sin el total de todos los grupos: `filas_usadas` es sólo de
    # los grupos mostrados.
    parcial: bool
    # La columna leída como número (la del cálculo, o el ponderador del conteo).
    columna_valorada: str | None
    # Sin valor que informar: ninguna fila cumplió los filtros (`vacio`) o
    # ninguna tenía un número reconocible (`sin_numeros`). `aviso` dice por qué.
    vacio: bool = False
    sin_numeros: bool = False
    aviso: str | None = None
    sugerencias: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    # Cómo se aplicaron los filtros y el período (`filtros_aplicados`).
    notas: list[str] = field(default_factory=list)
    # Sobre el cálculo: filas sin número, grupos que no entraron.
    avisos: list[str] = field(default_factory=list)


def _filas_del_calculo(rows: list[dict[str, Any]], por_grupo: str, de_todos: str) -> int:
    """Sobre cuántas filas se calculó: el total de todos los grupos si la consulta
    lo trae (``aggregates.FILAS_TOTAL``), si no la suma de las filas recibidas."""
    if rows[0].get(de_todos) is not None:
        return int(rows[0][de_todos])
    return sum(int(r.get(por_grupo) or 0) for r in rows)


# Por qué un número ambiguo quedó afuera del cálculo. Antes esas filas se
# contaban entre las que "no tienen un número reconocible", y sí lo tienen
# (H041).
_AMBIGUAS = (
    "tienen un número que se puede leer de dos formas (como «12.500»: doce mil quinientos "
    "o doce coma cinco) y la muestra de la columna no alcanza para decidir el formato"
)


def _por_que_sin_valor(cuales: str, filas: int, ambiguas: int, valorada: str | None) -> str:
    """Por qué ``filas`` filas («las otras 24») no entraron en el cálculo, si hay ambiguas."""
    sin_numero = filas - ambiguas
    if sin_numero <= 0:
        return f"{cuales} {filas} {_AMBIGUAS}"
    return (
        f"de {cuales} {filas}, {ambiguas} {_AMBIGUAS}; {sin_numero} no tienen un número "
        f"reconocible en {valorada!r}"
    )


# Si no se pudieron contar los ambiguos (la consulta aparte falló, p. ej. por
# timeout en una tabla grande), no se sabe cuál de las dos cosas pasa: se
# dicen las dos.
_SIN_CONTAR = "o tienen uno que se puede leer de dos formas (como «12.500»)"


async def _contar_ambiguas(
    sql: str, params: Mapping[str, Any], ejecutar: EjecutarSQL, maximo: int
) -> int | None:
    """Cuántas de las ``maximo`` filas sin valor tienen un número ambiguo.

    None si la consulta falla: el cálculo ya está hecho y no se lo pierde por
    un dato del aviso.
    """
    try:
        rows = await ejecutar(sql, params)
    except Exception:
        logger.warning("consultas: no se pudieron contar los números ambiguos", exc_info=True)
        return None
    return min(int(rows[0].get(FILAS_AMBIGUAS) or 0), maximo) if rows else 0


def _descripcion(req: AggregateRequest) -> str:
    what = req.operacion + (f" de {req.columna}" if req.columna else "")
    if req.ponderar_por:
        what += f" ponderado por {req.ponderar_por}"
    return what


async def agregar(sandbox: Any, pedido: PedidoAgregado, ejecutar: EjecutarSQL) -> ResultadoAgregado:
    """Calcula un agregado. Levanta ``CatalogRequestError`` si el pedido no es válido.

    Los errores del sandbox (timeout, tabla bloqueada, rechazo del validador)
    los levanta ``ejecutar``, con la forma que quiera el consumidor.
    """
    tipos = pedido.tipos
    filtros = validar_filtros(
        leer_filtros(pedido.filtros, MAX_FILTERS),
        visible_types([c for c, _ in tipos], tipos),
    )
    req = AggregateRequest(
        table=pedido.tabla,
        column_types=tipos,
        operacion=normalizar_operacion(pedido.operacion),
        columna=pedido.columna,
        ponderar_por=pedido.ponderar_por,
        agrupar_por=[str(g) for g in pedido.agrupar_por],
        filtros=filtros,
        desde=pedido.desde,
        hasta=pedido.hasta,
        orden=pedido.orden,
        limite=pedido.limite,
        columna_fecha=pedido.columna_fecha,
        ordenar_por=pedido.ordenar_por,
    )
    # Primero valida (puro); después mira el formato de los números y las
    # estadísticas de los filtros, y arma la consulta definitiva.
    query = build_aggregate_query(req)
    prep = await preparar(
        sandbox,
        pedido.tabla,
        query.tipos,
        query.filtros,
        numericas=numeric_columns(req),
        row_count=pedido.filas_tabla,
        fecha=query.fecha if (query.desde or query.hasta) else None,
    )
    req = replace(
        req,
        filtros=prep.filtros,
        tolerante=prep.tolerante,
        formatos=prep.formatos,
        formato_fecha=prep.formato_fecha,
    )
    query = build_aggregate_query(req)
    rows = await ejecutar(query.sql, query.params)

    truncado = len(rows) > query.limite
    rows = rows[: query.limite]
    # Sin las columnas de control (un sandbox de prueba que no las devuelve)
    # se sigue como antes: no se sabe cuántas filas entraron.
    controlado = bool(rows) and FILAS in rows[0]
    total = _filas_del_calculo(rows, FILAS, FILAS_TOTAL) if controlado else None
    con_valor = (
        _filas_del_calculo(rows, FILAS_CON_VALOR, FILAS_CON_VALOR_TOTAL)
        if controlado and FILAS_CON_VALOR in rows[0]
        else None
    )
    # Con más grupos que `limite` y sin el total de todos los grupos (un
    # sandbox que no lo devuelve), la suma es sólo de los grupos mostrados:
    # no se la presenta como el total del cálculo.
    parcial = truncado and not (rows and rows[0].get(FILAS_TOTAL) is not None)
    # Las filas con un número ambiguo se cuentan aparte y sólo si faltan filas
    # con valor (ver `AggregateQuery.sql_ambiguas`). Con `parcial`, las filas
    # con valor son de los grupos mostrados y la cuenta sería de todos.
    ambiguas: int | None = 0
    if (
        query.sql_ambiguas is not None
        and total
        and con_valor is not None
        and con_valor < total
        and not parcial
    ):
        ambiguas = await _contar_ambiguas(
            query.sql_ambiguas, query.params, ejecutar, total - con_valor
        )
    notas = notas_de_filtros(query.filtros, query.tipos, tolerante=prep.tolerante)
    aviso_fecha = aviso_lectura_fecha(query.fecha) if (query.desde or query.hasta) else None
    if aviso_fecha:
        notas.append(aviso_fecha)
    valorada = req.ponderar_por if req.operacion == "conteo" else req.columna
    resultado = ResultadoAgregado(
        req=req,
        query=query,
        calculo=_descripcion(req),
        grupos=[],
        filas_por_grupo=[],
        filas_usadas=total,
        filas_con_valor=con_valor,
        truncado=truncado,
        parcial=parcial,
        columna_valorada=valorada,
        notas=notas,
    )

    if not rows or total == 0:
        # Ninguna fila cumplió los filtros: no hay valor que citar. Antes
        # salía `valor: 0` (conteo) o `None` (suma) como un dato más.
        diag = await diagnosticar_vacio(
            sandbox,
            tabla=pedido.tabla,
            tipos=query.tipos,
            filtros=query.filtros,
            fecha=query.fecha,
            desde=query.desde,
            hasta=query.hasta,
            tolerante=prep.tolerante,
            formatos=prep.formatos,
            stats=prep.stats,
            filas_estimadas=prep.filas_estimadas,
        )
        resultado.vacio = True
        resultado.filas_usadas = 0
        resultado.aviso = diag.aviso
        resultado.sugerencias = diag.sugerencias
        return resultado

    if con_valor == 0:
        resultado.sin_numeros = True
        if ambiguas:
            por_que = _por_que_sin_valor("las", total or 0, ambiguas, valorada)
            resultado.aviso = (
                f"Ninguna de las {total} filas que cumplen los filtros entró en el cálculo: "
                f"{por_que}. No hay valor que informar."
            )
        elif ambiguas is None:
            resultado.aviso = (
                f"Ninguna de las {total} filas que cumplen los filtros entró en el cálculo: "
                f"no tienen un número reconocible en {valorada!r} {_SIN_CONTAR}. No hay valor "
                "que informar."
            )
        else:
            resultado.aviso = (
                f"Ninguna de las {total} filas que cumplen los filtros tiene un número "
                f"reconocible en {valorada!r}: no hay valor que informar."
            )
        return resultado

    resultado.grupos = [{k: v for k, v in r.items() if k not in COLUMNAS_DE_CONTROL} for r in rows]
    resultado.filas_por_grupo = [
        (int(r[FILAS]) if r.get(FILAS) is not None else None) if controlado else None for r in rows
    ]
    if con_valor is not None and total is not None and con_valor < total:
        donde = " de los grupos mostrados" if parcial else ""
        otras = total - con_valor
        if ambiguas:
            por_que = _por_que_sin_valor("las otras", otras, ambiguas, valorada)
            resultado.avisos.append(
                f"Se calculó sobre {con_valor} de {total} filas{donde}: {por_que}."
            )
        elif ambiguas is None:
            resultado.avisos.append(
                f"Se calculó sobre {con_valor} de {total} filas{donde}: las otras {otras} no "
                f"tienen un número reconocible en {valorada!r} {_SIN_CONTAR}."
            )
        else:
            resultado.avisos.append(
                f"Se calculó sobre {con_valor} de {total} filas{donde}: las otras "
                f"{otras} no tienen un número reconocible en {valorada!r}."
            )
    if truncado:
        resultado.avisos.append(
            f"Hay más de {query.limite} grupos: se muestran los primeros {query.limite} "
            "según el orden pedido."
            + ("" if parcial else f" `filas_usadas` ({total}) es de todos los grupos.")
        )
    return resultado
