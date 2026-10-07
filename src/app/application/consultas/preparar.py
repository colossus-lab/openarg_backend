"""Lo que hay que leer del sandbox antes de armar una consulta.

Dos cosas que el SQL no puede decidir solo:

- el formato de los números guardados como texto (``numeros``): se mira una
  muestra de la columna (``pg_stats`` más las primeras 200 filas no nulas) y,
  en las tablas chicas, se confirma en la columna entera;
- si conviene plegar mayúsculas y acentos fila por fila (``filtros``): en una
  tabla de 6 M de filas ``lower(translate(...))`` tarda 8,7 s y en 8,4 M pasa
  el timeout del sandbox (medido en staging el 04-oct). En las grandes se usa
  el valor real que ya conoce ``pg_stats``, o la igualdad exacta.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field, replace
from typing import Any

from app.application.consultas.fechas import (
    CASE_ANIO,
    ColumnaFecha,
    aviso_lectura_fecha,
    consulta_rango,
    es_tipo_fecha,
    fecha_iso,
    formato_uniforme,
    lectura_de,
    rama_fecha,
)
from app.application.consultas.filtros import (
    OPERADORES_DE_TEXTO,
    Filter,
    columnas_numericas,
    es_columna_de_texto,
)
from app.application.consultas.numeros import (
    ESPACIOS,
    PerfilNumerico,
    confirmar_formato,
    expresion_ambiguo,
    expresion_otro_formato,
    perfil_columna,
)
from app.application.consultas.sql import CatalogRequestError, quote_ident
from app.application.consultas.texto import plegar
from app.domain.ports.sandbox.sql_sandbox import SandboxResult, TableValueStats
from app.domain.value_objects.table_reference import quote_qualified

logger = logging.getLogger(__name__)

# Desde cuántas filas no se pliega cada fila (ver el docstring del módulo).
# Hasta ahí también se confirma en la columna entera el formato de números
# que decidió la muestra (`numeros.confirmar_formato`).
TOLERANTE_MAX_FILAS = 1_000_000
MUESTRA_NUMEROS = 200
# La cuenta de la confirmación: si no termina, vale lo que decidió la muestra.
TIMEOUT_CONFIRMAR_S = 5
# Con menos fechas reconocidas que esto, `describir_tabla` lo avisa: las demás
# quedan fuera de cualquier período (revisión independiente del 05-oct, H003:
# en la Pauta publicitaria de CABA se reconocían 936 de 3.655 y sólo se
# avisaba con cero).
MIN_RECONOCIDAS = 0.95


async def ejecutar(
    sandbox: Any, sql: str, params: Mapping[str, Any], timeout_seconds: int | None = None
) -> SandboxResult:
    """``execute_readonly`` con parámetros ligados (SQL armado por nuestro código)."""
    if timeout_seconds is None:
        result: SandboxResult = await sandbox.execute_readonly(sql, params=dict(params))
    else:
        result = await sandbox.execute_readonly(
            sql, timeout_seconds=timeout_seconds, params=dict(params)
        )
    return result


async def estadisticas(sandbox: Any, tabla: str, columnas: Iterable[str]) -> TableValueStats | None:
    """``pg_stats`` de las columnas, o None si el sandbox no las da o falla."""
    getter = getattr(sandbox, "get_value_stats", None)
    if getter is None:
        return None
    try:
        stats: TableValueStats | None = await getter(tabla, list(dict.fromkeys(columnas)))
    except Exception:
        logger.warning(
            "consultas: no se pudieron leer las estadísticas de %s", tabla, exc_info=True
        )
        return None
    return stats


def filas_estimadas(stats: TableValueStats | None, row_count: int | None) -> int | None:
    """Filas de la tabla según Postgres; si no las sabe, según el catálogo.

    Un ``row_count`` 0 del catálogo no cuenta: en staging lo tiene el 58 % de
    las tablas listas, aunque tengan filas.
    """
    if stats is not None and stats.estimated_rows and stats.estimated_rows > 0:
        return int(stats.estimated_rows)
    return int(row_count) if row_count else None


def es_tolerante(filas: int | None) -> bool:
    return filas is None or filas < TOLERANTE_MAX_FILAS


def resolver_canonicos(
    filtros: list[Filter], tipos: Mapping[str, str], stats: TableValueStats | None
) -> list[Filter]:
    """A cada igualdad de texto le agrega los valores reales que le corresponden.

    «educacion y cultura» → «Educación y Cultura», si ``pg_stats`` lo tiene
    entre los valores frecuentes. En tablas chicas sólo sirve para contarle
    al usuario con qué valor se comparó; en las grandes se suma a lo pedido
    en la igualdad exacta (``filtros.sql_filtro``), nunca lo reemplaza: un
    valor pedido que no está entre los frecuentes existe igual.
    """
    if stats is None:
        return filtros
    resueltos: list[Filter] = []
    for f in filtros:
        columna = stats.columns.get(f.columna)
        if (
            f.operador not in ("=", "!=", "en")
            or columna is None
            or not columna.most_common_vals
            or not es_columna_de_texto(tipos.get(f.columna, "text"))
        ):
            resueltos.append(f)
            continue
        pedidos = {plegar(v) for v in f.valores}
        canonicos = tuple(
            dict.fromkeys(v for v in columna.most_common_vals if plegar(v) in pedidos)
        )
        resueltos.append(replace(f, canonicos=canonicos) if canonicos else f)
    return resueltos


async def _contar_otro_formato(sandbox: Any, tabla: str, columna: str, formato: str) -> int | None:
    """Cuántos valores de la columna entera sólo se leen en el otro formato.

    None si la cuenta falla o pasa ``TIMEOUT_CONFIRMAR_S``.
    """
    try:
        result = await ejecutar(
            sandbox,
            f"SELECT count(*) AS del_otro FROM {quote_qualified(tabla)} "
            f"WHERE {expresion_otro_formato(columna, formato)}",
            {},
            timeout_seconds=TIMEOUT_CONFIRMAR_S,
        )
    except Exception:
        logger.warning(
            "consultas: no se pudo confirmar el formato de %s; vale el de la muestra",
            columna,
            exc_info=True,
        )
        return None
    if result.error or not result.rows or result.rows[0].get("del_otro") is None:
        logger.warning(
            "consultas: no se pudo confirmar el formato de %s; vale el de la muestra: %s",
            columna,
            result.error,
        )
        return None
    return int(result.rows[0]["del_otro"])


async def _buscar_ambiguos(sandbox: Any, tabla: str, columna: str) -> tuple[str, ...] | None:
    """Hasta tres números ambiguos de la columna entera; None si la búsqueda falla.

    Sólo corre si la columna mezcla los dos formatos y la muestra no trajo
    ambiguos (`numeros.confirmar_formato`): ahí hace falta saber si hay alguno.
    """
    try:
        result = await ejecutar(
            sandbox,
            f"SELECT {quote_ident(columna)}::text AS ambiguo FROM {quote_qualified(tabla)} "
            f"WHERE {expresion_ambiguo(columna, 'text')} LIMIT 3",
            {},
            timeout_seconds=TIMEOUT_CONFIRMAR_S,
        )
    except Exception:
        logger.warning("consultas: no se pudieron buscar ambiguos en %s", columna, exc_info=True)
        return None
    if result.error:
        logger.warning("consultas: no se pudieron buscar ambiguos en %s: %s", columna, result.error)
        return None
    valores = (str(row.get("ambiguo")).strip(ESPACIOS) for row in result.rows)
    return tuple(dict.fromkeys(valores))


async def perfiles_numericos(
    sandbox: Any,
    tabla: str,
    columnas: Mapping[str, str],
    stats: TableValueStats | None,
    filas: int | None = None,
) -> dict[str, PerfilNumerico]:
    """El formato de cada columna de texto que se va a leer como número.

    ``filas``: las de la tabla (``filas_estimadas``). Si es chica, o no se
    sabe, lo que decidió la muestra se confirma en la columna entera.
    """
    perfiles: dict[str, PerfilNumerico] = {}
    for columna, tipo in columnas.items():
        if not es_columna_de_texto(tipo):
            continue
        valores: list[Any] = []
        st = stats.columns.get(columna) if stats else None
        if st is not None:
            valores.extend(st.most_common_vals)
            valores.extend(st.histogram_bounds)
        ident = quote_ident(columna)
        result = await ejecutar(
            sandbox,
            f"SELECT {ident}::text AS v FROM {quote_qualified(tabla)} "
            f"WHERE {ident} IS NOT NULL LIMIT {MUESTRA_NUMEROS}",
            {},
        )
        if not result.error:
            valores.extend(row.get("v") for row in result.rows)
        perfil = perfil_columna(columna, valores)
        if perfil.formato is not None and es_tolerante(filas):
            del_otro = await _contar_otro_formato(sandbox, tabla, columna, perfil.formato)
            ambiguos: tuple[str, ...] | None = None
            if del_otro and not perfil.ambiguos:
                ambiguos = await _buscar_ambiguos(sandbox, tabla, columna)
            perfil = confirmar_formato(perfil, del_otro, ambiguos)
        perfiles[columna] = perfil
    return perfiles


@dataclass(frozen=True)
class Periodo:
    desde: str | None = None
    hasta: str | None = None
    aviso: str | None = None
    # El rango sale de la muestra de `pg_stats`, no de recorrer la tabla: el
    # máximo real puede ser posterior a `hasta`.
    aproximado: bool = False
    # Valores no nulos de la columna que no se reconocieron como fecha: quedan
    # fuera de `desde`/`hasta` (en los transportes autorizados de CABA, los
    # bimestres como «2016 NOVIEMBRE-DICIEMBRE").
    sin_reconocer: int = 0


async def describir_periodo(
    sandbox: Any, tabla: str, fecha: ColumnaFecha | None, timeout_seconds: int | None = None
) -> Periodo:
    """El período que cubre la columna de fecha, para ``describir_tabla``.

    Nunca falla: si la consulta del rango no corre (timeout en una tabla
    grande, palabra reservada en el nombre de una columna), se describe la
    tabla igual, con el aviso. Antes cualquier error acá era el 400 genérico
    "Probá con otros filtros" del modo datos (16 en prod desde el 30-sep).

    ``timeout_seconds``: un tope más corto que el del sandbox, para quien lo
    usa como dato de un aviso (``calcular``); al pasarlo vale el rango de la
    muestra, como con cualquier error.
    """
    if fecha is None:
        return Periodo()
    stats = await estadisticas(sandbox, tabla, [fecha.nombre])
    fecha = con_formato(fecha, stats)
    result = await ejecutar(
        sandbox, consulta_rango(quote_qualified(tabla), fecha), {}, timeout_seconds=timeout_seconds
    )
    if result.error_kind == "blocked":
        # Tabla retirada por un problema de calidad: no se informa un período
        # sacado de sus estadísticas como si se pudiera usar. Se dice por qué.
        return Periodo(aviso=result.error or "La tabla no se puede consultar.")
    if result.error or not result.rows:
        # Tabla grande: el rango exacto pasa el timeout. La muestra de
        # `pg_stats` da uno aproximado, que es mejor que nada.
        aproximado = _rango_de_muestra(fecha, stats)
        if aproximado is not None:
            return Periodo(
                desde=aproximado[0],
                hasta=aproximado[1],
                aviso=(
                    "Período aproximado (sale de una muestra de la tabla, que es muy grande "
                    "para recorrerla entera)."
                ),
                aproximado=True,
            )
        return Periodo(aviso="No pude calcular el período que cubre la tabla.")
    fila = result.rows[0]
    if fila.get("con_valor") and not fila.get("reconocidas"):
        return Periodo(
            aviso=(
                f"La columna «{fecha.nombre}» tiene fechas en un formato que no reconozco: "
                "no se puede filtrar por período (usá filtros sobre esa columna)."
            )
        )
    desde, hasta = fila.get("desde"), fila.get("hasta")
    avisos = [aviso_lectura_fecha(fecha)]
    reconocidas, con_valor = int(fila.get("reconocidas") or 0), int(fila.get("con_valor") or 0)
    if con_valor and reconocidas < MIN_RECONOCIDAS * con_valor:
        avisos.append(
            f"Reconocí como fecha {reconocidas} de {con_valor} valores de «{fecha.nombre}» "
            f"({100 * reconocidas / con_valor:.0f} %): los demás quedan fuera de "
            "`desde`/`hasta` y al final del orden."
        )
    return Periodo(
        desde=None if desde is None else str(desde),
        hasta=None if hasta is None else str(hasta),
        aviso=" ".join(a for a in avisos if a) or None,
        sin_reconocer=max(con_valor - reconocidas, 0),
    )


def _muestra_de(columna: str, stats: TableValueStats | None) -> list[str]:
    st = stats.columns.get(columna) if stats else None
    return [*st.most_common_vals, *st.histogram_bounds] if st else []


def con_formato(fecha: ColumnaFecha, stats: TableValueStats | None) -> ColumnaFecha:
    """La columna de fecha con la forma única de sus valores, si la muestra la tiene.

    En una columna de año sin forma única (un «Total» entre los años, una
    muestra de un solo valor o ninguna) se marca ``CASE_ANIO`` si la muestra
    no tiene nada más fino que un año: así un pedido de un mes sobre esa
    columna se rechaza en vez de devolver el año entero (H002). Lo mismo con
    otro nombre (`periodo`, `indice_tiempo`) si la muestra tiene algún año y
    nada más fino; sin muestra, ahí no se supone nada (revisión del PR #154).
    """
    muestra = _muestra_de(fecha.nombre, stats)
    formato = formato_uniforme(muestra, filas=stats.estimated_rows if stats else None)
    if formato is None:
        ramas = [rama_fecha(v) for v in muestra]
        if all(r in (None, "anio") for r in ramas) and (fecha.clase == "anio" or "anio" in ramas):
            formato = CASE_ANIO
    return replace(fecha, formato=formato) if formato else fecha


def _rango_de_muestra(fecha: ColumnaFecha, stats: TableValueStats | None) -> tuple[str, str] | None:
    lectura = lectura_de(fecha.formato)
    muestra = _muestra_de(fecha.nombre, stats)
    fechas = sorted(f for f in (fecha_iso(v, lectura=lectura) for v in muestra) if f)
    return (fechas[0], fechas[-1]) if fechas else None


@dataclass
class Preparado:
    filtros: list[Filter]
    tolerante: bool = True
    formatos: dict[str, str | None] = field(default_factory=dict)
    stats: TableValueStats | None = None
    filas_estimadas: int | None = None
    # Forma única de la columna de fecha (ver `fechas.formato_uniforme`).
    formato_fecha: str | None = None


async def preparar(
    sandbox: Any,
    tabla: str,
    tipos: Mapping[str, str],
    filtros: list[Filter],
    *,
    numericas: Iterable[str] = (),
    row_count: int | None = None,
    fecha: ColumnaFecha | None = None,
) -> Preparado:
    """Estadísticas, valores canónicos y formatos (números y fecha) para un pedido.

    ``filtros`` ya validados. ``numericas`` son las columnas que el cálculo
    lee como número (la de ``suma``, el ponderador). Si una columna de texto
    tiene números ambiguos, el pedido se rechaza acá, antes de consultar.
    ``fecha`` es la columna de fecha que la consulta va a filtrar u ordenar.
    """
    de_texto = [
        f.columna
        for f in filtros
        if f.operador in OPERADORES_DE_TEXTO and es_columna_de_texto(tipos.get(f.columna, ""))
    ]
    numeros = [
        c
        for c in dict.fromkeys([*numericas, *columnas_numericas(filtros)])
        if es_columna_de_texto(tipos.get(c, ""))
    ]
    # También una numérica: `periodo` o `indice_tiempo` bigint con años
    # aceptaba un pedido de junio y devolvía enero o el año entero (H002,
    # tercera revisión del PR #154). `pg_stats` da sus valores como texto.
    columna_fecha = [fecha.nombre] if fecha is not None and not es_tipo_fecha(fecha.tipo) else []
    if not de_texto and not numeros and not columna_fecha:
        return Preparado(filtros=filtros, filas_estimadas=row_count or None)
    stats = await estadisticas(sandbox, tabla, [*de_texto, *numeros, *columna_fecha])
    filas = filas_estimadas(stats, row_count)
    perfiles = await perfiles_numericos(
        sandbox, tabla, {c: tipos[c] for c in numeros}, stats, filas
    )
    for perfil in perfiles.values():
        if perfil.problema:
            raise CatalogRequestError(perfil.problema)
    return Preparado(
        filtros=resolver_canonicos(filtros, tipos, stats),
        tolerante=es_tolerante(filas),
        formatos={c: p.formato for c, p in perfiles.items()},
        stats=stats,
        filas_estimadas=filas,
        formato_fecha=con_formato(fecha, stats).formato if columna_fecha and fecha else None,
    )
