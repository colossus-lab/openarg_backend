"""Fechas: qué columna ordena la tabla y cómo filtrarla por período.

Lo que había (auditoría 4.1 y ok.1, verificado contra prod el 04-oct):

- la columna de fecha se elegía SÓLO por el nombre (``fecha``,
  ``indice_tiempo``, ``periodo`` o cualquier nombre con la subcadena
  ``date``). ``PUBLICACION_FECHA`` no se reconocía, ``anio`` tampoco, y en
  cambio ``updated_ts``/``updated_at`` (132 tablas) quedaban como "la fecha
  de la serie";
- el filtro era ``left(col::text, n) >= 'AAAA-MM'``: lexicográfico, sólo
  sirve con ISO. 743 de 4.332 columnas de texto reconocidas en prod no son ISO
  ("1/10/2017", "201801", "Junio de 2026") y ahí ``desde``/``hasta`` devolvía
  0 filas sin aviso, y ``orden=desc`` traía un "último dato" equivocado.

Ahora la fecha se normaliza EN LA CONSULTA con una expresión que entiende los
formatos conocidos y devuelve NULL para lo demás. Sin ``to_date``: con un
valor inválido ("31/02/2020") lanza un error y aborta la consulta entera.

La comparación es por solapamiento de períodos: un valor anual (2020) entra
en ``desde=2020-06`` porque el año 2020 llega hasta junio. Para eso cada valor
tiene una clave de inicio y una de fin (``2020`` → ``2020-01-01`` y
``2020-12-31``; ``2020-06`` → ``2020-06-01`` y ``2020-06-31``), todas de diez
caracteres, que se comparan bien en cualquier collation.
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime
from typing import Literal

from app.application.consultas.sql import CatalogRequestError, Params, quote_ident
from app.application.consultas.texto import plegar

Modo = Literal["iso", "inicio", "fin"]

# ── qué columna es la fecha ────────────────────────────────

# Nombres que, solos, son la fecha de la serie. `indice_tiempo` es el estándar
# de todas las series de datos.gob.ar.
_NOMBRES_FECHA = frozenset({"fecha", "indice_tiempo", "periodo", "period", "date"})
# Una palabra del nombre que dice "fecha" (fecha_inicio, PUBLICACION_FECHA).
_PALABRAS_FECHA = frozenset({"fecha", "date"})
# Columnas de año: se filtran como período anual.
_PALABRAS_ANIO = frozenset({"anio", "año", "ano", "year", "ejercicio"})
# Fechas de carga o de auditoría, no del dato: `ultima_actualizacion_fecha`
# (1.091 tablas en prod) es cuándo se publicó el archivo, no de cuándo es el
# dato; `updated_ts`/`updated_at` son de la base.
_PALABRAS_METADATO = frozenset(
    {
        "updated",
        "created",
        "modified",
        "deleted",
        "ingested",
        "inserted",
        "loaded",
        "actualizacion",
        "actualizado",
        "modificacion",
        "carga",
        "proceso",
        "procesamiento",
        "extraccion",
        "descarga",
        "ts",
        "timestamp",
    }
)
# Palabras que sugieren un período aunque no sean una fecha que sepamos
# filtrar: se nombran en el mensaje de "no hay columna de fecha".
_PALABRAS_PERIODO = frozenset({"mes", "trimestre", "semestre", "periodo", "bimestre", "dia"})

_DATE_TYPE = re.compile(r"^(date|timestamp)", re.IGNORECASE)
_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_SEPARADORES = re.compile(r"[^0-9a-zñ]+")


def _palabras(nombre: str) -> list[str]:
    return [p for p in _SEPARADORES.split(plegar(_CAMEL.sub("_", nombre))) if p]


def es_metadato(nombre: str) -> bool:
    """Una fecha de carga, actualización o auditoría (``updated_at``, ``*_ts``)."""
    palabras = _palabras(nombre)
    if not palabras:
        return False
    if palabras[-1] == "at" and len(palabras) > 1:  # created_at, updated_at
        return True
    return any(p in _PALABRAS_METADATO for p in palabras)


def es_nombre_de_fecha(nombre: str) -> bool:
    """True si el nombre dice que es la fecha de la serie (sin mirar el tipo).

    La subcadena "date" ya no alcanza: tiene que ser una palabra del nombre
    (``date``, ``start_date``), no ``candidate`` ni ``updated``.
    """
    if es_metadato(nombre):
        return False
    if plegar(nombre) in _NOMBRES_FECHA:
        return True
    return any(p in _PALABRAS_FECHA for p in _palabras(nombre))


def es_nombre_de_anio(nombre: str) -> bool:
    return not es_metadato(nombre) and any(p in _PALABRAS_ANIO for p in _palabras(nombre))


@dataclass(frozen=True)
class ColumnaFecha:
    nombre: str
    tipo: str = "text"
    # "fecha" o "anio" (informativo: la expresión es la misma).
    clase: str = "fecha"
    # La forma única de sus valores según la muestra de `pg_stats`
    # (`formato_uniforme`), o None: entonces se usa el CASE que entiende todas.
    formato: str | None = None


def resolver_columna_fecha(
    columnas: Iterable[tuple[str, str]] | Iterable[str],
    elegida: str | None = None,
) -> ColumnaFecha | None:
    """La columna que ordena la tabla en el tiempo, o None.

    Orden de prioridad: nombre exacto de fecha (``fecha``, ``indice_tiempo``,
    ``periodo``); tipo date/timestamp; una palabra "fecha"/"date" en el nombre
    (``PUBLICACION_FECHA``, ``fecha_inicio``); una columna de año (``anio``,
    ``ejercicio_presupuestario``). Las fechas de carga o auditoría nunca.

    ``elegida`` es la que pidió el usuario (``columna_fecha``): tiene que
    existir, y se usa aunque el nombre no parezca de fecha.
    """
    pares = [(c, "text") if isinstance(c, str) else (str(c[0]), str(c[1])) for c in columnas]
    if elegida:
        for nombre, tipo in pares:
            if nombre == elegida:
                clase = "anio" if es_nombre_de_anio(nombre) else "fecha"
                return ColumnaFecha(nombre, tipo, clase)
        raise CatalogRequestError(
            f"`columna_fecha` {elegida!r} no es una columna de la tabla. "
            "Usá describir_tabla para ver las disponibles."
        )
    candidatas = [(n, t) for n, t in pares if not es_metadato(n)]
    for nombre, tipo in candidatas:
        if plegar(nombre) in _NOMBRES_FECHA:
            return ColumnaFecha(nombre, tipo)
    for nombre, tipo in candidatas:
        if _DATE_TYPE.match(tipo or ""):
            return ColumnaFecha(nombre, tipo)
    for nombre, tipo in candidatas:
        if any(p in _PALABRAS_FECHA for p in _palabras(nombre)):
            return ColumnaFecha(nombre, tipo)
    for nombre, tipo in candidatas:
        if es_nombre_de_anio(nombre):
            return ColumnaFecha(nombre, tipo, "anio")
    return None


def sin_columna_fecha(nombres: Iterable[str]) -> str:
    """El mensaje para un pedido con período sobre una tabla sin fecha reconocida.

    Antes decía siempre "Esta tabla no tiene una columna de fecha", falso en
    miles de tablas: ahora nombra lo que hay.
    """
    nombres = list(nombres)
    metadatos = [n for n in nombres if es_metadato(n) and _parece_temporal(n)]
    periodo = [
        n
        for n in nombres
        if n not in metadatos and any(p in _PALABRAS_PERIODO for p in _palabras(n))
    ]
    partes = ["No reconocí una columna de fecha o de año para filtrar por período en esta tabla."]
    if periodo:
        partes.append(
            "Columnas que podrían indicar el período: "
            + ", ".join(periodo[:5])
            + ". Elegí una con `columna_fecha` si tiene fechas, o filtrala con `filtros`."
        )
    if metadatos:
        partes.append(
            "No uso como fecha del dato las de carga o actualización ("
            + ", ".join(metadatos[:3])
            + "); si es lo que buscás, pasala en `columna_fecha`."
        )
    if not periodo and not metadatos:
        partes.append("Pedí las filas sin `desde`/`hasta` o filtrá con `filtros`.")
    return " ".join(partes)


def _parece_temporal(nombre: str) -> bool:
    palabras = _palabras(nombre)
    return (palabras[-1:] == ["at"]) or any(
        p in _PALABRAS_FECHA | {"ts", "timestamp"} for p in palabras
    )


# ── la expresión que normaliza ─────────────────────────────

_MESES = (
    ("ene", "01"),
    ("feb", "02"),
    ("mar", "03"),
    ("abr", "04"),
    ("may", "05"),
    ("jun", "06"),
    ("jul", "07"),
    ("ago", "08"),
    ("sep", "09"),
    ("set", "09"),
    ("oct", "10"),
    ("nov", "11"),
    ("dic", "12"),
)
_MES_ABREV = dict(_MESES)

# Las mismas expresiones regulares en SQL (ARE de Postgres) y en Python.
RE_ISO_DIA = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}"
RE_ISO_MES = r"^[0-9]{4}-[0-9]{2}$"
RE_AAAA_MM_DD = r"^[0-9]{4}/[0-9]{1,2}/[0-9]{1,2}$"
RE_AAAA_MM = r"^[0-9]{4}/[0-9]{1,2}$"
RE_DMY = r"^(0?[1-9]|[12][0-9]|3[01])[/-](0?[1-9]|1[0-2])[/-][0-9]{4}([^0-9]|$)"
RE_AAAAMMDD = r"^(1[89]|20)[0-9]{2}(0[1-9]|1[0-2])(0[1-9]|[12][0-9]|3[01])$"
RE_AAAAMM = r"^(1[89]|20)[0-9]{2}(0[1-9]|1[0-2])$"
RE_ANIO = r"^(1[89]|20)[0-9]{2}(\.0+)?$"
RE_MES_NOMBRE = (
    r"^(ene|feb|mar|abr|may|jun|jul|ago|sep|set|oct|nov|dic)[a-z]*\.?"
    r"(\s+de\s+|\s+del\s+|\s*[-/]?\s*)(1[89]|20)[0-9]{2}$"
)
# "2018 SEPTIEMBRE" (año primero), visto en tablas de staging.
RE_ANIO_MES_NOMBRE = (
    r"^(1[89]|20)[0-9]{2}[ /-]+(ene|feb|mar|abr|may|jun|jul|ago|sep|set|oct|nov|dic)[a-z]*\.?$"
)


# Cada forma reconocida, en el orden en que se prueba (el mismo del CASE).
_RAMAS: tuple[tuple[str, str, int], ...] = (
    ("iso_dia", RE_ISO_DIA, 0),
    ("iso_mes", RE_ISO_MES, 0),
    ("aaaa_mm_dd", RE_AAAA_MM_DD, 0),
    ("aaaa_mm", RE_AAAA_MM, 0),
    ("dmy", RE_DMY, 0),
    ("aaaammdd", RE_AAAAMMDD, 0),
    ("aaaamm", RE_AAAAMM, 0),
    ("anio", RE_ANIO, 0),
    ("mes_nombre", RE_MES_NOMBRE, re.IGNORECASE),
    ("anio_mes_nombre", RE_ANIO_MES_NOMBRE, re.IGNORECASE),
)
# Formas con una expresión directa, sin el CASE de expresiones regulares.
FORMATOS_RAPIDOS = frozenset({"iso_dia", "iso_mes", "dmy", "aaaammdd", "aaaamm", "anio"})
_MIN_MUESTRA_FORMATO = 3
# En una tabla grande con una forma que domina la muestra (la de molinetes del
# subte, 8,4 M de filas: 200 valores d/m/aaaa y un "8/20/2025"), el CASE pasa
# el timeout. Ahí se usa la forma dominante con su guarda (una sola expresión
# regular por fila): los valores de otra forma quedan en NULL, no mal leídos.
# Se marca con este sufijo en `ColumnaFecha.formato`.
GUARDADO = "*"
_DOMINANTE = 0.9
FILAS_TABLA_GRANDE = 1_000_000


def _texto_de(valor: object) -> str | None:
    if valor is None or isinstance(valor, bool):
        return None
    if isinstance(valor, float):
        texto = str(int(valor)) if valor.is_integer() else str(valor)
    else:
        texto = str(valor).strip()
    return texto or None


def rama_fecha(valor: object) -> str | None:
    """Qué forma de fecha tiene un valor (``iso_dia``, ``dmy``, ``anio``…), o None."""
    if isinstance(valor, datetime | date):
        return "iso_dia"
    texto = _texto_de(valor)
    if texto is None:
        return None
    for nombre, patron, flags in _RAMAS:
        if re.match(patron, texto, flags):
            return nombre
    return None


def formato_uniforme(valores: Iterable[object], filas: int | None = None) -> str | None:
    """La forma de fecha de una columna si la muestra tiene una sola, o None.

    La muestra sale de ``pg_stats`` (el ANALYZE de Postgres toma filas al azar
    de toda la tabla). Con una sola forma se usa una expresión directa en vez
    del CASE: en tablas de 6 a 8 M de filas el CASE pasaba el timeout de 10 s
    del sandbox donde el ``left()`` de antes tardaba de 3 a 9 s (medido en
    staging el 04-oct).

    Con ``filas`` de una tabla grande y una forma que domina (90 %), devuelve
    esa forma con el sufijo ``GUARDADO`` (ver arriba).
    """
    ramas = [rama_fecha(v) for v in valores if _texto_de(v) is not None]
    if len(ramas) < _MIN_MUESTRA_FORMATO:
        return None
    if len(set(ramas)) == 1:
        unica = ramas[0]
        return unica if unica in FORMATOS_RAPIDOS else None
    if filas is None or filas < FILAS_TABLA_GRANDE:
        return None
    dominante, veces = Counter(ramas).most_common(1)[0]
    if dominante in FORMATOS_RAPIDOS and veces / len(ramas) >= _DOMINANTE:
        return dominante + GUARDADO
    return None


def _sufijo(modo: Modo, nivel: str) -> str:
    if modo == "iso":
        return ""
    if nivel == "mes":
        return " || '-01'" if modo == "inicio" else " || '-31'"
    return " || '-01-01'" if modo == "inicio" else " || '-12-31'"


_PATRON_DE = {nombre: patron for nombre, patron, _ in _RAMAS}


def _expresion_rapida(ident: str, formato: str, modo: Modo) -> str | None:
    guardado = formato.endswith(GUARDADO)
    forma = formato.rstrip(GUARDADO)
    # NULLIF: una celda vacía da NULL (como en el CASE), no una clave '' que
    # pasaría cualquier `hasta`.
    x = f"NULLIF(btrim({ident}::text), '')"
    sm, sy = _sufijo(modo, "mes"), _sufijo(modo, "anio")
    if forma == "iso_dia":
        expr = f"left({x}, 10)"
    elif forma == "iso_mes":
        expr = f"left({x}, 7){sm}"
    elif forma == "anio":
        expr = f"left({x}, 4){sy}"
    elif forma == "aaaamm":
        expr = f"(left({x}, 4) || '-' || right(left({x}, 6), 2)){sm}"
    elif forma == "aaaammdd":
        expr = f"(left({x}, 4) || '-' || right(left({x}, 6), 2) || '-' || right(left({x}, 8), 2))"
    elif forma == "dmy":
        t = f"translate({x}, '-', '/')"
        expr = (
            f"(left(split_part({t}, '/', 3), 4) || '-' || lpad(split_part({t}, '/', 2), 2, '0')"
            f" || '-' || lpad(split_part({t}, '/', 1), 2, '0'))"
        )
    else:
        return None
    if guardado:
        return f"(CASE WHEN {x} ~ '{_PATRON_DE[forma]}' THEN {expr} END)"
    return expr


def expresion_fecha(
    columna: str, tipo: str = "text", modo: Modo = "iso", formato: str | None = None
) -> str:
    """SQL que lleva la columna a texto ISO (``AAAA``, ``AAAA-MM`` o ``AAAA-MM-DD``).

    ``modo="inicio"``/``"fin"`` completa a diez caracteres con el primer o el
    último día del período (``2020`` → ``2020-01-01``/``2020-12-31``). Un
    valor que no reconoce da NULL: nunca un error.

    ``formato`` es la forma única que vio ``formato_uniforme`` en la muestra
    de la columna: con ella la expresión es directa y barata. Sin ella (o con
    formas mezcladas) se usa el CASE que entiende todas.
    """
    ident = quote_ident(columna)
    if _DATE_TYPE.match(tipo or ""):
        return f"left({ident}::text, 10)"
    if formato:
        rapida = _expresion_rapida(ident, formato, modo)
        if rapida is not None:
            return rapida
    x = f"btrim({ident}::text)"
    t = f"translate({x}, '-', '/')"
    sm, sy = _sufijo(modo, "mes"), _sufijo(modo, "anio")
    meses = " ".join(f"WHEN '{abrev}' THEN '{num}'" for abrev, num in _MESES)
    return (
        "(CASE"
        f" WHEN {x} ~ '{RE_ISO_DIA}' THEN left({x}, 10)"
        f" WHEN {x} ~ '{RE_ISO_MES}' THEN {x}{sm}"
        f" WHEN {x} ~ '{RE_AAAA_MM_DD}' THEN split_part({x}, '/', 1) || '-'"
        f" || lpad(split_part({x}, '/', 2), 2, '0') || '-' || lpad(split_part({x}, '/', 3), 2, '0')"
        f" WHEN {x} ~ '{RE_AAAA_MM}' THEN split_part({x}, '/', 1) || '-'"
        f" || lpad(split_part({x}, '/', 2), 2, '0'){sm}"
        f" WHEN {x} ~ '{RE_DMY}' THEN left(split_part({t}, '/', 3), 4) || '-'"
        f" || lpad(split_part({t}, '/', 2), 2, '0') || '-' || lpad(split_part({t}, '/', 1), 2, '0')"
        f" WHEN {x} ~ '{RE_AAAAMMDD}' THEN left({x}, 4) || '-' || right(left({x}, 6), 2)"
        f" || '-' || right({x}, 2)"
        f" WHEN {x} ~ '{RE_AAAAMM}' THEN left({x}, 4) || '-' || right({x}, 2){sm}"
        f" WHEN {x} ~ '{RE_ANIO}' THEN left({x}, 4){sy}"
        f" WHEN {x} ~* '{RE_MES_NOMBRE}' THEN right({x}, 4) || '-'"
        f" || (CASE lower(left({x}, 3)) {meses} END){sm}"
        f" WHEN {x} ~* '{RE_ANIO_MES_NOMBRE}' THEN left({x}, 4) || '-'"
        f" || (CASE lower(left(ltrim(substr({x}, 5), ' /-'), 3)) {meses} END){sm}"
        " END)"
    )


def fecha_iso(valor: object, modo: Modo = "iso") -> str | None:
    """La misma normalización que ``expresion_fecha``, en Python.

    La usan los gráficos para ordenar el eje temporal (antes se ordenaba con
    ``str()``: "1/10/2017" quedaba antes que "1/9/2017") y los tests, que
    comparan las dos contra Postgres.
    """
    if isinstance(valor, datetime | date):
        return valor.isoformat()[:10]
    texto = _texto_de(valor)
    if texto is None:
        return None
    sm = "" if modo == "iso" else ("-01" if modo == "inicio" else "-31")
    sy = "" if modo == "iso" else ("-01-01" if modo == "inicio" else "-12-31")
    if re.match(RE_ISO_DIA, texto):
        return texto[:10]
    if re.match(RE_ISO_MES, texto):
        return texto + sm
    if re.match(RE_AAAA_MM_DD, texto):
        a, m, d = texto.split("/")
        return f"{a}-{m.zfill(2)}-{d.zfill(2)}"
    if re.match(RE_AAAA_MM, texto):
        a, m = texto.split("/")
        return f"{a}-{m.zfill(2)}{sm}"
    if re.match(RE_DMY, texto):
        partes = texto.replace("-", "/").split("/")
        return f"{partes[2][:4]}-{partes[1].zfill(2)}-{partes[0].zfill(2)}"
    if re.match(RE_AAAAMMDD, texto):
        return f"{texto[:4]}-{texto[4:6]}-{texto[6:]}"
    if re.match(RE_AAAAMM, texto):
        return f"{texto[:4]}-{texto[4:]}{sm}"
    if re.match(RE_ANIO, texto):
        return texto[:4] + sy
    if re.match(RE_MES_NOMBRE, texto, re.IGNORECASE):
        return f"{texto[-4:]}-{_MES_ABREV[texto[:3].lower()]}{sm}"
    if re.match(RE_ANIO_MES_NOMBRE, texto, re.IGNORECASE):
        return f"{texto[:4]}-{_MES_ABREV[texto[4:].lstrip(' /-')[:3].lower()]}{sm}"
    return None


# ── filtrar por período ────────────────────────────────────

_DATE_RE = re.compile(r"^\d{4}(-\d{2}(-\d{2})?)?$")


def validar_fecha(valor: str | None, campo: str) -> str | None:
    if valor is None or valor == "":
        return None
    valor = valor.strip()
    if not _DATE_RE.match(valor):
        raise CatalogRequestError(
            f"`{campo}` tiene que ser una fecha AAAA, AAAA-MM o AAAA-MM-DD (recibí {valor!r})."
        )
    return valor


def _inicio(valor: str) -> str:
    return (valor + "-01-01")[:10] if len(valor) == 4 else (valor + "-01")[:10]


def _fin(valor: str) -> str:
    if len(valor) == 4:
        return valor + "-12-31"
    if len(valor) == 7:
        return valor + "-31"
    return valor


def condiciones_periodo(
    columna: ColumnaFecha, desde: str | None, hasta: str | None, params: Params
) -> list[str]:
    """El WHERE de ``desde``/``hasta``: el período del valor se solapa con el pedido."""
    condiciones: list[str] = []
    if desde:
        fin = expresion_fecha(columna.nombre, columna.tipo, "fin", columna.formato)
        condiciones.append(f"{fin} >= {params.bind(_inicio(desde))}")
    if hasta:
        inicio = expresion_fecha(columna.nombre, columna.tipo, "inicio", columna.formato)
        condiciones.append(f"{inicio} <= {params.bind(_fin(hasta))}")
    return condiciones


def orden_fecha(columna: ColumnaFecha) -> str:
    """Para ORDER BY: la clave de inicio, de largo fijo (los no reconocidos, NULL).

    Si la columna es de tipo fecha, o todos sus valores son ISO o años, el
    texto crudo ya ordena bien y es lo más barato (sin función por fila).
    """
    if _DATE_TYPE.match(columna.tipo or "") or columna.formato in ("iso_dia", "iso_mes", "anio"):
        return quote_ident(columna.nombre)
    return expresion_fecha(columna.nombre, columna.tipo, "inicio", columna.formato)


def consulta_rango(tabla_citada: str, columna: ColumnaFecha) -> str:
    """Desde, hasta, cuántos valores se reconocieron y cuántos había.

    ``reconocidas = 0`` con ``con_valor > 0`` es "la columna tiene fechas en un
    formato que no entiendo": se avisa en vez de informar un período vacío.
    """
    iso = expresion_fecha(columna.nombre, columna.tipo, "iso", columna.formato)
    ident = quote_ident(columna.nombre)
    # OFFSET 0 impide que Postgres aplane la subconsulta: así la expresión se
    # calcula una vez por fila y no tres (min, max y count).
    return (
        "SELECT min(f) AS desde, max(f) AS hasta, count(f) AS reconocidas, "
        f"count(c) AS con_valor FROM (SELECT {iso} AS f, {ident} AS c "
        f"FROM {tabla_citada} OFFSET 0) AS s"
    )
