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

Lo que encontró la revisión independiente del 05-oct sobre esto:

- el solapamiento hacía que un pedido de junio sobre una tabla con ``anio`` y
  ``mes`` separados devolviera el año entero (H002). Ahora un período más fino
  que el año usa la columna de mes (``anio || '-' || mes``) y, si la tabla no
  tiene, se rechaza; el orden usa el mes como segunda clave (H010);
- las fechas con barras se leían siempre d/m: en una columna m/d «3/4/2025»
  quedaba en abril y «5/31/2021» en NULL (H003). Ahora la lectura se decide
  por columna con la muestra (``lectura_dia_mes``);
- una fecha de nacimiento le ganaba a la fecha del dato (H044).
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import date, datetime
from typing import Literal

from app.application.consultas.numeros import es_tipo_numerico
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
# dato; `updated_ts`/`updated_at` son de la base; `update_date`/`last_update`
# son la fecha de la última edición del registro.
_PALABRAS_METADATO = frozenset(
    {
        "update",
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
    }
)
# Marcas de tiempo: casi siempre de la base (`ingest_ts`), pero en tablas de
# sensores o viajes `timestamp` es la única marca del dato. Cuentan como
# metadato salvo que la tabla no tenga otra candidata (ver
# `resolver_columna_fecha`).
_MARCAS_DE_TIEMPO = frozenset({"ts", "timestamp"})
# Palabras que sugieren un período aunque no sean una fecha que sepamos
# filtrar: se nombran en el mensaje de "no hay columna de fecha".
_PALABRAS_PERIODO = frozenset({"mes", "trimestre", "semestre", "periodo", "bimestre", "dia"})
# La columna de mes de una tabla con año y mes separados: "mes" o "month",
# sola o con un calificador (`mes_nro`, `id_mes`, `nombre_mes`, `n° mes`), o
# con el mismo prefijo que la de año (`reclamo_mes_nro` para `reclamo_ano`,
# `impacto_presupuestario_mes` para `impacto_presupuestario_anio`). Otra con
# "mes" en el nombre no: `pp04b3_mes` de la EPH es el mes en que empezó un
# trabajo y `mes_transferencia` el de otro evento.
_PALABRAS_MES = frozenset({"mes", "month"})
_CALIFICADORES_MES = frozenset(
    {"n", "nro", "num", "numero", "id", "cod", "codigo", "nombre", "desc", "descripcion"}
)
_NOMBRE_DEL_MES = frozenset({"nombre", "desc", "descripcion"})
# Fechas que describen a alguien o algo de la fila, no cuándo pasó el dato:
# se usan sólo si la tabla no tiene otra candidata, y con aviso (H044).
_PALABRAS_ATRIBUTO = frozenset(
    {"nacimiento", "defuncion", "fallecimiento", "vencimiento", "alta", "baja"}
)

_DATE_TYPE = re.compile(r"^(date|timestamp)", re.IGNORECASE)
_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")
_SEPARADORES = re.compile(r"[^0-9a-zñ]+")


def es_tipo_fecha(tipo: str) -> bool:
    """date o timestamp: el texto del valor ya es ISO, no hace falta mirar la muestra."""
    return bool(_DATE_TYPE.match(tipo or ""))


def _palabras(nombre: str) -> list[str]:
    return [p for p in _SEPARADORES.split(plegar(_CAMEL.sub("_", nombre))) if p]


def _es_metadato_seguro(palabras: list[str]) -> bool:
    if palabras[-1] == "at" and len(palabras) > 1:  # created_at, updated_at
        return True
    return any(p in _PALABRAS_METADATO for p in palabras)


def es_metadato(nombre: str) -> bool:
    """Una fecha de carga, actualización o auditoría (``updated_at``, ``*_ts``)."""
    palabras = _palabras(nombre)
    if not palabras:
        return False
    return _es_metadato_seguro(palabras) or any(p in _MARCAS_DE_TIEMPO for p in palabras)


def _solo_marca_de_tiempo(nombre: str) -> bool:
    """``timestamp``, ``fecha_ts``: metadato sólo por la marca, no por otra palabra."""
    palabras = _palabras(nombre)
    return (
        bool(palabras)
        and not _es_metadato_seguro(palabras)
        and any(p in _MARCAS_DE_TIEMPO for p in palabras)
    )


def es_nombre_de_fecha(nombre: str) -> bool:
    """True si el nombre dice que es la fecha de la serie (sin mirar el tipo).

    La subcadena "date" ya no alcanza: tiene que ser una palabra del nombre
    (``date``, ``start_date``), no ``candidate`` ni ``updated``. Con una
    marca de tiempo y nada más de metadato (``fecha_timestamp``), la palabra
    "fecha" manda.
    """
    if es_metadato(nombre) and not _solo_marca_de_tiempo(nombre):
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
    # "fecha" o "anio". Con "anio" y una columna de mes, un período más fino
    # que el año se filtra con las dos (`condiciones_periodo`) y el orden usa
    # el mes como segunda clave (`claves_orden`).
    clase: str = "fecha"
    # La forma única de sus valores según la muestra de `pg_stats`
    # (`formato_uniforme`), o None: entonces se usa el CASE que entiende todas.
    # También dice cómo leer las fechas con barras (`lectura_de`) y si una
    # columna de año tiene años (`CASE_ANIO`).
    formato: str | None = None
    # La columna de mes de una tabla con año y mes separados (H002, H010).
    mes: str | None = None
    tipo_mes: str = "text"
    # Una fecha de nacimiento, vencimiento, alta o baja que se eligió porque la
    # tabla no tenía otra (H044): se avisa (`aviso_lectura_fecha`).
    atributo: bool = False


# Cómo las abrevian los portales: `fecha_nac` (personas buscadas), `fecha_vto`
# (transportes autorizados), `Fecha Primer Vto.` (acceso a la información).
_ABREVIATURAS_ATRIBUTO = {"nac": "nacimiento", "vto": "vencimiento", "venc": "vencimiento"}


def _eventos(nombre: str) -> list[str]:
    return [_ABREVIATURAS_ATRIBUTO.get(p, p) for p in _palabras(nombre)]


def _es_de_atributo(nombre: str) -> bool:
    return any(p in _PALABRAS_ATRIBUTO for p in _eventos(nombre))


def _evento_de(nombre: str) -> str:
    """De qué es una fecha de atributo: ``defuncion``, ``nacimiento``…, o ""."""
    return next((p for p in _eventos(nombre) if p in _PALABRAS_ATRIBUTO), "")


def _plural(evento: str) -> str:
    return evento + ("es" if evento.endswith("n") else "s")


def _de_la_tabla(nombre: str, tabla: str | None) -> bool:
    """La fecha es del evento que registra la tabla (`fecha_defuncion` en caba__defunciones)."""
    evento = _evento_de(nombre)
    return bool(evento and tabla) and _plural(evento) in _palabras(tabla or "")


def es_fecha_de_atributo(nombre: str, tabla: str | None = None) -> bool:
    """Una fecha que describe a alguien de la fila (nacimiento, vencimiento, alta…),
    no cuándo pasó el dato, salvo que sea el evento que registra la tabla (H044)."""
    return _es_de_atributo(nombre) and not _de_la_tabla(nombre, tabla)


def _es_columna_de_mes(nombre: str, anio: str) -> bool:
    palabras = _palabras(nombre)
    if (
        not any(p in _PALABRAS_MES for p in palabras)
        or any(p in _PALABRAS_ANIO for p in palabras)
        or es_metadato(nombre)
    ):
        return False
    resto = [p for p in palabras if p not in _PALABRAS_MES and p not in _CALIFICADORES_MES]
    return not resto or resto == [p for p in _palabras(anio) if p not in _PALABRAS_ANIO]


def _columna_de_anio(nombre: str, tipo: str, pares: list[tuple[str, str]]) -> ColumnaFecha:
    """La columna de año, con la de mes de la misma tabla si la hay (H002)."""
    meses = [(n, t) for n, t in pares if n != nombre and _es_columna_de_mes(n, nombre)]
    if not meses:
        return ColumnaFecha(nombre, tipo, "anio")
    # El número del mes antes que su nombre (`reclamo_mes_nro`, no `reclamo_mes_nombre`).
    mes, tipo_mes = min(
        meses,
        key=lambda c: (
            not es_tipo_numerico(c[1]),
            any(p in _NOMBRE_DEL_MES for p in _palabras(c[0])),
        ),
    )
    return ColumnaFecha(nombre, tipo, "anio", mes=mes, tipo_mes=tipo_mes)


def resolver_columna_fecha(
    columnas: Iterable[tuple[str, str]] | Iterable[str],
    elegida: str | None = None,
    tabla: str | None = None,
) -> ColumnaFecha | None:
    """La columna que ordena la tabla en el tiempo, o None.

    Orden de prioridad: nombre exacto de fecha (``fecha``, ``indice_tiempo``,
    ``periodo``); tipo date/timestamp; una palabra "fecha"/"date" en el nombre
    (``PUBLICACION_FECHA``, ``fecha_inicio``); una columna de año (``anio``,
    ``ejercicio_presupuestario``), con la de mes si la tabla la tiene. Las
    fechas de carga o auditoría nunca. Una marca de tiempo (``timestamp``,
    ``event_ts`` de tipo timestamp) sólo si no hay ninguna otra: en una tabla
    de sensores es la fecha del dato. Las de nacimiento, vencimiento, alta o
    baja (``consultante_fecha_nacimiento``), al final: describen a alguien de
    la fila, no cuándo pasó el dato.

    ``elegida`` es la que pidió el usuario (``columna_fecha``): tiene que
    existir, y se usa aunque el nombre no parezca de fecha.

    ``tabla`` es el nombre de la tabla: en la que registra ese mismo evento
    (`fecha_defuncion` en caba__defunciones, `hijo_fecha_nacimiento` en
    caba__nacimientos) la fecha de atributo es la del dato. Va primero entre
    las de atributo y sin aviso; la elección entre las demás no cambia.
    """
    pares = [(c, "text") if isinstance(c, str) else (str(c[0]), str(c[1])) for c in columnas]
    if elegida:
        for nombre, tipo in pares:
            if nombre == elegida:
                if es_nombre_de_anio(nombre):
                    return _columna_de_anio(nombre, tipo, pares)
                return ColumnaFecha(nombre, tipo)
        raise CatalogRequestError(
            f"`columna_fecha` {elegida!r} no es una columna de la tabla. "
            "Usá describir_tabla para ver las disponibles."
        )
    # `fecha_timestamp` entra: la marca no la hace metadato si dice "fecha".
    candidatas = [(n, t) for n, t in pares if not es_metadato(n) or es_nombre_de_fecha(n)]
    del_dato = [(n, t) for n, t in candidatas if not _es_de_atributo(n)]
    for nombre, tipo in del_dato:
        if plegar(nombre) in _NOMBRES_FECHA:
            return ColumnaFecha(nombre, tipo)
    for nombre, tipo in del_dato:
        if _DATE_TYPE.match(tipo or ""):
            return ColumnaFecha(nombre, tipo)
    for nombre, tipo in del_dato:
        if any(p in _PALABRAS_FECHA for p in _palabras(nombre)):
            return ColumnaFecha(nombre, tipo)
    for nombre, tipo in del_dato:
        if es_nombre_de_anio(nombre):
            return _columna_de_anio(nombre, tipo, pares)
    for nombre, tipo in pares:
        if _solo_marca_de_tiempo(nombre) and (
            _DATE_TYPE.match(tipo or "") or plegar(nombre) == "timestamp"
        ):
            return ColumnaFecha(nombre, tipo)
    atributos = [(n, t) for n, t in candidatas if _es_de_atributo(n)]
    atributos.sort(key=lambda c: not _de_la_tabla(c[0], tabla))
    for nombre, tipo in atributos:
        if _DATE_TYPE.match(tipo or "") or any(p in _PALABRAS_FECHA for p in _palabras(nombre)):
            return ColumnaFecha(nombre, tipo, atributo=not _de_la_tabla(nombre, tabla))
    for nombre, tipo in atributos:
        if es_nombre_de_anio(nombre):
            return replace(
                _columna_de_anio(nombre, tipo, pares), atributo=not _de_la_tabla(nombre, tabla)
            )
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
# La misma forma leída mes/día (Pauta publicitaria de CABA: «5/31/2021»).
RE_MDY = r"^(0?[1-9]|1[0-2])[/-](0?[1-9]|[12][0-9]|3[01])[/-][0-9]{4}([^0-9]|$)"
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


# Cada forma reconocida, en el orden en que se prueba (el mismo del CASE). Las
# fechas con barras se prueban d/m, m/d o las dos según la lectura de la
# columna (`_ramas`).
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
_RAMA_MDY = ("mdy", RE_MDY, 0)
# Formas con una expresión directa, sin el CASE de expresiones regulares.
FORMATOS_RAPIDOS = frozenset({"iso_dia", "iso_mes", "dmy", "mdy", "aaaammdd", "aaaamm", "anio"})
_MIN_MUESTRA_FORMATO = 3
# Sin forma única, el CASE; estas marcas le dicen cómo leer las fechas con
# barras (m/d, o las dos lecturas en una columna que las mezcla) y, en una
# columna de año, que sus valores son años aunque la muestra tenga otra cosa
# («Total») o no haya muestra.
CASE_MDY = "case_mdy"
CASE_MIXTA = "case_mixta"
CASE_ANIO = "case_anio"
Lectura = Literal["dmy", "mdy", "mixta"]
_DIA_MES = re.compile(r"^([0-9]{1,2})[/-]([0-9]{1,2})[/-][0-9]{4}([^0-9]|$)")
# Meses distintos con el día 1 para reconocer una serie mensual
# (`_evidencia_dia_mes`). Con tres, una tabla diaria de enero leída d/m
# («6/1/2020», «7/1/2020», «8/1/2020») se confundía con una mensual m/d.
_MIN_MESES_SERIE = 6
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


def _ramas(lectura: Lectura) -> tuple[tuple[str, str, int], ...]:
    if lectura == "dmy":
        return _RAMAS
    i = next(i for i, (nombre, _, _) in enumerate(_RAMAS) if nombre == "dmy")
    barras = (_RAMA_MDY,) if lectura == "mdy" else (_RAMAS[i], _RAMA_MDY)
    return _RAMAS[:i] + barras + _RAMAS[i + 1 :]


def rama_fecha(valor: object, lectura: Lectura = "dmy") -> str | None:
    """Qué forma de fecha tiene un valor (``iso_dia``, ``dmy``, ``anio``…), o None."""
    if isinstance(valor, datetime | date):
        return "iso_dia"
    texto = _texto_de(valor)
    if texto is None:
        return None
    for nombre, patron, flags in _ramas(lectura):
        if re.match(patron, texto, flags):
            return nombre
    return None


def _evidencia_dia_mes(valores: Iterable[object]) -> tuple[int, int]:
    """``(d/m, m/d)``: cuántos valores con barras dicen cómo leer la columna.

    Un valor que sólo se puede leer de una forma («31/5/2021», «5/31/2021»)
    es evidencia de esa forma. Los ambiguos (las dos partes hasta 12) también
    lo son si tienen la forma de una serie mensual: el día es siempre 1 y el
    mes cambia. «10/1/2017», «11/1/2017», «12/1/2017»… son m/d (el segundo
    campo, el que no cambia, es el día) y «1/10/2017», «1/11/2017»… son d/m.
    Sin esto, en biodiésel y bioetanol 26bc8483 (staging, todas «M/1/AAAA»)
    cada mes caía en enero (revisión del PR #154).

    Esa forma no cuenta si los valores inequívocos de la otra lectura caen en
    el mismo mes que la explica: una tabla diaria de enero leída d/m («27/1/
    2026», «12/1/2026»…) tiene todos los ambiguos con el segundo campo en 1, y
    el «27/1/2026» dice que es enero, no que haya meses m/d (radares de AUSA,
    molinetes del subte 91ca9141 y otras cinco en staging).
    """
    solo_dm = solo_md = 0
    ambiguos: list[tuple[int, int]] = []
    meses_dm: set[int] = set()  # el segundo campo de los que sólo son d/m
    meses_md: set[int] = set()  # el primer campo de los que sólo son m/d
    for valor in valores:
        m = _DIA_MES.match(_texto_de(valor) or "")
        if m is None:
            continue
        primero, segundo = int(m.group(1)), int(m.group(2))
        if 12 < primero <= 31 and 1 <= segundo <= 12:
            solo_dm += 1
            meses_dm.add(segundo)
        elif 12 < segundo <= 31 and 1 <= primero <= 12:
            solo_md += 1
            meses_md.add(primero)
        elif 1 <= primero <= 12 and 1 <= segundo <= 12:
            ambiguos.append((primero, segundo))
    primeros = {p for p, _ in ambiguos}
    segundos = {s for _, s in ambiguos}
    if segundos == {1} and len(primeros) >= _MIN_MESES_SERIE and meses_dm != {1}:
        solo_md += len(ambiguos)
    elif primeros == {1} and len(segundos) >= _MIN_MESES_SERIE and meses_md != {1}:
        solo_dm += len(ambiguos)
    return solo_dm, solo_md


def _lectura(solo_dm: int, solo_md: int) -> Lectura:
    if solo_md and solo_dm:
        return "mixta"
    return "mdy" if solo_md else "dmy"


def lectura_dia_mes(valores: Iterable[object]) -> Lectura:
    """Cómo leer las fechas con barras de una columna: ``dmy``, ``mdy`` o ``mixta``.

    Lo decide la muestra, como el formato de los números: «5/31/2021» sólo
    puede ser mes/día y «31/5/2021» sólo día/mes. Con alguno que sólo puede
    ser m/d y ninguno que sólo puede ser d/m, la columna es m/d (la Pauta
    publicitaria de CABA); con de los dos, mezcla las lecturas (``mart.
    pauta_oficial`` une fuentes de las dos). Una serie mensual con el día 1
    cuenta como evidencia de su forma (``_evidencia_dia_mes``): si choca con
    un valor que sólo se lee de la otra en otro mes, también es mixta. Si
    nada lo decide, d/m.
    """
    return _lectura(*_evidencia_dia_mes(valores))


def lectura_de(formato: str | None) -> Lectura:
    """La lectura de las fechas con barras que corresponde a un ``formato`` de columna."""
    forma = (formato or "").rstrip(GUARDADO)
    if forma in ("mdy", CASE_MDY):
        return "mdy"
    return "mixta" if forma == CASE_MIXTA else "dmy"


def _formato_case(lectura: Lectura) -> str | None:
    return {"dmy": None, "mdy": CASE_MDY, "mixta": CASE_MIXTA}[lectura]


def formato_uniforme(valores: Iterable[object], filas: int | None = None) -> str | None:
    """La forma de fecha de una columna si la muestra tiene una sola, o None.

    La muestra sale de ``pg_stats`` (el ANALYZE de Postgres toma filas al azar
    de toda la tabla). Con una sola forma se usa una expresión directa en vez
    del CASE: en tablas de 6 a 8 M de filas el CASE pasaba el timeout de 10 s
    del sandbox donde el ``left()`` de antes tardaba de 3 a 9 s (medido en
    staging el 04-oct).

    Con ``filas`` de una tabla grande y una forma que domina (90 %), devuelve
    esa forma con el sufijo ``GUARDADO`` (ver arriba).

    Las fechas con barras se leen según ``lectura_dia_mes``: una columna m/d
    da ``mdy`` (o ``CASE_MDY`` si tiene otras formas) y una que mezcla d/m y
    m/d, ``CASE_MIXTA``, salvo en una tabla grande donde una de las dos
    lecturas domina: ahí se usa esa, con su guarda (molinetes del subte).
    """
    textos = [v for v in valores if _texto_de(v) is not None]
    solo_dm, solo_md = _evidencia_dia_mes(textos)
    lectura = _lectura(solo_dm, solo_md)
    grande = filas is not None and filas >= FILAS_TABLA_GRANDE
    if lectura == "mixta" and grande and max(solo_dm, solo_md) >= _DOMINANTE * (solo_dm + solo_md):
        lectura = "dmy" if solo_dm > solo_md else "mdy"
    if lectura == "mixta":
        return CASE_MIXTA
    ramas = [rama_fecha(v, lectura) for v in textos]
    if len(ramas) < _MIN_MUESTRA_FORMATO:
        return _formato_case(lectura)
    if len(set(ramas)) == 1:
        unica = ramas[0]
        return unica if unica in FORMATOS_RAPIDOS else _formato_case(lectura)
    if not grande:
        return _formato_case(lectura)
    dominante, veces = Counter(ramas).most_common(1)[0]
    if dominante in FORMATOS_RAPIDOS and veces / len(ramas) >= _DOMINANTE:
        return dominante + GUARDADO
    return _formato_case(lectura)


_DESCRIPCION_FORMA = {
    "iso_dia": "AAAA-MM-DD",
    "iso_mes": "AAAA-MM",
    "dmy": "d/m/aaaa",
    "mdy": "m/d/aaaa",
    "aaaammdd": "AAAAMMDD",
    "aaaamm": "AAAAMM",
    "anio": "AAAA",
}


def aviso_formato_guardado(columna: ColumnaFecha | None) -> str | None:
    """Si la fecha se lee sólo en su forma dominante (``GUARDADO``), qué se pierde; si no, None.

    Las filas con la fecha escrita de otra forma quedan en NULL: fuera de
    cualquier ``desde``/``hasta`` y al final del orden. Sin este aviso, un
    cálculo con período sobre esa tabla las excluía en silencio
    (``filas_usadas`` cuenta las que pasaron el WHERE, así que no lo deja
    ver).
    """
    if columna is None or not columna.formato or not columna.formato.endswith(GUARDADO):
        return None
    forma = columna.formato.rstrip(GUARDADO)
    return (
        f"La tabla es muy grande para reconocer todas las formas de fecha: «{columna.nombre}» "
        f"se leyó sólo en su forma dominante ({_DESCRIPCION_FORMA.get(forma, forma)}). Las "
        "filas con la fecha escrita de otra manera (hasta un 10 % en la muestra) quedaron "
        "fuera del período y al final del orden."
    )


def aviso_lectura_fecha(columna: ColumnaFecha | None) -> str | None:
    """Lo que hay que saber de cómo se leyó la columna de fecha, o None.

    Junta los casos en que la lectura pierde o supone algo: la forma dominante
    de una tabla grande (``aviso_formato_guardado``), una columna que mezcla
    fechas d/m y m/d (H003) y una fecha de nacimiento, vencimiento, alta o
    baja que se usa porque la tabla no tiene otra (H044).
    """
    if columna is None:
        return None
    avisos = [aviso_formato_guardado(columna)]
    if lectura_de(columna.formato) == "mixta":
        avisos.append(
            f"«{columna.nombre}» mezcla fechas día/mes/año y mes/día/año: el año de cada fila "
            "es seguro, pero en las que no se distinguen (las dos partes hasta 12) el mes y el "
            "día pueden estar invertidos. Por eso sólo filtro años enteros con `desde`/`hasta` "
            "y el orden dentro de cada año puede no ser exacto."
        )
    if columna.atributo:
        # No afirma que no es la fecha del dato: en una tabla de defunciones
        # que no lo dice en el nombre, la de defunción lo es (revisión del PR
        # #154).
        de = _evento_de(columna.nombre)
        avisos.append(
            f"Uso «{columna.nombre}» como fecha de la tabla porque no tiene otra. Es una fecha "
            f"de {'defunción' if de == 'defuncion' else de}: si la tabla no registra "
            f"{_plural(de)}, puede no ser la fecha del dato, y `desde`/`hasta` y el orden van "
            "igual por ella. Si la tabla tiene otra columna con el período, pasala en "
            "`columna_fecha`."
        )
    texto = " ".join(a for a in avisos if a)
    return texto or None


def _sufijo(modo: Modo, nivel: str) -> str:
    if modo == "iso":
        return ""
    if nivel == "mes":
        return " || '-01'" if modo == "inicio" else " || '-31'"
    return " || '-01-01'" if modo == "inicio" else " || '-12-31'"


_PATRON_DE = {nombre: patron for nombre, patron, _ in (*_RAMAS, _RAMA_MDY)}


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
    elif forma == "mdy":
        t = f"translate({x}, '-', '/')"
        expr = (
            f"(left(split_part({t}, '/', 3), 4) || '-' || lpad(split_part({t}, '/', 1), 2, '0')"
            f" || '-' || lpad(split_part({t}, '/', 2), 2, '0'))"
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
    formas mezcladas) se usa el CASE que entiende todas, con las fechas con
    barras leídas como dice ``lectura_de(formato)``.
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
    dmy = (
        f" WHEN {x} ~ '{RE_DMY}' THEN left(split_part({t}, '/', 3), 4) || '-'"
        f" || lpad(split_part({t}, '/', 2), 2, '0') || '-' || lpad(split_part({t}, '/', 1), 2, '0')"
    )
    mdy = (
        f" WHEN {x} ~ '{RE_MDY}' THEN left(split_part({t}, '/', 3), 4) || '-'"
        f" || lpad(split_part({t}, '/', 1), 2, '0') || '-' || lpad(split_part({t}, '/', 2), 2, '0')"
    )
    barras = {"dmy": dmy, "mdy": mdy, "mixta": dmy + mdy}[lectura_de(formato)]
    return (
        "(CASE"
        f" WHEN {x} ~ '{RE_ISO_DIA}' THEN left({x}, 10)"
        f" WHEN {x} ~ '{RE_ISO_MES}' THEN {x}{sm}"
        f" WHEN {x} ~ '{RE_AAAA_MM_DD}' THEN split_part({x}, '/', 1) || '-'"
        f" || lpad(split_part({x}, '/', 2), 2, '0') || '-' || lpad(split_part({x}, '/', 3), 2, '0')"
        f" WHEN {x} ~ '{RE_AAAA_MM}' THEN split_part({x}, '/', 1) || '-'"
        f" || lpad(split_part({x}, '/', 2), 2, '0'){sm}"
        f"{barras}"
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


def fecha_iso(valor: object, modo: Modo = "iso", lectura: Lectura = "dmy") -> str | None:
    """La misma normalización que ``expresion_fecha``, en Python.

    La usan los gráficos para ordenar el eje temporal (antes se ordenaba con
    ``str()``: "1/10/2017" quedaba antes que "1/9/2017") y los tests, que
    comparan las dos contra Postgres. ``lectura`` es la de las fechas con
    barras (``lectura_de`` del formato de la columna).
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
    if lectura != "mdy" and re.match(RE_DMY, texto):
        partes = texto.replace("-", "/").split("/")
        return f"{partes[2][:4]}-{partes[1].zfill(2)}-{partes[0].zfill(2)}"
    if lectura != "dmy" and re.match(RE_MDY, texto):
        partes = texto.replace("-", "/").split("/")
        return f"{partes[2][:4]}-{partes[0].zfill(2)}-{partes[1].zfill(2)}"
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


# ── la columna de mes de una tabla con año y mes separados ─

# Las formas que tienen los valores de esas columnas en staging (05-oct): el
# número (bigint o texto, "06", "6.0"), el nombre ("Junio", "ENERO",
# "Diciembre*", "ene-17"), "10/2020" y una fecha ISO ("2024-07-01 00:00:00").
RE_MES_NUMERO = r"^(0?[1-9]|1[0-2])(\.0+)?$"
RE_MES_SOLO_NOMBRE = (
    r"^(ene(ro)?|feb(rero)?|mar(zo)?|abr(il)?|may(o)?|jun(io)?|jul(io)?|ago(sto)?"
    r"|sep(t|tiembre)?|set(iembre)?|oct(ubre)?|nov(iembre)?|dic(iembre)?)\.?([^a-z]|$)"
)
RE_MES_Y_ANIO = r"^(0?[1-9]|1[0-2])[/-][0-9]{4}$"
RE_ISO_CON_MES = r"^[0-9]{4}-(0[1-9]|1[0-2])"
# Los valores de RE_MES_NUMERO sin decimales, como lista (`expresion_mes`).
_NUMEROS_DE_MES = ", ".join(
    f"'{m}'" for m in [*(str(n) for n in range(1, 13)), *(f"0{n}" for n in range(1, 10))]
)


def expresion_mes(columna: str) -> str:
    """SQL que lleva una columna de mes a dos dígitos (``06``), o NULL ("Total", "99").

    El número de mes («6», «06»), que es casi siempre, se resuelve con una
    lista y sin ``btrim`` antes que con las expresiones regulares (el resto,
    con blancos o con nombre, sigue por ellas): en mart.estadistica_
    mediaciones (3,5 M de filas, staging) obtener_datos con orden=desc tardaba
    4,8-5,8 s con el mes en el ORDER BY y así 2,8-3,3 s (sin el mes, 0,6-0,9 s,
    pero con enero como el último dato). Revisión del PR #154.
    """
    crudo = f"{quote_ident(columna)}::text"
    x = f"btrim({crudo})"
    meses = " ".join(f"WHEN '{abrev}' THEN '{num}'" for abrev, num in _MESES)
    return (
        "(CASE"
        f" WHEN {crudo} IN ({_NUMEROS_DE_MES}) THEN lpad({crudo}, 2, '0')"
        f" WHEN {x} ~ '{RE_MES_NUMERO}' THEN lpad(split_part({x}, '.', 1), 2, '0')"
        f" WHEN {x} ~* '{RE_MES_SOLO_NOMBRE}' THEN (CASE lower(left({x}, 3)) {meses} END)"
        f" WHEN {x} ~ '{RE_MES_Y_ANIO}' THEN lpad(split_part(translate({x}, '-', '/'), '/', 1),"
        " 2, '0')"
        f" WHEN {x} ~ '{RE_ISO_CON_MES}' THEN substr({x}, 6, 2)"
        " END)"
    )


def mes_de(valor: object) -> str | None:
    """La misma lectura que ``expresion_mes``, en Python (para los tests de paridad)."""
    texto = _texto_de(valor)
    if texto is None:
        return None
    if re.match(RE_MES_NUMERO, texto):
        return texto.split(".")[0].zfill(2)
    if re.match(RE_MES_SOLO_NOMBRE, texto, re.IGNORECASE):
        return _MES_ABREV[texto[:3].lower()]
    if re.match(RE_MES_Y_ANIO, texto):
        return texto.replace("-", "/").split("/")[0].zfill(2)
    if re.match(RE_ISO_CON_MES, texto):
        return texto[5:7]
    return None


def _clave_anio_mes(columna: ColumnaFecha, mes: str, modo: Modo) -> str:
    """La clave de cada fila con el año de una columna y el mes de otra.

    ``modo`` como en ``expresion_fecha``: ``AAAA-MM`` con ``"iso"``, de inicio
    o de fin del mes con ``"inicio"``/``"fin"``. Una fila cuyo "año" ya trae
    el mes o el día ("2016-05" en `ano_mes`) se lee como siempre; una con el
    mes irreconocible ("Total") da NULL y queda fuera del período.
    """
    x = f"btrim({quote_ident(columna.nombre)}::text)"
    sufijo = _sufijo(modo, "mes")
    if columna.formato == "anio":
        # Todos años en la muestra: se lee como en `expresion_fecha`, sin
        # volver a mirar cada valor (el costo, en `expresion_mes`).
        return f"(left(NULLIF({x}, ''), 4) || '-' || {expresion_mes(mes)}{sufijo})"
    otra = expresion_fecha(columna.nombre, columna.tipo, modo, columna.formato)
    return (
        f"(CASE WHEN {x} ~ '{RE_ANIO}' THEN left({x}, 4) || '-' || "
        f"{expresion_mes(mes)}{sufijo} ELSE {otra} END)"
    )


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


def _anios_enteros(desde: str | None, hasta: str | None) -> bool:
    """El pedido va del 1 de enero al 31 de diciembre (``2019``, ``2019-01``…``2019-12``)."""
    return (not desde or desde[4:] in ("", "-01", "-01-01")) and (
        not hasta or hasta[4:] in ("", "-12", "-12-31")
    )


def _de_anios(columna: ColumnaFecha) -> bool:
    """Una columna con años (2019), no meses ni días, hasta donde se sabe.

    Una columna sin muestra todavía (la primera validación, que no toca la
    base) no se decide: ``consultas.preparar.con_formato`` le pone ``anio`` o
    ``CASE_ANIO`` si la muestra tiene años, o nada si sus valores traen el mes
    (`ano_mes` con "2016-05"). Vale para cualquier nombre y tipo: un `periodo`
    o un `indice_tiempo` con años, de texto o bigint, es tan anual como un
    `anio` (revisiones del PR #154). Una numérica sin muestra es de años sólo
    si se llama como tal.
    """
    if (columna.formato or "").rstrip(GUARDADO) in ("anio", CASE_ANIO):
        return True
    return columna.clase == "anio" and columna.formato is None and es_tipo_numerico(columna.tipo)


def condiciones_periodo(
    columna: ColumnaFecha, desde: str | None, hasta: str | None, params: Params
) -> list[str]:
    """El WHERE de ``desde``/``hasta``: el período del valor se solapa con el pedido.

    Un período más fino que el año (junio de 2019) sobre una columna de año
    se solapaba con el año entero y devolvía los doce meses (H002): ahí se
    usa la columna de mes, y si la tabla no tiene, se rechaza. En una columna
    que mezcla fechas d/m y m/d sólo el año es seguro: también se rechaza.
    """
    fino = bool(desde or hasta) and not _anios_enteros(desde, hasta)
    mes = columna.mes if fino and columna.clase == "anio" else None
    if fino and mes is None and _de_anios(columna):
        es = "es una columna de año" if columna.clase == "anio" else "tiene años, no meses ni días,"
        raise CatalogRequestError(
            f"«{columna.nombre}» {es} y la tabla no tiene una columna de mes que "
            "reconozca: con `desde`/`hasta` sólo puedo filtrar años enteros (AAAA, o de "
            "AAAA-01 a AAAA-12). Pedí el año entero, o filtrá el período más fino con "
            "`filtros` sobre la columna que lo indique (trimestre, semestre…)."
        )
    if fino and lectura_de(columna.formato) == "mixta":
        raise CatalogRequestError(
            f"«{columna.nombre}» mezcla fechas día/mes/año y mes/día/año, así que no sé de qué "
            "mes es cada una: con `desde`/`hasta` sólo puedo filtrar años enteros (AAAA, o de "
            "AAAA-01 a AAAA-12)."
        )
    condiciones: list[str] = []
    if mes and columna.formato == "anio":
        # La clave empieza con el año: filtrarlo antes evita calcular el mes en
        # las filas de otros años (mart.estadistica_mediaciones, un mes: de
        # 7,8-9,0 s a 2,1-2,9 s en staging). Es redundante con la clave.
        anio = f"left(NULLIF(btrim({quote_ident(columna.nombre)}::text), ''), 4)"
        if desde:
            condiciones.append(f"{anio} >= {params.bind(desde[:4])}")
        if hasta:
            condiciones.append(f"{anio} <= {params.bind(hasta[:4])}")
    if desde:
        fin = (
            _clave_anio_mes(columna, mes, "fin")
            if mes
            else expresion_fecha(columna.nombre, columna.tipo, "fin", columna.formato)
        )
        condiciones.append(f"{fin} >= {params.bind(_inicio(desde))}")
    if hasta:
        inicio = (
            _clave_anio_mes(columna, mes, "inicio")
            if mes
            else expresion_fecha(columna.nombre, columna.tipo, "inicio", columna.formato)
        )
        condiciones.append(f"{inicio} <= {params.bind(_fin(hasta))}")
    return condiciones


def orden_fecha(columna: ColumnaFecha) -> str:
    """Para ORDER BY: la clave de inicio, de largo fijo (los no reconocidos, NULL).

    Si la columna es de tipo fecha, o todos sus valores son ISO o años, el
    texto crudo ya ordena bien y es lo más barato (sin función por fila).

    Un año numérico se ordena comparando números, con NULL donde ``RE_ANIO``
    no reconoce un año (fuera de 1800-2099 o con decimales): el CASE de
    expresiones regulares sobre ``anio::text`` daba timeout en tablas de 900
    mil filas (tercera revisión del PR #154).
    """
    ident = quote_ident(columna.nombre)
    if es_tipo_numerico(columna.tipo) and _de_anios(columna):
        return _entre(ident, columna.tipo, 1800, 2099)
    if _DATE_TYPE.match(columna.tipo or "") or columna.formato in ("iso_dia", "iso_mes", "anio"):
        return ident
    return expresion_fecha(columna.nombre, columna.tipo, "inicio", columna.formato)


def claves_orden(columna: ColumnaFecha) -> list[str]:
    """Las claves del ORDER BY por fecha: la fecha y, en una tabla con año y mes, el mes.

    Con el año solo, dentro de cada año desempataba la posición física y
    ``orden=desc`` traía enero como el último dato (H010). Un mes numérico,
    como el año, se compara como número (NULL donde ``expresion_mes`` no ve
    un mes: fuera de 1 a 12 o con decimales).
    """
    claves = [orden_fecha(columna)]
    if columna.clase == "anio" and columna.mes is not None:
        claves.append(
            _entre(quote_ident(columna.mes), columna.tipo_mes, 1, 12)
            if es_tipo_numerico(columna.tipo_mes)
            else expresion_mes(columna.mes)
        )
    return claves


_TIPOS_ENTEROS = frozenset({"smallint", "integer", "bigint"})


def _entre(ident: str, tipo: str, desde: int, hasta: int) -> str:
    """El valor numérico si es un entero entre ``desde`` y ``hasta``; si no, NULL."""
    entero = "" if (tipo or "").lower() in _TIPOS_ENTEROS else f" AND {ident} = trunc({ident})"
    return f"(CASE WHEN {ident} BETWEEN {desde} AND {hasta}{entero} THEN {ident} END)"


def consulta_rango(tabla_citada: str, columna: ColumnaFecha) -> str:
    """Desde, hasta, cuántos valores se reconocieron y cuántos había.

    ``reconocidas = 0`` con ``con_valor > 0`` es "la columna tiene fechas en un
    formato que no entiendo": se avisa en vez de informar un período vacío.

    En una tabla con año y mes, el rango va por las dos (``2026-03``): con el
    año solo, una tabla de 2026 con meses hasta marzo daba de 2026 a 2026 y
    describir_tabla la presentaba como una foto vigente al día en que se leyó
    (revisión de ola 3, #144 × #154). Una fila con el mes irreconocible
    ("Total") cuenta por su año, como antes.
    """
    iso = expresion_fecha(columna.nombre, columna.tipo, "iso", columna.formato)
    if columna.clase == "anio" and columna.mes is not None:
        iso = f"COALESCE({_clave_anio_mes(columna, columna.mes, 'iso')}, {iso})"
    ident = quote_ident(columna.nombre)
    # OFFSET 0 impide que Postgres aplane la subconsulta: así la expresión se
    # calcula una vez por fila y no tres (min, max y count).
    return (
        "SELECT min(f) AS desde, max(f) AS hasta, count(f) AS reconocidas, "
        f"count(c) AS con_valor FROM (SELECT {iso} AS f, {ident} AS c "
        f"FROM {tabla_citada} OFFSET 0) AS s"
    )
