"""Declaraciones juradas de la Oficina Anticorrupción: qué archivo usar y cómo leerlo.

La OA publica en datos.jus.gob.ar (dataset
`declaraciones-juradas-patrimoniales-integrales`, CC-BY 4.0) un ZIP por año de
publicación y, para el último año, los CSV sueltos. Cada ZIP trae los CSV
"consolidados al <fecha de corte>" de varios años fiscales: el de 2024 trae el
principal de 2012 a 2024, el de 2023 el de 2012 a 2023, etc. Lo que importa de
cada archivo es (tipo, año fiscal, corte), no en qué ZIP vino.

Lo que se midió el 08-oct-2026 y explica las reglas:

- **El último corte (20251222) multiplica por 10.** Trae los totales de TODOS
  los años multiplicados por 10 cuando los centavos eran "00": el suelto dice
  "326035240-00" y el ZIP "3260352400.00". Es ~25 % de las declaraciones de cada
  año (13.565 de 48.346 en 2024, contra la suma del detalle de bienes). El corte
  anterior (20250218) está sano. Por eso, para cada año se elige el corte más
  nuevo que no esté multiplicado contra el anterior (`elegir_principal`), y el
  CSV suelto del último año, que trae los montos en el formato original, gana a
  su copia en el ZIP.
- **Los montos vienen en tres formatos.** "35278884-41" (guion decimal, el
  original), "35278884.41" y, para los negativos, "-15036-00", ".150360.00" o
  "..47" (el signo menos convertido en punto). `parse_monto` entiende todos.
- **El suelto de 2024 repite filas.** Repite ~4.300 declaraciones al final en
  formato con punto y trae una fila basura (dj_id ".16"). Se queda la primera
  aparición de cada dj_id y se descartan las filas sin dj_id o CUIT numéricos.
- **El detalle de bienes de 2016 y 2017 no tiene importe** (corte 20190524). Se
  usa sólo un detalle que lo traiga.
- **El ZIP de 2019 trae miembros en Deflate64**, que `zipfile` no abre. Son
  copias de cortes que también vienen en otros ZIP, así que se saltean.
- **El grupo familiar no se carga.** Trae CUIT y fecha de nacimiento de
  cónyuges e hijos, que es el anexo reservado.
"""

from __future__ import annotations

import csv
import io
import os
import re
import unicodedata
import zipfile
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import IO

FUENTE = "oficina_anticorrupcion"
JURISDICCION = "nacional"
URL_DATASET = "https://datos.jus.gob.ar/dataset/declaraciones-juradas-patrimoniales-integrales"

TIPO_PRINCIPAL = "principal"
TIPO_BIENES = "bienes"
TIPO_DEUDAS = "deudas"
TIPO_GRUPO_FAMILIAR = "grupo-familiar"

# Los métodos de compresión que `zipfile` sabe abrir (stored, deflate, bzip2, lzma).
COMPRESIONES_SOPORTADAS = frozenset({0, 8, 12, 14})

# Un corte se descarta si más del 1 % de las declaraciones que comparte con el
# corte anterior tiene el total de bienes multiplicado por 10 (en el corte malo
# es ~25 %; entre dos cortes sanos, 0).
UMBRAL_X10 = 0.01
# Y si trae menos del 70 % de las declaraciones del corte anterior: el corte
# 20190524 de 2018 tenía 10.176 filas contra las 58.608 del siguiente.
UMBRAL_PARCIAL = 0.7

_RE_ARCHIVO = re.compile(
    r"declaraciones-juradas-(?:(bienes|deudas|grupo-familiar)-)?(\d{4})"
    r"-consolidado-al-(\d{8})\.csv$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Archivo:
    """Un CSV de la OA: suelto (`miembro=None`) o dentro de un ZIP."""

    tipo: str
    anio: int
    corte: date
    nombre: str
    origen: str  # ruta local del ZIP o del CSV suelto
    url: str  # URL del recurso en datos.jus.gob.ar
    miembro: str | None = None

    @property
    def suelto(self) -> bool:
        return self.miembro is None

    def describir(self) -> str:
        donde = "suelto" if self.suelto else f"en {os.path.basename(self.origen)}"
        return f"{self.nombre} ({donde})"


def clasificar(nombre: str) -> tuple[str, int, date] | None:
    """`(tipo, año fiscal, corte)` de un nombre de archivo de la OA, o `None`."""
    m = _RE_ARCHIVO.search(os.path.basename(nombre or ""))
    if not m:
        return None
    tipo = (m.group(1) or TIPO_PRINCIPAL).lower()
    corte = m.group(3)
    try:
        fecha = date(int(corte[:4]), int(corte[4:6]), int(corte[6:]))
    except ValueError:
        return None
    return tipo, int(m.group(2)), fecha


def archivos_del_zip(ruta: str, url: str) -> tuple[list[Archivo], list[str]]:
    """Los CSV de la OA que trae un ZIP, y los que se saltean (con el motivo)."""
    archivos: list[Archivo] = []
    salteados: list[str] = []
    with zipfile.ZipFile(ruta) as z:
        for info in z.infolist():
            clase = clasificar(info.filename)
            if clase is None:
                continue
            if info.compress_type not in COMPRESIONES_SOPORTADAS:
                salteados.append(f"{info.filename}: compresión {info.compress_type}")
                continue
            tipo, anio, corte = clase
            archivos.append(
                Archivo(
                    tipo=tipo,
                    anio=anio,
                    corte=corte,
                    nombre=os.path.basename(info.filename),
                    origen=ruta,
                    url=url,
                    miembro=info.filename,
                )
            )
    return archivos, salteados


def archivo_suelto(ruta: str, url: str) -> Archivo | None:
    clase = clasificar(url) or clasificar(ruta)
    if clase is None:
        return None
    tipo, anio, corte = clase
    return Archivo(
        tipo=tipo,
        anio=anio,
        corte=corte,
        nombre=os.path.basename(url.split("?")[0]),
        origen=ruta,
        url=url,
    )


@contextmanager
def abrir(archivo: Archivo) -> Iterator[csv.DictReader]:
    """Un `DictReader` sobre el CSV, con los encabezados sin espacios y en minúscula."""
    crudo: IO[bytes]
    z: zipfile.ZipFile | None
    if archivo.miembro is None:
        crudo = open(archivo.origen, "rb")  # noqa: SIM115 — se cierra en el finally
        z = None
    else:
        z = zipfile.ZipFile(archivo.origen)
        crudo = z.open(archivo.miembro)
    try:
        texto = io.TextIOWrapper(crudo, encoding="utf-8-sig", errors="replace", newline="")
        lector = csv.reader(texto)
        encabezado = [c.strip().lower() for c in next(lector, [])]
        yield csv.DictReader(texto, fieldnames=encabezado)
    finally:
        crudo.close()
        if z is not None:
            z.close()


def encabezado(archivo: Archivo) -> list[str]:
    with abrir(archivo) as lector:
        return list(lector.fieldnames or [])


# ── montos ───────────────────────────────────────────────────────────────────

_RE_NEG_CERO = re.compile(r"\.\.(\d+)")  # "..47" → -0.47
_RE_NEG_PUNTO = re.compile(r"\.(\d+\.\d+)")  # ".150360.00" → -150360.00
_RE_PUNTO = re.compile(r"-?\d*\.\d+")  # "35278884.41", ".27"
_RE_GUION = re.compile(r"(-?)(\d*)-(\d{1,2})")  # "35278884-41", "-00", "-15036-00", "--47"


def parse_monto(texto: str | None) -> Decimal | None:
    """Un monto de la OA en cualquiera de sus formatos, o `None` si no es un monto.

    Con punto es el formato convertido: un punto adelante de otro número es el
    signo menos que se perdió. Sin punto y con guion es el original, donde el
    último guion separa los centavos y uno adelante es el signo: "-47" es 0,47
    (como "-00" es cero), no -47.
    """
    s = (texto or "").strip()
    if not s:
        return None
    try:
        if "." in s:
            if m := _RE_NEG_CERO.fullmatch(s):
                return -Decimal(f"0.{m.group(1)}")
            if m := _RE_NEG_PUNTO.fullmatch(s):
                return -Decimal(m.group(1))
            if _RE_PUNTO.fullmatch(s):
                return Decimal(s)
            return None
        if "-" in s:
            if m := _RE_GUION.fullmatch(s):
                valor = Decimal(f"{m.group(2) or '0'}.{m.group(3)}")
                return -valor if m.group(1) else valor
            return None
        if s.isdigit():
            return Decimal(s)
    except InvalidOperation:
        return None
    return None


# ── poder del Estado ─────────────────────────────────────────────────────────


def _normalizar(texto: str | None) -> str:
    sin_tildes = unicodedata.normalize("NFD", texto or "")
    return "".join(c for c in sin_tildes if unicodedata.category(c) != "Mn").upper().strip()


# El orden importa: "PODER JUDICIAL MINISTERIO PUBLICO" es Ministerio Público.
_PODER_POR_ORGANISMO: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "ministerio_publico",
        ("MINISTERIO PUBLICO", "PROCURACION GENERAL", "DEFENSORIA GENERAL"),
    ),
    (
        "judicial",
        (
            "PODER JUDICIAL",
            "CORTE SUPREMA",
            "CONSEJO DE LA MAGISTRATURA",
            "JUZGADO",
            "CAMARA NACIONAL DE APELACIONES",
            "CAMARA FEDERAL",
            "TRIBUNAL ORAL",
        ),
    ),
    (
        "legislativo",
        (
            "DIPUTADOS",
            "SENADO",
            "SENADORES",
            "LEGISLATURA",
            "CONGRESO",
            "CONCEJO DELIBERANTE",
            "AUDITORIA GENERAL DE LA NACION",
            "DEFENSOR DEL PUEBLO",
        ),
    ),
)

# El cargo manda cuando nombra una banca o un juzgado: el organismo a veces es
# otra actividad de la persona. De los 195 diputados de 2024, 15 tienen de
# organismo una empresa, un municipio, un sindicato o el partido
# ("CULTIVATE LA BUENA VIDA S.A.", "MUNICIPALIDAD DE MERLO") y de cargo
# "DIPUTADO NACIONAL", a veces mal escrito ("DIUTADA", "dipurado").
_PODER_POR_CARGO_FUERTE: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("legislativo", ("DIPUTAD", "DIUTAD", "DIPURAD", "SENADOR", "LEGISLADOR")),
    ("judicial", ("JUEZ", "JUEZA", "CAMARISTA")),
)
# Sin organismo (12 % de las filas de 2024), el cargo también sirve con claves
# más ambiguas: "FISCAL" con organismo puede ser un asesor fiscal de ARCA.
_PODER_POR_CARGO: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("ministerio_publico", ("FISCAL", "DEFENSOR OFICIAL", "PROCURADOR")),
    ("judicial", ("MAGISTRAD",)),
    ("legislativo", ("CONCEJAL",)),
)


def _por_claves(texto: str, tabla: tuple[tuple[str, tuple[str, ...]], ...]) -> str | None:
    for poder, claves in tabla:
        if any(c in texto for c in claves):
            return poder
    return None


def poder_de(organismo: str | None, cargo: str | None = None) -> str:
    """`ejecutivo`, `legislativo`, `judicial`, `ministerio_publico` o `sin_dato`."""
    car = _normalizar(cargo)
    org = _normalizar(organismo)
    # Los candidatos también declaran ("CANDIDATA A DIPUTADA NACIONAL", 37 en
    # 2024) y no ocupan la banca: para ellos manda el organismo.
    poder = None if "CANDIDAT" in car else _por_claves(car, _PODER_POR_CARGO_FUERTE)
    if poder is None and org:
        poder = _por_claves(org, _PODER_POR_ORGANISMO) or "ejecutivo"
    if poder is None:
        poder = _por_claves(car, _PODER_POR_CARGO)
    return poder or "sin_dato"


# ── filas ────────────────────────────────────────────────────────────────────

COLUMNAS_DECLARACION: tuple[str, ...] = (
    "fuente",
    "dj_id",
    "jurisdiccion",
    "poder",
    "cuit",
    "nombre",
    "anio",
    "tipo",
    "rectificativa",
    "sector",
    "organismo",
    "cargo",
    "en_funciones_desde",
    "bienes_inicio",
    "deudas_inicio",
    "bienes_cierre",
    "deudas_cierre",
    "ingresos_netos",
    "ingresos_trabajo_alquileres_rentas",
    "ingresos_no_alcanzados",
    "gastos_personales",
    "bienes_heredados",
    "corte",
    "archivo_fuente",
    "url_fuente",
)

COLUMNAS_BIEN: tuple[str, ...] = (
    "dj_id",
    "periodo",
    "tipo",
    "descripcion",
    "origen_fondos",
    "titularidad",
    "importe",
)

COLUMNAS_DEUDA: tuple[str, ...] = (
    "dj_id",
    "periodo",
    "tipo",
    "descripcion",
    "radicacion",
    "clasificacion",
    "importe",
)

_PERIODOS = {"I": "inicio", "C": "cierre"}
_TIPOS_DJ = {"anual": "Anual", "inicial": "Inicial", "baja": "Baja"}


def _texto(fila: Mapping[str, str | None], clave: str) -> str | None:
    valor = (fila.get(clave) or "").strip()
    return valor or None


def _entero(texto: str | None) -> int | None:
    s = (texto or "").strip()
    return int(s) if s.isdigit() else None


def _desde(texto: str | None) -> date | None:
    """`desde` viene como AAAAMM."""
    s = (texto or "").strip()
    if len(s) != 6 or not s.isdigit():
        return None
    anio, mes = int(s[:4]), int(s[4:])
    if not (1900 <= anio <= 2100 and 1 <= mes <= 12):
        return None
    return date(anio, mes, 1)


def es_fila_valida(fila: Mapping[str, str | None]) -> bool:
    """Descarta la basura: dj_id y CUIT tienen que ser números (el CUIT, de 11 cifras)."""
    dj_id = (fila.get("dj_id") or "").strip()
    cuit = (fila.get("cuit") or "").strip()
    return dj_id.isdigit() and cuit.isdigit() and len(cuit) == 11


def fila_declaracion(fila: Mapping[str, str | None], archivo: Archivo) -> tuple | None:
    """Una fila de `cache_ddjj_declaraciones`, en el orden de `COLUMNAS_DECLARACION`."""
    if not es_fila_valida(fila):
        return None
    organismo = _texto(fila, "organismo")
    cargo = _texto(fila, "cargo")
    tipo = _TIPOS_DJ.get((fila.get("tipo_declaracion_jurada_descripcion") or "").strip().lower())
    heredados = parse_monto(fila.get("bienes_heredados"))
    if heredados is None:
        heredados = parse_monto(fila.get("bienes_por_herencia"))
    return (
        FUENTE,
        int((fila.get("dj_id") or "").strip()),
        JURISDICCION,
        poder_de(organismo, cargo),
        (fila.get("cuit") or "").strip(),
        _texto(fila, "funcionario_apellido_nombre"),
        _entero(fila.get("anio")) or archivo.anio,
        tipo,
        _entero(fila.get("rectificativa")),
        _texto(fila, "sector") if _texto(fila, "sector") not in {"0", "0.00"} else None,
        organismo,
        cargo,
        _desde(fila.get("desde")),
        parse_monto(fila.get("total_bienes_inicio")),
        parse_monto(fila.get("deudas_inicio")),
        parse_monto(fila.get("total_bienes_final")),
        parse_monto(fila.get("total_deudas_final")),
        parse_monto(fila.get("ingresos_neto_gastos")),
        parse_monto(fila.get("ingresos_trabajos_alquileres_rentas")),
        parse_monto(fila.get("ingresos_no_alcanzados")),
        parse_monto(fila.get("gastos_personales")),
        heredados,
        archivo.corte,
        archivo.nombre,
        archivo.url,
    )


def tiene_importe(columnas: Sequence[str], tipo: str) -> bool:
    """Si el detalle trae importe. Al de bienes de 2016-2017 le falta (y le falta
    `bien_tipo`, así que el importe que dice el encabezado es la titularidad)."""
    cols = set(columnas)
    if tipo == TIPO_BIENES:
        return {"bien_tipo", "bien_importe"} <= cols
    if tipo == TIPO_DEUDAS:
        return "deuda_importe" in cols
    return False


def fila_bien(fila: Mapping[str, str | None]) -> tuple | None:
    dj_id = (fila.get("dj_id") or "").strip()
    periodo = _PERIODOS.get((fila.get("periodo_inicio_cierre") or "").strip().upper())
    tipo = _texto(fila, "bien_tipo")
    importe = parse_monto(fila.get("bien_importe"))
    if not dj_id.isdigit() or periodo is None or (tipo is None and importe is None):
        return None
    return (
        int(dj_id),
        periodo,
        tipo,
        _texto(fila, "bien_descripcion"),
        _texto(fila, "bien_origen_fondos"),
        parse_monto(fila.get("bien_titularidad")),
        importe,
    )


def fila_deuda(fila: Mapping[str, str | None]) -> tuple | None:
    dj_id = (fila.get("dj_id") or "").strip()
    periodo = _PERIODOS.get((fila.get("periodo_inicio_cierre") or "").strip().upper())
    tipo = _texto(fila, "deuda_tipo")
    importe = parse_monto(fila.get("deuda_importe"))
    if not dj_id.isdigit() or periodo is None or (tipo is None and importe is None):
        return None
    return (
        int(dj_id),
        periodo,
        tipo,
        _texto(fila, "deuda_descripcion"),
        _texto(fila, "deuda_radicacion_localizacion"),
        _texto(fila, "deuda_clasificacion"),
        importe,
    )


# ── elección del corte ───────────────────────────────────────────────────────


def agrupar(archivos: Iterable[Archivo]) -> dict[tuple[str, int], list[Archivo]]:
    """Candidatos por (tipo, año): el corte más nuevo primero y, a igual corte,
    el suelto antes que su copia en el ZIP."""
    grupos: dict[tuple[str, int], list[Archivo]] = {}
    for a in archivos:
        grupos.setdefault((a.tipo, a.anio), []).append(a)
    for lista in grupos.values():
        lista.sort(key=lambda a: (a.corte, a.suelto, a.nombre), reverse=True)
    return grupos


@dataclass
class ResumenPrincipal:
    filas: int
    totales: dict[int, Decimal]  # dj_id → total de bienes al cierre


def resumir_principal(archivo: Archivo) -> ResumenPrincipal:
    totales: dict[int, Decimal] = {}
    with abrir(archivo) as lector:
        for fila in lector:
            if not es_fila_valida(fila):
                continue
            dj_id = int((fila.get("dj_id") or "").strip())
            if dj_id in totales:
                continue
            totales[dj_id] = parse_monto(fila.get("total_bienes_final")) or Decimal(0)
    return ResumenPrincipal(filas=len(totales), totales=totales)


def proporcion_x10(nuevo: Mapping[int, Decimal], viejo: Mapping[int, Decimal]) -> float:
    """Qué parte de las declaraciones con total positivo en los dos cortes tiene en
    `nuevo` exactamente 10 veces el total de `viejo`."""
    compartidas = 0
    multiplicadas = 0
    for dj_id, valor in nuevo.items():
        previo = viejo.get(dj_id)
        if not previo or previo <= 0 or valor <= 0:
            continue
        compartidas += 1
        if abs(valor / previo - 10) < Decimal("0.05"):
            multiplicadas += 1
    return multiplicadas / compartidas if compartidas else 0.0


@dataclass
class Eleccion:
    archivo: Archivo
    descartados: list[tuple[Archivo, str]] = field(default_factory=list)


def elegir_principal(
    candidatos: Sequence[Archivo],
    resumir: Callable[[Archivo], ResumenPrincipal] = resumir_principal,
) -> Eleccion | None:
    """El corte más nuevo que no esté vacío, ni sea parcial, ni tenga los totales
    multiplicados por 10 contra el corte anterior con filas."""
    cache: dict[Archivo, ResumenPrincipal] = {}

    def resumen(a: Archivo) -> ResumenPrincipal:
        if a not in cache:
            cache[a] = resumir(a)
        return cache[a]

    descartados: list[tuple[Archivo, str]] = []
    for i, archivo in enumerate(candidatos):
        propio = resumen(archivo)
        if propio.filas == 0:
            descartados.append((archivo, "vacío"))
            continue
        referencia = next((b for b in candidatos[i + 1 :] if resumen(b).filas > 0), None)
        if referencia is not None:
            otro = resumen(referencia)
            if propio.filas < UMBRAL_PARCIAL * otro.filas:
                descartados.append(
                    (
                        archivo,
                        f"parcial: {propio.filas} declaraciones contra {otro.filas} "
                        f"del corte {referencia.corte.isoformat()}",
                    )
                )
                continue
            proporcion = proporcion_x10(propio.totales, otro.totales)
            if proporcion > UMBRAL_X10:
                descartados.append(
                    (
                        archivo,
                        f"totales multiplicados por 10 en el {proporcion:.0%} de las "
                        f"declaraciones contra el corte {referencia.corte.isoformat()}",
                    )
                )
                continue
        return Eleccion(archivo=archivo, descartados=descartados)
    # Sólo se llega acá si ningún corte tiene filas: el último con filas no
    # tiene referencia y siempre se acepta. Si ése tiene totales ×10 (un año que
    # sólo viene en un corte malo), los corrige la carga contra el detalle.
    return None


def elegir_detalle(
    candidatos: Sequence[Archivo],
    corte_principal: date | None,
    columnas: Callable[[Archivo], Sequence[str]] = encabezado,
) -> Eleccion | None:
    """El detalle del mismo corte que el principal si lo hay; si no, el más nuevo.
    Siempre uno que traiga importe."""
    descartados: list[tuple[Archivo, str]] = []
    con_importe: list[Archivo] = []
    for archivo in candidatos:
        if tiene_importe(columnas(archivo), archivo.tipo):
            con_importe.append(archivo)
        else:
            descartados.append((archivo, "sin importe"))
    if not con_importe:
        return None
    mismo_corte = [a for a in con_importe if a.corte == corte_principal]
    elegido = (mismo_corte or con_importe)[0]
    return Eleccion(archivo=elegido, descartados=descartados)
