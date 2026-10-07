"""Una sola entrada por archivo en los resultados de búsqueda del catálogo.

Lo usan los dos buscadores: ``/catalogo/buscar`` (``buscar_datasets`` del MCP)
y ``buscar_datos`` del agente. Antes cada uno tenía su copia del mismo
agrupado: mismo título y misma URL eran el mismo dataset, y se mostraba una
vez **con las tablas de todas las copias**. La copia vieja se escondía sólo
porque en staging tenía 0 filas, que era un artefacto de la reconstrucción de
``raw.cached_datasets``; en prod ninguna tabla lista tiene 0 filas y salían las
dos. Medido en prod el 04-oct:

- 5.471 gemelos de la migración de datos.gob.ar a CKAN 2.11 (IDs regenerados,
  misma URL de descarga), 4.676 mostrados con dos tablas;
- 2.344 espejos entre portales (el mismo archivo en datos_gob_ar y en
  energia, justicia, produccion...) con tabla en los dos;
- unos 3.735 recursos que son el mismo archivo en otro formato (el CSV y el
  JSON de "Votaciones Nominales", 231.043 filas cada uno).

Qué se junta (``_Families``):

- la misma URL de descarga **con el mismo título o la misma tabla** (mismas
  filas reales y columnas). La URL sola no alcanza: INDEC publica cada cuadro
  de un .xls como un dataset aparte con la misma URL ("EPH — Tasas e
  indicadores laborales — Cuadro 1.1 … 3.3", 11 hojas con tablas distintas), y
  hay URLs que no son un archivo (una carpeta de SharePoint con "Base de Datos
  por Escuela 2011 … 2024", una página de categoría). Con el mismo título son
  los gemelos de la migración; con la misma tabla y otro título, el mismo CSV
  publicado varias veces (``clae_agg.csv`` con nueve títulos, los espejos de
  PAMI);
- en un archivo que guarda varias tablas (.xls, .xlsx, .ods, .zip), con otro
  título **la forma no alcanza: hace falta el mismo contenido**. Las hojas de
  un .xls de INDEC tienen las mismas columnas y filas y otros valores: el
  Cuadro 3.1 del ISAC (serie original, asfalto de septiembre 102,2) y el 4.1
  (desestacionalizada, 86,6) se juntaban por forma y uno quedaba escondido
  (revisión del 05-oct, H092: 14 pares en prod). El contenido lo da
  ``table_fingerprints`` sólo para los pares en duda (``fingerprint_candidates``);
  sin huella (tabla grande, sandbox que no la sabe) no se juntan. Con la misma
  huella sí: las hojas de "Informe de Pobreza — 31 aglomerados" que quedaron
  con la misma tabla son 136 pares en staging;
- el mismo archivo en otra extensión dentro del mismo package (sólo si las
  extensiones difieren: "Reservas internacionales" 92.1 mensual y 92.2 diaria
  comparten nombre de archivo y son series distintas), con la misma condición
  de título o tabla;
- gemelos con distinta URL: mismo título, portal, nombre de archivo, filas y
  columnas.

Cuál copia se muestra (``_copy_rank``), en este orden:

1. una con tabla consultable antes que una sin tabla;
2. un encabezado que no sea una fila de datos (Proyectos Parlamentarios en CSV
   quedó con "HCDN110412 / HCDN110416" como nombre de columna);
3. una tabla completa antes que una cortada en ``MAX_TABLE_ROWS`` (13 gemelos
   de la era nueva quedaron en 500.000 o 2.500.000 filas y la vieja tiene
   hasta 14 M);
4. más filas reales (las de ``raw_table_versions``, no las de
   ``cached_datasets``: Proyectos en CSV anuncia 111.091 y tiene 11.089);
5. recién después, la era nueva y el CSV. Preferir "la más nueva" o "el CSV"
   de entrada elegía mal en 153 gemelos y en Proyectos.

Qué tablas lleva la entrada (``_entry_tables``): todas las de la copia elegida
y, si el archivo guarda varias tablas (.zip, .xls...), también las de las
otras copias que son **otra tabla**. Cada copia de un .zip puede traer una
tabla distinta del mismo archivo: los tres gemelos de "igj-2022-semestre-1.zip"
en staging traen las asambleas, las bajas y los domicilios, uno cada uno, y en
prod uno trae los administradores. Quedarse con las de la copia elegida
escondía esas tablas, que main (agrupar por título y URL con las tablas de
todas las copias) mostraba (revisión de #177: 32 tablas distintas en 31
archivos de staging, 34 en 32 de prod). Qué tabla es cada una lo dicen sus
primeras columnas visibles (``_table_kind``): una tabla que la elegida ya trae
en otra versión (cortada, de la era vieja) no se suma, salvo que otra copia
traiga más archivos con esas columnas. En un CSV o un JSON (una sola tabla)
otras columnas son otra versión del mismo archivo y se muestra sólo la de la
copia elegida.

No borra ni modifica nada: sólo decide qué se muestra. ``datasets.title`` no se
toca (la clave (título, url) la usa ``reconcile_dataset_identities``).
"""

from __future__ import annotations

import logging
import os
import re
import unicodedata
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any
from urllib.parse import unquote, urlsplit

from app.domain.ports.sandbox.sql_sandbox import CachedTableInfo, TableProfile
from app.domain.ports.search.vector_search import SearchResult
from app.domain.value_objects.table_reference import bare_name
from app.setup.config.constants import MAX_TABLE_ROWS

logger = logging.getLogger(__name__)

# Topes de filas con que el colector cortó tablas. 500.000 es el de hoy;
# 2.500.000 aparece en 13 gemelos de prod de una época con otro tope.
_ROW_CAPS = frozenset({MAX_TABLE_ROWS, 500_000, 2_500_000})

_DATA_EXTENSIONS = frozenset(
    {"csv", "json", "xlsx", "xls", "ods", "txt", "geojson", "zip", "xml", "tsv"}
)

# Archivos que pueden guardar varias tablas: cada hoja de un .xls o cada
# archivo de un .zip puede ser un dataset aparte con la misma URL.
_MULTI_TABLE_FORMATS = frozenset({"xls", "xlsx", "ods", "zip"})

# Cuántas columnas visibles dicen qué tabla de un archivo es cada una. El
# perfil (``table_profiles``) trae las primeras 15 columnas de pg y, cuando las
# del colector (``_source_*``) van primero, quedan 10 visibles; ``columns_json``
# trae todas. Con las primeras 10 las dos fuentes dicen lo mismo de una tabla.
_KIND_COLUMNS = 10

# La marca de orden de bytes de un CSV queda pegada al primer encabezado de
# una carga y no de otra, a veces mal decodificada: "ï»¿anio",
# "ď»żejercicio_presupuestario" (presupuesto 1995-2000 en staging).
_BOMS = (
    "\ufeff",  # la marca misma
    "\u00ef\u00bb\u00bf",  # "ï»¿", leída como cp1252
    "\u010f\u00bb\u017c",  # "ď»ż", cp1250
    "\u013c\u00bb\u00e6",  # "ļ»æ", cp1257
    "\u00ff\u00fe",  # UTF-16 LE leída como latin-1
    "\u00fe\u00ff",  # UTF-16 BE
)


# Hasta cuántas filas reales se pide la huella del contenido. La suma de md5
# por fila tarda ~80 ms con 30.475 filas en staging (IPC aperturas); más
# grande, el par queda separado.
MAX_FINGERPRINT_ROWS = 50_000


@dataclass
class CollapsedResult:
    """Un archivo del catálogo, con la copia que se muestra."""

    hit: SearchResult  # la copia elegida
    # Las tablas del archivo, con ``row_count`` = filas reales: las de la copia
    # elegida y, en un .zip/.xls, las de otras copias que son otra tabla.
    tables: list[CachedTableInfo]
    score: float  # el mejor puntaje del grupo, más el prior si lo hay
    copies: int  # cuántos datasets del catálogo son este mismo archivo
    archivo: str | None  # nombre del archivo de descarga, para distinguir hermanos
    formato: str | None  # extensión o formato declarado ("CSV", "JSON"...)


# ── normalización de URLs y archivos ────────────────────────


def _url_key(url: str | None) -> str | None:
    """La URL de descarga, sin las diferencias que no cambian el archivo."""
    raw = (url or "").strip()
    if not raw:
        return None
    parts = urlsplit(raw)
    host = (parts.hostname or "").lower()
    if not host:
        return raw
    port = parts.port
    netloc = host if port in (None, 80, 443) else f"{host}:{port}"
    path = unquote(parts.path).rstrip("/")
    query = f"?{parts.query}" if parts.query else ""
    return f"{netloc}{path}{query}"


def _file_name(url: str | None) -> str | None:
    path = unquote(urlsplit((url or "").strip()).path).rstrip("/")
    name = path.rsplit("/", 1)[-1] if path else ""
    return name or None


def _extension(name: str | None) -> str | None:
    if not name:
        return None
    ext = os.path.splitext(name)[1].lower().lstrip(".")
    return ext if ext in _DATA_EXTENSIONS else None


def _file_family(url: str | None) -> tuple[str, str, str] | None:
    """``(host, package, archivo sin extensión)`` de una URL de CKAN.

    ``/dataset/<package>/resource/<id>/download/<archivo>`` en CKAN y
    ``/catalog/<catálogo>/dataset/<n>/distribution/<id>/download/<archivo>``
    en infra.datos.gob.ar. Fuera de esa forma no se sabe qué es un package y
    no se agrupa nada.
    """
    raw = (url or "").strip()
    if not raw:
        return None
    parts = urlsplit(raw)
    segments = [s for s in unquote(parts.path).split("/") if s]
    if "dataset" not in segments:
        return None
    i = segments.index("dataset")
    if i + 1 >= len(segments):
        return None
    package = segments[i + 1]
    if "catalog" in segments[:i]:
        j = segments.index("catalog")
        if j + 1 < i:
            package = f"{segments[j + 1]}/{package}"
    name = _file_name(raw)
    if not name or _extension(name) is None:
        return None
    stem = os.path.splitext(name)[0].lower()
    return ((parts.hostname or "").lower(), package.lower(), stem)


# ── encabezados que son filas de datos ──────────────────────

_VALUE_LIKE = (
    re.compile(r"^\d{4}-\d{2}-\d{2}([ t_]\d{2}[:_]\d{2})?"),  # 2009-11-12T00:00:00
    re.compile(r"^\d{1,2}[/_-]\d{1,2}[/_-]\d{2,4}$"),  # 12/11/2009
    re.compile(r"^-?\d+[.,]\d+$"),  # 123,45
    re.compile(r"^-?\d{1,3}([.,]\d{3})+$"),  # 1.234.567
    re.compile(r"^[a-z]{2,6}\d{5,}"),  # hcdn110412
)


def _looks_like_value(name: str) -> bool:
    n = name.strip().lower()
    if not n:
        return False
    if any(p.search(n) for p in _VALUE_LIKE):
        return True
    # Una frase larga con espacios es un valor de texto, no un nombre de
    # columna: "CONCURSOS Y QUIEBRAS - MODIFICACION DE LA LEY...".
    return len(n) > 40 and n.count(" ") >= 3


def header_looks_like_data(columns: Iterable[str]) -> bool:
    """Si el "encabezado" de la tabla es en realidad su primera fila de datos.

    Conservador a propósito: sólo desempata entre copias del mismo archivo.
    Los años sueltos ("2019", "2020") no cuentan, porque son el encabezado
    legítimo de las tablas anchas del INDEC.
    """
    visible = [str(c) for c in columns if c and not str(c).startswith("_")]
    if len(visible) < 2:
        return False
    hits = sum(1 for c in visible if _looks_like_value(c))
    return hits >= 2 and hits * 3 >= len(visible)


# ── agrupado ────────────────────────────────────────────────


class _Families:
    """Union-find sobre los índices de los hits."""

    def __init__(self, n: int) -> None:
        self._parent = list(range(n))

    def find(self, i: int) -> int:
        while self._parent[i] != i:
            self._parent[i] = self._parent[self._parent[i]]
            i = self._parent[i]
        return i

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            # El de menor índice (mejor puntaje) queda de raíz.
            self._parent[max(ra, rb)] = min(ra, rb)

    def join_all(self, members: Sequence[int]) -> None:
        for m in members[1:]:
            self.union(members[0], m)


def _real_rows(table: CachedTableInfo, profile: TableProfile | None) -> int:
    if profile is not None and profile.rows is not None:
        return int(profile.rows)
    return int(table.row_count or 0)


def _is_truncated(rows: int, profile: TableProfile | None) -> bool:
    return bool(profile and profile.truncated) or rows in _ROW_CAPS


def _columns(table: CachedTableInfo, profile: TableProfile | None) -> list[str]:
    if table.columns:
        return [str(c) for c in table.columns]
    return list(profile.columns) if profile else []


def _column_key(name: str) -> str:
    """El nombre de una columna sin lo que cambia entre cargas del mismo
    archivo: mayúsculas, acentos, signos y la marca de orden de bytes."""
    for bom in _BOMS:
        name = name.replace(bom, "")
    plain = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "", plain.lower())


def _table_kind(table: CachedTableInfo) -> tuple[str, ...]:
    """Qué tabla de un archivo es: sus primeras columnas visibles.

    Dos copias de la misma tabla (gemelos de la migración, espejos) traen las
    mismas columnas aunque cambien las filas: una cortada en el tope, otra
    actualizada. Dos tablas distintas de un .zip, no: en las copias de
    "igj-2022-semestre-1.zip", después de ``razon_social`` unas siguen con la
    asamblea, otras con la baja y otras con el domicilio. Sin columnas no se
    sabe y cuenta como otra tabla.
    """
    keys = [
        key
        for c in table.columns
        if c and not str(c).startswith("_") and (key := _column_key(str(c)))
    ]
    return tuple(keys[:_KIND_COLUMNS]) or ("", table.table_name)


def _norm_title(title: str) -> str:
    return " ".join((title or "").lower().split())


class _Hits:
    """Los hits con sus tablas, su forma y su formato, para compararlos."""

    def __init__(
        self,
        hits: Sequence[SearchResult],
        tables: Iterable[CachedTableInfo],
        profiles: Mapping[str, TableProfile],
    ) -> None:
        self.hits = hits
        self.profiles = profiles
        self.by_dataset: dict[str, list[CachedTableInfo]] = {}
        for t in tables:
            if not t.dataset_id:
                continue
            profile = profiles.get(bare_name(t.table_name))
            rows = _real_rows(t, profile)
            if rows <= 0:
                continue
            self.by_dataset.setdefault(str(t.dataset_id), []).append(
                replace(t, row_count=rows, columns=_columns(t, profile))
            )

        # La forma de las tablas de cada hit: (filas reales, columnas) de cada
        # una. None si no tiene tablas o alguna no trae columnas: sin forma no
        # se puede afirmar que dos datasets sean la misma tabla.
        self.shapes: list[tuple | None] = []
        for i in range(len(hits)):
            own = self.own(i)
            shape = tuple(sorted((t.row_count or 0, tuple(t.columns)) for t in own))
            self.shapes.append(shape if own and all(cols for _, cols in shape) else None)
        self.titles = [_norm_title(h.title) for h in hits]
        self.multi = [self.format(h) in _MULTI_TABLE_FORMATS for h in hits]

    def own(self, i: int) -> list[CachedTableInfo]:
        return self.by_dataset.get(str(self.hits[i].dataset_id), [])

    def format(self, h: SearchResult) -> str | None:
        ext = _extension(_file_name(h.download_url))
        if ext:
            return ext
        for t in self.by_dataset.get(str(h.dataset_id), []):
            p = self.profiles.get(bare_name(t.table_name))
            if p and p.format:
                return p.format.lower()
        return None

    def same_title(self, a: int, b: int) -> bool:
        return bool(self.titles[a]) and self.titles[a] == self.titles[b]

    def same_shape(self, a: int, b: int) -> bool:
        return self.shapes[a] is not None and self.shapes[a] == self.shapes[b]

    def needs_content(self, a: int, b: int) -> bool:
        """Otro título y la misma forma en un archivo que guarda varias tablas:
        pueden ser dos hojas distintas (ISAC 3.1 y 4.1) o la misma tabla."""
        return (
            not self.same_title(a, b) and self.same_shape(a, b) and (self.multi[a] or self.multi[b])
        )

    def same_file_groups(self) -> list[list[int]]:
        """Los hits que pueden ser el mismo archivo: la misma URL, o el mismo
        archivo en otra extensión dentro del mismo package."""
        groups: list[list[int]] = []
        by_url: dict[str, list[int]] = {}
        for i, h in enumerate(self.hits):
            key = _url_key(h.download_url)
            if key:
                by_url.setdefault(key, []).append(i)
        groups.extend(by_url.values())

        # Si una extensión se repite dentro del package (dos .csv con el mismo
        # nombre), no se sabe qué va con qué: nada.
        by_file: dict[tuple[str, str, str], dict[str, list[int]]] = {}
        for i, h in enumerate(self.hits):
            fam = _file_family(h.download_url)
            ext = _extension(_file_name(h.download_url))
            if fam and ext:
                by_file.setdefault(fam, {}).setdefault(ext, []).append(i)
        for exts in by_file.values():
            if len(exts) < 2:
                continue
            urls_per_ext = [
                {_url_key(self.hits[i].download_url) for i in ids} for ids in exts.values()
            ]
            if any(len(u) > 1 for u in urls_per_ext):
                continue
            groups.append([i for ids in exts.values() for i in ids])
        return groups


def fingerprint_candidates(
    hits: Sequence[SearchResult],
    tables: Iterable[CachedTableInfo],
    profiles: Mapping[str, TableProfile] | None = None,
) -> list[str]:
    """Las tablas cuyo contenido hace falta para decidir si dos hits se juntan.

    Sólo las de pares en duda (``_Hits.needs_content``) y hasta
    ``MAX_FINGERPRINT_ROWS`` filas reales: si una tabla del hit es más grande,
    el hit no tiene huella y no se junta por forma. Casi todas las búsquedas no
    piden ninguna. Los nombres salen como los da ``find_tables`` (calificados
    si la tabla vive en ``raw``).
    """
    view = _Hits(hits, tables, profiles or {})
    wanted: dict[str, None] = {}
    for members in view.same_file_groups():
        for x, a in enumerate(members):
            for b in members[x + 1 :]:
                if not view.needs_content(a, b):
                    continue
                for i in (a, b):
                    own = view.own(i)
                    if all((t.row_count or 0) <= MAX_FINGERPRINT_ROWS for t in own):
                        wanted.update(dict.fromkeys(t.table_name for t in own))
    return list(wanted)


async def content_fingerprints(
    sandbox: Any,
    hits: Sequence[SearchResult],
    tables: Sequence[CachedTableInfo],
    profiles: Mapping[str, TableProfile] | None = None,
) -> dict[str, str]:
    """``table_fingerprints`` de las tablas en duda; vacío si no hay ninguna,
    si el sandbox no lo sabe (un fake de test) o si falla. Sin huella, dos
    hojas de la misma forma y otro título se muestran separadas."""
    names = fingerprint_candidates(hits, tables, profiles)
    getter = getattr(sandbox, "table_fingerprints", None)
    if not names or getter is None:
        return {}
    try:
        found: dict[str, str] = await getter(names)
    except Exception:
        logger.warning(
            "collapse: no se pudo leer la huella de %d tablas", len(names), exc_info=True
        )
        return {}
    return found


def collapse_hits(
    hits: Sequence[SearchResult],
    tables: Iterable[CachedTableInfo],
    profiles: Mapping[str, TableProfile] | None = None,
    *,
    prior: Callable[[SearchResult], float] | None = None,
    fingerprints: Mapping[str, str] | None = None,
) -> list[CollapsedResult]:
    """Agrupa los hits que son el mismo archivo y elige qué copia mostrar.

    ``hits`` vienen en orden de puntaje (``search_datasets_ann``); ``tables``
    son las de ``find_tables`` para esos datasets y ``profiles`` lo que trae
    ``table_profiles`` (vacío si el sandbox no lo sabe). Las tablas con 0
    filas reales no se ofrecen: son descargas fallidas o vacías.

    ``fingerprints`` es la huella del contenido por nombre pelado
    (``content_fingerprints``): dos hits del mismo .xls/.zip con otro título y
    la misma forma se juntan sólo si sus tablas tienen la misma huella.

    ``prior`` suma un ajuste chico al puntaje de cada hit antes de ordenar
    (la prioridad a fuentes nacionales, ``national_prior``). El resultado
    sale ordenado por el mejor puntaje ajustado de cada grupo.
    """
    profiles = profiles or {}
    fingerprints = fingerprints or {}
    view = _Hits(hits, tables, profiles)
    by_dataset = view.by_dataset
    families = _Families(len(hits))

    # El contenido de cada hit: las huellas de sus tablas, o None si falta
    # alguna (sin huella no se puede afirmar que sea la misma tabla).
    contents: list[tuple[str, ...] | None] = []
    for i in range(len(hits)):
        prints = [fingerprints.get(bare_name(t.table_name)) for t in view.own(i)]
        contents.append(tuple(sorted(p for p in prints if p)) if prints and all(prints) else None)

    def _alike(a: int, b: int) -> bool:
        if view.same_title(a, b):
            return True
        if not view.same_shape(a, b):
            return False
        if view.needs_content(a, b):
            return contents[a] is not None and contents[a] == contents[b]
        return True

    # Dentro de un mismo archivo, junta sólo lo que es la misma cosa.
    for members in view.same_file_groups():
        for x, a in enumerate(members):
            for b in members[x + 1 :]:
                if _alike(a, b):
                    families.union(a, b)

    # Gemelos con distinta URL: mismo título, portal, archivo, filas y
    # columnas. El nombre de archivo es lo que impide juntar recursos
    # hermanos de igual forma y distinto contenido (los resultados de una
    # elección por categoría tienen una fila por mesa y las mismas columnas).
    by_twin: dict[tuple, list[int]] = {}
    for i, h in enumerate(hits):
        name = (_file_name(h.download_url) or "").lower()
        if view.shapes[i] is None or not name:
            continue
        by_twin.setdefault((view.titles[i], h.portal, name, view.shapes[i]), []).append(i)
    for members in by_twin.values():
        families.join_all(members)

    groups: dict[int, list[int]] = {}
    for i in range(len(hits)):
        groups.setdefault(families.find(i), []).append(i)

    def _dataset_created(dataset_id: str) -> float:
        stamps: list[datetime] = [
            p.dataset_created_at
            for t in by_dataset.get(dataset_id, [])
            if (p := profiles.get(bare_name(t.table_name))) and p.dataset_created_at
        ]
        return max(stamps).timestamp() if stamps else 0.0

    def _table_rank(t: CachedTableInfo) -> tuple[bool, bool, int]:
        """Encabezado sano, completa y con más filas, en ese orden."""
        return (
            header_looks_like_data(t.columns),
            _is_truncated(t.row_count or 0, profiles.get(bare_name(t.table_name))),
            -(t.row_count or 0),
        )

    def _copy_rank(i: int) -> tuple:
        h = hits[i]
        own = by_dataset.get(str(h.dataset_id), [])
        if not own:
            return (1, 0, 0, 0, 0.0, 0, i)
        garbage, truncated, minus_rows = _table_rank(min(own, key=_table_rank))
        return (
            0,
            int(garbage),
            int(truncated),
            minus_rows,
            -_dataset_created(str(h.dataset_id)),
            int(view.format(h) != "csv"),
            i,
        )

    def _entry_tables(members: list[int], chosen: int) -> list[CachedTableInfo]:
        """Las tablas de la copia elegida, todas, más las de las otras copias
        de un archivo de varias tablas que la elegida no trae.

        Por clase de tabla (``_table_kind``), mirando las copias de la que
        tiene la mejor versión de esa clase a la peor (``_table_rank``):

        - si la entrada todavía no trae esa clase, se suman todas las tablas
          de esa clase de la copia (los administradores de la IGJ);
        - si ya la trae, sólo cuando la copia tiene **más** tablas de esa clase
          que la entrada: son archivos del .zip con las mismas columnas que
          la entrada no tiene. Se suman las que faltan, sin las que tienen las
          mismas filas que una ya mostrada ni las cortadas en el tope (son
          otra versión de una que ya está).

        Con una tabla por copia, la de otra versión (la copia cortada de las
        entidades, la de la era vieja) no se suma: la elegida ganó por tener
        la mejor.
        """
        entry = list(view.own(chosen))
        kind_of: dict[str, tuple[str, ...]] = {}

        def kind(t: CachedTableInfo) -> tuple[str, ...]:
            if t.table_name not in kind_of:
                kind_of[t.table_name] = _table_kind(t)
            return kind_of[t.table_name]

        offers: dict[tuple[str, ...], list[tuple[tuple, list[CachedTableInfo]]]] = {}
        for i in members:
            if i == chosen or not view.multi[i]:
                continue
            by_kind: dict[tuple[str, ...], list[CachedTableInfo]] = {}
            for t in view.own(i):
                by_kind.setdefault(kind(t), []).append(t)
            for k, same in by_kind.items():
                same.sort(key=_table_rank)
                offers.setdefault(k, []).append(((_table_rank(same[0]), _copy_rank(i)), same))

        for k, copies in offers.items():
            shown = [t for t in entry if kind(t) == k]
            for _, same in sorted(copies, key=lambda c: c[0]):
                if not shown:
                    fresh = same
                else:
                    rows = {t.row_count for t in shown}
                    fresh = [t for t in same if t.row_count not in rows and not _table_rank(t)[1]]
                    fresh = fresh[: max(0, len(same) - len(shown))]
                entry += fresh
                shown += fresh
        return sorted(entry, key=lambda t: -(t.row_count or 0))

    results: list[tuple[float, int, CollapsedResult]] = []
    for root, members in groups.items():
        best = min(members, key=_copy_rank)
        chosen = hits[best]
        score = max(hits[i].score + (prior(hits[i]) if prior else 0.0) for i in members)
        own = _entry_tables(members, best)
        fmt = view.format(chosen)
        results.append(
            (
                score,
                root,
                CollapsedResult(
                    hit=chosen,
                    tables=own,
                    score=score,
                    copies=len(members),
                    archivo=_file_name(chosen.download_url),
                    formato=fmt.upper() if fmt else None,
                ),
            )
        )
    results.sort(key=lambda r: (-r[0], r[1]))
    return [r for _, _, r in results]
