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

No borra ni modifica nada: sólo decide qué se muestra. ``datasets.title`` no se
toca (la clave (título, url) la usa ``reconcile_dataset_identities``).
"""

from __future__ import annotations

import os
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime
from urllib.parse import unquote, urlsplit

from app.domain.ports.sandbox.sql_sandbox import CachedTableInfo, TableProfile
from app.domain.ports.search.vector_search import SearchResult
from app.domain.value_objects.table_reference import bare_name
from app.setup.config.constants import MAX_TABLE_ROWS

# Topes de filas con que el colector cortó tablas. 500.000 es el de hoy;
# 2.500.000 aparece en 13 gemelos de prod de una época con otro tope.
_ROW_CAPS = frozenset({MAX_TABLE_ROWS, 500_000, 2_500_000})

_DATA_EXTENSIONS = frozenset(
    {"csv", "json", "xlsx", "xls", "ods", "txt", "geojson", "zip", "xml", "tsv"}
)


@dataclass
class CollapsedResult:
    """Un archivo del catálogo, con la copia que se muestra."""

    hit: SearchResult  # la copia elegida
    tables: list[CachedTableInfo]  # sus tablas, con ``row_count`` = filas reales
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


def _norm_title(title: str) -> str:
    return " ".join((title or "").lower().split())


def collapse_hits(
    hits: Sequence[SearchResult],
    tables: Iterable[CachedTableInfo],
    profiles: Mapping[str, TableProfile] | None = None,
    *,
    prior: Callable[[SearchResult], float] | None = None,
) -> list[CollapsedResult]:
    """Agrupa los hits que son el mismo archivo y elige qué copia mostrar.

    ``hits`` vienen en orden de puntaje (``search_datasets_ann``); ``tables``
    son las de ``find_tables`` para esos datasets y ``profiles`` lo que trae
    ``table_profiles`` (vacío si el sandbox no lo sabe). Las tablas con 0
    filas reales no se ofrecen: son descargas fallidas o vacías.

    ``prior`` suma un ajuste chico al puntaje de cada hit antes de ordenar
    (la prioridad a fuentes nacionales, ``national_prior``). El resultado
    sale ordenado por el mejor puntaje ajustado de cada grupo.
    """
    profiles = profiles or {}
    by_dataset: dict[str, list[CachedTableInfo]] = {}
    for t in tables:
        if not t.dataset_id:
            continue
        profile = profiles.get(bare_name(t.table_name))
        rows = _real_rows(t, profile)
        if rows <= 0:
            continue
        by_dataset.setdefault(str(t.dataset_id), []).append(
            replace(t, row_count=rows, columns=_columns(t, profile))
        )

    families = _Families(len(hits))

    # La forma de las tablas de cada hit: (filas reales, columnas) de cada una.
    # None si no tiene tablas o alguna no trae columnas: sin forma no se puede
    # afirmar que dos datasets sean la misma tabla.
    shapes: list[tuple | None] = []
    for h in hits:
        own = by_dataset.get(str(h.dataset_id), [])
        shape = tuple(sorted((t.row_count or 0, tuple(t.columns)) for t in own))
        shapes.append(shape if own and all(cols for _, cols in shape) else None)
    titles = [_norm_title(h.title) for h in hits]

    def _alike(a: int, b: int) -> bool:
        if titles[a] and titles[a] == titles[b]:
            return True
        return shapes[a] is not None and shapes[a] == shapes[b]

    def _join_alike(members: Sequence[int]) -> None:
        """Dentro de un mismo archivo, junta sólo lo que es la misma cosa."""
        for x, a in enumerate(members):
            for b in members[x + 1 :]:
                if _alike(a, b):
                    families.union(a, b)

    by_url: dict[str, list[int]] = {}
    for i, h in enumerate(hits):
        key = _url_key(h.download_url)
        if key:
            by_url.setdefault(key, []).append(i)
    for members in by_url.values():
        _join_alike(members)

    # El mismo archivo en otra extensión. Si una extensión se repite dentro del
    # package (dos .csv con el mismo nombre), no se sabe qué va con qué: nada.
    by_file: dict[tuple[str, str, str], dict[str, list[int]]] = {}
    for i, h in enumerate(hits):
        fam = _file_family(h.download_url)
        ext = _extension(_file_name(h.download_url))
        if fam and ext:
            by_file.setdefault(fam, {}).setdefault(ext, []).append(i)
    for exts in by_file.values():
        if len(exts) < 2:
            continue
        urls_per_ext = [{_url_key(hits[i].download_url) for i in ids} for ids in exts.values()]
        if any(len(u) > 1 for u in urls_per_ext):
            continue
        _join_alike([i for ids in exts.values() for i in ids])

    # Gemelos con distinta URL: mismo título, portal, archivo, filas y
    # columnas. El nombre de archivo es lo que impide juntar recursos
    # hermanos de igual forma y distinto contenido (los resultados de una
    # elección por categoría tienen una fila por mesa y las mismas columnas).
    by_twin: dict[tuple, list[int]] = {}
    for i, h in enumerate(hits):
        name = (_file_name(h.download_url) or "").lower()
        if shapes[i] is None or not name:
            continue
        by_twin.setdefault((titles[i], h.portal, name, shapes[i]), []).append(i)
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

    def _format(h: SearchResult) -> str | None:
        ext = _extension(_file_name(h.download_url))
        if ext:
            return ext
        for t in by_dataset.get(str(h.dataset_id), []):
            p = profiles.get(bare_name(t.table_name))
            if p and p.format:
                return p.format.lower()
        return None

    def _copy_rank(i: int) -> tuple:
        h = hits[i]
        own = by_dataset.get(str(h.dataset_id), [])
        if not own:
            return (1, 0, 0, 0, 0.0, 0, i)
        best = min(
            own,
            key=lambda t: (
                header_looks_like_data(t.columns),
                _is_truncated(t.row_count or 0, profiles.get(bare_name(t.table_name))),
                -(t.row_count or 0),
            ),
        )
        garbage = header_looks_like_data(best.columns)
        truncated = _is_truncated(best.row_count or 0, profiles.get(bare_name(best.table_name)))
        return (
            0,
            int(garbage),
            int(truncated),
            -(best.row_count or 0),
            -_dataset_created(str(h.dataset_id)),
            int(_format(h) != "csv"),
            i,
        )

    results: list[tuple[float, int, CollapsedResult]] = []
    for root, members in groups.items():
        chosen = hits[min(members, key=_copy_rank)]
        score = max(hits[i].score + (prior(hits[i]) if prior else 0.0) for i in members)
        own = sorted(by_dataset.get(str(chosen.dataset_id), []), key=lambda t: -(t.row_count or 0))
        fmt = _format(chosen)
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
