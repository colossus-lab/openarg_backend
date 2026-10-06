"""How old is the data behind an answer, and can the reader tell.

A user asking about poverty gets a confident paragraph of numbers and no way to
know the source was last read in May. Measured in production on 2026-08-23:
**78.5 % of the resources we serve were last collected more than 90 days ago**,
and 3,289 of them have been changed by their portal since. The answer is not
wrong — it is the best reading of what we hold — but presenting it without its
date lets the reader assume a currency nobody promised.

This module answers one question: *as of when* is this?

**A mart's rebuild time is not its data's date.** A mart rebuilt this morning
over sources last read in May is May's data with a fresh timestamp on it, and
reporting `last_refreshed_at` as freshness would be precisely the kind of
number that reads as reassurance while meaning nothing. So a mart's age comes
from `source_data_oldest`, recorded when the mart was built from the tables its
macros actually resolved to, and never from when the build ran.

Everything here fails open. A freshness lookup that cannot answer must not cost
the user their answer — it returns `None` and the response carries no date,
which is the state we are in today anyway.

**Two questions, two answers (04-oct-2026).**

- *When did we last read this table?* (`data_age_for`, `staleness_warning`).
  It used to come from `raw_table_versions.created_at`, which is when the live
  version first appeared: a resource re-read every day and found unchanged
  keeps its May timestamp, and the vía-B upsert never touches `created_at`
  either. Measured in staging: `cache_bcra_cotizaciones` read that morning
  said "mayo de 2026", and so did every one of the 71 marts with a source
  date. Now it is `raw.cached_datasets.updated_at` of the ready row — the last
  read or verification against the source — and, for a mart, the oldest of
  that over the tables the matview actually reads (`pg_depend`), so a mart is
  as old as its oldest source and never younger. A source with no ready row
  counts by its registry date, and one with neither pulls in
  `source_data_oldest` (05-oct: ignoring it made a mart look fresher).
- *How old is the observation itself?* (`observation_staleness`,
  `freshness_notices`). A live connector has no table to look up; what it has
  is the date of the last observation and the frequency. "Reservas: USD
  35.001 M" with the last observation in April 2023 is not an old read — it is
  an old datum, and the reader has to be told before the number, not after.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# Ninety days is the line the collector's own backstop uses, so a resource past
# it is one the system already considers worth re-reading. Using a different
# number here would mean the chat and the refresh disagree about "stale".
STALE_AFTER_DAYS = 90

_MONTHS_ES = (
    "enero",
    "febrero",
    "marzo",
    "abril",
    "mayo",
    "junio",
    "julio",
    "agosto",
    "septiembre",
    "octubre",
    "noviembre",
    "diciembre",
)


@dataclass(frozen=True)
class DataAge:
    """When the data behind an answer was last read from its source."""

    as_of: datetime
    days: int
    source: str  # "cached" | "registry" | "mart" | "mart_definition"

    @property
    def is_stale(self) -> bool:
        return self.days >= STALE_AFTER_DAYS

    def phrase_es(self) -> str:
        """A sentence a reader can act on, not a machine timestamp."""
        mes = _MONTHS_ES[self.as_of.month - 1]
        return (
            f"Los datos de esta respuesta se leyeron por última vez en {mes} de {self.as_of.year}."
        )


# A served table is either a raw table, or a mart. For a raw table, the last
# time the collector (or the vía-B writer) read or verified it against its
# source: `cached_datasets.updated_at` of the ready row. The collector bumps it
# also when it finds the file unchanged, which is exactly "read by us on…".
_CACHED_SQL = text(
    """
    SELECT max(updated_at) AS as_of
    FROM raw.cached_datasets
    WHERE table_name = :table AND status = 'ready'
    """
)

# A table with no ready row in `cached_datasets`: the registry's own date, the
# moment the live version appeared. Older than the truth at worst, never newer.
_REGISTRY_SQL = text(
    """
    SELECT max(created_at) AS as_of
    FROM public.raw_table_versions
    WHERE table_name = :table AND superseded_at IS NULL
    """
)

# A mart is as old as the oldest table it reads. The matview's own rewrite rule
# says which tables those are, today — no need to trust a list recorded at
# build time. Each source is dated the way it would be if it were served
# directly: its ready row in `cached_datasets`, else its live version in the
# registry. A source with neither is counted in `undated`, and the caller then
# takes the older of this and `source_data_oldest`: ignoring it could make the
# mart look fresher than it is (staging, 05-oct: `presupuesto_consolidado`
# said "read today" while one of its 11 sources, with no ready row, was last
# registered in May). Both joins hit unique indexes — (table_name) and
# (schema_name, table_name) —; joining the registry on `table_name` alone
# seq-scanned it and took 10 s on a mart with 287 sources, 49 ms this way.
_MART_SOURCES_SQL = text(
    """
    WITH src AS (
        SELECT DISTINCT tn.nspname AS schema_name, t.relname AS table_name
        FROM pg_class v
        JOIN pg_namespace vn ON vn.oid = v.relnamespace
        JOIN pg_rewrite r ON r.ev_class = v.oid
        JOIN pg_depend d ON d.objid = r.oid AND d.classid = 'pg_rewrite'::regclass
        JOIN pg_class t ON t.oid = d.refobjid AND t.oid <> v.oid
        JOIN pg_namespace tn ON tn.oid = t.relnamespace
        WHERE vn.nspname = 'mart' AND v.relname = :name AND t.relkind IN ('r', 'p', 'm', 'v')
    )
    SELECT min(coalesce(cd.updated_at, rtv.created_at)) AS as_of,
           count(*) FILTER (WHERE cd.updated_at IS NULL AND rtv.created_at IS NULL) AS undated
    FROM src
    LEFT JOIN raw.cached_datasets cd
           ON cd.table_name = src.table_name AND cd.status = 'ready'
    LEFT JOIN public.raw_table_versions rtv
           ON rtv.schema_name = src.schema_name AND rtv.table_name = src.table_name
          AND rtv.superseded_at IS NULL
    """
)

_MART_SQL = text(
    """
    SELECT source_data_oldest AS as_of
    FROM mart_definitions
    WHERE mart_id = :name OR mart_view_name = :name
    LIMIT 1
    """
)


def _strip_schema(name: str) -> str:
    return name.split(".")[-1].strip().strip('"')


def _aware(value: datetime | None) -> datetime | None:
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=UTC)


def data_age_for(engine: Engine, served: str | None) -> DataAge | None:
    """When was the data behind `served` last read from its source?

    `served` is whatever the pipeline recorded as the served table — a bare or
    qualified table name, or a mart id. Returns `None` when nothing can be said,
    which is not an error: many answers come from paths that do not name a
    table, and inventing a date for those would be worse than staying quiet.
    """
    if not served:
        return None
    name = _strip_schema(str(served))
    if not name:
        return None
    is_mart = str(served).strip().lower().startswith("mart.")

    lookups: list[tuple[str, Any]] = (
        [("mart", _MART_SOURCES_SQL), ("mart_definition", _MART_SQL)]
        if is_mart
        else [
            ("cached", _CACHED_SQL),
            ("registry", _REGISTRY_SQL),
            ("mart", _MART_SOURCES_SQL),
            ("mart_definition", _MART_SQL),
        ]
    )
    as_of = None
    source = ""
    try:
        with engine.connect() as conn:
            for source, sql in lookups:
                params = {"name": name} if source.startswith("mart") else {"table": name}
                row = conn.execute(sql, params).fetchone()
                as_of = _aware(row.as_of) if row else None
                if source == "mart" and row is not None and row.undated:
                    # A source we cannot date: the date recorded at build time
                    # may be older, and the older one wins.
                    recorded = conn.execute(_MART_SQL, {"name": name}).fetchone()
                    recorded_at = _aware(recorded.as_of) if recorded else None
                    if recorded_at is not None and (as_of is None or recorded_at < as_of):
                        as_of, source = recorded_at, "mart_definition"
                if as_of is not None:
                    break
            conn.rollback()
    except Exception:
        # Never cost the user their answer over a freshness lookup.
        logger.debug("data_age_for(%s) failed", served, exc_info=True)
        return None

    if as_of is None:
        return None
    days = (datetime.now(UTC) - as_of).days
    return DataAge(as_of=as_of, days=max(days, 0), source=source)


@dataclass(frozen=True)
class TableFreshness:
    """Qué tan fresca es una tabla del catálogo, para ``describir_tabla``.

    Dos fechas que no hay que confundir (auditoría 3.4):

    - ``actualizada``: la última vez que OpenArg leyó la tabla de su fuente
      (``data_age_for``: ``cached_datasets.updated_at`` de la fila lista). Si
      el archivo no había cambiado, se conservó el contenido: igual cuenta,
      porque la copia era la vigente ese día. No es ``raw_table_versions.
      created_at``: en staging, 654 tablas tienen una versión "creada" el
      01-ago por un backfill del registro, sin que nadie las leyera, y en las
      tablas vía B (presupuesto, BCRA) queda congelada en mayo aunque se
      reescriban todos los días (verificado el 05-oct).
    - ``ultimo_dato``: el último período con datos según la columna de fecha.
      Una serie leída ayer puede terminar en 2023.

    Una tabla con un solo período es una foto, con ``fecha_corte`` el día en
    que se leyó, sólo si ese período es el de la lectura (el crédito
    presupuestario del ejercicio en curso). Si es otro, aunque sea el anterior,
    la nota dice de cuándo son los datos y no hay ``fecha_corte``: un ejercicio
    2019, o el 2025 ya cerrado, releído hoy no es "vigente" hoy (revisión
    independiente del 05-oct, H022). Una tabla sin columna de fecha tampoco
    tiene ``fecha_corte``: la fecha de lectura no dice de cuándo son sus
    datos, que suele decirlo el título del dataset.

    ``serie`` es None cuando la tabla tiene columna de fecha pero no se pudo
    calcular qué período cubre (fechas en un formato que no se reconoce, una
    tabla enorme sin fechas en la muestra de ``pg_stats``). No es una foto:
    puede ser una serie que termina en 2019, y declararla vigente a la fecha
    de lectura es justo la confusión que 3.4 quería evitar.
    """

    actualizada: date | None
    dias_desde_actualizacion: int | None
    ultimo_dato: str | None
    fecha_corte: date | None
    serie: bool | None
    nota: str | None = None
    # El período sale de la muestra de `pg_stats`: puede haber datos
    # posteriores a `ultimo_dato`.
    aproximado: bool = False


def _fecha_es(day: date) -> str:
    return f"{day.day} de {_MONTHS_ES[day.month - 1]} de {day.year}"


# Un período como lo da `describir_periodo`: AAAA, AAAA-MM o AAAA-MM-DD.
_PERIODO_RE = re.compile(r"^(\d{4})(?:-(\d{2})(?:-(\d{2}))?)?$")


def _periodo(valor: str) -> tuple[str, date] | None:
    """La granularidad (``anio``, ``mes``, ``dia``) y el inicio de un período ISO."""
    m = _PERIODO_RE.match(valor.strip())
    if not m:
        return None
    anio, mes, dia = m.groups()
    try:
        inicio = date(int(anio), int(mes or 1), int(dia or 1))
    except ValueError:
        return None
    return ("dia" if dia else "mes" if mes else "anio"), inicio


def _es_periodo_de_la_lectura(valor: str, leida: date) -> bool:
    """¿El período es el del día de lectura, según su granularidad?

    El anterior no: el ejercicio que se cerró el año pasado no es el vigente,
    y la nota de foto no nombra el período, así que «vigentes al <lectura>»
    haría pasar el crédito de 2025 por el de hoy (revisión del PR #144). Un
    período que no se reconoce tampoco: sin saber de cuándo es, no se lo
    declara vigente.
    """
    periodo = _periodo(valor)
    if periodo is None:
        return False
    granularidad, inicio = periodo
    if granularidad == "dia":
        atraso = (leida - inicio).days
    elif granularidad == "mes":
        atraso = (leida.year * 12 + leida.month) - (inicio.year * 12 + inicio.month)
    else:
        atraso = leida.year - inicio.year
    return atraso == 0


def _de_periodo(valor: str) -> str:
    """«de 2019», «de marzo de 2019», «del 3 de marzo de 2019»: como lo diría una persona."""
    periodo = _periodo(valor)
    if periodo is None:
        return f"de {valor}"
    granularidad, inicio = periodo
    if granularidad == "dia":
        return f"del {_fecha_es(inicio)}"
    if granularidad == "mes":
        return f"de {_MONTHS_ES[inicio.month - 1]} de {inicio.year}"
    return f"de {inicio.year}"


def table_freshness(
    age: DataAge | None,
    *,
    columna_fecha: str | None,
    desde: str | None,
    hasta: str | None,
    aproximado: bool = False,
) -> TableFreshness:
    """Combina la fecha de lectura (``data_age_for``) con el período de la tabla.

    ``desde``/``hasta`` son los de la columna de fecha (``describir_periodo``,
    que ya acota el costo: timeout y, en tablas enormes, el rango de la
    muestra de ``pg_stats``, con ``aproximado``). Puro: se puede probar sin base.
    """
    actualizada = age.as_of.date() if age else None
    dias = age.days if age else None
    con_rango = bool(desde and hasta)
    serie: bool | None
    if not columna_fecha:
        serie = False
    elif not con_rango:
        # Tiene fechas pero no se sabe cuáles: ni serie ni foto.
        serie = None
    else:
        serie = desde != hasta
    ultimo = hasta if columna_fecha and con_rango else None
    aproximado = bool(aproximado and ultimo)
    # Una foto sólo si el único período es el de la lectura: sin columna de
    # fecha, o con un período pasado, la fecha de lectura no es la de los
    # datos (H022).
    foto = (
        serie is False
        and ultimo is not None
        and actualizada is not None
        and _es_periodo_de_la_lectura(ultimo, actualizada)
    )
    corte = actualizada if foto else None
    leida = f"OpenArg la leyó de su fuente el {_fecha_es(actualizada)}" if actualizada else None
    partes: list[str] = []
    if serie is None:
        partes.append(
            f"La tabla tiene columna de fecha ({columna_fecha}) pero no se pudo determinar qué "
            "período cubre: no se sabe cuál es el último dato. No la presentes como vigente; "
            'para ver las fechas más recientes pedí las filas con orden="desc".'
        )
    if corte is not None:
        partes.append(
            "Es una foto, sin serie temporal: los datos son los vigentes al "
            f"{_fecha_es(corte)}, cuando OpenArg la leyó de su fuente."
        )
    elif serie is False and ultimo is not None:
        partes.append(
            f"La tabla tiene un solo período: los datos son {_de_periodo(ultimo)}."
            + (f" {leida}, pero eso no los hace vigentes a esa fecha." if leida else "")
        )
    elif serie is False:
        partes.append(
            "La tabla no tiene columna de fecha: "
            + (
                f"{leida}, pero eso no dice de cuándo son los datos; lo suele decir"
                if leida
                else "de cuándo son los datos lo suele decir"
            )
            + " el título del dataset. No la presentes como vigente."
        )
    if aproximado:
        partes.append(
            f"El último dato ({ultimo}) es aproximado: sale de una muestra de la tabla, que es "
            "muy grande para recorrerla entera, y puede haber datos posteriores."
        )
    if age is not None and age.is_stale and actualizada is not None:
        partes.append(
            f"OpenArg la leyó de su fuente por última vez hace {age.days} días "
            f"({_fecha_es(actualizada)}): el portal puede tener datos más nuevos."
        )
    nota = " ".join(partes) or None
    return TableFreshness(
        actualizada=actualizada,
        dias_desde_actualizacion=dias,
        ultimo_dato=ultimo,
        fecha_corte=corte,
        serie=serie,
        nota=nota,
        aproximado=aproximado,
    )


def staleness_warning(engine: Engine, served: str | None) -> str | None:
    """The sentence to show, or `None` when the data is current enough.

    Only stale data earns a line. A notice on every answer becomes furniture the
    reader stops seeing, and then it is not there on the day it matters.
    """
    age = data_age_for(engine, served)
    if age is None or not age.is_stale:
        return None
    return age.phrase_es()


# ── how old is the observation ─────────────────────────────

# Margin past the end of the last period before a series counts as behind.
# Calibrated against the API's own `is_updated` on 04-oct-2026: the EMAE is 65
# days past the end of July and the API says it is current (the INDEC publishes
# it ~50 days after the month), so "two periods" would have flagged it. The
# daily reserves series, 34 days behind, is flagged by any margin.
FRESHNESS_MARGIN_DAYS: dict[str, int] = {
    "diaria": 7,
    "semanal": 21,
    "mensual": 75,
    "trimestral": 120,
    "semestral": 270,
    "anual": 550,
}

_FREQUENCY_ALIASES = {
    "diaria": "diaria",
    "daily": "diaria",
    "day": "diaria",
    "r/p1d": "diaria",
    "semanal": "semanal",
    "weekly": "semanal",
    "week": "semanal",
    "r/p1w": "semanal",
    "mensual": "mensual",
    "monthly": "mensual",
    "month": "mensual",
    "r/p1m": "mensual",
    "trimestral": "trimestral",
    "quarterly": "trimestral",
    "quarter": "trimestral",
    "r/p3m": "trimestral",
    "semestral": "semestral",
    "semester": "semestral",
    "r/p6m": "semestral",
    "anual": "anual",
    "yearly": "anual",
    "year": "anual",
    "r/p1y": "anual",
}

_PERIOD_MONTHS = {"mensual": 1, "trimestral": 3, "semestral": 6, "anual": 12}


def _as_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    m = re.match(r"^\s*((?:19|20)\d{2})-(\d{1,2})(?:-(\d{1,2}))?", str(value or ""))
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3) or 1))
    except ValueError:
        return None


def _normalize_frequency(value: Any) -> str | None:
    return _FREQUENCY_ALIASES.get(str(value or "").strip().lower())


def _infer_frequency(dates: Sequence[date]) -> str | None:
    """La frecuencia por el paso entre las dos últimas fechas distintas."""
    distinct = sorted(set(dates))
    if len(distinct) < 2:
        return None
    step = (distinct[-1] - distinct[-2]).days
    if step <= 4:  # un fin de semana o un feriado en una serie diaria
        return "diaria"
    if step <= 10:
        return "semanal"
    if 27 <= step <= 32:
        return "mensual"
    if 88 <= step <= 93:
        return "trimestral"
    if 180 <= step <= 186:
        return "semestral"
    if 364 <= step <= 367:
        return "anual"
    return None


def _period_end(last: date, frequency: str) -> date:
    """El último día del período que empieza en ``last``.

    Las series de la API fechan cada período por su primer día (el IPC de
    agosto es `2026-08-01`). Una serie fechada por su fin da un período que
    termina en el futuro: el atraso sale negativo y no hay aviso, que es lo
    prudente.
    """
    months = _PERIOD_MONTHS.get(frequency)
    if not months:
        return last
    total = last.year * 12 + (last.month - 1) + months
    return date(total // 12, total % 12 + 1, 1) - timedelta(days=1)


@dataclass(frozen=True)
class ObservationAge:
    """De cuándo es la última observación de un resultado, y si está atrasada."""

    title: str
    last: date
    frequency: str | None
    days_behind: int
    updated_at_source: bool | None
    stale: bool


def observation_staleness(
    last: date,
    frequency: str | None,
    today: date,
    *,
    updated_at_source: bool | None = None,
    title: str = "",
) -> ObservationAge:
    """¿La última observación está atrasada para su frecuencia?

    Atrasada si la fuente dice que la serie no se actualiza
    (``is_updated=False`` en la API de Series de Tiempo) o si pasaron más días
    que el margen de su frecuencia desde el fin del último período. Sin
    frecuencia conocida sólo cuenta lo que dice la fuente.
    """
    freq = _normalize_frequency(frequency) or frequency
    behind = (today - _period_end(last, freq)).days if freq else (today - last).days
    margin = FRESHNESS_MARGIN_DAYS.get(freq or "")
    stale = updated_at_source is False or (margin is not None and behind > margin)
    return ObservationAge(title, last, freq, behind, updated_at_source, stale)


_LIVE_SOURCES_DAILY = frozenset({"dolarapi", "argentina_datos", "bcra"})
_CONTRACT_KEYS = ("ultima_observacion", "frecuencia", "fecha_fin_fuente", "actualizada_en_fuente")

# "¿Cuánto exportó Argentina en el primer semestre de 2026?", "¿qué relación
# hubo entre inflación y salarios en 2025?": la pregunta pide un período que
# nombra, y que la serie termine ahí no es un atraso. Medido en la calibración
# del 04-oct: sin esta guarda, 3 de los 9 avisos de atraso eran de este tipo.
_CURRENT_INTENT_RE = re.compile(
    r"\b(?:actual\w*|hoy|ahora|[uú]ltim[oa]s?|reciente\w*|vigente|desde|"
    r"c[oó]mo\s+(?:viene|est[aá]|va)|en\s+este\s+momento)\b",
    re.IGNORECASE,
)
_YEAR_IN_QUESTION_RE = re.compile(r"\b(?:19|20)\d{2}\b")


def asks_for_named_period(question: str) -> bool:
    """¿La pregunta nombra un período (un año) y no pide el valor actual?"""
    text = question or ""
    return bool(_YEAR_IN_QUESTION_RE.search(text)) and not _CURRENT_INTENT_RE.search(text)


def _observations_for(result: Any, today: date, question: str = "") -> list[ObservationAge]:
    """La última observación de un resultado, con el contrato de metadatos de los conectores.

    Lee ``ultima_observacion``, ``frecuencia``, ``fecha_fin_fuente`` y
    ``actualizada_en_fuente``; si faltan, las deduce de la columna ``fecha``.
    Sólo series de tiempo: las tablas del catálogo no entran (sus fechas
    dependen del filtro que eligió el modelo; su atraso lo dice
    ``staleness_warning``), y tampoco los fragmentos de sesiones, que tienen
    fecha pero no son una serie.

    Con varias series en un pedido (``metadata["series"]``) va una por serie.
    El agregado que arma el adaptador lleva el título del primer dataset, la
    fecha de fin de la más atrasada y el «desactualizada» de cualquiera: el
    aviso decía «el último dato del IPC es de mayo de 2026» cuando el IPC
    llega a agosto y la que terminaba en mayo era el salario real (revisión
    del 05-oct, H020). Cada serie se mide con su fecha de fin, su última fila
    con valor y su «actualizada», y el aviso la nombra.
    """
    source = str(getattr(result, "source", "") or "")
    meta = getattr(result, "metadata", None) or {}
    has_contract = any(k in meta for k in _CONTRACT_KEYS)
    if source.startswith("sandbox:"):
        return []
    if not has_contract and getattr(result, "format", "") != "time_series":
        return []
    records = [r for r in (getattr(result, "records", None) or []) if isinstance(r, dict)]
    dates = [d for d in (_as_date(r.get("fecha")) for r in records) if d]
    last = _as_date(meta.get("ultima_observacion")) or (max(dates) if dates else None)
    if last is None:
        return []
    frequency = _normalize_frequency(meta.get("frecuencia")) or _infer_frequency(dates)
    if frequency is None and (meta.get("realtime") or source in _LIVE_SOURCES_DAILY):
        frequency = "diaria"
    truncated = bool(meta.get("truncada"))
    series = [s for s in (meta.get("series") or []) if isinstance(s, dict)]
    if len(series) < 2:
        age = _observation_age(
            last,
            meta.get("fecha_fin_fuente"),
            frequency,
            meta.get("actualizada_en_fuente"),
            title=str(getattr(result, "dataset_title", "") or ""),
            source_end_inferred=meta.get("fecha_fin_fuente_inferida") is True,
            truncated=truncated,
            question=question,
            today=today,
        )
        return [age] if age is not None else []
    out: list[ObservationAge] = []
    for entry in series:
        title = str(entry.get("titulo") or entry.get("id") or "")
        # Las filas traen cada serie bajo su título (o su id, si no tiene).
        keys = {k for k in (entry.get("titulo"), entry.get("id")) if k}
        with_value = (r for r in records if any(r.get(k) is not None for k in keys))
        age = _observation_age(
            max(filter(None, (_as_date(r.get("fecha")) for r in with_value)), default=last),
            entry.get("fecha_fin_fuente"),
            # Las fechas de las filas van en la frecuencia de la respuesta; la
            # de la serie, sólo si no se sabe otra.
            frequency or _normalize_frequency(entry.get("frecuencia")),
            entry.get("actualizada_en_fuente"),
            title=title,
            source_end_inferred=entry.get("fecha_fin_fuente_inferida") is True,
            truncated=truncated,
            question=question,
            today=today,
        )
        if age is not None:
            out.append(age)
    return out


def _observation_age(
    last: date,
    source_end_value: Any,
    frequency: str | None,
    updated: Any,
    *,
    title: str,
    source_end_inferred: bool,
    truncated: bool,
    question: str,
    today: date,
) -> ObservationAge | None:
    """El atraso de una serie, o None si lo que se trajo es un período pedido a propósito."""
    source_end = _as_date(source_end_value)
    # Si la fuente llega más lejos que lo que se trajo y no fue un corte, la
    # pregunta pidió un período pasado: no es un dato atrasado. Se compara el
    # fin del período de la última fila: una diaria pedida junto con una
    # mensual llega agregada por mes, fechada el 1.º, y su fecha de fin en la
    # fuente es un día de ese mismo mes.
    if source_end is not None and _period_end(last, frequency or "") < source_end and not truncated:
        return None
    # Sin la fecha de fin de la fuente no se distingue "la serie termina acá"
    # de "se pidió hasta acá": si la pregunta nombra un período, no se avisa.
    # Tampoco si el conector la infirió del último dato traído
    # (`fecha_fin_fuente_inferida`: la metadata no traía time_index_end, o
    # era anterior): con un período pasado es el fin de lo pedido, y «¿cuál
    # fue la inflación de 2019?» salía con «Dato atrasado… es de diciembre de
    # 2019».
    if (source_end is None or source_end_inferred) and asks_for_named_period(question):
        return None
    return observation_staleness(
        source_end or last,
        frequency,
        today,
        updated_at_source=updated if isinstance(updated, bool) else None,
        title=title,
    )


_QUARTERS = {1: "1.er", 2: "2.º", 3: "3.er", 4: "4.º"}


def observation_label(last: date, frequency: str | None) -> str:
    """La fecha de la observación como la diría una persona."""
    mes = _MONTHS_ES[last.month - 1]
    if frequency in ("diaria", "semanal"):
        return f"{last.day} de {mes} de {last.year}"
    if frequency == "trimestral":
        return f"el {_QUARTERS[(last.month - 1) // 3 + 1]} trimestre de {last.year}"
    if frequency == "semestral":
        return f"el {'1.er' if last.month <= 6 else '2.º'} semestre de {last.year}"
    if frequency == "anual":
        return str(last.year)
    return f"{mes} de {last.year}"


_FREQUENCY_NOUN = {
    "diaria": "serie diaria",
    "semanal": "serie semanal",
    "mensual": "serie mensual",
    "trimestral": "serie trimestral",
    "semestral": "serie semestral",
    "anual": "serie anual",
}


def _notice(age: ObservationAge) -> str:
    title = " ".join(age.title.split())
    if len(title) > 90:
        title = title[:89].rstrip() + "…"
    what = f"de «{title}» " if title else ""
    kind = _FREQUENCY_NOUN.get(age.frequency or "")
    detail = f" ({kind})" if kind else ""
    reason = " y la fuente no la actualizó desde entonces" if age.updated_at_source is False else ""
    return (
        f"**Dato atrasado:** el último dato {what}es de "
        f"{observation_label(age.last, age.frequency)}{detail}{reason}, así que no refleja "
        "el valor actual."
    )


_MAX_NOTICES = 2


def freshness_notices(
    evidence: Sequence[Any], today: date | None = None, question: str = ""
) -> list[str]:
    """Los avisos de atraso de la evidencia que respalda la respuesta.

    Van ARRIBA de la respuesta, en el texto: así los ven igual /ask, el MCP y
    el chat web, sin depender de cómo cada uno muestre las advertencias.
    Uno por título, como mucho dos: un aviso en cada respuesta se vuelve
    mobiliario y deja de leerse.
    """
    day = today or date.today()
    out: list[str] = []
    seen: set[str] = set()
    for result in evidence:
        try:
            ages = _observations_for(result, day, question)
        except Exception:
            logger.debug("freshness: could not date %r", result, exc_info=True)
            continue
        for age in ages:
            if not age.stale or age.title in seen:
                continue
            seen.add(age.title)
            out.append(_notice(age))
            if len(out) >= _MAX_NOTICES:
                return out
    return out
