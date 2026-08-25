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
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime

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
    source: str  # "registry" | "mart"

    @property
    def is_stale(self) -> bool:
        return self.days >= STALE_AFTER_DAYS

    def phrase_es(self) -> str:
        """A sentence a reader can act on, not a machine timestamp."""
        mes = _MONTHS_ES[self.as_of.month - 1]
        return (
            f"Los datos de esta respuesta se leyeron por última vez en {mes} de {self.as_of.year}."
        )


# A served table is either a raw table the registry knows, or a mart. Ask the
# registry first: it holds the exact moment we read the source, which is the
# thing the reader wants and the only date here that is not an approximation.
_REGISTRY_SQL = text(
    """
    SELECT max(created_at) AS as_of
    FROM public.raw_table_versions
    WHERE table_name = :table AND superseded_at IS NULL
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

    try:
        with engine.connect() as conn:
            row = conn.execute(_REGISTRY_SQL, {"table": name}).fetchone()
            as_of = row.as_of if row else None
            source = "registry"
            if as_of is None:
                row = conn.execute(_MART_SQL, {"name": name}).fetchone()
                as_of = row.as_of if row else None
                source = "mart"
            conn.rollback()
    except Exception:
        # Never cost the user their answer over a freshness lookup.
        logger.debug("data_age_for(%s) failed", served, exc_info=True)
        return None

    if as_of is None:
        return None
    if as_of.tzinfo is None:
        as_of = as_of.replace(tzinfo=UTC)
    days = (datetime.now(UTC) - as_of).days
    return DataAge(as_of=as_of, days=max(days, 0), source=source)


def staleness_warning(engine: Engine, served: str | None) -> str | None:
    """The sentence to show, or `None` when the data is current enough.

    Only stale data earns a line. A notice on every answer becomes furniture the
    reader stops seeing, and then it is not there on the day it matters.
    """
    age = data_age_for(engine, served)
    if age is None or not age.is_stale:
        return None
    return age.phrase_es()
