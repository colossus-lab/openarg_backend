"""Stop serving a table nobody could repair.

The ladder has two outcomes today: fixed, or a message to a person. Production
systems have a third, and four of the ten reviews named its absence
independently as the largest hole in this design.

The asymmetry is what makes it obvious once stated. A table whose columns are
`col_1`, `col_2`, `Unnamed: 3` is not merely unhelpful to a chatbot that writes
SQL — it is *dangerous*, because the model will infer a meaning for `col_1` and
answer fluently, with a citation, from a column whose contents nobody has read.
For a public civic assistant, **"no tengo el dato" beats a sourced wrong number
every time**, and that trade is not close.

The machinery already existed and was never used. `catalog_resources` has a
`materialization_corrupted` status, and `catalog_discovery` already excludes it
from everything the query pipeline can see. Production held **zero** rows in that
state: the door was built and never opened.

**Bounded, reversible, and narrow by design.**

- *Narrow*: only the defect classes that make a column unreadable — placeholder
  names, a delimiter left inside a name. A merely long column name is ugly, not
  dangerous, and withholding those would quarantine thousands of usable tables.
- *Bounded*: a cap per run. The recurring lesson in every automation incident
  reviewed — Azure's remediation job, Google's Diskerase — is that the action was
  correct and the *scope* was wrong.
- *Reversible*: a repair that later succeeds releases the table. Quarantine is a
  pause, not a verdict, and one that could not be undone would be a worse bug
  than the one it prevents.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from typing import Any

from sqlalchemy import text

logger = logging.getLogger(__name__)

# Symptoms that make a column's contents unreadable to anything downstream.
#
# Two are deliberately absent. `long_name` is unpleasant to read and perfectly
# usable. And `one_or_two_columns` was here until 2026-09-01, when measurement
# showed it marked **1,383 healthy tables** — `BARRIO | POBLACION` is not a
# defect, it is what a population-by-neighbourhood table looks like. Both would
# withhold working data from the chat to protect it from being readable.
UNREADABLE_SYMPTOMS = frozenset({"col_n", "unnamed", "delimiter_in_name"})

# Most this may withhold in a single run. Small, deliberately: quarantining is
# cheap to do and expensive to be wrong about, and the sweep runs daily.
MAX_PER_RUN = 15

_QUARANTINE_SQL = text(
    """
    UPDATE public.catalog_resources
    SET materialization_status = 'materialization_corrupted',
        updated_at = NOW()
    WHERE materialized_table_name = :table
      AND materialization_status = 'ready'
    """
)

_RELEASE_SQL = text(
    """
    UPDATE public.catalog_resources
    SET materialization_status = 'ready',
        updated_at = NOW()
    WHERE materialized_table_name = :table
      AND materialization_status = 'materialization_corrupted'
    """
)


def is_quarantinable(symptoms: Sequence[str]) -> bool:
    """Would serving this table risk an answer nobody can check?"""
    return bool(UNREADABLE_SYMPTOMS.intersection(symptoms))


def quarantine(engine: Any, table_name: str) -> bool:
    """Withhold a table from everything the query pipeline can see.

    Returns whether a row changed. Only moves a resource that is currently
    `ready`: a table already `failed` or `pending` is not being served anyway,
    and rewriting its status would lose why it got there.
    """
    try:
        with engine.begin() as conn:
            changed = conn.execute(_QUARANTINE_SQL, {"table": table_name}).rowcount
    except Exception:
        logger.warning("quarantine: could not withhold %s", table_name, exc_info=True)
        return False
    if changed:
        logger.warning("quarantine: %s retirada del servicio", table_name)
    return bool(changed)


def release(engine: Any, table_name: str) -> bool:
    """Put a repaired table back into service.

    Only releases what quarantine withheld. A resource that reached
    `materialization_corrupted` by another route keeps its status until whatever
    put it there is satisfied.
    """
    try:
        with engine.begin() as conn:
            changed = conn.execute(_RELEASE_SQL, {"table": table_name}).rowcount
    except Exception:
        logger.warning("quarantine: could not release %s", table_name, exc_info=True)
        return False
    if changed:
        logger.info("quarantine: %s vuelve al servicio", table_name)
    return bool(changed)
