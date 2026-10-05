"""Tell a person when a source stopped arriving.

The counterpart to every guard that refuses to write. Refusing is right — a
blank payroll is worse than a stale one — but by itself it trades a loud failure
for a silent one: the mart keeps answering and nothing says the data froze.

Deliberately ranked by **how late relative to its own cadence**, not by age. A
census that arrives yearly and a exchange rate that arrives hourly are both
interesting at very different absolute ages, and sorting by age alone would put
the census at the top forever.

Scheduled tasks are the exception to "learned, not declared": their period is
written in the beat schedule, so it is read from there (`cadencias_declaradas`)
instead of being estimated from sightings. A monthly task used to need four
months of history and then ninety days of silence before it could be called
late.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from datetime import date, timedelta
from typing import Any

from app.infrastructure.celery.app import celery_app
from app.infrastructure.celery.tasks._db import get_sync_engine

logger = logging.getLogger(__name__)

# Three years from a leap year: every month length, a 29 of February and every
# weekday on every date, so the longest gap of any crontab shows up.
_VENTANA_DIAS = 3 * 366
_INICIO_VENTANA = date(2024, 1, 1)


def intervalo_maximo_segundos(cron: Any) -> float | None:
    """The longest gap between two consecutive runs of a crontab, in seconds.

    Exact for the fields a crontab can restrict: within a matching day the runs
    are the same every day, so the longest gap is either inside one day or from
    the last run of a matching day to the first run of the next one. `None` for
    anything that is not a crontab or never fires.

    Celery requires day-of-month AND day-of-week to match (not cron's OR), and
    counts Sunday as 0.
    """
    try:
        minutos = sorted(int(m) for m in cron.minute)
        horas = sorted(int(h) for h in cron.hour)
        dias_semana = {int(d) for d in cron.day_of_week}
        dias_mes = {int(d) for d in cron.day_of_month}
        meses = {int(m) for m in cron.month_of_year}
    except (AttributeError, TypeError, ValueError):
        return None
    if not minutos or not horas:
        return None

    del_dia = [h * 60 + m for h in horas for m in minutos]
    hueco_en_el_dia = max((b - a for a, b in zip(del_dia, del_dia[1:], strict=False)), default=0)

    dias = [
        d
        for d in (_INICIO_VENTANA + timedelta(days=i) for i in range(_VENTANA_DIAS))
        if d.month in meses and d.day in dias_mes and (d.isoweekday() % 7) in dias_semana
    ]
    if not dias:
        return None
    if len(dias) == 1:
        return None  # fires once in three years: not a cadence worth judging

    hueco_entre_dias = max(
        (b - a).days * 1440 - del_dia[-1] + del_dia[0] for a, b in zip(dias, dias[1:], strict=False)
    )
    return float(max(hueco_en_el_dia, hueco_entre_dias) * 60)


def cadencias_declaradas(beat_schedule: Mapping[str, Any] | None) -> dict[str, float]:
    """`task:<name>` → the longest gap its beat schedule allows, in seconds.

    A task scheduled in several entries (one per queue, say) records its runs on
    one heartbeat row, so the shortest of its periods is the one to hold it to.
    """
    from app.application.quality.heartbeat import TASK_PREFIX

    cadencias: dict[str, float] = {}
    for entrada in (beat_schedule or {}).values():
        if not isinstance(entrada, dict) or not entrada.get("task"):
            continue
        segundos = intervalo_maximo_segundos(entrada.get("schedule"))
        if not segundos:
            continue
        clave = f"{TASK_PREFIX}{entrada['task']}"
        cadencias[clave] = min(segundos, cadencias.get(clave, segundos))
    return cadencias


@celery_app.task(
    name="openarg.alert_stale_ingests",
    bind=True,
    soft_time_limit=600,
    time_limit=900,
)
def alert_stale_ingests(self, *, multiple: float = 3.0, limit: int = 50) -> dict[str, Any]:
    """Report sources that are late by their own standards."""
    from app.application.quality.heartbeat import find_late

    engine = get_sync_engine()
    try:
        declaradas = cadencias_declaradas(celery_app.conf.beat_schedule)
    except Exception:
        # Without them the tasks fall back to their learned cadence, which is
        # what this did before; not a reason to skip the sweep.
        logger.warning("stale ingests: could not read the beat schedule", exc_info=True)
        declaradas = {}
    late = find_late(engine, multiple=multiple, limit=limit, declared=declaradas)

    report: dict[str, Any] = {
        "late": len(late),
        "worst": [
            {
                "recurso": item.resource_identity,
                "dias_tarde": round(item.days_late, 1),
                "cadencia_dias": round(item.cadence_days, 2),
            }
            for item in late[:10]
        ],
    }
    logger.info("stale ingests: %s", report)

    if not late:
        return report

    try:
        from app.application.quality.alerting import Alert, notify

        report["alerting"] = notify(
            engine,
            [
                Alert(
                    kind="ingest_late",
                    # Identity of the source. A source that stays late is
                    # reported once, not every morning.
                    key=item.resource_identity,
                    title=f"{item.resource_identity[:60]} dejó de llegar",
                    detail=item.phrase_es(),
                )
                for item in late
            ],
            heading="OpenArg · fuentes que dejaron de llegar",
        )
    except Exception:
        logger.warning("stale ingests: alerting skipped", exc_info=True)
    return report
