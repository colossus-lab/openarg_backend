"""Send a person a message when something needs a person.

The data-quality plan asks for one human alert per new CRITICAL, and that half
stayed open for a reason that was never about code: **there was nowhere to send
it**.
The drift report has run in shadow since 2026-08-21 producing verdicts nobody
reads, and the three broken marts were found by someone poking at a database on
an unrelated errand, weeks after they broke.

This is the channel. Telegram because it was available and reaches a phone;
nothing here depends on that choice beyond `_send`.

**Why alerting is harder than sending a message.** A monitor that fires on every
occurrence teaches its reader to swipe it away, and a channel people mute is
worse than no channel — it converts a real signal into a habit of dismissal. So
this carries three constraints the send path cannot bypass:

- **Only new things.** A finding already reported stays quiet, by a fingerprint
  of what it is rather than when it was seen. The drift report re-evaluates the
  same pairs every week; without this, the first Monday would produce N alerts
  and every Monday after would produce the same N.
- **A ceiling per run.** Something systemic — a portal changing every file, a
  parser regression — produces hundreds of findings at once. Sending hundreds of
  messages communicates less than sending five and saying there are 300.
- **Silence is a real answer.** No findings means no message. A daily "all
  clear" is exactly the furniture that trains people to stop looking.

Three more, added after a review measured what this design would actually do:

- **Not every kind deserves a line.** "Repaired automatically" is the highest
  volume and the lowest actionability — a textbook dead-end alert. With
  thousands of broken tables it would spend the whole weekly attention budget in
  one run and collapse engagement for the kinds that do need a person. It is
  counted, not listed.
- **A problem that keeps coming back is news again.** Fingerprinting by the
  identity of the thing means a table that breaks, gets repaired and breaks again
  is silently deduped — and a repair loop is the *most* actionable signal there
  is. Crossing a repeat threshold re-opens it.
- **The cap must not let chronic findings squat.** Capping a single ordered list
  meant the same long-standing items filled the slots every run and genuinely new
  breakage never surfaced. New keys go first, and the cap is per kind so one
  noisy kind cannot crowd out the others.

Failing to alert must never fail the job that raised the alert. Every send is
wrapped, and a channel that is down costs the notification, not the sweep.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import urllib.parse
import urllib.request
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# Enough to show the shape of a problem; past that the count says more than the
# list does. Applied **per kind**, so a hundred repairs cannot crowd out the one
# mart that broke.
MAX_PER_RUN = 5

# Counted, never listed. Non-actionable by construction: nothing is expected of
# the reader, and at this system's volume these would be every message.
DIGEST_KINDS = frozenset({"repaired"})

# Never capped, never digested. This is the one that says a repair was withheld
# because applying it would break something a person built, and it is the single
# most actionable line this channel can carry.
ALWAYS_SHOW_KINDS = frozenset({"repair_would_break_mart"})

# A finding seen this many times is not the same finding being re-reported — it
# is a loop, and a loop is news. Sparse on purpose: re-opening on every sighting
# would undo the deduplication entirely.
REOPEN_AT = (3, 10, 30, 100)

_TELEGRAM_TIMEOUT = 15


@dataclass(frozen=True)
class Alert:
    """One thing worth a person's attention."""

    kind: str  # "drift" | "mart_failed" | ...
    key: str  # stable identity of the thing, NOT of this sighting
    title: str
    detail: str = ""

    @property
    def digest_only(self) -> bool:
        return self.kind in DIGEST_KINDS

    def fingerprint(self) -> str:
        """Identity of the problem, so the same one is not reported twice.

        Deliberately excludes any timestamp. The weekly report re-derives the
        same findings from the same pairs; fingerprinting the sighting instead
        of the thing would make every Monday look like a fresh outbreak.
        """
        return hashlib.sha256(f"{self.kind}::{self.key}".encode()).hexdigest()[:32]


def _enabled() -> tuple[str, str] | None:
    """Credentials, or `None` when alerting is not configured.

    Absent configuration is the normal state in development and in tests, and
    must read as "not set up" rather than as an error.
    """
    token = os.getenv("OPENARG_TELEGRAM_TOKEN", "").strip()
    chat = os.getenv("OPENARG_TELEGRAM_CHAT_ID", "").strip()
    return (token, chat) if token and chat else None


def _send(text_body: str) -> bool:
    """Post one message. Returns whether it landed; never raises."""
    creds = _enabled()
    if creds is None:
        logger.info("alerting: not configured, so nothing is sent")
        return False
    token, chat = creds
    payload = urllib.parse.urlencode(
        {
            "chat_id": chat,
            "text": text_body,
            "parse_mode": "HTML",
            "disable_web_page_preview": "true",
        }
    ).encode()
    try:
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{token}/sendMessage", data=payload
        )
        with urllib.request.urlopen(req, timeout=_TELEGRAM_TIMEOUT) as resp:
            body = json.loads(resp.read())
        if not body.get("ok"):
            logger.warning("alerting: telegram refused: %s", str(body)[:200])
            return False
        return True
    except Exception:
        # The alert is lost, the caller is not. A monitoring channel that can
        # break the thing it monitors is worse than no channel.
        logger.warning("alerting: send failed", exc_info=True)
        return False


_ENSURE_SQL = text(
    """
    CREATE TABLE IF NOT EXISTS public.alert_log (
        fingerprint TEXT PRIMARY KEY,
        kind        TEXT NOT NULL,
        key         TEXT NOT NULL,
        title       TEXT NOT NULL,
        first_seen  TIMESTAMPTZ NOT NULL DEFAULT now(),
        last_seen   TIMESTAMPTZ NOT NULL DEFAULT now(),
        times_seen  INTEGER NOT NULL DEFAULT 1
    )
    """
)

# ON CONFLICT so a repeat sighting updates the record instead of alerting again.
# `xmax = 0` is Postgres's way of saying the row was inserted rather than
# updated — which is exactly "this is new", decided by the database rather than
# by a read-then-write that two workers could both win.
_CLAIM_SQL = text(
    """
    INSERT INTO public.alert_log (fingerprint, kind, key, title)
    VALUES (:fp, :kind, :key, :title)
    ON CONFLICT (fingerprint) DO UPDATE
        SET last_seen = now(), times_seen = public.alert_log.times_seen + 1
    RETURNING (xmax = 0) AS is_new, times_seen
    """
)


def _claim(engine: Engine, alerts: list[Alert]) -> tuple[list[Alert], list[tuple[Alert, int]]]:
    """Which are new, and which have come back often enough to be news again.

    Records the sighting either way. The second list is the one that did not
    exist before: a table that breaks, gets repaired and breaks again dedups to
    silence under a stable fingerprint, and that oscillation is the most
    actionable thing this channel can report.
    """
    fresh: list[Alert] = []
    reopened: list[tuple[Alert, int]] = []
    try:
        with engine.begin() as conn:
            conn.execute(_ENSURE_SQL)
            for a in alerts:
                row = conn.execute(
                    _CLAIM_SQL,
                    {"fp": a.fingerprint(), "kind": a.kind, "key": a.key, "title": a.title},
                ).fetchone()
                if not row:
                    continue
                if row.is_new:
                    fresh.append(a)
                elif row.times_seen in REOPEN_AT:
                    reopened.append((a, int(row.times_seen)))
    except Exception:
        # Cannot tell new from old, so send nothing. The alternative is
        # re-sending everything the dedup was there to suppress, which is how a
        # channel gets muted.
        logger.warning("alerting: could not claim alerts; staying quiet", exc_info=True)
        return [], []
    return fresh, reopened


def _select_shown(fresh: list[Alert]) -> tuple[list[Alert], int]:
    """What to list, capped per kind, with the exempt kind never trimmed.

    Ordering matters as much as the cap. A single global list let the same
    long-standing findings fill every slot, so genuinely new breakage in a
    quieter kind never reached anyone.
    """
    shown: list[Alert] = []
    por_tipo: dict[str, int] = {}
    for a in fresh:
        if a.kind in ALWAYS_SHOW_KINDS:
            shown.append(a)
            continue
        n = por_tipo.get(a.kind, 0)
        if n < MAX_PER_RUN:
            shown.append(a)
            por_tipo[a.kind] = n + 1
    return shown, len(fresh) - len(shown)


def notify(engine: Engine, alerts: list[Alert], *, heading: str) -> dict[str, object]:
    """Report what is new, what came back, and a count of the rest."""
    if not alerts:
        # Silence is the answer. A daily "all clear" is furniture.
        return {"considered": 0, "new": 0, "sent": 0}

    fresh, reopened = _claim(engine, alerts)
    if not fresh and not reopened:
        return {"considered": len(alerts), "new": 0, "sent": 0}

    listables = [a for a in fresh if not a.digest_only]
    shown, resto = _select_shown(listables)
    digested = [a for a in fresh if a.digest_only]

    lines = [f"<b>{heading}</b>", ""]
    for a in shown:
        lines.append(f"• {a.title}")
        if a.detail:
            lines.append(f"  <i>{a.detail}</i>")

    for a, veces in reopened:
        # Said differently on purpose: this is not a new problem, it is one that
        # will not stay fixed, and that distinction is the point of reporting it.
        lines.append(f"🔁 <b>{a.title}</b> — vuelve por {veces}ª vez")
        if a.detail:
            lines.append(f"  <i>{a.detail}</i>")

    if resto:
        # The count, not the list. Something systemic produces hundreds and the
        # number is the finding.
        lines.append("")
        lines.append(f"…y {resto} más sin listar.")

    if digested:
        # One line for the whole class. Nothing is expected of the reader, so
        # listing them would spend the attention budget on the least actionable
        # thing this channel carries.
        lines.append("")
        lines.append(f"<i>Además, {len(digested)} arreglo(s) automático(s).</i>")

    sent = _send("\n".join(lines))
    return {
        "considered": len(alerts),
        "new": len(fresh),
        "reopened": len(reopened),
        "digested": len(digested),
        "sent": (len(shown) + len(reopened)) if sent else 0,
    }
