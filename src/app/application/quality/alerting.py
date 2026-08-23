"""Send a person a message when something needs a person.

Dante's §5.5 asks for one human alert per new CRITICAL, and that half stayed
open for a reason that was never about code: **there was nowhere to send it**.
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
# list does.
MAX_PER_RUN = 5

_TELEGRAM_TIMEOUT = 15


@dataclass(frozen=True)
class Alert:
    """One thing worth a person's attention."""

    kind: str  # "drift" | "mart_failed" | ...
    key: str  # stable identity of the thing, NOT of this sighting
    title: str
    detail: str = ""

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
    RETURNING (xmax = 0) AS is_new
    """
)


def _claim_new(engine: Engine, alerts: list[Alert]) -> list[Alert]:
    """Which of these has never been reported? Records the sighting either way."""
    fresh: list[Alert] = []
    try:
        with engine.begin() as conn:
            conn.execute(_ENSURE_SQL)
            for a in alerts:
                row = conn.execute(
                    _CLAIM_SQL,
                    {"fp": a.fingerprint(), "kind": a.kind, "key": a.key, "title": a.title},
                ).fetchone()
                if row and row.is_new:
                    fresh.append(a)
    except Exception:
        # Cannot tell new from old, so send nothing. The alternative is
        # re-sending everything the dedup was there to suppress, which is how a
        # channel gets muted.
        logger.warning("alerting: could not claim alerts; staying quiet", exc_info=True)
        return []
    return fresh


def notify(engine: Engine, alerts: list[Alert], *, heading: str) -> dict[str, object]:
    """Report what is new, at most `MAX_PER_RUN`, or say nothing at all."""
    if not alerts:
        # Silence is the answer. A daily "all clear" is furniture.
        return {"considered": 0, "new": 0, "sent": 0}

    fresh = _claim_new(engine, alerts)
    if not fresh:
        return {"considered": len(alerts), "new": 0, "sent": 0}

    shown = fresh[:MAX_PER_RUN]
    lines = [f"<b>{heading}</b>", ""]
    for a in shown:
        lines.append(f"• {a.title}")
        if a.detail:
            lines.append(f"  <i>{a.detail}</i>")
    if len(fresh) > len(shown):
        # The count, not the list. Something systemic produces hundreds and the
        # number is the finding.
        lines.append("")
        lines.append(f"…y {len(fresh) - len(shown)} más sin listar.")

    sent = _send("\n".join(lines))
    return {"considered": len(alerts), "new": len(fresh), "sent": len(shown) if sent else 0}
