"""Structured logging (structlog) and Sentry error tracking configuration."""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Mapping

import structlog

# A query-string parameter whose name ends in key/token/secret/password
# (api_key, apikey, api-key, key, token, access_token, client_secret, ...).
# Only inside a URL: the name must follow `?` or `&`.
_URL_SECRET_RE = re.compile(
    r"(?i)([?&][\w.-]*(?:key|token|secret|password)=)[^&\s\"'#]*",
)
_REDACTED = "[REDACTED]"

# Loggers that print URLs with their query string. `uvicorn.error` is the one
# that mattered: all three uvicorn WebSocket protocols log
# `<ip> - "WebSocket /path?query" [accepted]` there, and in prod that line
# carried the frontend→backend service key. `uvicorn.access` prints the HTTP
# request line (silenced below, but covered in case someone turns it back on).
_LOGGERS_WITH_URLS = (
    "uvicorn",
    "uvicorn.error",
    "uvicorn.access",
    "uvicorn.asgi",
    "httpx",
    "httpcore",
)


def redact_url_secrets(text: str) -> str:
    """Replace the value of credential-like query-string params in ``text``."""
    return _URL_SECRET_RE.sub(rf"\1{_REDACTED}", text)


def _redact_arg(arg: object) -> object:
    if isinstance(arg, str):
        return redact_url_secrets(arg)
    if isinstance(arg, BaseException):
        # httpx builds its error messages with the full URL.
        text = str(arg)
        redacted = redact_url_secrets(text)
        return redacted if redacted != text else arg
    return arg


class UrlSecretRedactionFilter(logging.Filter):
    """Redact credentials from URLs in a log record, in place.

    Redacts ``msg`` and each argument separately instead of collapsing the
    record into a finished string: uvicorn's ``AccessFormatter`` unpacks
    ``record.args`` into exactly five values.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        args = record.args
        if isinstance(args, tuple):
            record.args = tuple(_redact_arg(a) for a in args)
        elif isinstance(args, Mapping):
            record.args = {k: _redact_arg(v) for k, v in args.items()}
        if isinstance(record.msg, str):
            redacted = redact_url_secrets(record.msg)
            if redacted != record.msg:
                if record.args:
                    # The format string itself has a credential slot
                    # (`?token=%s`): redacting it would eat the placeholder
                    # and break formatting, so render first, then freeze.
                    try:
                        redacted = redact_url_secrets(record.msg % record.args)
                    except (TypeError, ValueError, KeyError):
                        pass
                    record.args = ()
                record.msg = redacted
        return True


_URL_SECRET_FILTER = UrlSecretRedactionFilter()


def install_url_secret_redaction() -> None:
    """Attach the redaction filter where URLs get logged. Idempotent.

    On the loggers that emit URLs (a logger filter only sees records created
    on that same logger, and survives uvicorn re-applying its dictConfig) and
    on the handlers present now: uvicorn's own and the root one, which also
    catch records propagated from child loggers.
    """
    root = logging.getLogger()
    targets: list[logging.Filterer] = [*root.handlers]
    for name in _LOGGERS_WITH_URLS:
        logger = logging.getLogger(name)
        targets.append(logger)
        targets.extend(logger.handlers)
    for target in targets:
        target.addFilter(_URL_SECRET_FILTER)  # no-op if already attached


def setup_logging(log_level: str = "INFO") -> None:
    """Configure structlog to wrap stdlib logging.

    - JSON output when APP_ENV=prod (for log aggregation).
    - Human-readable colored output otherwise (local/dev/test).
    - Existing ``logging.getLogger()`` calls continue to work unchanged.
    """
    env = os.getenv("APP_ENV", "local")
    level = getattr(logging, log_level.upper(), logging.INFO)

    # -- Shared processors applied to every log event --
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    if env == "prod":
        renderer: structlog.types.Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()

    # Configure structlog itself
    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Formatter that stdlib handlers use — applies the renderer.
    # ``foreign_pre_chain`` processes records emitted by plain stdlib loggers
    # (i.e. ``logging.getLogger()``) so they also get timestamps, level, etc.
    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)

    # Quiet noisy third-party libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("celery").setLevel(logging.INFO)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

    # After the root handler exists. Under uvicorn this runs inside the app
    # factory, so uvicorn's dictConfig (re-applied in every worker) already
    # created its handlers.
    install_url_secret_redaction()


def setup_sentry() -> None:
    """Initialise Sentry SDK if SENTRY_DSN is set. Safe to call unconditionally."""
    dsn = os.getenv("SENTRY_DSN")
    if not dsn:
        return

    import sentry_sdk

    sentry_sdk.init(
        dsn=dsn,
        environment=os.getenv("APP_ENV", "local"),
        traces_sample_rate=0.1,
        profiles_sample_rate=0.1,
        send_default_pii=False,
    )
