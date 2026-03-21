"""Structured logging (structlog) and Sentry error tracking configuration."""

from __future__ import annotations

import logging
import os

import structlog


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
