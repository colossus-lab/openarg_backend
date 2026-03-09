from __future__ import annotations

from dishka import Provider
from dishka.integrations.fastapi import setup_dishka
from fastapi import FastAPI

from app.presentation.http.controllers.root_router import create_root_router
from app.setup.app_factory import configure_app, create_app
from app.setup.config.loader import get_current_env
from app.setup.config.settings import AppSettings, load_settings
from app.setup.ioc.provider_registry import create_async_ioc_container, get_providers
from app.setup.logging_config import setup_logging, setup_sentry


def make_app(*di_providers: Provider, settings: AppSettings | None = None) -> FastAPI:
    if settings is None:
        settings = load_settings()

    setup_logging(log_level=settings.logs.LEVEL)
    setup_sentry()

    app = create_app()
    current_env = get_current_env()

    configure_app(
        app=app,
        root_router=create_root_router(),
        environment=current_env.value,
        settings=settings,
    )

    app.state.settings = settings
    app.state.environment = current_env.value

    container = create_async_ioc_container(
        providers=(*get_providers(), *di_providers),
        settings=settings,
    )
    setup_dishka(container=container, app=app)

    return app  # type: ignore[no-any-return]
