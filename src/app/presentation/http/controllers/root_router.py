from __future__ import annotations

from fastapi import APIRouter

from app.presentation.http.controllers.admin.tasks_router import (
    router as admin_tasks_router,
)
from app.presentation.http.controllers.conversations.conversations_router import (
    router as conversations_router,
)
from app.presentation.http.controllers.datasets.datasets_router import (
    router as datasets_router,
)
from app.presentation.http.controllers.health.health_router import (
    router as health_router,
)
from app.presentation.http.controllers.monitoring.metrics_router import (
    router as metrics_router,
)
from app.presentation.http.controllers.query.query_router import (
    router as query_router,
)
from app.presentation.http.controllers.query.smart_query_router import (
    router as smart_query_router,
)
from app.presentation.http.controllers.sandbox.sandbox_router import (
    router as sandbox_router,
)
from app.presentation.http.controllers.transparency.transparency_router import (
    router as transparency_router,
)
from app.presentation.http.controllers.users.users_router import (
    router as users_router,
)


def create_root_router() -> APIRouter:
    root = APIRouter()

    # Health (no prefix)
    root.include_router(health_router)

    # API v1
    api_v1 = APIRouter(prefix="/api/v1")
    api_v1.include_router(query_router)
    api_v1.include_router(smart_query_router)
    api_v1.include_router(datasets_router)
    api_v1.include_router(sandbox_router)
    api_v1.include_router(conversations_router)
    api_v1.include_router(users_router)
    api_v1.include_router(metrics_router)
    api_v1.include_router(transparency_router)
    api_v1.include_router(admin_tasks_router)

    root.include_router(api_v1)

    return root
