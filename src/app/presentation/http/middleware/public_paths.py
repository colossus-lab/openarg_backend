"""Rutas que los middlewares de auth dejan pasar sin `X-API-Key` ni JWT de Google.

Una sola lista para `APIKeyMiddleware` y `GoogleJwtAuthMiddleware`. Eran dos
copias y `/catalogo/agregar` quedó afuera de las dos: en local anda (sin
`BACKEND_API_KEY` ni `GOOGLE_OAUTH_CLIENT_ID` no se instala ningún middleware)
y en staging y prod da 401 antes de llegar al router.
`tests/unit/test_api_publica_pasa_los_middlewares.py` compara
`PUBLIC_API_PATHS` con las rutas de `controllers/public_api/`.
"""

from __future__ import annotations

import os

# /ask, /fuentes y /catalogo/* hacen su propia auth con la clave `oarg_sk_` del usuario.
PUBLIC_API_PATHS = frozenset(
    {
        "/api/v1/ask",
        "/api/v1/fuentes",
        "/api/v1/catalogo/buscar",
        "/api/v1/catalogo/tabla",
        "/api/v1/catalogo/datos",
        "/api/v1/catalogo/agregar",
    }
)
ALWAYS_PUBLIC = frozenset({"/health", "/health/ready"}) | PUBLIC_API_PATHS
SERVICE_PREFIXES = (
    "/api/v1/data/",  # Own auth via Bearer service token
    "/api/v1/admin/",  # Own auth via X-Admin-Key (verify_admin_key dependency)
)
DEV_PUBLIC = frozenset({"/docs", "/openapi.json", "/redoc"})

_env = os.getenv("APP_ENV", "local").lower()
PUBLIC_PATHS = ALWAYS_PUBLIC | DEV_PUBLIC if _env != "prod" else ALWAYS_PUBLIC
