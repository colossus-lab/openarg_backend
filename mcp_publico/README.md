# MCP público de OpenArg

Servidor MCP público con datos oficiales de Argentina y la web
**mcp.openarg.org**. Spec: [`specs/029-mcp-publico/`](../specs/029-mcp-publico/spec.md).

No es el MCP de operaciones ([`scripts/ops_mcp/`](../scripts/ops_mcp/README.md)).
Ése lee los servidores por SSH y es sólo para el equipo.

```
mcp_publico/
├── core.py           # lógica pura, sólo stdlib: clave, IP, errores, formato
├── server.py         # SDK de MCP + rutas HTTP: /mcp, /health y la web en /
├── requirements.txt  # dependencias del contenedor (no van en pyproject.toml)
└── site/             # la web estática
```

El servidor no guarda nada y no decide nada de cuotas. Reenvía la clave del
usuario a `POST /api/v1/ask` y el backend valida, cobra y responde.

## Correrlo local

Con el backend levantado en `localhost:8081` (`make docker.up`):

```bash
uv venv .venv-mcp && uv pip install --python .venv-mcp -r mcp_publico/requirements.txt
BACKEND_URL=http://localhost:8081 .venv-mcp/bin/uvicorn mcp_publico.server:app --port 8000
claude mcp add --transport http openarg-local http://localhost:8000/mcp \
  --header "Authorization: Bearer oarg_sk_…"
```

La web queda en `http://localhost:8000/`.

## Tests

```bash
pytest tests/unit/test_mcp_publico_core.py      # sin mcp: corre en CI
pytest tests/unit/test_mcp_publico_server.py    # necesita requirements.txt instalado
```

## Variables

| Variable | Default | Para qué |
|---|---|---|
| `BACKEND_URL` | `http://backend:8080` | Dónde está la API |
| `MCP_ALLOWED_HOSTS` | `mcp.openarg.org,mcp.staging.openarg.org,localhost:*,127.0.0.1:*,mcp:8000` | Host aceptados (protección contra DNS rebinding) |
| `MCP_BACKEND_TIMEOUT_SECONDS` | `75` | Tiene que ser mayor que `PUBLIC_API_TIMEOUT_SECONDS` del backend |
