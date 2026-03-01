# Generic MCP Server Dockerfile
# Single Dockerfile for all MCP servers (parametrized via ARG MCP_SERVER)

FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 1000 app && useradd --uid 1000 --gid app --create-home app

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src/ src/
COPY config/ config/

RUN pip install --no-cache-dir uv && uv pip install --system -e '.'

ENV PYTHONPATH=/app/src
ENV PYTHONUNBUFFERED=1
ENV APP_ENV=local

RUN chown -R app:app /app
USER app

# Which MCP server to run (series_tiempo_mcp, ckan_mcp, argentina_datos_mcp, sesiones_mcp)
ARG MCP_SERVER=series_tiempo_mcp
ENV MCP_SERVER=$MCP_SERVER

ARG MCP_PORT=8091
ENV MCP_PORT=$MCP_PORT

EXPOSE ${MCP_PORT}

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:${MCP_PORT}/health || exit 1

CMD python -m app.infrastructure.mcp.servers.${MCP_SERVER}
