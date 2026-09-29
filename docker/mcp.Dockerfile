# MCP público de datos de OpenArg + la web mcp.openarg.org.
# Imagen chica y separada de la API: no importa nada de `src/`, sólo habla
# con el backend por HTTP (BACKEND_URL). Si se cae, el chat no se entera.
FROM python:3.12-slim

RUN groupadd --gid 1000 app && useradd --uid 1000 --gid app --create-home app

WORKDIR /app

COPY mcp_publico/ mcp_publico/
RUN pip install --no-cache-dir -r mcp_publico/requirements.txt

RUN chown -R app:app /app
USER app

ENV PYTHONUNBUFFERED=1
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=4).status == 200 else 1)"

# --no-access-log: la línea de acceso no tiene la clave, pero sí la IP de cada
# usuario, y no hace falta guardarla acá (el backend ya registra el uso).
CMD ["uvicorn", "mcp_publico.server:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
