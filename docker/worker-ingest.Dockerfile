FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 1000 app && useradd --uid 1000 --gid app --create-home app

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src/ src/
COPY config/ config/
RUN pip install --no-cache-dir uv && uv pip install --system -e '.' \
    && python -c "import openpyxl; print(f'openpyxl {openpyxl.__version__} OK')" \
    && python -c "import xlrd; print(f'xlrd {xlrd.__version__} OK')" \
    && python -c "import odf; print('odfpy OK')"

ENV PYTHONPATH=/app/src
ENV APP_ENV=local

RUN chown -R app:app /app
USER app

# `orchestrator` va acá porque es lo que ya consume este worker en staging y
# en prod: los dos compose de los servidores le agregan la cola a mano, y no
# están versionados. Mientras el CMD dijera sólo `ingest`, recrear el stack
# desde el repo dejaba `bulk_collect_all` sin consumidor.
CMD ["celery", "-A", "app.infrastructure.celery.app:celery_app", "worker", "-Q", "ingest,orchestrator", "-c", "2", "--max-memory-per-child=512000", "--loglevel=info"]
