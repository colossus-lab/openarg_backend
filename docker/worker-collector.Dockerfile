FROM python:3.12-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src/ src/
COPY config/ config/
RUN pip install --no-cache-dir uv && uv pip install --system -e '.' \
    && python -c "import openpyxl; print(f'openpyxl {openpyxl.__version__} OK')" \
    && python -c "import xlrd; print(f'xlrd {xlrd.__version__} OK')" \
    && python -c "import odf; print('odfpy OK')" \
    && python -c "import fiona; print(f'fiona {fiona.__version__} OK')"

# ── Runtime stage (no dev headers) ─────────────────────
FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 libgdal36 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 1000 app && useradd --uid 1000 --gid app --create-home app

WORKDIR /app

# Copy installed Python packages + app code from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /app /app

RUN python -c "import fiona; print(f'fiona {fiona.__version__} OK')"

ENV PYTHONPATH=/app/src
ENV APP_ENV=local

RUN chown -R app:app /app
USER app

CMD ["celery", "-A", "app.infrastructure.celery.app:celery_app", "worker", "-Q", "collector", "-c", "2", "--max-memory-per-child=512000", "--loglevel=info"]
