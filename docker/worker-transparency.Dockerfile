FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src/ src/
COPY config/ config/
RUN pip install --no-cache-dir uv && uv pip install --system -e '.'

ENV PYTHONPATH=/app/src
ENV APP_ENV=local

CMD ["celery", "-A", "app.infrastructure.celery.app:celery_app", "worker", "-Q", "transparency", "-c", "2", "--loglevel=info"]
