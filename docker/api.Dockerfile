FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 1000 app && useradd --uid 1000 --gid app --create-home app

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src/ src/
COPY config/ config/
COPY alembic.ini .

RUN pip install --no-cache-dir uv && uv pip install --system -e '.'

ENV PYTHONPATH=/app/src
ENV APP_ENV=local

RUN chown -R app:app /app
USER app

EXPOSE 8080

CMD ["uvicorn", "app.run:make_app", "--factory", "--host", "0.0.0.0", "--port", "8080", "--loop", "uvloop"]
