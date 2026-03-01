from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from app.setup.config.settings import (
    AnthropicSecrets,
    AppSettings,
    GeminiSecrets,
    PostgresSettings,
)


class TestPostgresSettings:
    def test_dsn_from_fields(self):
        with patch.dict(os.environ, {}, clear=False):
            # Ensure DATABASE_URL is not set
            os.environ.pop("DATABASE_URL", None)
            s = PostgresSettings(USER="u", PASSWORD="p", DB="db", HOST="h", PORT=5432, DRIVER="psycopg")
            assert s.dsn == "postgresql+psycopg://u:p@h:5432/db"

    def test_dsn_from_env_override(self):
        with patch.dict(os.environ, {"DATABASE_URL": "postgresql://override"}, clear=False):
            s = PostgresSettings()
            assert s.dsn == "postgresql://override"


class TestGeminiSecrets:
    def test_env_override_takes_priority(self):
        with patch.dict(os.environ, {"GEMINI_API_KEY": "real-key"}, clear=False):
            s = GeminiSecrets(API_KEY="placeholder")
            assert s.API_KEY == "real-key"

    def test_empty_env_keeps_config_value(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GEMINI_API_KEY", None)
            s = GeminiSecrets(API_KEY="from-config")
            assert s.API_KEY == "from-config"

    def test_env_set_over_empty_default(self):
        with patch.dict(os.environ, {"GEMINI_API_KEY": "env-key"}, clear=False):
            s = GeminiSecrets()
            assert s.API_KEY == "env-key"


class TestAnthropicSecrets:
    def test_env_override_takes_priority(self):
        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "sk-ant-real"}, clear=False):
            s = AnthropicSecrets(API_KEY="sk-ant-placeholder")
            assert s.API_KEY == "sk-ant-real"


class TestAppSettings:
    def test_default_values(self):
        s = AppSettings()
        assert s.agents.EMBEDDING_DIMENSIONS == 768
        assert s.scraper.DATOS_GOB_AR_BASE_URL == "https://datos.gob.ar/api/3/action"
