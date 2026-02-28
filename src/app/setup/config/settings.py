from __future__ import annotations

from pydantic import BaseModel

from app.setup.config.loader import load_full_config


class PostgresSettings(BaseModel):
    USER: str = "postgres"
    PASSWORD: str = "postgres"
    DB: str = "openarg_db"
    HOST: str = "localhost"
    PORT: int = 5432
    DRIVER: str = "psycopg"

    @property
    def dsn(self) -> str:
        import os

        db_url = os.getenv("DATABASE_URL")
        if db_url:
            return db_url
        return f"postgresql+{self.DRIVER}://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DB}"


class SqlaEngineSettings(BaseModel):
    ECHO: bool = False
    ECHO_POOL: bool = False
    POOL_SIZE: int = 20
    MAX_OVERFLOW: int = 10


class SecuritySettings(BaseModel):
    JWT_SECRET_KEY: str = "change-me"
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7


class LoggingSettings(BaseModel):
    LEVEL: str = "INFO"


class AgentSettings(BaseModel):
    DEFAULT_LLM_PROVIDER: str = "gemini"
    EMBEDDING_MODEL: str = "text-embedding-3-small"
    EMBEDDING_DIMENSIONS: int = 1536
    MAX_CONCURRENT_COLLECTORS: int = 5
    SANDBOX_TIMEOUT_SECONDS: int = 30


class ScraperSettings(BaseModel):
    DATOS_GOB_AR_BASE_URL: str = "https://datos.gob.ar/api/3/action"
    CABA_BASE_URL: str = "https://data.buenosaires.gob.ar/api/3/action"
    SCRAPE_INTERVAL_HOURS: int = 24
    SERIES_TIEMPO_BASE_URL: str = "https://apis.datos.gob.ar/series/api"
    ARGENTINA_DATOS_BASE_URL: str = "https://api.argentinadatos.com/v1"
    GEOREF_BASE_URL: str = "https://apis.datos.gob.ar/georef/api"


class OpenAISecrets(BaseModel):
    API_KEY: str = ""

    def model_post_init(self, __context: object) -> None:
        import os
        env_key = os.getenv("OPENAI_API_KEY", "")
        if env_key:
            self.API_KEY = env_key


class GeminiSecrets(BaseModel):
    API_KEY: str = ""

    def model_post_init(self, __context: object) -> None:
        import os
        env_key = os.getenv("GEMINI_API_KEY", "")
        if env_key:
            self.API_KEY = env_key


class AnthropicSecrets(BaseModel):
    API_KEY: str = ""

    def model_post_init(self, __context: object) -> None:
        import os
        env_key = os.getenv("ANTHROPIC_API_KEY", "")
        if env_key:
            self.API_KEY = env_key


class AppSettings(BaseModel):
    postgres: PostgresSettings = PostgresSettings()
    sqla: SqlaEngineSettings = SqlaEngineSettings()
    security: SecuritySettings = SecuritySettings()
    logs: LoggingSettings = LoggingSettings()
    agents: AgentSettings = AgentSettings()
    scraper: ScraperSettings = ScraperSettings()
    openai: OpenAISecrets = OpenAISecrets()
    gemini: GeminiSecrets = GeminiSecrets()
    anthropic: AnthropicSecrets = AnthropicSecrets()


def load_settings() -> AppSettings:
    raw = load_full_config()
    return AppSettings(**raw)
