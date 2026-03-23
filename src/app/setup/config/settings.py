from __future__ import annotations

from pydantic import BaseModel

from app.setup.config.loader import load_full_config


class PostgresSettings(BaseModel):
    USER: str = "postgres"
    PASSWORD: str = ""
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
    JWT_SECRET_KEY: str = ""
    JWT_ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 60
    REFRESH_TOKEN_EXPIRE_DAYS: int = 7
    BACKEND_API_KEY: str = ""
    CORS_ALLOWED_ORIGINS: list[str] = []

    def model_post_init(self, __context: object) -> None:
        import os

        env_key = os.getenv("BACKEND_API_KEY", "")
        if env_key:
            self.BACKEND_API_KEY = env_key
        env_origins = os.getenv("CORS_ALLOWED_ORIGINS", "")
        if env_origins:
            self.CORS_ALLOWED_ORIGINS = [o.strip() for o in env_origins.split(",") if o.strip()]


class LoggingSettings(BaseModel):
    LEVEL: str = "INFO"


class AgentSettings(BaseModel):
    EMBEDDING_MODEL: str = "cohere.embed-multilingual-v3"
    EMBEDDING_DIMENSIONS: int = 1024
    MAX_CONCURRENT_COLLECTORS: int = 5
    SANDBOX_TIMEOUT_SECONDS: int = 30


class ScraperSettings(BaseModel):
    DATOS_GOB_AR_BASE_URL: str = "https://datos.gob.ar/api/3/action"
    CABA_BASE_URL: str = "https://data.buenosaires.gob.ar/api/3/action"
    SCRAPE_INTERVAL_HOURS: int = 24
    SERIES_TIEMPO_BASE_URL: str = "https://apis.datos.gob.ar/series/api"
    ARGENTINA_DATOS_BASE_URL: str = "https://api.argentinadatos.com/v1"
    GEOREF_BASE_URL: str = "https://apis.datos.gob.ar/georef/api"


class GeminiSecrets(BaseModel):
    API_KEY: str = ""
    MODEL: str = "gemini-2.5-flash"

    def model_post_init(self, __context: object) -> None:
        import os

        env_key = os.getenv("GEMINI_API_KEY", "")
        if env_key:
            self.API_KEY = env_key
        env_model = os.getenv("GEMINI_MODEL", "")
        if env_model:
            self.MODEL = env_model


class AnthropicSecrets(BaseModel):
    API_KEY: str = ""
    MODEL: str = "claude-sonnet-4-20250514"

    def model_post_init(self, __context: object) -> None:
        import os

        env_key = os.getenv("ANTHROPIC_API_KEY", "")
        if env_key:
            self.API_KEY = env_key
        env_model = os.getenv("ANTHROPIC_MODEL", "")
        if env_model:
            self.MODEL = env_model


class BedrockSettings(BaseModel):
    REGION: str = "us-east-1"
    LLM_MODEL: str = "us.anthropic.claude-haiku-4-5-20251001-v1:0"
    EMBEDDING_MODEL: str = "cohere.embed-multilingual-v3"

    def model_post_init(self, __context: object) -> None:
        import os

        self.REGION = os.getenv("AWS_REGION", self.REGION)
        self.LLM_MODEL = os.getenv("BEDROCK_LLM_MODEL", self.LLM_MODEL)
        self.EMBEDDING_MODEL = os.getenv("BEDROCK_EMBEDDING_MODEL", self.EMBEDDING_MODEL)


class S3Settings(BaseModel):
    BUCKET: str = "openarg-datasets"
    REGION: str = "us-east-1"

    def model_post_init(self, __context: object) -> None:
        import os

        self.BUCKET = os.getenv("S3_BUCKET", self.BUCKET)
        self.REGION = os.getenv("AWS_REGION", self.REGION)


class AppSettings(BaseModel):
    postgres: PostgresSettings = PostgresSettings()
    sqla: SqlaEngineSettings = SqlaEngineSettings()
    security: SecuritySettings = SecuritySettings()
    logs: LoggingSettings = LoggingSettings()
    agents: AgentSettings = AgentSettings()
    scraper: ScraperSettings = ScraperSettings()
    gemini: GeminiSecrets = GeminiSecrets()
    anthropic: AnthropicSecrets = AnthropicSecrets()
    bedrock: BedrockSettings = BedrockSettings()
    s3: S3Settings = S3Settings()


def load_settings() -> AppSettings:
    raw = load_full_config()
    return AppSettings(**raw)
