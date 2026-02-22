from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Literal
from functools import lru_cache


class Settings(BaseSettings):
    """
    Application wide settings loaded from environment variables or .env file.
    Provides strict validation on startup to prevent silent failures.
    """

    APP_NAME: str = "Aurora Reporting Engine"
    ENVIRONMENT: Literal["development", "production", "testing"] = "development"

    # Database Configuration
    DB_ENGINE: Literal["oracledb"] = "oracledb"

    # Oracle Specifics
    ORACLE_USER: str
    ORACLE_PASSWORD: str
    ORACLE_DSN: str
    ORACLE_MIN_POOL: int = 5
    ORACLE_MAX_POOL: int = 20

    # Security & Governance
    ALLOWED_ORIGINS: list[str] = ["https://mycompany.com", "https://reports.internal"]
    ORACLE_SCHEMA_FILTER: str = ""
    EXPORT_EXCEL_MAX_ROWS: int = 100000
    EXPORT_EXCEL_ASYNC_ROWS: int = 10000
    EXPLAIN_PLAN_THRESHOLD: int = 1000000
    QUERY_TIMEOUT_SECONDS: int = 300
    MAX_ROW_LIMIT: int = 10000000

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # Ignore extra env vars passed by system that aren't defined here
    )


@lru_cache()
def get_settings() -> Settings:
    """Cached singleton of application settings."""
    return Settings()
