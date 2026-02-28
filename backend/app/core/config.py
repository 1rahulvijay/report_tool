import os
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
    ORACLE_MIN_POOL: int = int(os.getenv("ORACLE_MIN_POOL", "2"))
    ORACLE_MAX_POOL: int = int(os.getenv("ORACLE_MAX_POOL", "10"))

    # Scaling & Performance
    PREVIEW_MAX_ROWS: int = int(os.getenv("PREVIEW_MAX_ROWS", "500"))
    PREVIEW_RATE_LIMIT: str = os.getenv("PREVIEW_RATE_LIMIT", "60/minute")
    EXPORT_RATE_LIMIT: str = os.getenv("EXPORT_RATE_LIMIT", "5/minute")
    EXPORT_QUEUE_MAX: int = int(os.getenv("EXPORT_QUEUE_MAX", "50"))
    TRUSTED_PROXY: bool = os.getenv("TRUSTED_PROXY", "false").lower() == "true"

    # Security & Governance
    ALLOWED_ORIGINS: list[str] = ["https://mycompany.com", "https://reports.internal"]
    ORACLE_SCHEMA_FILTER: str = ""
    EXPORT_EXCEL_MAX_ROWS: int = 100000
    EXPORT_EXCEL_ASYNC_ROWS: int = 10000
    EXPLAIN_PLAN_THRESHOLD: int = 1000000
    QUERY_TIMEOUT_SECONDS: int = int(os.getenv("QUERY_TIMEOUT_SECONDS", "60"))
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
