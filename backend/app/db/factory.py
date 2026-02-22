from .base import BaseDatabaseAdapter
from app.core.config import get_settings

_ADAPTER_INSTANCE = None


def get_database_adapter() -> BaseDatabaseAdapter:
    """
    Factory method to retrieve the global singleton database adapter based on
    environment configuration dynamically loaded via pydantic-settings.
    """
    global _ADAPTER_INSTANCE
    if _ADAPTER_INSTANCE is not None:
        return _ADAPTER_INSTANCE

    settings = get_settings()
    db_engine = settings.DB_ENGINE.lower()

    if db_engine == "oracledb":
        from .oracle_adapter import OracleAdapter

        _ADAPTER_INSTANCE = OracleAdapter(
            user=settings.ORACLE_USER,
            password=settings.ORACLE_PASSWORD,
            dsn=settings.ORACLE_DSN,
            min_pool=settings.ORACLE_MIN_POOL,
            max_pool=settings.ORACLE_MAX_POOL,
        )
    else:
        raise ValueError(f"Unsupported DB_ENGINE option: {db_engine}")

    return _ADAPTER_INSTANCE


def close_database_adapter():
    """Cleanly shutdown the global database pool."""
    global _ADAPTER_INSTANCE
    if _ADAPTER_INSTANCE is not None:
        _ADAPTER_INSTANCE.close()
        _ADAPTER_INSTANCE = None
