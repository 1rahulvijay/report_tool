from fastapi import FastAPI, Depends, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
from app.core.config import get_settings
from app.db.factory import get_database_adapter
from app.api.endpoints import router as query_router
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from app.core.rate_limit import limiter
from starlette.middleware.base import BaseHTTPMiddleware
import uuid

logger = logging.getLogger(__name__)


def setup_logging():
    from pythonjsonlogger import jsonlogger
    import sys

    # Get the root logger
    root_logger = logging.getLogger()
    # Clear any existing handlers
    root_logger.handlers = []

    log_handler = logging.StreamHandler(sys.stdout)
    formatter = jsonlogger.JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(module)s %(lineno)d %(message)s",
        rename_fields={
            "levelname": "level",
            "asctime": "timestamp",
        },
    )
    log_handler.setFormatter(formatter)
    root_logger.addHandler(log_handler)
    root_logger.setLevel(logging.INFO)

    # Re-apply to uvicorn loggers so they match our format
    for logger_name in ["uvicorn", "uvicorn.error", "uvicorn.access", "fastapi"]:
        uvicorn_logger = logging.getLogger(logger_name)
        uvicorn_logger.handlers = []
        uvicorn_logger.addHandler(log_handler)
        uvicorn_logger.propagate = False


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Production-ready startup sequence.
    Validates environment settings and verifies database connectivity before accepting requests.
    """
    setup_logging()
    settings = get_settings()
    logger.info(f"Starting {settings.APP_NAME} in {settings.ENVIRONMENT} mode.")
    logger.info(f"Configured Database Engine: {settings.DB_ENGINE}")

    # Validate connection pool / adapter configuration
    try:
        db = get_database_adapter()
        datasets = db.get_datasets()
        logger.info(
            f"Successfully connected to Database. Found {len(datasets)} readable datasets."
        )
        # Do NOT close here: this is now our Singleton Connection Pool!
    except Exception as e:
        logger.error(f"FATAL: Could not connect to the database. Error: {str(e)}")
        raise RuntimeError(f"Database connection failed on startup: {str(e)}")

    yield

    # Shutdown logic
    logger.info("Shutting down Aurora Reporting Engine...")
    from app.db.factory import close_database_adapter

    logger.info("Initiating database adapter shutdown...")
    close_database_adapter()
    logger.info("Application shutdown complete.")


app = FastAPI(
    title="Aurora Reporting Portal Engine",
    description="Metadata-driven backend query engine for dynamic self-service reporting.",
    version="1.0.0",
    lifespan=lifespan,
)

# Attach rate limiter to app
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


# Global handler: fail fast with 503 when DB pool is exhausted


@app.exception_handler(ValueError)
async def pool_exhaustion_handler(request: Request, exc: ValueError):
    if "DATABASE_POOL_EXHAUSTED" in str(exc):
        logger.warning(f"503 Pool Exhausted: {exc}")
        return JSONResponse(
            status_code=503,
            content={"detail": str(exc)},
            headers={"Retry-After": "5"},
        )
    # Re-raise non-pool ValueErrors as 500
    raise exc


app.add_middleware(
    CORSMiddleware,
    allow_origins=[origin.strip() for origin in get_settings().ALLOWED_ORIGINS],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
        request.state.correlation_id = correlation_id
        response = await call_next(request)
        response.headers["X-Correlation-ID"] = correlation_id
        return response


app.add_middleware(CorrelationIdMiddleware)

app.include_router(query_router, prefix="/api/v1", tags=["Query Engine"])


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.get("/metrics")
def get_metrics(settings=Depends(get_settings)):
    """Expose basic application and DB metrics."""
    db = get_database_adapter()
    metrics = {
        "app_name": settings.APP_NAME,
        "environment": settings.ENVIRONMENT,
    }
    if hasattr(db, "get_pool_metrics"):
        metrics["db_pool"] = db.get_pool_metrics()
    return metrics
