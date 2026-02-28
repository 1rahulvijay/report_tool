from slowapi import Limiter
from slowapi.util import get_remote_address
from fastapi import Request, HTTPException
from app.core.config import get_settings
from collections import defaultdict
import threading

# Thread-safe in-memory concurrency tracking
# Note: This is per-process. Nginx limit_conn provides additional cross-process protection.
_concurrency_map = defaultdict(int)
_lock = threading.Lock()


def get_user_identifier(request: Request) -> str:
    settings = get_settings()
    if settings.TRUSTED_PROXY:
        # Honor X-Forwarded-For if behind Nginx/F5
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            return forwarded_for.split(",")[0].strip()

    # Fallback to standard remote address or X-User-ID
    return request.headers.get("X-User-ID", get_remote_address(request))


# Per-user rate limiter
limiter = Limiter(key_func=get_user_identifier)


def check_concurrency(request: Request):
    """
    Ensures a user doesn't have more than 2 concurrent analytical previews.
    Should be used as a FastAPI dependency.
    """
    user_id = get_user_identifier(request)
    with _lock:
        if _concurrency_map[user_id] >= 2:
            raise HTTPException(
                status_code=429,
                detail="Too many concurrent analytical requests. Please wait for your previous query to finish.",
            )
        _concurrency_map[user_id] += 1


def release_concurrency(request: Request):
    """Releases the concurrency slot."""
    user_id = get_user_identifier(request)
    with _lock:
        _concurrency_map[user_id] = max(0, _concurrency_map[user_id] - 1)
