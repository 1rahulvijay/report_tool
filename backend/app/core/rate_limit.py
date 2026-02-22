from slowapi import Limiter
from slowapi.util import get_remote_address
from fastapi import Request


def get_user_identifier(request: Request) -> str:
    # Use real user ID if passed by middle tier, fallback to IP for local testing
    return request.headers.get("X-User-ID", get_remote_address(request))


# Per-user rate limiter
limiter = Limiter(key_func=get_user_identifier)
