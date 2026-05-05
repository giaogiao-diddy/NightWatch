from fastapi import Header, HTTPException, status

from nightwatch.config import get_settings


def _extract_bearer_token(authorization: str | None) -> str:
    if not authorization:
        return ""

    value = authorization.strip()
    if not value:
        return ""

    if value.lower().startswith("bearer "):
        return value[7:].strip()
    return value


async def require_api_key(
    x_api_key: str | None = Header(default=None, alias="X-API-Key"),
    authorization: str | None = Header(default=None, alias="Authorization"),
) -> None:
    settings = get_settings()
    expected_key = settings.api_auth_key.strip()

    # Keep local demo friction low when auth key is not configured.
    if not expected_key:
        return

    provided_key = (x_api_key or "").strip() or _extract_bearer_token(authorization)
    if provided_key == expected_key:
        return

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="invalid api key",
        headers={"WWW-Authenticate": "Bearer"},
    )