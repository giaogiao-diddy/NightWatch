import asyncio
from collections.abc import Awaitable
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import httpx


class ConnectorError(RuntimeError):
    pass


def build_auth_headers(api_token: str) -> dict[str, str]:
    if not api_token.strip():
        return {}
    return {"Authorization": f"Bearer {api_token.strip()}"}


def run_async(coro: Awaitable[Any]) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    # Called from an active loop in a sync context; execute in a helper thread.
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(asyncio.run, coro)
        return future.result()


async def request_json(
    method: str,
    url: str,
    *,
    timeout_seconds: float,
    verify_tls: bool,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    json_body: dict[str, Any] | None = None,
) -> dict[str, Any] | list[Any]:
    try:
        async with httpx.AsyncClient(timeout=timeout_seconds, verify=verify_tls) as client:
            response = await client.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_body,
            )
            response.raise_for_status()
            if not response.content:
                return {}
            return response.json()
    except httpx.TimeoutException as exc:
        raise ConnectorError(f"request timeout: {method} {url}") from exc
    except httpx.HTTPStatusError as exc:
        raise ConnectorError(
            f"request failed with status {exc.response.status_code}: {method} {url}"
        ) from exc
    except httpx.HTTPError as exc:
        raise ConnectorError(f"request transport error: {method} {url}") from exc
    except ValueError as exc:
        raise ConnectorError(f"request returned non-json payload: {method} {url}") from exc


async def request_text(
    method: str,
    url: str,
    *,
    timeout_seconds: float,
    verify_tls: bool,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
) -> tuple[str, int]:
    try:
        async with httpx.AsyncClient(timeout=timeout_seconds, verify=verify_tls) as client:
            response = await client.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
            )
            response.raise_for_status()
            return response.text, response.status_code
    except httpx.TimeoutException as exc:
        raise ConnectorError(f"request timeout: {method} {url}") from exc
    except httpx.HTTPStatusError as exc:
        raise ConnectorError(
            f"request failed with status {exc.response.status_code}: {method} {url}"
        ) from exc
    except httpx.HTTPError as exc:
        raise ConnectorError(f"request transport error: {method} {url}") from exc