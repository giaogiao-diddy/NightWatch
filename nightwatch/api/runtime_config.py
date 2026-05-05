from fastapi import APIRouter, Depends

from nightwatch.api.deps import require_api_key
from nightwatch.config import get_settings
from nightwatch.models.api import (
    RuntimeConnectorConfig,
    RuntimeConnectorConfigUpdate,
    RuntimeConfigResponse,
)


router = APIRouter(
    prefix="/api/v1/runtime-config",
    tags=["runtime-config"],
    dependencies=[Depends(require_api_key)],
)


_SECRET_FIELDS: set[str] = {
    "scheduler_api_token",
    "scheduler_password",
    "spark_api_token",
    "flink_api_token",
    "qdrant_api_key",
    "llm_api_key",
}


def _mask_secret(value: str) -> str:
    if not value:
        return ""
    return "********"


def _build_config_response() -> RuntimeConnectorConfig:
    settings = get_settings()
    return RuntimeConnectorConfig(
        connectors_demo_fallback=settings.connectors_demo_fallback,
        scheduler_backend=settings.scheduler_backend,
        scheduler_base_url=settings.scheduler_base_url,
        scheduler_api_token=_mask_secret(settings.scheduler_api_token),
        scheduler_username=settings.scheduler_username,
        scheduler_password=_mask_secret(settings.scheduler_password),
        scheduler_verify_tls=settings.scheduler_verify_tls,
        spark_history_base_url=settings.spark_history_base_url,
        spark_yarn_rm_base_url=settings.spark_yarn_rm_base_url,
        spark_control_base_url=settings.spark_control_base_url,
        spark_api_token=_mask_secret(settings.spark_api_token),
        spark_verify_tls=settings.spark_verify_tls,
        flink_base_url=settings.flink_base_url,
        flink_api_token=_mask_secret(settings.flink_api_token),
        flink_verify_tls=settings.flink_verify_tls,
        qdrant_url=settings.qdrant_url,
        qdrant_api_key=_mask_secret(settings.qdrant_api_key),
        llm_base_url=settings.llm_base_url,
        llm_model=settings.llm_model,
        llm_api_key=_mask_secret(settings.llm_api_key),
    )


def _apply_runtime_updates(payload: RuntimeConnectorConfigUpdate) -> None:
    settings = get_settings()
    updates = payload.model_dump(exclude_none=True)

    for key, raw_value in updates.items():
        if isinstance(raw_value, str):
            normalized = raw_value.strip()
            if key in _SECRET_FIELDS and not normalized:
                continue
            setattr(settings, key, normalized)
            continue

        setattr(settings, key, raw_value)


@router.get(
    "",
    response_model=RuntimeConfigResponse,
    summary="Get runtime connector configuration",
)
async def get_runtime_config() -> RuntimeConfigResponse:
    return RuntimeConfigResponse(
        config=_build_config_response(),
        persisted=False,
        note="Changes made through this endpoint are runtime-only and reset after process restart.",
    )


@router.put(
    "",
    response_model=RuntimeConfigResponse,
    summary="Update runtime connector configuration",
)
async def update_runtime_config(payload: RuntimeConnectorConfigUpdate) -> RuntimeConfigResponse:
    _apply_runtime_updates(payload)
    return RuntimeConfigResponse(
        config=_build_config_response(),
        persisted=False,
        note="Runtime configuration updated. Mirror the same values into environment variables for persistence.",
    )
