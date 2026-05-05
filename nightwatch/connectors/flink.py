import asyncio
import time
from typing import Any
from urllib.parse import quote

from nightwatch.config import get_settings
from nightwatch.connectors.base import ConnectorError, build_auth_headers, request_json, run_async


class FlinkConnector:
    def __init__(self) -> None:
        settings = get_settings()
        self.base_url = settings.flink_base_url.strip().rstrip("/")
        self.api_token = settings.flink_api_token
        self.timeout_seconds = settings.flink_timeout_seconds
        self.verify_tls = settings.flink_verify_tls
        self.poll_interval_seconds = settings.flink_poll_interval_seconds
        self.operation_timeout_seconds = settings.flink_operation_timeout_seconds
        self.demo_fallback = settings.connectors_demo_fallback

    def _headers(self) -> dict[str, str]:
        return build_auth_headers(self.api_token)

    @staticmethod
    def _normalize_status(raw_status: str) -> str:
        normalized = raw_status.strip().upper()
        if normalized in {"RUNNING", "CREATED", "RESTARTING", "RECONCILING"}:
            return "RUNNING"
        if normalized in {"FINISHED", "SUCCEEDED"}:
            return "SUCCESS"
        if normalized in {"FAILED", "CANCELED", "CANCELLING"}:
            return "FAILED"
        return "UNKNOWN"

    async def get_job_status(self, job_id: str) -> dict[str, object]:
        normalized_job_id = job_id.strip()
        if not normalized_job_id:
            return {
                "job_id": job_id,
                "status": "UNKNOWN",
                "result": "failed",
                "error": "job_id cannot be empty",
            }

        if not self.base_url:
            if self.demo_fallback:
                return {
                    "job_id": normalized_job_id,
                    "status": "RUNNING",
                    "raw_status": "RUNNING",
                    "result": "queried",
                    "mode": "demo_fallback",
                }
            return {
                "job_id": normalized_job_id,
                "status": "UNKNOWN",
                "result": "failed",
                "error": "flink_base_url is not configured",
            }

        endpoint = f"{self.base_url}/jobs/{quote(normalized_job_id, safe='')}"
        try:
            payload = await request_json(
                method="GET",
                url=endpoint,
                timeout_seconds=self.timeout_seconds,
                verify_tls=self.verify_tls,
                headers=self._headers(),
            )
            raw_status = ""
            if isinstance(payload, dict):
                raw_status = str(payload.get("state", ""))

            return {
                "job_id": normalized_job_id,
                "status": self._normalize_status(raw_status),
                "raw_status": raw_status or "UNKNOWN",
                "result": "queried",
            }
        except ConnectorError as exc:
            return {
                "job_id": normalized_job_id,
                "status": "UNKNOWN",
                "result": "failed",
                "error": str(exc),
            }

    async def get_checkpoint_latency(self, job_id: str) -> dict[str, object]:
        normalized_job_id = job_id.strip()
        if not normalized_job_id:
            return {
                "job_id": job_id,
                "result": "failed",
                "error": "job_id cannot be empty",
            }

        if not self.base_url:
            if self.demo_fallback:
                return {
                    "job_id": normalized_job_id,
                    "result": "queried",
                    "checkpoint_latency_ms": 0,
                    "mode": "demo_fallback",
                }
            return {
                "job_id": normalized_job_id,
                "result": "failed",
                "error": "flink_base_url is not configured",
            }

        endpoint = f"{self.base_url}/jobs/{quote(normalized_job_id, safe='')}/checkpoints"
        try:
            payload = await request_json(
                method="GET",
                url=endpoint,
                timeout_seconds=self.timeout_seconds,
                verify_tls=self.verify_tls,
                headers=self._headers(),
            )

            latency_ms = -1
            checkpoint_id = ""
            if isinstance(payload, dict):
                latest = payload.get("latest", {})
                if isinstance(latest, dict):
                    completed = latest.get("completed", {})
                    if isinstance(completed, dict):
                        latency_ms = int(completed.get("end_to_end_duration", -1) or -1)
                        checkpoint_id = str(completed.get("id", ""))

            return {
                "job_id": normalized_job_id,
                "result": "queried",
                "checkpoint_latency_ms": latency_ms,
                "checkpoint_id": checkpoint_id,
            }
        except ConnectorError as exc:
            return {
                "job_id": normalized_job_id,
                "result": "failed",
                "error": str(exc),
            }

    async def _poll_stop_trigger(self, job_id: str, trigger_id: str) -> dict[str, object]:
        endpoint = f"{self.base_url}/jobs/{quote(job_id, safe='')}/savepoints/{quote(trigger_id, safe='')}"
        deadline = time.monotonic() + self.operation_timeout_seconds

        while time.monotonic() < deadline:
            try:
                payload = await request_json(
                    method="GET",
                    url=endpoint,
                    timeout_seconds=self.timeout_seconds,
                    verify_tls=self.verify_tls,
                    headers=self._headers(),
                )
            except ConnectorError as exc:
                return {
                    "done": True,
                    "success": False,
                    "error": str(exc),
                }

            if isinstance(payload, dict):
                status_info = payload.get("status", {})
                if isinstance(status_info, dict):
                    status_id = str(status_info.get("id", "")).upper()
                    if status_id == "COMPLETED":
                        return {"done": True, "success": True}
                    if status_id == "FAILED":
                        return {
                            "done": True,
                            "success": False,
                            "error": str(payload.get("failure-cause", "stop operation failed")),
                        }

            await asyncio.sleep(self.poll_interval_seconds)

        return {
            "done": False,
            "success": False,
            "error": "stop operation polling timeout",
        }

    async def pause_job(self, job_id: str, reason: str) -> dict[str, object]:
        normalized_job_id = job_id.strip()
        if not normalized_job_id:
            return {
                "job_id": job_id,
                "result": "failed",
                "reason": reason,
                "error": "job_id cannot be empty",
            }

        if not self.base_url:
            if self.demo_fallback:
                return {
                    "job_id": normalized_job_id,
                    "result": "paused",
                    "reason": reason,
                    "mode": "demo_fallback",
                }
            return {
                "job_id": normalized_job_id,
                "result": "failed",
                "reason": reason,
                "error": "flink_base_url is not configured",
            }

        endpoint = f"{self.base_url}/jobs/{quote(normalized_job_id, safe='')}/stop"
        try:
            payload = await request_json(
                method="POST",
                url=endpoint,
                timeout_seconds=self.timeout_seconds,
                verify_tls=self.verify_tls,
                headers=self._headers(),
                json_body={"drain": False},
            )
        except ConnectorError as exc:
            return {
                "job_id": normalized_job_id,
                "result": "failed",
                "reason": reason,
                "error": str(exc),
            }

        trigger_id = ""
        if isinstance(payload, dict):
            trigger_id = str(payload.get("request-id", "") or payload.get("triggerid", ""))

        if trigger_id:
            poll_result = await self._poll_stop_trigger(job_id=normalized_job_id, trigger_id=trigger_id)
            if poll_result.get("success") is True:
                return {
                    "job_id": normalized_job_id,
                    "result": "paused",
                    "reason": reason,
                    "trigger_id": trigger_id,
                }
            return {
                "job_id": normalized_job_id,
                "result": "failed",
                "reason": reason,
                "trigger_id": trigger_id,
                "error": str(poll_result.get("error", "failed to pause flink job")),
            }

        # Some deployments return 202 without trigger id; treat as accepted pause.
        return {
            "job_id": normalized_job_id,
            "result": "paused",
            "reason": reason,
            "trigger_id": "",
        }


async def pause_cdc_job(job_id: str, reason: str) -> dict[str, object]:
    connector = FlinkConnector()
    return await connector.pause_job(job_id=job_id, reason=reason)


async def get_job_status_async(job_id: str) -> dict[str, object]:
    connector = FlinkConnector()
    return await connector.get_job_status(job_id=job_id)


async def get_checkpoint_latency_async(job_id: str) -> dict[str, object]:
    connector = FlinkConnector()
    return await connector.get_checkpoint_latency(job_id=job_id)


def mock_pause_cdc_job(job_id: str, reason: str) -> dict[str, object]:
    connector = FlinkConnector()
    return run_async(connector.pause_job(job_id=job_id, reason=reason))


def get_job_status(job_id: str) -> dict[str, object]:
    connector = FlinkConnector()
    return run_async(connector.get_job_status(job_id=job_id))


def get_checkpoint_latency(job_id: str) -> dict[str, object]:
    connector = FlinkConnector()
    return run_async(connector.get_checkpoint_latency(job_id=job_id))
