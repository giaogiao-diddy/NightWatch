import base64
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

from nightwatch.config import get_settings
from nightwatch.connectors.base import ConnectorError, build_auth_headers, request_json, run_async


class AirflowSchedulerConnector:
    def __init__(self) -> None:
        settings = get_settings()
        self.backend = settings.scheduler_backend.strip().lower() or "airflow"
        self.base_url = settings.scheduler_base_url.strip().rstrip("/")
        self.timeout_seconds = settings.scheduler_timeout_seconds
        self.verify_tls = settings.scheduler_verify_tls
        self.demo_fallback = settings.connectors_demo_fallback
        self.api_token = settings.scheduler_api_token
        self.username = settings.scheduler_username
        self.password = settings.scheduler_password

    def _build_headers(self) -> dict[str, str]:
        headers = build_auth_headers(self.api_token)
        if headers:
            return headers

        if self.username.strip() and self.password.strip():
            raw = f"{self.username.strip()}:{self.password.strip()}".encode("utf-8")
            headers["Authorization"] = f"Basic {base64.b64encode(raw).decode('utf-8')}"
        return headers

    @staticmethod
    def _normalize_state(raw_state: str) -> str:
        normalized = raw_state.strip().lower()
        if normalized in {"success", "successful", "done"}:
            return "SUCCESS"
        if normalized in {"running", "queued", "scheduled", "up_for_retry"}:
            return "RUNNING"
        if normalized in {"failed", "upstream_failed", "error"}:
            return "FAILED"
        if not normalized:
            return "UNKNOWN"
        return normalized.upper()

    async def retry_task(self, task_id: str, parameters: dict[str, object]) -> dict[str, object]:
        normalized_task_id = task_id.strip()
        if not normalized_task_id:
            return {
                "task_id": task_id,
                "result": "failed",
                "error": "task_id cannot be empty",
                "parameters": parameters,
            }

        if self.backend != "airflow":
            return {
                "task_id": normalized_task_id,
                "result": "failed",
                "error": f"unsupported scheduler backend: {self.backend}",
                "parameters": parameters,
            }

        if not self.base_url:
            if self.demo_fallback:
                return {
                    "task_id": normalized_task_id,
                    "result": "submitted",
                    "parameters": parameters,
                    "mode": "demo_fallback",
                }
            return {
                "task_id": normalized_task_id,
                "result": "failed",
                "error": "scheduler_base_url is not configured",
                "parameters": parameters,
            }

        dag_id = quote(normalized_task_id, safe="")
        endpoint = f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns"
        run_id = f"nightwatch_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
        payload = {
            "dag_run_id": run_id,
            "conf": parameters,
        }

        try:
            response_payload = await request_json(
                method="POST",
                url=endpoint,
                timeout_seconds=self.timeout_seconds,
                verify_tls=self.verify_tls,
                headers=self._build_headers(),
                json_body=payload,
            )
            if not isinstance(response_payload, dict):
                response_payload = {}

            return {
                "task_id": normalized_task_id,
                "result": "submitted",
                "parameters": parameters,
                "scheduler": "airflow",
                "dag_run_id": str(response_payload.get("dag_run_id", run_id)),
                "state": str(response_payload.get("state", "queued")),
            }
        except ConnectorError as exc:
            return {
                "task_id": normalized_task_id,
                "result": "failed",
                "error": str(exc),
                "parameters": parameters,
                "scheduler": "airflow",
            }

    async def get_task_status(self, task_id: str) -> dict[str, object]:
        normalized_task_id = task_id.strip()
        if not normalized_task_id:
            return {
                "task_id": task_id,
                "status": "UNKNOWN",
                "result": "failed",
                "error": "task_id cannot be empty",
            }

        if self.backend != "airflow":
            return {
                "task_id": normalized_task_id,
                "status": "UNKNOWN",
                "result": "failed",
                "error": f"unsupported scheduler backend: {self.backend}",
            }

        if not self.base_url:
            if self.demo_fallback:
                return {
                    "task_id": normalized_task_id,
                    "status": "SUCCESS",
                    "result": "queried",
                    "mode": "demo_fallback",
                }
            return {
                "task_id": normalized_task_id,
                "status": "UNKNOWN",
                "result": "failed",
                "error": "scheduler_base_url is not configured",
            }

        dag_id = quote(normalized_task_id, safe="")
        endpoint = f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns"

        try:
            response_payload = await request_json(
                method="GET",
                url=endpoint,
                timeout_seconds=self.timeout_seconds,
                verify_tls=self.verify_tls,
                headers=self._build_headers(),
                params={
                    "order_by": "-execution_date",
                    "limit": 1,
                },
            )

            raw_state = ""
            dag_run_id = ""
            if isinstance(response_payload, dict):
                dag_runs = response_payload.get("dag_runs", [])
                if isinstance(dag_runs, list) and dag_runs:
                    latest = dag_runs[0]
                    if isinstance(latest, dict):
                        raw_state = str(latest.get("state", ""))
                        dag_run_id = str(latest.get("dag_run_id", ""))
            elif isinstance(response_payload, list) and response_payload:
                latest = response_payload[0]
                if isinstance(latest, dict):
                    raw_state = str(latest.get("state", ""))
                    dag_run_id = str(latest.get("dag_run_id", ""))

            return {
                "task_id": normalized_task_id,
                "status": self._normalize_state(raw_state),
                "raw_state": raw_state or "unknown",
                "dag_run_id": dag_run_id,
                "result": "queried",
                "scheduler": "airflow",
            }
        except ConnectorError as exc:
            return {
                "task_id": normalized_task_id,
                "status": "UNKNOWN",
                "result": "failed",
                "error": str(exc),
                "scheduler": "airflow",
            }

    async def suspend_downstream_dag(self, dag_id: str, reason: str) -> dict[str, object]:
        normalized_dag_id = dag_id.strip()
        if not normalized_dag_id:
            return {
                "dag_id": dag_id,
                "result": "failed",
                "reason": reason,
                "error": "dag_id cannot be empty",
            }

        if self.backend != "airflow":
            return {
                "dag_id": normalized_dag_id,
                "result": "failed",
                "reason": reason,
                "error": f"unsupported scheduler backend: {self.backend}",
            }

        if not self.base_url:
            if self.demo_fallback:
                return {
                    "dag_id": normalized_dag_id,
                    "result": "suspended",
                    "reason": reason,
                    "mode": "demo_fallback",
                }
            return {
                "dag_id": normalized_dag_id,
                "result": "failed",
                "reason": reason,
                "error": "scheduler_base_url is not configured",
            }

        endpoint = f"{self.base_url}/api/v1/dags/{quote(normalized_dag_id, safe='')}"
        try:
            await request_json(
                method="PATCH",
                url=endpoint,
                timeout_seconds=self.timeout_seconds,
                verify_tls=self.verify_tls,
                headers=self._build_headers(),
                json_body={"is_paused": True},
            )
            return {
                "dag_id": normalized_dag_id,
                "result": "suspended",
                "reason": reason,
                "scheduler": "airflow",
            }
        except ConnectorError as exc:
            return {
                "dag_id": normalized_dag_id,
                "result": "failed",
                "reason": reason,
                "error": str(exc),
                "scheduler": "airflow",
            }


def mock_retry_task(task_id: str, parameters: dict[str, object]) -> dict[str, object]:
    connector = AirflowSchedulerConnector()
    return run_async(connector.retry_task(task_id=task_id, parameters=parameters))


def mock_get_task_status(task_id: str) -> dict[str, object]:
    connector = AirflowSchedulerConnector()
    return run_async(connector.get_task_status(task_id=task_id))


def mock_suspend_downstream_dag(dag_id: str, reason: str) -> dict[str, object]:
    connector = AirflowSchedulerConnector()
    return run_async(connector.suspend_downstream_dag(dag_id=dag_id, reason=reason))


def retry_task(task_id: str, parameters: dict[str, object]) -> dict[str, object]:
    return mock_retry_task(task_id=task_id, parameters=parameters)


def get_task_status(task_id: str) -> dict[str, object]:
    return mock_get_task_status(task_id=task_id)


def suspend_downstream_dag(dag_id: str, reason: str) -> dict[str, object]:
    return mock_suspend_downstream_dag(dag_id=dag_id, reason=reason)
