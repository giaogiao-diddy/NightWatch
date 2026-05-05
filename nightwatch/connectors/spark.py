from typing import Any
from urllib.parse import quote

from nightwatch.config import get_settings
from nightwatch.connectors.base import ConnectorError, build_auth_headers, request_json, request_text, run_async


class SparkConnector:
    def __init__(self) -> None:
        settings = get_settings()
        self.history_base_url = settings.spark_history_base_url.strip().rstrip("/")
        self.yarn_rm_base_url = settings.spark_yarn_rm_base_url.strip().rstrip("/")
        self.control_base_url = settings.spark_control_base_url.strip().rstrip("/")
        self.api_token = settings.spark_api_token
        self.timeout_seconds = settings.spark_timeout_seconds
        self.verify_tls = settings.spark_verify_tls
        self.log_max_bytes = settings.spark_log_max_bytes
        self.demo_fallback = settings.connectors_demo_fallback

    def _headers(self) -> dict[str, str]:
        return build_auth_headers(self.api_token)

    @staticmethod
    def _normalize_status(raw_state: str, final_status: str = "") -> str:
        state = raw_state.strip().upper()
        terminal = final_status.strip().upper()

        if state in {"RUNNING", "SUBMITTED", "ACCEPTED"}:
            return "RUNNING"
        if state == "FINISHED" and terminal in {"SUCCEEDED", "SUCCESS"}:
            return "SUCCESS"
        if state in {"FAILED", "KILLED"}:
            return "FAILED"
        if state == "COMPLETED" and terminal in {"SUCCEEDED", "SUCCESS"}:
            return "SUCCESS"
        if state == "COMPLETED":
            return "FAILED"
        if terminal in {"SUCCEEDED", "SUCCESS"}:
            return "SUCCESS"
        if terminal in {"FAILED", "KILLED"}:
            return "FAILED"
        return "UNKNOWN"

    async def get_application_status(self, application_id: str) -> dict[str, object]:
        normalized_app_id = application_id.strip()
        if not normalized_app_id:
            return {
                "application_id": application_id,
                "status": "UNKNOWN",
                "result": "failed",
                "error": "application_id cannot be empty",
            }

        history_state = ""
        history_final = ""
        history_error = ""

        if self.history_base_url:
            endpoint = f"{self.history_base_url}/api/v1/applications/{quote(normalized_app_id, safe='')}"
            try:
                payload = await request_json(
                    method="GET",
                    url=endpoint,
                    timeout_seconds=self.timeout_seconds,
                    verify_tls=self.verify_tls,
                    headers=self._headers(),
                )
                if isinstance(payload, list) and payload:
                    first = payload[0]
                    if isinstance(first, dict):
                        completed = bool(first.get("completed", False))
                        history_state = "COMPLETED" if completed else "RUNNING"
                elif isinstance(payload, dict):
                    attempts = payload.get("attempts", [])
                    if isinstance(attempts, list) and attempts:
                        first = attempts[0]
                        if isinstance(first, dict):
                            completed = bool(first.get("completed", False))
                            history_state = "COMPLETED" if completed else "RUNNING"
            except ConnectorError as exc:
                history_error = str(exc)

        yarn_state = ""
        yarn_final_status = ""
        yarn_error = ""
        if self.yarn_rm_base_url:
            endpoint = f"{self.yarn_rm_base_url}/ws/v1/cluster/apps/{quote(normalized_app_id, safe='')}"
            try:
                payload = await request_json(
                    method="GET",
                    url=endpoint,
                    timeout_seconds=self.timeout_seconds,
                    verify_tls=self.verify_tls,
                    headers=self._headers(),
                )
                if isinstance(payload, dict):
                    app = payload.get("app", {})
                    if isinstance(app, dict):
                        yarn_state = str(app.get("state", ""))
                        yarn_final_status = str(app.get("finalStatus", ""))
            except ConnectorError as exc:
                yarn_error = str(exc)

        normalized_status = self._normalize_status(
            raw_state=yarn_state or history_state,
            final_status=yarn_final_status or history_final,
        )

        if normalized_status == "UNKNOWN" and self.demo_fallback and not self.history_base_url and not self.yarn_rm_base_url:
            return {
                "application_id": normalized_app_id,
                "status": "RUNNING",
                "result": "queried",
                "mode": "demo_fallback",
            }

        if normalized_status == "UNKNOWN" and history_error and yarn_error:
            return {
                "application_id": normalized_app_id,
                "status": "UNKNOWN",
                "result": "failed",
                "error": f"history error: {history_error}; yarn error: {yarn_error}",
            }

        return {
            "application_id": normalized_app_id,
            "status": normalized_status,
            "result": "queried",
            "history_state": history_state,
            "yarn_state": yarn_state,
            "yarn_final_status": yarn_final_status,
        }

    async def get_application_state(self, application_id: str) -> dict[str, object]:
        return await self.get_application_status(application_id=application_id)

    async def get_executor_metrics(self, application_id: str, limit: int = 20) -> dict[str, object]:
        normalized_app_id = application_id.strip()
        if not normalized_app_id:
            return {
                "application_id": application_id,
                "result": "failed",
                "error": "application_id cannot be empty",
                "executors": [],
            }

        if limit < 1:
            limit = 1

        if not self.history_base_url:
            if self.demo_fallback:
                return {
                    "application_id": normalized_app_id,
                    "result": "queried",
                    "executors": [],
                    "mode": "demo_fallback",
                }
            return {
                "application_id": normalized_app_id,
                "result": "failed",
                "error": "spark_history_base_url is not configured",
                "executors": [],
            }

        endpoint = f"{self.history_base_url}/api/v1/applications/{quote(normalized_app_id, safe='')}/executors"
        try:
            payload = await request_json(
                method="GET",
                url=endpoint,
                timeout_seconds=self.timeout_seconds,
                verify_tls=self.verify_tls,
                headers=self._headers(),
            )
            executors: list[dict[str, object]] = []
            if isinstance(payload, list):
                for raw in payload[:limit]:
                    if not isinstance(raw, dict):
                        continue
                    executors.append(
                        {
                            "id": str(raw.get("id", "")),
                            "hostPort": str(raw.get("hostPort", "")),
                            "isActive": bool(raw.get("isActive", False)),
                            "totalTasks": int(raw.get("totalTasks", 0) or 0),
                            "failedTasks": int(raw.get("failedTasks", 0) or 0),
                            "totalDuration": int(raw.get("totalDuration", 0) or 0),
                            "totalGCTime": int(raw.get("totalGCTime", 0) or 0),
                            "memoryUsed": int(raw.get("memoryUsed", 0) or 0),
                            "maxMemory": int(raw.get("maxMemory", 0) or 0),
                        }
                    )

            return {
                "application_id": normalized_app_id,
                "result": "queried",
                "executor_count": len(executors),
                "executors": executors,
            }
        except ConnectorError as exc:
            return {
                "application_id": normalized_app_id,
                "result": "failed",
                "error": str(exc),
                "executors": [],
            }

    async def get_executor_logs(
        self,
        application_id: str,
        executor_id: str = "",
        offset: int = 0,
        max_bytes: int | None = None,
    ) -> dict[str, object]:
        normalized_app_id = application_id.strip()
        if not normalized_app_id:
            return {
                "application_id": application_id,
                "result": "failed",
                "error": "application_id cannot be empty",
            }

        if offset < 0:
            offset = 0

        hard_limit = max_bytes if isinstance(max_bytes, int) and max_bytes > 0 else self.log_max_bytes
        hard_limit = min(max(hard_limit, 1024), self.log_max_bytes)

        metrics_payload = await self.get_executor_metrics(application_id=normalized_app_id, limit=200)
        if metrics_payload.get("result") != "queried" and not self.demo_fallback:
            return {
                "application_id": normalized_app_id,
                "result": "failed",
                "error": str(metrics_payload.get("error", "failed to load executor metadata")),
            }

        if not self.history_base_url:
            if self.demo_fallback:
                return {
                    "application_id": normalized_app_id,
                    "executor_id": executor_id,
                    "result": "queried",
                    "offset": offset,
                    "next_offset": offset,
                    "truncated": False,
                    "content": "",
                    "mode": "demo_fallback",
                }
            return {
                "application_id": normalized_app_id,
                "executor_id": executor_id,
                "result": "failed",
                "error": "spark_history_base_url is not configured",
            }

        endpoint = f"{self.history_base_url}/api/v1/applications/{quote(normalized_app_id, safe='')}/executors"
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
                "application_id": normalized_app_id,
                "executor_id": executor_id,
                "result": "failed",
                "error": str(exc),
            }

        selected_executor: dict[str, Any] | None = None
        if isinstance(payload, list):
            for item in payload:
                if not isinstance(item, dict):
                    continue
                current_id = str(item.get("id", ""))
                if executor_id.strip() and current_id != executor_id.strip():
                    continue
                selected_executor = item
                break

            if selected_executor is None and payload:
                first = payload[0]
                if isinstance(first, dict):
                    selected_executor = first

        if selected_executor is None:
            return {
                "application_id": normalized_app_id,
                "executor_id": executor_id,
                "result": "failed",
                "error": "executor not found",
            }

        log_urls = selected_executor.get("logs", {})
        log_url = ""
        if isinstance(log_urls, dict):
            log_url = str(log_urls.get("stderr") or log_urls.get("stdout") or "")

        if not log_url:
            return {
                "application_id": normalized_app_id,
                "executor_id": str(selected_executor.get("id", "")),
                "result": "failed",
                "error": "executor log url is unavailable",
            }

        headers = self._headers()
        headers["Range"] = f"bytes={offset}-{offset + hard_limit - 1}"

        try:
            text, _ = await request_text(
                method="GET",
                url=log_url,
                timeout_seconds=self.timeout_seconds,
                verify_tls=self.verify_tls,
                headers=headers,
            )
            payload_bytes = text.encode("utf-8", errors="ignore")
            truncated = len(payload_bytes) > hard_limit
            sliced = payload_bytes[:hard_limit]
            content = sliced.decode("utf-8", errors="ignore")
            next_offset = offset + len(sliced)

            return {
                "application_id": normalized_app_id,
                "executor_id": str(selected_executor.get("id", "")),
                "result": "queried",
                "offset": offset,
                "next_offset": next_offset,
                "truncated": truncated,
                "content": content,
                "fetched_bytes": len(sliced),
                "log_url": log_url,
            }
        except ConnectorError as exc:
            return {
                "application_id": normalized_app_id,
                "executor_id": str(selected_executor.get("id", "")),
                "result": "failed",
                "error": str(exc),
            }

    async def tune_spark_parameters(self, application_id: str, parameters: dict[str, object]) -> dict[str, object]:
        normalized_app_id = application_id.strip()
        if not normalized_app_id:
            return {
                "application_id": application_id,
                "result": "failed",
                "error": "application_id cannot be empty",
                "parameters": parameters,
            }

        if not self.control_base_url:
            if self.demo_fallback:
                return {
                    "application_id": normalized_app_id,
                    "result": "applied",
                    "parameters": parameters,
                    "mode": "demo_fallback",
                }
            return {
                "application_id": normalized_app_id,
                "result": "failed",
                "error": "spark_control_base_url is not configured",
                "parameters": parameters,
            }

        endpoint = f"{self.control_base_url}/api/v1/applications/{quote(normalized_app_id, safe='')}/runtime-config"
        try:
            await request_json(
                method="PATCH",
                url=endpoint,
                timeout_seconds=self.timeout_seconds,
                verify_tls=self.verify_tls,
                headers=self._headers(),
                json_body={"parameters": parameters},
            )
            return {
                "application_id": normalized_app_id,
                "result": "applied",
                "parameters": parameters,
            }
        except ConnectorError as exc:
            return {
                "application_id": normalized_app_id,
                "result": "failed",
                "error": str(exc),
                "parameters": parameters,
            }


def mock_tune_spark_parameters(
    application_id: str,
    parameters: dict[str, object],
) -> dict[str, object]:
    connector = SparkConnector()
    return run_async(
        connector.tune_spark_parameters(
            application_id=application_id,
            parameters=parameters,
        )
    )


def get_application_status(application_id: str) -> dict[str, object]:
    connector = SparkConnector()
    return run_async(connector.get_application_status(application_id=application_id))


def get_application_state(application_id: str) -> dict[str, object]:
    connector = SparkConnector()
    return run_async(connector.get_application_state(application_id=application_id))


def get_executor_metrics(application_id: str, limit: int = 20) -> dict[str, object]:
    connector = SparkConnector()
    return run_async(connector.get_executor_metrics(application_id=application_id, limit=limit))


def get_executor_logs(
    application_id: str,
    executor_id: str = "",
    offset: int = 0,
    max_bytes: int | None = None,
) -> dict[str, object]:
    connector = SparkConnector()
    return run_async(
        connector.get_executor_logs(
            application_id=application_id,
            executor_id=executor_id,
            offset=offset,
            max_bytes=max_bytes,
        )
    )
