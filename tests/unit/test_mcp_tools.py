from nightwatch.mcp.tools import (
    create_github_issue,
    pause_flink_job,
    retry_scheduler_task,
    tune_spark_parameters,
)


def test_tune_spark_parameters_calls_spark_connector(monkeypatch):
    called: dict[str, object] = {}

    def fake_tune(application_id: str, parameters: dict[str, object]) -> dict[str, object]:
        called["application_id"] = application_id
        called["parameters"] = parameters
        return {"result": "applied", "application_id": application_id, "parameters": parameters}

    monkeypatch.setattr("nightwatch.mcp.tools.mock_tune_spark_parameters", fake_tune)

    result = tune_spark_parameters(
        application_id="application_001",
        executor_memory_gb=12,
        shuffle_partitions=900,
        skew_optimization="salting",
    )

    assert result["result"] == "applied"
    assert called["application_id"] == "application_001"
    assert called["parameters"] == {
        "executor_memory_gb": 12,
        "shuffle_partitions": 900,
        "skew_optimization": "salting",
    }


def test_retry_scheduler_task_calls_scheduler_connector(monkeypatch):
    called: dict[str, object] = {}

    def fake_retry(task_id: str, parameters: dict[str, object]) -> dict[str, object]:
        called["task_id"] = task_id
        called["parameters"] = parameters
        return {"result": "submitted", "task_id": task_id, "parameters": parameters}

    monkeypatch.setattr("nightwatch.mcp.tools.mock_retry_task", fake_retry)

    result = retry_scheduler_task("dag_dws_user_growth", {"executor_memory_gb": 10})

    assert result["result"] == "submitted"
    assert called["task_id"] == "dag_dws_user_growth"
    assert called["parameters"] == {"executor_memory_gb": 10}


def test_pause_flink_job_and_create_github_issue(monkeypatch):
    pause_called: dict[str, object] = {}
    issue_called: dict[str, object] = {}

    def fake_pause(job_id: str, reason: str) -> dict[str, object]:
        pause_called["job_id"] = job_id
        pause_called["reason"] = reason
        return {"result": "paused", "job_id": job_id, "reason": reason}

    def fake_ticket(
        system: str,
        owner: str,
        title: str,
        body: str,
        labels: list[str] | None = None,
    ) -> dict[str, object]:
        issue_called["system"] = system
        issue_called["owner"] = owner
        issue_called["title"] = title
        issue_called["body"] = body
        issue_called["labels"] = labels
        return {"result": "created", "system": system, "owner": owner}

    monkeypatch.setattr("nightwatch.mcp.tools.mock_pause_cdc_job", fake_pause)
    monkeypatch.setattr("nightwatch.mcp.tools.mock_create_collaboration_ticket", fake_ticket)

    pause_result = pause_flink_job("flink_cdc_orders", "schema drift")
    issue_result = create_github_issue(
        owner="team.ordering_platform",
        title="Schema drift accountability",
        body="Please rollback upstream DDL",
        labels=["schema_drift"],
    )

    assert pause_result["result"] == "paused"
    assert pause_called == {"job_id": "flink_cdc_orders", "reason": "schema drift"}

    assert issue_result["result"] == "created"
    assert issue_called["system"] == "github"
    assert issue_called["owner"] == "team.ordering_platform"
    assert issue_called["labels"] == ["schema_drift"]
