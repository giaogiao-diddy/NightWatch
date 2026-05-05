from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = Field(default="NightWatch")
    environment: str = Field(default="dev")
    log_level: str = Field(default="INFO")
    api_auth_key: str = Field(default="")
    api_cors_allow_origins: str = Field(default="http://127.0.0.1:3000,http://localhost:3000")
    connectors_demo_fallback: bool = Field(default=True)
    sqlite_mount_dir: str = Field(default="./runtime")

    scheduler_backend: str = Field(default="airflow")
    scheduler_base_url: str = Field(default="")
    scheduler_api_token: str = Field(default="")
    scheduler_username: str = Field(default="")
    scheduler_password: str = Field(default="")
    scheduler_timeout_seconds: float = Field(default=10.0, gt=0)
    scheduler_verify_tls: bool = Field(default=True)

    spark_history_base_url: str = Field(default="")
    spark_yarn_rm_base_url: str = Field(default="")
    spark_control_base_url: str = Field(default="")
    spark_api_token: str = Field(default="")
    spark_timeout_seconds: float = Field(default=15.0, gt=0)
    spark_verify_tls: bool = Field(default=True)
    spark_log_max_bytes: int = Field(default=65536, ge=1024)

    flink_base_url: str = Field(default="")
    flink_api_token: str = Field(default="")
    flink_timeout_seconds: float = Field(default=10.0, gt=0)
    flink_verify_tls: bool = Field(default=True)
    flink_poll_interval_seconds: float = Field(default=1.0, gt=0)
    flink_operation_timeout_seconds: float = Field(default=15.0, gt=0)

    llm_api_key: str = Field(default="")
    llm_base_url: str = Field(default="https://api.openai.com/v1")
    llm_model: str = Field(default="gpt-4o-mini")
    llm_timeout_seconds: float = Field(default=30.0, gt=0)
    llm_max_retries: int = Field(default=2, ge=1)
    llm_temperature: float = Field(default=0.0, ge=0.0, le=1.0)
    llm_max_prompt_chars: int = Field(default=12000, ge=1000)
    qdrant_url: str = Field(default="")
    qdrant_api_key: str = Field(default="")
    qdrant_timeout_seconds: float = Field(default=10.0, gt=0)
    qdrant_path: str = Field(default="./qdrant_data")
    graph_checkpointer_backend: str = Field(default="sqlite")
    graph_checkpointer_sqlite_path: str = Field(default="")

    worker_enable: bool = Field(default=True)
    worker_poll_interval_seconds: float = Field(default=2.0, gt=0)
    worker_batch_size: int = Field(default=10, ge=1, le=100)
    worker_claim_ttl_seconds: int = Field(default=60, ge=5)
    worker_retry_backoff_seconds: int = Field(default=30, ge=1)
    worker_approval_recheck_seconds: int = Field(default=120, ge=5)
    worker_max_attempts: int = Field(default=20, ge=1)
    worker_runtime_sqlite_path: str = Field(default="")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="NW_",
        extra="ignore",
    )

    def resolve_sqlite_path(self, configured_path: str, default_filename: str) -> str:
        normalized_configured_path = configured_path.strip()
        if normalized_configured_path:
            path = Path(normalized_configured_path).expanduser()
        else:
            path = Path(self.sqlite_mount_dir.strip() or "./runtime") / default_filename

        if not path.is_absolute():
            path = Path.cwd() / path

        path.parent.mkdir(parents=True, exist_ok=True)
        return str(path.resolve())

    def get_graph_checkpointer_sqlite_path(self) -> str:
        return self.resolve_sqlite_path(
            configured_path=self.graph_checkpointer_sqlite_path,
            default_filename="nightwatch_checkpoints.sqlite",
        )

    def get_worker_runtime_sqlite_path(self) -> str:
        return self.resolve_sqlite_path(
            configured_path=self.worker_runtime_sqlite_path,
            default_filename="nightwatch_runtime.sqlite",
        )

    def get_cors_allow_origins(self) -> list[str]:
        return [
            origin.strip()
            for origin in self.api_cors_allow_origins.split(",")
            if origin.strip()
        ]


@lru_cache
def get_settings() -> Settings:
    return Settings()