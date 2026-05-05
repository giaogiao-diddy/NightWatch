"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  Activity,
  AlertTriangle,
  Braces,
  CheckCircle2,
  Database,
  Loader2,
  RefreshCw,
  ShieldAlert,
} from "lucide-react";

type IncidentSource = "spark" | "flink";
type IncidentStatus = "pending" | "diagnosed" | "healed" | "isolated" | "escalated" | "failed";
type LogLevel = "INFO" | "ACTION" | "SUCCESS" | "WARN" | "ERROR";
type ConsoleTab = "overview" | "incident" | "config";

interface IncidentPayload {
  cluster: string;
  job_name: string;
  application_id: string;
  severity: string;
  message: string;
  host: string;
  labels: Record<string, string>;
  incident_type?: "flink_schema_drift";
  scheduler_task_id?: string;
}

interface IncidentIngestRequest {
  source: IncidentSource;
  auto_execute: boolean;
  payload: IncidentPayload;
}

interface ExecutionRecord {
  action_type: string;
  result?: string;
  [key: string]: unknown;
}

interface AgentStateSnapshot {
  mode?: "assist" | "auto";
  status?: IncidentStatus | string;
  diagnosis?: Record<string, unknown>;
  heal_plan?: Record<string, unknown>;
  blast_radius?: Record<string, unknown>;
  guardrail_result?: Record<string, unknown>;
  notification_targets?: Array<Record<string, unknown>>;
  notification_payloads?: Array<Record<string, unknown>>;
  execution_records?: ExecutionRecord[];
  diagnosis_errors?: string[];
  [key: string]: unknown;
}

interface IncidentResponse {
  incident_id: string;
  status: IncidentStatus | string;
  incident_type: string;
  severity: string;
  diagnosis_summary: string;
  recommended_action: string;
  approval_required: boolean;
  execution_result?: Record<string, unknown> | null;
  state_snapshot?: AgentStateSnapshot;
  error?: string | null;
}

interface LogEntry {
  id: string;
  timestamp: string;
  level: LogLevel;
  message: string;
}

interface MetricSeries {
  labels: Record<string, string>;
  value: number;
}

interface MetricsSnapshot {
  incidentsTotal: number;
  rcaAvgSeconds: number | null;
  actionSuccessRate: number | null;
  actionSuccessCount: number;
  actionFailureCount: number;
  fetchedAt: string;
}

interface RuntimeConnectorConfig {
  connectors_demo_fallback: boolean;
  scheduler_backend: string;
  scheduler_base_url: string;
  scheduler_api_token: string;
  scheduler_username: string;
  scheduler_password: string;
  scheduler_verify_tls: boolean;
  spark_history_base_url: string;
  spark_yarn_rm_base_url: string;
  spark_control_base_url: string;
  spark_api_token: string;
  spark_verify_tls: boolean;
  flink_base_url: string;
  flink_api_token: string;
  flink_verify_tls: boolean;
  qdrant_url: string;
  qdrant_api_key: string;
  llm_base_url: string;
  llm_model: string;
  llm_api_key: string;
}

interface RuntimeConfigResponse {
  config: RuntimeConnectorConfig;
  persisted: boolean;
  note: string;
}

const MASKED_SECRET = "********";

const SECRET_RUNTIME_FIELDS = new Set<string>([
  "scheduler_api_token",
  "scheduler_password",
  "spark_api_token",
  "flink_api_token",
  "qdrant_api_key",
  "llm_api_key",
]);

const STATUS_LABELS: Record<string, string> = {
  idle: "空闲",
  pending: "处理中",
  diagnosed: "已诊断",
  healed: "已修复",
  isolated: "已隔离",
  escalated: "已升级处理",
  failed: "失败",
};

const TAB_ITEMS: Array<{ id: ConsoleTab; label: string; description: string }> = [
  { id: "overview", label: "运行总览", description: "指标与当前状态" },
  { id: "incident", label: "事件演练", description: "触发、日志、状态快照" },
  { id: "config", label: "集群配置", description: "连接器与模型参数" },
];

const INPUT_CLASS_NAME =
  "h-10 w-full rounded-md border border-zinc-300 bg-white px-3 text-sm outline-none transition focus:border-zinc-900";
const SMALL_INPUT_CLASS_NAME =
  "h-9 w-full rounded border border-zinc-300 bg-white px-2 text-xs outline-none transition focus:border-zinc-900";

const DEFAULT_RUNTIME_CONFIG: RuntimeConnectorConfig = {
  connectors_demo_fallback: false,
  scheduler_backend: "airflow",
  scheduler_base_url: "",
  scheduler_api_token: "",
  scheduler_username: "",
  scheduler_password: "",
  scheduler_verify_tls: true,
  spark_history_base_url: "",
  spark_yarn_rm_base_url: "",
  spark_control_base_url: "",
  spark_api_token: "",
  spark_verify_tls: true,
  flink_base_url: "",
  flink_api_token: "",
  flink_verify_tls: true,
  qdrant_url: "",
  qdrant_api_key: "",
  llm_base_url: "https://api.openai.com/v1",
  llm_model: "gpt-4o-mini",
  llm_api_key: "",
};

const LOG_LEVEL_STYLES: Record<LogLevel, string> = {
  INFO: "border-zinc-300 bg-zinc-100 text-zinc-700",
  ACTION: "border-indigo-200 bg-indigo-50 text-indigo-700",
  SUCCESS: "border-emerald-200 bg-emerald-50 text-emerald-700",
  WARN: "border-amber-200 bg-amber-50 text-amber-700",
  ERROR: "border-red-200 bg-red-50 text-red-700",
};

const createSparkOomRequest = (): IncidentIngestRequest => ({
  source: "spark",
  auto_execute: true,
  payload: {
    cluster: "dw-prod",
    job_name: "dws_user_growth_nightly",
    application_id: "application_1714858800_1024",
    severity: "P0",
    message: "ExecutorLostFailure: Container killed by YARN for exceeding memory limits",
    host: "worker-01",
    scheduler_task_id: "dws_user_growth_nightly",
    labels: {
      approval: "approved",
    },
  },
});

const createSchemaDriftRequest = (): IncidentIngestRequest => ({
  source: "flink",
  auto_execute: false,
  payload: {
    cluster: "dw-prod",
    job_name: "ods.orders",
    application_id: "flink_cdc_orders",
    severity: "P0",
    message:
      "Unsupported CDC data type; table=ods.orders, column=total_amount, old_type=DECIMAL(10,2), new_type=JSON",
    incident_type: "flink_schema_drift",
    host: "tm-01",
    labels: {
      approval: "approved",
    },
  },
});

const DEFAULT_CUSTOM_REQUEST = JSON.stringify(createSparkOomRequest(), null, 2);

const nextLogId = () => `${Date.now()}-${Math.random().toString(16).slice(2)}`;

const escapeRegex = (value: string) => value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const parseLabelSet = (rawLabels: string): Record<string, string> => {
  const labels: Record<string, string> = {};
  if (!rawLabels.trim()) {
    return labels;
  }

  const pairRegex = /(\w+)="([^"]*)"/g;
  for (const match of rawLabels.matchAll(pairRegex)) {
    labels[match[1]] = match[2];
  }

  return labels;
};

const parseMetricSeries = (rawMetrics: string, metricName: string): MetricSeries[] => {
  const escapedName = escapeRegex(metricName);
  const lineRegex = new RegExp(
    `^${escapedName}(?:\\{([^}]*)\\})?\\s+([-+]?[0-9]*\\.?[0-9]+(?:[eE][-+]?[0-9]+)?)$`,
  );

  const points: MetricSeries[] = [];

  for (const line of rawMetrics.split("\n")) {
    if (!line || line.startsWith("#")) {
      continue;
    }

    const match = line.match(lineRegex);
    if (!match) {
      continue;
    }

    points.push({
      labels: parseLabelSet(match[1] ?? ""),
      value: Number.parseFloat(match[2]),
    });
  }

  return points;
};

const computeMetricsSnapshot = (rawMetrics: string): MetricsSnapshot => {
  const incidentsTotal = parseMetricSeries(rawMetrics, "nightwatch_incidents_total").reduce(
    (sum, item) => sum + item.value,
    0,
  );

  const rcaSum = parseMetricSeries(rawMetrics, "nightwatch_rca_duration_seconds_sum").reduce(
    (sum, item) => sum + item.value,
    0,
  );
  const rcaCount = parseMetricSeries(rawMetrics, "nightwatch_rca_duration_seconds_count").reduce(
    (sum, item) => sum + item.value,
    0,
  );

  const actionSeries = parseMetricSeries(rawMetrics, "nightwatch_action_success_total");
  const actionSuccessCount = actionSeries
    .filter((item) => item.labels.result === "success")
    .reduce((sum, item) => sum + item.value, 0);
  const actionFailureCount = actionSeries
    .filter((item) => item.labels.result === "failure")
    .reduce((sum, item) => sum + item.value, 0);

  const actionTotal = actionSuccessCount + actionFailureCount;

  return {
    incidentsTotal,
    rcaAvgSeconds: rcaCount > 0 ? rcaSum / rcaCount : null,
    actionSuccessRate: actionTotal > 0 ? actionSuccessCount / actionTotal : null,
    actionSuccessCount,
    actionFailureCount,
    fetchedAt: new Date().toLocaleTimeString(),
  };
};

const normalizeAuthorization = (value: string): string => {
  const trimmed = value.trim();
  if (!trimmed) {
    return "";
  }
  return trimmed.toLowerCase().startsWith("bearer ") ? trimmed : `Bearer ${trimmed}`;
};

const localizeConfigNote = (note: string): string => {
  if (!note) {
    return "";
  }

  if (note.includes("runtime-only") || note.includes("reset after process restart")) {
    return "当前配置仅在运行时生效，服务重启后会恢复默认值或环境变量值。";
  }

  if (note.includes("Runtime configuration updated")) {
    return "配置已更新并立即生效。若需长期保存，请同步写入环境变量或持久化配置。";
  }

  return note;
};

const getStatusLabel = (status?: string): string => {
  if (!status) {
    return STATUS_LABELS.idle;
  }
  return STATUS_LABELS[status] ?? status;
};

const buildRuntimeConfigUpdatePayload = (
  runtimeConfig: RuntimeConnectorConfig,
): Record<string, string | boolean> => {
  const payload: Record<string, string | boolean> = {};

  for (const [key, rawValue] of Object.entries(runtimeConfig)) {
    if (typeof rawValue === "boolean") {
      payload[key] = rawValue;
      continue;
    }

    if (SECRET_RUNTIME_FIELDS.has(key)) {
      const normalizedSecret = rawValue.trim();
      if (!normalizedSecret || normalizedSecret === MASKED_SECRET) {
        continue;
      }
      payload[key] = normalizedSecret;
      continue;
    }

    payload[key] = rawValue;
  }

  return payload;
};

export default function Page() {
  const [activeTab, setActiveTab] = useState<ConsoleTab>("overview");
  const [apiBaseUrl, setApiBaseUrl] = useState<string>("http://127.0.0.1:8000");
  const [apiKey, setApiKey] = useState<string>("");
  const [authorization, setAuthorization] = useState<string>("");

  const [isSparkLoading, setIsSparkLoading] = useState<boolean>(false);
  const [isDriftLoading, setIsDriftLoading] = useState<boolean>(false);
  const [isCustomLoading, setIsCustomLoading] = useState<boolean>(false);
  const [isMetricsLoading, setIsMetricsLoading] = useState<boolean>(false);

  const [customPayloadText, setCustomPayloadText] = useState<string>(DEFAULT_CUSTOM_REQUEST);

  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [lastResponse, setLastResponse] = useState<IncidentResponse | null>(null);
  const [snapshot, setSnapshot] = useState<AgentStateSnapshot | null>(null);
  const [metrics, setMetrics] = useState<MetricsSnapshot | null>(null);
  const [runtimeConfig, setRuntimeConfig] = useState<RuntimeConnectorConfig>(DEFAULT_RUNTIME_CONFIG);
  const [isConfigLoading, setIsConfigLoading] = useState<boolean>(false);
  const [isConfigSaving, setIsConfigSaving] = useState<boolean>(false);
  const [configNote, setConfigNote] = useState<string>("");

  const logContainerRef = useRef<HTMLDivElement | null>(null);

  const appendLog = useCallback((level: LogLevel, message: string) => {
    const entry: LogEntry = {
      id: nextLogId(),
      timestamp: new Date().toLocaleTimeString(),
      level,
      message,
    };

    setLogs((prev) => [...prev, entry]);
  }, []);

  const buildRequestHeaders = useCallback(
    (includeJsonContentType: boolean): Record<string, string> => {
      const headers: Record<string, string> = {};

      if (includeJsonContentType) {
        headers["Content-Type"] = "application/json";
      }

      if (apiKey.trim()) {
        headers["X-API-Key"] = apiKey.trim();
      }

      if (authorization.trim()) {
        headers.Authorization = normalizeAuthorization(authorization);
      }

      return headers;
    },
    [apiKey, authorization],
  );

  const refreshMetrics = useCallback(async () => {
    setIsMetricsLoading(true);

    try {
      const response = await fetch(`${apiBaseUrl}/metrics`, {
        method: "GET",
        headers: buildRequestHeaders(false),
      });

      if (!response.ok) {
        const detail = await response.text();
        throw new Error(`HTTP ${response.status}: ${detail}`);
      }

      const payload = await response.text();
      const nextMetrics = computeMetricsSnapshot(payload);
      setMetrics(nextMetrics);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "未知指标错误";
      appendLog("WARN", `指标刷新失败：${errorMessage}`);
    } finally {
      setIsMetricsLoading(false);
    }
  }, [apiBaseUrl, appendLog, buildRequestHeaders]);

  const refreshRuntimeConfig = useCallback(async () => {
    setIsConfigLoading(true);

    try {
      const response = await fetch(`${apiBaseUrl}/api/v1/runtime-config`, {
        method: "GET",
        headers: buildRequestHeaders(false),
      });

      if (!response.ok) {
        const detail = await response.text();
        throw new Error(`HTTP ${response.status}: ${detail}`);
      }

      const payload = (await response.json()) as RuntimeConfigResponse;
      setRuntimeConfig(payload.config ?? DEFAULT_RUNTIME_CONFIG);
      setConfigNote(localizeConfigNote(payload.note ?? ""));
      appendLog("INFO", "运行时连接器配置已加载。");
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "未知配置错误";
      appendLog("WARN", `配置加载失败：${errorMessage}`);
    } finally {
      setIsConfigLoading(false);
    }
  }, [apiBaseUrl, appendLog, buildRequestHeaders]);

  const saveRuntimeConfig = useCallback(async () => {
    setIsConfigSaving(true);

    try {
      const response = await fetch(`${apiBaseUrl}/api/v1/runtime-config`, {
        method: "PUT",
        headers: buildRequestHeaders(true),
        body: JSON.stringify(buildRuntimeConfigUpdatePayload(runtimeConfig)),
      });

      if (!response.ok) {
        const detail = await response.text();
        throw new Error(`HTTP ${response.status}: ${detail}`);
      }

      const payload = (await response.json()) as RuntimeConfigResponse;
      setRuntimeConfig(payload.config ?? runtimeConfig);
      setConfigNote(localizeConfigNote(payload.note ?? ""));
      appendLog("SUCCESS", "运行时连接器配置已更新。");
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "未知配置保存错误";
      appendLog("ERROR", `配置保存失败：${errorMessage}`);
    } finally {
      setIsConfigSaving(false);
    }
  }, [apiBaseUrl, appendLog, buildRequestHeaders, runtimeConfig]);

  useEffect(() => {
    appendLog("INFO", "NightWatch 控制台已就绪。");
    void refreshMetrics();
  }, [appendLog, refreshMetrics]);

  useEffect(() => {
    void refreshRuntimeConfig();
  }, [refreshRuntimeConfig]);

  useEffect(() => {
    const timer = window.setInterval(() => {
      void refreshMetrics();
    }, 15000);

    return () => window.clearInterval(timer);
  }, [refreshMetrics]);

  useEffect(() => {
    if (logContainerRef.current) {
      logContainerRef.current.scrollTop = logContainerRef.current.scrollHeight;
    }
  }, [logs]);

  const snapshotCode = useMemo(() => {
    if (!snapshot) {
      return [
        "{",
        '  "state": "idle",',
        '  "hint": "请先触发演练事件，随后查看状态快照。"',
        "}",
      ].join("\n");
    }

    return JSON.stringify(snapshot, null, 2);
  }, [snapshot]);

  const pendingRequest = isSparkLoading || isDriftLoading || isCustomLoading;
  const systemHealthy =
    lastResponse == null ? true : !["failed", "escalated"].includes(String(lastResponse.status));

  const executeIncident = useCallback(
    async (payload: IncidentIngestRequest, label: string, mode: "spark" | "drift" | "custom") => {
      if (mode === "spark") {
        setIsSparkLoading(true);
      } else if (mode === "drift") {
        setIsDriftLoading(true);
      } else {
        setIsCustomLoading(true);
      }

      appendLog("INFO", `开始派发 ${label} -> ${apiBaseUrl}/api/v1/incidents/ingest`);

      try {
        const response = await fetch(`${apiBaseUrl}/api/v1/incidents/ingest`, {
          method: "POST",
          headers: buildRequestHeaders(true),
          body: JSON.stringify(payload),
        });

        if (!response.ok) {
          const detail = await response.text();
          throw new Error(`HTTP ${response.status}: ${detail}`);
        }

        const result = (await response.json()) as IncidentResponse;
        setLastResponse(result);
        setSnapshot(result.state_snapshot ?? null);

        const normalizedStatus = String(result.status);
        const statusLevel: LogLevel =
          normalizedStatus === "failed" || normalizedStatus === "escalated" ? "ERROR" : "SUCCESS";

        appendLog(
          statusLevel,
          `流程结束 | incident=${result.incident_id} | status=${result.status} | type=${result.incident_type}`,
        );

        if (result.diagnosis_summary) {
          appendLog("ACTION", `诊断摘要：${result.diagnosis_summary}`);
        }

        if (result.recommended_action) {
          appendLog("ACTION", `建议动作：${result.recommended_action}`);
        }

        const executionRecords = result.state_snapshot?.execution_records ?? [];
        for (const record of executionRecords) {
          appendLog(
            "ACTION",
            `执行记录 -> action=${String(record.action_type)} result=${String(record.result ?? "unknown")}`,
          );
        }

        const diagnosisErrors = result.state_snapshot?.diagnosis_errors ?? [];
        for (const diagnosisError of diagnosisErrors) {
          appendLog("WARN", `诊断告警：${diagnosisError}`);
        }

        if (result.error) {
          appendLog("ERROR", result.error);
        }

        void refreshMetrics();
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : "未知请求错误";
        appendLog("ERROR", `请求失败：${errorMessage}`);
      } finally {
        if (mode === "spark") {
          setIsSparkLoading(false);
        } else if (mode === "drift") {
          setIsDriftLoading(false);
        } else {
          setIsCustomLoading(false);
        }
      }
    },
    [apiBaseUrl, appendLog, buildRequestHeaders, refreshMetrics],
  );

  const executeCustomIncident = useCallback(async () => {
    try {
      const parsed = JSON.parse(customPayloadText) as IncidentIngestRequest;
      if (!parsed || typeof parsed !== "object") {
        throw new Error("payload 必须是 JSON 对象");
      }
      if (!parsed.source || !parsed.payload) {
        throw new Error("payload 必须包含 source 和 payload 字段");
      }

      await executeIncident(parsed, "自定义事件", "custom");
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "无效 JSON";
      appendLog("ERROR", `自定义事件解析失败：${errorMessage}`);
    }
  }, [appendLog, customPayloadText, executeIncident]);

  const updateRuntimeField = <K extends keyof RuntimeConnectorConfig>(
    key: K,
    value: RuntimeConnectorConfig[K],
  ) => {
    setRuntimeConfig((prev) => ({
      ...prev,
      [key]: value,
    }));
  };

  const successRateLabel =
    metrics?.actionSuccessRate == null ? "n/a" : `${(metrics.actionSuccessRate * 100).toFixed(1)}%`;

  const lastStatus = String(lastResponse?.status ?? "idle");
  const statusBadgeClass =
    systemHealthy
      ? "border-emerald-200 bg-emerald-50 text-emerald-700"
      : "border-red-200 bg-red-50 text-red-700";

  return (
    <div className="min-h-screen bg-zinc-50 text-sm text-zinc-900">
      <header className="flex h-14 items-center justify-between border-b border-zinc-200 bg-white px-4 md:px-6">
        <div className="flex items-center gap-2">
          <Database className="h-4 w-4 text-zinc-700" aria-hidden="true" />
          <span className="font-medium tracking-tight">NightWatch 数据智能运维控制台</span>
        </div>

        <div className="flex items-center gap-2 text-xs text-zinc-600" aria-live="polite">
          <span
            className={`h-2 w-2 rounded-full ${systemHealthy ? "bg-emerald-500" : "bg-red-500"}`}
            aria-hidden="true"
          />
          <span>{systemHealthy ? "系统状态正常" : "系统状态告警"}</span>
        </div>
      </header>

      <main className="mx-auto flex w-full max-w-[1440px] flex-col gap-4 p-4 md:p-6">
        <section className="rounded-lg border border-zinc-200 bg-white p-4 md:p-5">
          <div className="mb-4 flex flex-col gap-1">
            <h2 className="text-sm font-semibold text-zinc-800">基础连接信息</h2>
            <p className="text-xs text-zinc-500">所有请求将使用以下地址和鉴权头发送到后端 API。</p>
          </div>

          <div className="grid gap-3 md:grid-cols-3">
            <div className="space-y-1.5">
              <label htmlFor="api-base-url" className="text-xs text-zinc-600">
                FastAPI 地址
              </label>
              <input
                id="api-base-url"
                type="url"
                value={apiBaseUrl}
                onChange={(event) => setApiBaseUrl(event.target.value)}
                placeholder="http://127.0.0.1:8000"
                className={INPUT_CLASS_NAME}
              />
            </div>

            <div className="space-y-1.5">
              <label htmlFor="api-key" className="text-xs text-zinc-600">
                X-API-Key（可选）
              </label>
              <input
                id="api-key"
                type="password"
                value={apiKey}
                onChange={(event) => setApiKey(event.target.value)}
                placeholder="输入 API Key"
                className={INPUT_CLASS_NAME}
              />
            </div>

            <div className="space-y-1.5">
              <label htmlFor="authorization" className="text-xs text-zinc-600">
                Authorization（可选）
              </label>
              <input
                id="authorization"
                type="password"
                value={authorization}
                onChange={(event) => setAuthorization(event.target.value)}
                placeholder="Bearer xxx"
                className={INPUT_CLASS_NAME}
              />
            </div>
          </div>
        </section>

        <nav className="rounded-lg border border-zinc-200 bg-white p-2">
          <div className="grid gap-2 md:grid-cols-3">
            {TAB_ITEMS.map((tab) => {
              const isActive = activeTab === tab.id;
              return (
                <button
                  key={tab.id}
                  type="button"
                  onClick={() => setActiveTab(tab.id)}
                  className={`rounded-md border px-3 py-2 text-left transition ${
                    isActive
                      ? "border-zinc-900 bg-zinc-900 text-white"
                      : "border-zinc-200 bg-zinc-50 text-zinc-700 hover:bg-zinc-100"
                  }`}
                >
                  <p className="text-sm font-medium">{tab.label}</p>
                  <p className={`mt-0.5 text-xs ${isActive ? "text-zinc-300" : "text-zinc-500"}`}>
                    {tab.description}
                  </p>
                </button>
              );
            })}
          </div>
        </nav>

        {activeTab === "overview" ? (
          <section className="grid gap-4 xl:grid-cols-[1.2fr_0.8fr]">
            <article className="rounded-lg border border-zinc-200 bg-white p-4">
              <div className="mb-3 flex items-center justify-between">
                <h3 className="text-sm font-semibold text-zinc-700">核心指标</h3>
                <div className="flex items-center gap-2">
                  <span className="text-xs text-zinc-500">
                    {metrics ? `更新于 ${metrics.fetchedAt}` : "尚未拉取"}
                  </span>
                  <button
                    type="button"
                    onClick={() => void refreshMetrics()}
                    disabled={isMetricsLoading}
                    className="inline-flex h-8 items-center gap-1 rounded border border-zinc-300 px-2 text-xs font-medium text-zinc-700 transition hover:bg-zinc-100 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    {isMetricsLoading ? (
                      <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden="true" />
                    ) : (
                      <RefreshCw className="h-3.5 w-3.5" aria-hidden="true" />
                    )}
                    刷新
                  </button>
                </div>
              </div>

              <div className="grid gap-3 md:grid-cols-3">
                <div className="rounded border border-zinc-200 bg-zinc-50 p-3">
                  <p className="text-[11px] uppercase tracking-wide text-zinc-500">累计事件数</p>
                  <p className="mt-1 font-mono text-xl font-semibold text-zinc-900">
                    {metrics ? metrics.incidentsTotal.toFixed(0) : "n/a"}
                  </p>
                </div>

                <div className="rounded border border-zinc-200 bg-zinc-50 p-3">
                  <p className="text-[11px] uppercase tracking-wide text-zinc-500">平均 RCA 耗时</p>
                  <p className="mt-1 font-mono text-xl font-semibold text-zinc-900">
                    {metrics?.rcaAvgSeconds == null ? "n/a" : `${metrics.rcaAvgSeconds.toFixed(2)}s`}
                  </p>
                </div>

                <div className="rounded border border-zinc-200 bg-zinc-50 p-3">
                  <p className="text-[11px] uppercase tracking-wide text-zinc-500">自动修复成功率</p>
                  <p className="mt-1 font-mono text-xl font-semibold text-zinc-900">{successRateLabel}</p>
                  <p className="mt-1 text-[11px] text-zinc-500">
                    成功={metrics?.actionSuccessCount.toFixed(0) ?? "0"} / 失败={metrics?.actionFailureCount.toFixed(0) ?? "0"}
                  </p>
                </div>
              </div>
            </article>

            <article className="rounded-lg border border-zinc-200 bg-white p-4">
              <h3 className="mb-3 text-sm font-semibold text-zinc-700">当前状态</h3>

              <div className="space-y-2 text-xs text-zinc-600">
                <div className="flex items-center justify-between">
                  <span>最近事件 ID</span>
                  <span className="font-mono text-[11px] text-zinc-700">{lastResponse?.incident_id ?? "n/a"}</span>
                </div>
                <div className="flex items-center justify-between">
                  <span>事件类型</span>
                  <span className="font-mono text-[11px] text-zinc-700">{lastResponse?.incident_type ?? "n/a"}</span>
                </div>
                <div className="flex items-center justify-between">
                  <span>严重等级</span>
                  <span className="font-mono text-[11px] text-zinc-700">{lastResponse?.severity ?? "n/a"}</span>
                </div>
                <div className="flex items-center justify-between">
                  <span>执行状态</span>
                  <span className={`inline-flex rounded border px-2 py-0.5 font-medium ${statusBadgeClass}`}>
                    {getStatusLabel(lastStatus)}
                  </span>
                </div>
              </div>

              <div className="mt-4 rounded border border-zinc-200 bg-zinc-50 p-3 text-xs text-zinc-700">
                <p className="font-medium text-zinc-800">诊断摘要</p>
                <p className="mt-1 whitespace-pre-wrap break-words text-zinc-600">
                  {lastResponse?.diagnosis_summary ?? "暂无诊断信息"}
                </p>
              </div>

              <div className="mt-3 rounded border border-zinc-200 bg-zinc-50 p-3 text-xs text-zinc-700">
                <p className="font-medium text-zinc-800">建议动作</p>
                <p className="mt-1 whitespace-pre-wrap break-words text-zinc-600">
                  {lastResponse?.recommended_action ?? "暂无建议动作"}
                </p>
              </div>
            </article>

            <article className="xl:col-span-2 flex min-h-0 flex-col rounded-lg border border-zinc-200 bg-white">
              <div className="flex items-center justify-between border-b border-zinc-200 px-4 py-3">
                <div className="flex items-center gap-2">
                  <Braces className="h-4 w-4 text-zinc-600" aria-hidden="true" />
                  <h3 className="font-medium">AgentState 快照</h3>
                </div>

                <div className="flex items-center gap-2 text-xs text-zinc-600">
                  {systemHealthy ? (
                    <CheckCircle2 className="h-4 w-4 text-emerald-600" aria-hidden="true" />
                  ) : (
                    <ShieldAlert className="h-4 w-4 text-red-600" aria-hidden="true" />
                  )}
                  <span>{systemHealthy ? "稳定" : "需关注"}</span>
                </div>
              </div>

              <pre className="min-h-[260px] overflow-auto bg-zinc-50 p-4 font-mono text-xs leading-5 text-zinc-800">
                <code>{snapshotCode}</code>
              </pre>
            </article>
          </section>
        ) : null}

        {activeTab === "incident" ? (
          <section className="grid gap-4 xl:grid-cols-[420px_1fr]">
            <div className="space-y-4">
              <article className="rounded-lg border border-zinc-200 bg-white p-4">
                <h3 className="mb-3 text-sm font-semibold text-zinc-700">快速触发</h3>

                <div className="space-y-3">
                  <button
                    type="button"
                    onClick={() => void executeIncident(createSparkOomRequest(), "Spark OOM", "spark")}
                    disabled={pendingRequest}
                    className="inline-flex h-10 w-full items-center justify-center gap-2 rounded-md bg-zinc-900 px-3 font-medium text-white transition hover:bg-zinc-800 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    {isSparkLoading ? (
                      <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" />
                    ) : (
                      <Activity className="h-4 w-4" aria-hidden="true" />
                    )}
                    <span>模拟 Spark OOM</span>
                  </button>

                  <button
                    type="button"
                    onClick={() =>
                      void executeIncident(createSchemaDriftRequest(), "Flink Schema Drift", "drift")
                    }
                    disabled={pendingRequest}
                    className="inline-flex h-10 w-full items-center justify-center gap-2 rounded-md border border-zinc-300 bg-white px-3 font-medium text-zinc-900 transition hover:bg-zinc-100 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    {isDriftLoading ? (
                      <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" />
                    ) : (
                      <AlertTriangle className="h-4 w-4" aria-hidden="true" />
                    )}
                    <span>模拟 Schema Drift</span>
                  </button>
                </div>
              </article>

              <article className="rounded-lg border border-zinc-200 bg-white p-4">
                <h3 className="mb-2 text-sm font-semibold text-zinc-700">自定义事件 JSON</h3>
                <p className="mb-3 text-xs text-zinc-500">可直接粘贴请求体后发送到 ingest 接口进行演练。</p>

                <textarea
                  value={customPayloadText}
                  onChange={(event) => setCustomPayloadText(event.target.value)}
                  className="h-72 w-full resize-y rounded-md border border-zinc-300 bg-white px-3 py-2 font-mono text-xs outline-none transition focus:border-zinc-900"
                  spellCheck={false}
                />

                <button
                  type="button"
                  onClick={() => void executeCustomIncident()}
                  disabled={pendingRequest}
                  className="mt-3 inline-flex h-10 w-full items-center justify-center gap-2 rounded-md border border-zinc-900 bg-zinc-900 px-3 font-medium text-white transition hover:bg-zinc-800 disabled:cursor-not-allowed disabled:opacity-60"
                >
                  {isCustomLoading ? (
                    <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" />
                  ) : (
                    <Braces className="h-4 w-4" aria-hidden="true" />
                  )}
                  <span>提交自定义事件</span>
                </button>
              </article>
            </div>

            <div className="space-y-4">
              <article className="flex min-h-0 flex-col rounded-lg border border-zinc-200 bg-white">
                <div className="flex items-center justify-between border-b border-zinc-200 px-4 py-3">
                  <div className="flex items-center gap-2">
                    <Activity className="h-4 w-4 text-zinc-600" aria-hidden="true" />
                    <h3 className="font-medium">执行日志流</h3>
                  </div>
                  <span className="text-xs text-zinc-500">{logs.length} 条</span>
                </div>

                <div
                  ref={logContainerRef}
                  className="max-h-[360px] min-h-[360px] overflow-auto bg-zinc-50 font-mono text-xs"
                >
                  {logs.length === 0 ? (
                    <div className="p-4 text-zinc-500">等待事件执行...</div>
                  ) : (
                    <ul className="divide-y divide-zinc-200">
                      {logs.map((log, index) => (
                        <li key={log.id} className="flex items-start gap-3 px-4 py-2">
                          <span className="w-8 text-right text-zinc-400">{index + 1}</span>
                          <span
                            className={`inline-flex rounded border px-1.5 py-0.5 text-[10px] font-semibold ${LOG_LEVEL_STYLES[log.level]}`}
                          >
                            {log.level}
                          </span>
                          <span className="w-20 text-zinc-500">{log.timestamp}</span>
                          <span className="whitespace-pre-wrap break-words text-zinc-800">{log.message}</span>
                        </li>
                      ))}
                    </ul>
                  )}
                </div>
              </article>

              <article className="flex min-h-0 flex-col rounded-lg border border-zinc-200 bg-white">
                <div className="flex items-center justify-between border-b border-zinc-200 px-4 py-3">
                  <h3 className="font-medium">当前 AgentState</h3>
                  <span className={`inline-flex rounded border px-2 py-0.5 text-xs font-medium ${statusBadgeClass}`}>
                    {getStatusLabel(lastStatus)}
                  </span>
                </div>
                <pre className="max-h-[320px] min-h-[320px] overflow-auto bg-zinc-50 p-4 font-mono text-xs leading-5 text-zinc-800">
                  <code>{snapshotCode}</code>
                </pre>
              </article>
            </div>
          </section>
        ) : null}

        {activeTab === "config" ? (
          <section className="space-y-4">
            <article className="rounded-lg border border-zinc-200 bg-white p-4">
              <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
                <h3 className="text-sm font-semibold text-zinc-700">运行时连接器配置</h3>
                <div className="flex items-center gap-2">
                  <button
                    type="button"
                    onClick={() => void refreshRuntimeConfig()}
                    disabled={isConfigLoading || isConfigSaving}
                    className="inline-flex h-8 items-center gap-1 rounded border border-zinc-300 px-2 text-xs font-medium text-zinc-700 transition hover:bg-zinc-100 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    {isConfigLoading ? (
                      <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden="true" />
                    ) : (
                      <RefreshCw className="h-3.5 w-3.5" aria-hidden="true" />
                    )}
                    重新加载
                  </button>
                  <button
                    type="button"
                    onClick={() => void saveRuntimeConfig()}
                    disabled={isConfigLoading || isConfigSaving}
                    className="inline-flex h-8 items-center gap-1 rounded border border-zinc-900 bg-zinc-900 px-2 text-xs font-medium text-white transition hover:bg-zinc-800 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    {isConfigSaving ? <Loader2 className="h-3.5 w-3.5 animate-spin" aria-hidden="true" /> : null}
                    保存配置
                  </button>
                </div>
              </div>

              {configNote ? <p className="mb-3 text-xs text-zinc-500">{configNote}</p> : null}

              <label className="inline-flex items-center gap-2 text-xs text-zinc-600">
                <input
                  type="checkbox"
                  checked={runtimeConfig.connectors_demo_fallback}
                  onChange={(event) => updateRuntimeField("connectors_demo_fallback", event.target.checked)}
                />
                启用 demo fallback（真实集群联调建议关闭）
              </label>
            </article>

            <div className="grid gap-4 lg:grid-cols-2">
              <article className="rounded-lg border border-zinc-200 bg-white p-4">
                <h4 className="mb-3 text-xs font-semibold uppercase tracking-wide text-zinc-500">Scheduler</h4>
                <div className="space-y-2.5">
                  <input
                    type="text"
                    value={runtimeConfig.scheduler_backend}
                    onChange={(event) => updateRuntimeField("scheduler_backend", event.target.value)}
                    placeholder="调度后端（如 airflow）"
                    className={SMALL_INPUT_CLASS_NAME}
                  />
                  <input
                    type="url"
                    value={runtimeConfig.scheduler_base_url}
                    onChange={(event) => updateRuntimeField("scheduler_base_url", event.target.value)}
                    placeholder="调度服务地址"
                    className={SMALL_INPUT_CLASS_NAME}
                  />
                  <input
                    type="text"
                    value={runtimeConfig.scheduler_username}
                    onChange={(event) => updateRuntimeField("scheduler_username", event.target.value)}
                    placeholder="调度用户名"
                    className={SMALL_INPUT_CLASS_NAME}
                  />
                  <input
                    type="password"
                    value={runtimeConfig.scheduler_password}
                    onChange={(event) => updateRuntimeField("scheduler_password", event.target.value)}
                    placeholder="调度密码 "
                    className={SMALL_INPUT_CLASS_NAME}
                  />
                  <input
                    type="password"
                    value={runtimeConfig.scheduler_api_token}
                    onChange={(event) => updateRuntimeField("scheduler_api_token", event.target.value)}
                    placeholder="调度 Token "
                    className={SMALL_INPUT_CLASS_NAME}
                  />
                  <label className="inline-flex items-center gap-2 text-xs text-zinc-600">
                    <input
                      type="checkbox"
                      checked={runtimeConfig.scheduler_verify_tls}
                      onChange={(event) => updateRuntimeField("scheduler_verify_tls", event.target.checked)}
                    />
                    校验 Scheduler TLS
                  </label>
                </div>
              </article>

              <article className="rounded-lg border border-zinc-200 bg-white p-4">
                <h4 className="mb-3 text-xs font-semibold uppercase tracking-wide text-zinc-500">Spark</h4>
                <div className="space-y-2.5">
                  <input
                    type="url"
                    value={runtimeConfig.spark_history_base_url}
                    onChange={(event) => updateRuntimeField("spark_history_base_url", event.target.value)}
                    placeholder="Spark History 地址"
                    className={SMALL_INPUT_CLASS_NAME}
                  />
                  <input
                    type="url"
                    value={runtimeConfig.spark_yarn_rm_base_url}
                    onChange={(event) => updateRuntimeField("spark_yarn_rm_base_url", event.target.value)}
                    placeholder="YARN ResourceManager 地址"
                    className={SMALL_INPUT_CLASS_NAME}
                  />
                  <input
                    type="url"
                    value={runtimeConfig.spark_control_base_url}
                    onChange={(event) => updateRuntimeField("spark_control_base_url", event.target.value)}
                    placeholder="Spark 控制 API 地址"
                    className={SMALL_INPUT_CLASS_NAME}
                  />
                  <input
                    type="password"
                    value={runtimeConfig.spark_api_token}
                    onChange={(event) => updateRuntimeField("spark_api_token", event.target.value)}
                    placeholder="Spark Token "
                    className={SMALL_INPUT_CLASS_NAME}
                  />
                  <label className="inline-flex items-center gap-2 text-xs text-zinc-600">
                    <input
                      type="checkbox"
                      checked={runtimeConfig.spark_verify_tls}
                      onChange={(event) => updateRuntimeField("spark_verify_tls", event.target.checked)}
                    />
                    校验 Spark TLS
                  </label>
                </div>
              </article>

              <article className="rounded-lg border border-zinc-200 bg-white p-4">
                <h4 className="mb-3 text-xs font-semibold uppercase tracking-wide text-zinc-500">Flink</h4>
                <div className="space-y-2.5">
                  <input
                    type="url"
                    value={runtimeConfig.flink_base_url}
                    onChange={(event) => updateRuntimeField("flink_base_url", event.target.value)}
                    placeholder="Flink REST 地址"
                    className={SMALL_INPUT_CLASS_NAME}
                  />
                  <input
                    type="password"
                    value={runtimeConfig.flink_api_token}
                    onChange={(event) => updateRuntimeField("flink_api_token", event.target.value)}
                    placeholder="Flink Token "
                    className={SMALL_INPUT_CLASS_NAME}
                  />
                  <label className="inline-flex items-center gap-2 text-xs text-zinc-600">
                    <input
                      type="checkbox"
                      checked={runtimeConfig.flink_verify_tls}
                      onChange={(event) => updateRuntimeField("flink_verify_tls", event.target.checked)}
                    />
                    校验 Flink TLS
                  </label>
                </div>
              </article>

              <article className="rounded-lg border border-zinc-200 bg-white p-4">
                <h4 className="mb-3 text-xs font-semibold uppercase tracking-wide text-zinc-500">LLM / Qdrant</h4>
                <div className="space-y-2.5">
                  <input
                    type="url"
                    value={runtimeConfig.llm_base_url}
                    onChange={(event) => updateRuntimeField("llm_base_url", event.target.value)}
                    placeholder="LLM API 地址"
                    className={SMALL_INPUT_CLASS_NAME}
                  />
                  <input
                    type="text"
                    value={runtimeConfig.llm_model}
                    onChange={(event) => updateRuntimeField("llm_model", event.target.value)}
                    placeholder="模型名称"
                    className={SMALL_INPUT_CLASS_NAME}
                  />
                  <input
                    type="password"
                    value={runtimeConfig.llm_api_key}
                    onChange={(event) => updateRuntimeField("llm_api_key", event.target.value)}
                    placeholder="LLM Key "
                    className={SMALL_INPUT_CLASS_NAME}
                  />
                  <input
                    type="url"
                    value={runtimeConfig.qdrant_url}
                    onChange={(event) => updateRuntimeField("qdrant_url", event.target.value)}
                    placeholder="Qdrant 地址"
                    className={SMALL_INPUT_CLASS_NAME}
                  />
                  <input
                    type="password"
                    value={runtimeConfig.qdrant_api_key}
                    onChange={(event) => updateRuntimeField("qdrant_api_key", event.target.value)}
                    placeholder="Qdrant Key "
                    className={SMALL_INPUT_CLASS_NAME}
                  />
                </div>
              </article>
            </div>
          </section>
        ) : null}
      </main>
    </div>
  );
}
