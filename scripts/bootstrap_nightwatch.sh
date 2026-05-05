#!/usr/bin/env bash
set -euo pipefail

mkdir -p \
  nightwatch/{models,agent,ingest,retrieval,diagnostics,healing,lineage,connectors,mcp,api,ui} \
  tests/{unit,integration,fixtures} \
  scripts

touch \
  nightwatch/__init__.py \
  nightwatch/main.py \
  nightwatch/worker.py \
  nightwatch/config.py \
  nightwatch/models/__init__.py \
  nightwatch/models/incident.py \
  nightwatch/models/action.py \
  nightwatch/models/lineage.py \
  nightwatch/models/api.py \
  nightwatch/models/state.py \
  nightwatch/agent/__init__.py \
  nightwatch/agent/graph.py \
  nightwatch/agent/nodes.py \
  nightwatch/agent/policies.py \
  nightwatch/agent/prompts.py \
  nightwatch/ingest/__init__.py \
  nightwatch/ingest/normalizer.py \
  nightwatch/ingest/correlator.py \
  nightwatch/ingest/signatures.py \
  nightwatch/retrieval/__init__.py \
  nightwatch/retrieval/slicer.py \
  nightwatch/retrieval/embedder.py \
  nightwatch/retrieval/indexer.py \
  nightwatch/retrieval/retriever.py \
  nightwatch/diagnostics/__init__.py \
  nightwatch/diagnostics/spark_rca.py \
  nightwatch/diagnostics/flink_rca.py \
  nightwatch/diagnostics/schema_drift.py \
  nightwatch/diagnostics/scorer.py \
  nightwatch/healing/__init__.py \
  nightwatch/healing/planner.py \
  nightwatch/healing/guardrails.py \
  nightwatch/healing/executor.py \
  nightwatch/healing/verifier.py \
  nightwatch/lineage/__init__.py \
  nightwatch/lineage/resolver.py \
  nightwatch/lineage/blast_radius.py \
  nightwatch/lineage/ownership.py \
  nightwatch/connectors/__init__.py \
  nightwatch/connectors/base.py \
  nightwatch/connectors/yarn.py \
  nightwatch/connectors/flink.py \
  nightwatch/connectors/scheduler.py \
  nightwatch/connectors/catalog.py \
  nightwatch/connectors/paimon.py \
  nightwatch/connectors/starrocks.py \
  nightwatch/connectors/github.py \
  nightwatch/connectors/jira.py \
  nightwatch/mcp/__init__.py \
  nightwatch/mcp/server.py \
  nightwatch/mcp/tools.py \
  nightwatch/mcp/registry.py \
  nightwatch/api/__init__.py \
  nightwatch/api/incidents.py \
  nightwatch/api/actions.py \
  nightwatch/api/knowledge.py \
  nightwatch/api/health.py \
  nightwatch/api/deps.py \
  nightwatch/ui/__init__.py \
  nightwatch/ui/app.py \
  tests/conftest.py \
  tests/unit/test_state.py \
  tests/integration/test_minimal_loop.py \
  tests/fixtures/spark_oom_logs.txt \
  tests/fixtures/flink_cdc_drift.json \
  tests/fixtures/lineage_graph.json \
  tests/fixtures/incident_cases.json

printf 'NightWatch skeleton created.\n'