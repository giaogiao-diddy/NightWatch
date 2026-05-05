from importlib import import_module

__all__: list[str] = [
	"build_graph",
	"build_runtime_config",
	"list_checkpoint_states",
	"recover_state_by_conversation_id",
	"recover_state_by_incident_id",
	"resolve_thread_id",
	"run_minimal_loop",
	"ingest_alert",
	"diagnose_rca",
	"circuit_break",
	"notify_owners",
	"plan_heal",
	"retrieve_cases",
	"guardrail_check",
	"human_approval",
	"execute_heal",
	"verify_recovery",
]


def __getattr__(name: str):
	if name in {
		"build_graph",
		"build_runtime_config",
		"list_checkpoint_states",
		"recover_state_by_conversation_id",
		"recover_state_by_incident_id",
		"resolve_thread_id",
		"run_minimal_loop",
	}:
		module = import_module("nightwatch.agent.graph")
		return getattr(module, name)
	if name in {
		"ingest_alert",
		"diagnose_rca",
		"circuit_break",
		"notify_owners",
		"plan_heal",
		"retrieve_cases",
		"guardrail_check",
		"human_approval",
		"execute_heal",
		"verify_recovery",
	}:
		module = import_module("nightwatch.agent.nodes")
		return getattr(module, name)
	raise AttributeError(f"module 'nightwatch.agent' has no attribute {name!r}")