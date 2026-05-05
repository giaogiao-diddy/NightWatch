from importlib import import_module

__all__: list[str] = [
	"build_heal_plan",
	"execute_circuit_break",
	"generate_heal_plan_with_llm",
	"notify_accountability_owners",
	"evaluate_guardrails",
	"evaluate_human_approval",
	"execute_controlled_retry",
	"verify_recovery",
]


def __getattr__(name: str):
	if name == "build_heal_plan":
		module = import_module("nightwatch.healing.planner")
		return getattr(module, name)
	if name == "generate_heal_plan_with_llm":
		module = import_module("nightwatch.healing.planner")
		return getattr(module, name)
	if name == "execute_circuit_break":
		module = import_module("nightwatch.healing.circuit_breaker")
		return getattr(module, name)
	if name == "notify_accountability_owners":
		module = import_module("nightwatch.healing.notifier")
		return getattr(module, name)
	if name in {"evaluate_guardrails", "evaluate_human_approval"}:
		module = import_module("nightwatch.healing.guardrails")
		return getattr(module, name)
	if name == "execute_controlled_retry":
		module = import_module("nightwatch.healing.executor")
		return getattr(module, name)
	if name == "verify_recovery":
		module = import_module("nightwatch.healing.verifier")
		return getattr(module, name)
	raise AttributeError(f"module 'nightwatch.healing' has no attribute {name!r}")