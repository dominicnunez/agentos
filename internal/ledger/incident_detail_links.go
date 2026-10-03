package ledger

// Transition details live in the event payload, not the materialized record.
// Only admissions whose owners consume these fields introduce incoming links.
// Forbidden references on these transitions also require selection so their
// validators can reject them. Other transitions and ordinary notes do not expand
// incident scope merely by spelling one of these fields.
var incidentDetailLinkRules = []incidentLinkRule{
	{target: "event", sources: "work", detail: true, eventTypes: "WORK_COMPLETED", field: "evidence_event_ref"},
	{target: "event", sources: "goal", detail: true, eventTypes: "GOAL_ACHIEVED", field: "evidence_event_ref"},
	{target: "event", sources: "task", detail: true, eventTypes: "TASK_VERIFIED_COMPLETE", field: "outcome_event_ref"},
	{target: "event", sources: "task", detail: true, eventTypes: "TASK_VERIFIED_COMPLETE", field: "submission_event_ref"},
	{target: "event", sources: "task", detail: true, eventTypes: "TASK_VERIFIED_COMPLETE", field: "judgment_ref"},
	{target: "event", sources: "task", detail: true, eventTypes: "EXECUTION_STARTED", field: "input_event_ref"},
	{target: "event", sources: "task", detail: true, eventTypes: "EXECUTION_STARTED", field: "strategic_event_refs", array: true},
	{target: "event", sources: "task", detail: true, eventTypes: "EXECUTION_STARTED", field: "dispatch_binding.agent_event_ref"},
	{target: "event", sources: "task", detail: true, eventTypes: "EXECUTION_STARTED", field: "dispatch_binding.blueprint_event_ref"},
	{target: "event", sources: "task", detail: true, eventTypes: "EXECUTION_STARTED", field: "dispatch_binding.execution_profile_event_ref"},
	{target: "event", sources: "task", detail: true, eventTypes: "TASK_EXECUTION_SUSPENDED", field: "stop_request_ref"},
	// The legacy suspension contract does not consume execution_start_ref.
	{target: "event", sources: "task", detail: true, eventTypes: "TASK_EXECUTION_SUSPENDED", field: "execution_start_ref", requires: "stop_request_ref"},
}
