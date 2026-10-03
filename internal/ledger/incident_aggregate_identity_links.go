package ledger

// Runtime aggregate evidence claims exact globally keyed Work, Intent, Task,
// and Mission identities before the completion owners compare tenant, state,
// and causal history. Goal IDs already have an independent reverse selector.
var incidentAggregateIdentityRules = []incidentLinkRule{
	{target: "work", payload: true, eventTypes: "WORK_COMPLETION_EVALUATED", field: "work_id"},
	{target: "intent", payload: true, eventTypes: "WORK_COMPLETION_EVALUATED", field: "intent_id"},
	{target: "task", payload: true, eventTypes: "WORK_COMPLETION_EVALUATED", field: "tasks", array: true, element: "task_id"},
	{target: "mission", payload: true, eventTypes: "GOAL_PROGRESS_EVALUATED", field: "mission_id"},
}
