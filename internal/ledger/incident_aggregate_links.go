package ledger

// Goal progress consumes global Work evidence identities, including each
// criterion's exact coverage. Work evaluations bind exact verification and
// completion events before their owner checks tenant, Task, and causal order.
// Artifact references and criterion text do not introduce event dependencies.
var incidentAggregateLinkRules = []incidentLinkRule{
	{target: "event", payload: true, eventTypes: "GOAL_PROGRESS_EVALUATED", field: "work_evidence_refs", array: true},
	{target: "event", payload: true, eventTypes: "GOAL_PROGRESS_EVALUATED", field: "criteria", array: true, element: "work_evidence_refs", elementArray: true},
	{target: "event", payload: true, eventTypes: "WORK_COMPLETION_EVALUATED", field: "tasks", array: true, element: "verification_event_ref"},
	{target: "event", payload: true, eventTypes: "WORK_COMPLETION_EVALUATED", field: "tasks", array: true, element: "completion_event_ref"},
}
