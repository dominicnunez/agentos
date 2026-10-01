package ledger

// Review owners compare the payload Task against the globally identified Task
// separately from the event envelope. An incoming review can therefore claim
// a selected Task even when its envelope belongs to another Task.
var incidentReviewIdentityRules = []incidentLinkRule{
	{target: "task", payload: true, eventTypes: "COMPLETION_REVIEW_REQUESTED,COMPLETION_REVIEW_DECIDED", field: "task_id"},
}
