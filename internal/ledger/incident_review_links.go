package ledger

// Completion review binds its request and decision to exact global evidence
// events. A candidate also binds the exact published result, even when a
// foreign envelope falsely claims another Task or organization.
var incidentReviewLinkRules = []incidentLinkRule{
	{target: "event", payload: true, eventTypes: "COMPLETION_REVIEW_REQUESTED", field: "evidence_refs", array: true},
	{target: "event", payload: true, eventTypes: "COMPLETION_REVIEW_DECIDED", field: "evidence_refs", array: true},
	{target: "event", payload: true, eventTypes: "CANDIDATE_COMPLETE", field: "result_event_id"},
}
