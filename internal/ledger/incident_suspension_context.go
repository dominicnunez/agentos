package ledger

// A legacy planning suspension without a model-stop request does not bind its
// context_event_ref. Once an earlier request opens the same model execution,
// the stop validator requires the suspension to name that request's context.
// Discover foreign claims only in that applicable state.
func incidentForeignPlanningSuspension(membership string) string {
	return `events.organization_id<>? AND events.event_type='PLANNING_CONTAINMENT_SUSPENDED' AND
EXISTS (SELECT 1 FROM events AS request WHERE request.event_type='MODEL_STOP_REQUESTED' AND
request.organization_id=events.organization_id AND request.task_id=events.task_id AND
request.correlation_id=events.correlation_id AND request.source_execution_id=events.source_execution_id AND
request.sequence<events.sequence) AND
EXISTS (SELECT 1 FROM json_each(CASE WHEN json_valid(events.payload) THEN events.payload ELSE '{}' END)
WHERE key='context_event_ref' AND value IN (` + membership + `))`
}
