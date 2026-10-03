package ledger

// Stop validators resolve these references as global event identities before
// checking tenant and execution bindings. Execution IDs remain tenant-scoped.
// References forbidden on uncertain results must still select the malformed
// result so its owner can reject it; legacy informational references are omitted.
var incidentStopLinkRules = []incidentLinkRule{
	{target: "event", payload: true, eventTypes: "EXECUTION_STOP_REQUESTED", field: "execution_start_ref"},
	{target: "event", payload: true, eventTypes: "EXECUTION_STOP_UNCERTAIN,EXECUTION_STOP_CONFIRMED", field: "stop_request_ref"},
	{target: "event", payload: true, eventTypes: "EXECUTION_STOP_UNCERTAIN,EXECUTION_STOP_CONFIRMED", field: "outcome_event_ref"},
	{target: "event", payload: true, eventTypes: "EXECUTION_STOP_UNCERTAIN,EXECUTION_STOP_CONFIRMED", field: "usage_event_ref"},
	{target: "event", payload: true, eventTypes: "EXECUTION_STOP_UNCERTAIN,EXECUTION_STOP_CONFIRMED", field: "finish_event_ref"},
	{target: "event", payload: true, eventTypes: "MODEL_STOP_REQUESTED", field: "context_event_ref"},
	{target: "event", payload: true, eventTypes: "MODEL_STOP_UNCERTAIN,MODEL_STOP_CONFIRMED", field: "stop_request_ref"},
	{target: "event", payload: true, eventTypes: "MODEL_STOP_UNCERTAIN,MODEL_STOP_CONFIRMED", field: "usage_event_ref"},
	{target: "event", payload: true, eventTypes: "PLANNING_FAILED", field: "evidence_event_ref"},
	{target: "event", payload: true, eventTypes: "TOOL_OUTCOME_RECORDED", field: "observed_effect.stop_request_ref", discriminator: "tool_id", equals: "runtime-containment"},
}
