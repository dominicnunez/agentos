package ledger

// The task execution manifest binds these globally identified source events
// to the exact runtime-selected context. Auxiliary model input_event_refs only
// receive a shape check and do not define incoming evidence links.
var incidentContextLinkRules = []incidentLinkRule{
	{target: "event", payload: true, eventTypes: "EXECUTION_CONTEXT_MANIFESTED", field: "event_refs", array: true},
	{target: "event", payload: true, eventTypes: "INFERENCE_RESERVED", field: "execution_manifest_ref"},
}
