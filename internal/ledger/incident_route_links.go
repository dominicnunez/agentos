package ledger

// Routing diagnostics bind a global origin event before their owner verifies
// its organization, correlation, purpose, and causal order.
var incidentRouteLinkRules = []incidentLinkRule{
	{target: "event", payload: true, eventTypes: "INFERENCE_ROUTE_REJECTED", field: "origin_event_ref"},
}
