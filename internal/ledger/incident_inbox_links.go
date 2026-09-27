package ledger

// Atomic inbox observations bind globally identified input and execution-start
// events. Their owner then checks backing rows, tenant, recipient, and time;
// a foreign observation cannot hide an explicit reference to selected evidence.
var incidentInboxLinkRules = []incidentLinkRule{
	{target: "event", payload: true, eventTypes: "INBOX_EVENTS_OBSERVED", field: "event_ids", array: true},
	{target: "event", payload: true, eventTypes: "INBOX_EVENTS_OBSERVED", field: "execution_start_event_ref"},
}
