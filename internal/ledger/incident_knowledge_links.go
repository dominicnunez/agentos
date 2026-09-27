package ledger

// A judgment names its exact capability decision before the Knowledge owner
// checks the principal, organization, candidate revision, and causal order.
var incidentKnowledgeLinkRules = []incidentLinkRule{
	{target: "event", payload: true, eventTypes: "HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED,A2A_KNOWLEDGE_JUDGMENT_RECEIVED,KNOWLEDGE_JUDGMENT_PUBLISHED", field: "capability_check_event_id"},
}
