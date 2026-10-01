package ledger

// Consumed execution manifests bind exact globally keyed record revisions.
// Strategy IDs carry an explicit namespace; only the owning mission/goal forms
// introduce links. Agent start input_event_refs are shape-only during replay
// and deliberately do not define additional incoming relationships.
var incidentManifestRecordLinks = []incidentLinkRule{
	{target: "task", payload: true, eventTypes: "EXECUTION_CONTEXT_MANIFESTED", field: "task_id"},
	{target: "agent", payload: true, eventTypes: "EXECUTION_CONTEXT_MANIFESTED", field: "agent_id"},
	{target: "knowledge", payload: true, eventTypes: "EXECUTION_CONTEXT_MANIFESTED", field: "knowledge_refs", array: true, element: "id"},
	{target: "task", payload: true, eventTypes: "EXECUTION_CONTEXT_MANIFESTED", field: "coordination_refs", array: true, element: "id"},
	{target: "mission", payload: true, eventTypes: "EXECUTION_CONTEXT_MANIFESTED", field: "additional_context_refs", array: true, element: "id", prefix: "mission/"},
	{target: "goal", payload: true, eventTypes: "EXECUTION_CONTEXT_MANIFESTED", field: "additional_context_refs", array: true, element: "id", prefix: "goal/"},
	{target: "mission", sources: "task", detail: true, eventTypes: "EXECUTION_STARTED", field: "strategic_context_refs", array: true, element: "id", prefix: "mission/"},
	{target: "goal", sources: "task", detail: true, eventTypes: "EXECUTION_STARTED", field: "strategic_context_refs", array: true, element: "id", prefix: "goal/"},
}
