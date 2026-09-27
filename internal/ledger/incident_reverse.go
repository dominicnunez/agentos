package ledger

import "strings"

// Use the same typed relationships for event and record discovery. Either side
// may be changed independently, so neither can substitute for the other.
// Organization ownership defines the scope; it does not make all projections
// in that organization dependencies of every incident.
func incidentReverseProjection(key incidentKey, record bool) (string, []any) {
	body, prefix := "payload", "$.projection."
	kind := "json_extract(payload,'$.projection.projection_kind')"
	if record {
		body, prefix, kind = "r.body", "$.", "r.kind"
	}
	field := func(name string) string {
		return "json_extract(" + body + ",'" + prefix + "value." + name + "')"
	}
	match := func(kinds, name string) string {
		return "(" + kind + " IN (" + kinds + ") AND " + field(name) + "=?)"
	}
	var predicate string
	switch key.kind {
	case "mission":
		predicate = match("'goal'", "mission_id")
	case "goal":
		predicate = match("'work','intent'", "goal_id")
	case "intent":
		predicate = match("'work'", "intent_id")
	case "work":
		predicate = match("'task','lab_experiment'", "work_id") + " OR " + match("'work','intent'", "replaces_work_id")
	case "task":
		predicate = "(" + kind + "='task' AND (" + field("parent_id") + "=? OR EXISTS (SELECT 1 FROM json_each(" + body + ",'" + prefix + "value.depends_on') WHERE value=?)))"
	case "lab_experiment":
		predicate = match("'lab_promotion_candidate'", "experiment_id")
	case "knowledge":
		predicate = "(" + kind + "='knowledge' AND EXISTS (SELECT 1 FROM json_each(" + body + ",'" + prefix + "value.derived_knowledge_refs') WHERE json_extract(value,'$.id')=?))"
	case "agent":
		predicate = "(" + match("'task'", "assignee_id") + " AND " + field("assignee_type") + "='AGENT') OR " +
			"(" + kind + "='team' AND EXISTS (SELECT 1 FROM json_each(" + body + ",'" + prefix + "value.member_agent_ids') WHERE value=?)) OR " +
			"(" + match("'knowledge'", "scope_id") + " AND " + field("scope") + "='AGENT') OR " +
			"(" + match("'knowledge'", "created_by") + " AND " + field("created_by_kind") + "='AGENT')"
	case "team":
		predicate = "(" + match("'task'", "assignee_id") + " AND " + field("assignee_type") + "='TEAM') OR " +
			"(" + match("'knowledge'", "scope_id") + " AND " + field("scope") + "='TEAM')"
	case "agent_blueprint":
		predicate = match("'agent'", "blueprint_id") + " OR " + match("'task'", "agent_config.blueprint_id")
	case "execution_profile":
		predicate = match("'agent'", "execution_profile_id") + " OR " + match("'task'", "agent_config.profile_id")
	}
	args := make([]any, strings.Count(predicate, "?"))
	for i := range args {
		args[i] = key.id
	}
	return predicate, args
}
