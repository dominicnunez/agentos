package ledger

import "strings"

// incidentLinkRule is one typed reference carried by a projection. The same
// registry drives frontier predicates and the durable incoming-link index.
// Ownership and arbitrary note fields deliberately do not define references.
type incidentLinkRule struct {
	target, sources, field string
	array                  bool
	element                string
	discriminator, equals  string
}

var incidentLinkRules = []incidentLinkRule{
	{target: "mission", sources: "goal", field: "mission_id"},
	{target: "goal", sources: "work,intent", field: "goal_id"},
	{target: "intent", sources: "work", field: "intent_id"},
	{target: "work", sources: "task,lab_experiment", field: "work_id"},
	{target: "work", sources: "work,intent", field: "replaces_work_id"},
	{target: "task", sources: "task", field: "parent_id"},
	{target: "task", sources: "task", field: "depends_on", array: true},
	{target: "lab_experiment", sources: "lab_promotion_candidate", field: "experiment_id"},
	{target: "knowledge", sources: "knowledge", field: "derived_knowledge_refs", array: true, element: "id"},
	{target: "agent", sources: "task", field: "assignee_id", discriminator: "assignee_type", equals: "AGENT"},
	{target: "agent", sources: "team", field: "member_agent_ids", array: true},
	{target: "agent", sources: "knowledge", field: "scope_id", discriminator: "scope", equals: "AGENT"},
	{target: "agent", sources: "knowledge", field: "created_by", discriminator: "created_by_kind", equals: "AGENT"},
	{target: "team", sources: "task", field: "assignee_id", discriminator: "assignee_type", equals: "TEAM"},
	{target: "team", sources: "knowledge", field: "scope_id", discriminator: "scope", equals: "TEAM"},
	{target: "agent_blueprint", sources: "agent", field: "blueprint_id"},
	{target: "agent_blueprint", sources: "task", field: "agent_config.blueprint_id"},
	{target: "execution_profile", sources: "agent", field: "execution_profile_id"},
	{target: "execution_profile", sources: "task", field: "agent_config.profile_id"},
}

// Invalid raw JSON must not make maintenance triggers reject the source write.
// Semantic validation still owns rejection when that source is selected.
func incidentLinkSource(record bool, source string) (body, prefix, kind string) {
	if source != "" {
		source += "."
	}
	body, prefix = source+"payload", "$.projection."
	if record {
		body, prefix = source+"body", "$."
	}
	body = "(CASE WHEN json_valid(" + body + ") THEN " + body + " ELSE '{}' END)"
	kind = "json_extract(" + body + ",'" + prefix + "projection_kind')"
	if record {
		kind = source + "kind"
	}
	return
}

func (rule incidentLinkRule) selection(record bool, source string) string {
	body, prefix, kind := incidentLinkSource(record, source)
	path := "'" + prefix + "value." + rule.field + "'"
	id := "json_extract(" + body + "," + path + ")"
	valid := "json_type(" + body + "," + path + ")='text'"
	from := ""
	if rule.array {
		// json_each accepts scalar values too; only declared arrays are references.
		from = " FROM json_each(CASE WHEN json_type(" + body + "," + path + ")='array' THEN json_extract(" + body + "," + path + ") ELSE '[]' END) AS link"
		id, valid = "link.value", "link.type='text'"
		if rule.element != "" {
			element := "(CASE WHEN link.type='object' THEN link.value ELSE '{}' END)"
			id = "json_extract(" + element + ",'$." + rule.element + "')"
			valid = "json_type(" + element + ",'$." + rule.element + "')='text'"
		}
	}
	condition := kind + " IN ('" + strings.ReplaceAll(rule.sources, ",", "','") + "') AND " + valid + " AND " + id + "<>''"
	if rule.discriminator != "" {
		condition += " AND json_extract(" + body + ",'" + prefix + "value." + rule.discriminator + "')='" + rule.equals + "'"
	}
	return "SELECT '" + rule.target + "' AS target_kind, " + id + " AS target_id" + from + " WHERE " + condition
}

// incidentLinkSelect returns links for one source row (NEW/OLD in triggers or
// a caller's row alias). Event identity is itself a link so incoming projections
// remain discoverable when their materialized record is absent.
func incidentLinkSelect(record bool, source string) string {
	parts := make([]string, 0, len(incidentLinkRules)+1)
	for _, rule := range incidentLinkRules {
		parts = append(parts, rule.selection(record, source))
	}
	if !record {
		parts = append(parts, incidentEventIdentitySelect(source))
	}
	return "SELECT DISTINCT target_kind,target_id FROM (" + strings.Join(parts, " UNION ALL ") + ")"
}

func incidentEventIdentitySelect(source string) string {
	body, prefix, kind := incidentLinkSource(false, source)
	id := "json_extract(" + body + ",'" + prefix + "record_id')"
	return "SELECT " + kind + " AS target_kind, " + id + " AS target_id WHERE json_type(" + body + ",'" + prefix + "projection_kind')='text' AND " + kind + "<>'' AND json_type(" + body + ",'" + prefix + "record_id')='text' AND " + id + "<>''"
}

// incidentLinkMatch verifies a single claimed link without materializing the
// complete distinct outgoing set. All arguments are internal SQL expressions.
func incidentLinkMatch(record bool, source, targetKind, targetID string) string {
	parts := make([]string, 0, len(incidentLinkRules)+1)
	for _, rule := range incidentLinkRules {
		parts = append(parts, "("+targetKind+"='"+rule.target+"' AND EXISTS (SELECT 1 FROM ("+rule.selection(record, source)+") candidate WHERE candidate.target_id="+targetID+"))")
	}
	if !record {
		parts = append(parts, "EXISTS (SELECT 1 FROM ("+incidentEventIdentitySelect(source)+") candidate WHERE candidate.target_kind="+targetKind+" AND candidate.target_id="+targetID+")")
	}
	return "(" + strings.Join(parts, " OR ") + ")"
}
