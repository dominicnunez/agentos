package ledger

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"strings"

	"modernc.org/sqlite"
)

// incidentLinkRule is one typed reference carried by a projection. The same
// registry drives extraction and guards for the durable incoming-link index.
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

// Pure functions keep trigger programs small: expanding every relationship into
// each maintenance and guard trigger makes even ordinary event inserts expensive
// to compile. Neither function reads a connection or retains source state.
func init() {
	sqlite.MustRegisterDeterministicScalarFunction("agentos_incident_links_v1", 3, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
		links := []incidentSelector{}
		seen := map[incidentKey]bool{}
		visitIncidentLinks(args, "", func(kind, id string) bool {
			key := incidentKey{kind, id}
			if !seen[key] {
				seen[key] = true
				links = append(links, incidentSelector{kind, id})
			}
			return false
		})
		encoded, err := json.Marshal(links)
		return string(encoded), err
	})
	sqlite.MustRegisterDeterministicScalarFunction("agentos_incident_link_match_v1", 5, func(_ *sqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
		kind, kindOK := args[3].(string)
		id, idOK := args[4].(string)
		if !kindOK || !idOK || kind == "" || id == "" {
			return int64(0), nil
		}
		found := visitIncidentLinks(args[:3], kind, func(candidateKind, candidateID string) bool { return candidateKind == kind && candidateID == id })
		if found {
			return int64(1), nil
		}
		return int64(0), nil
	})
}

func incidentLinkArguments(record bool, source string) string {
	if source != "" {
		source += "."
	}
	if record {
		return "1," + source + "kind," + source + "body"
	}
	return "0,NULL," + source + "payload"
}

// incidentLinkSelect returns distinct links for one source row. Event projection
// identity remains a link even when its materialized record is missing.
func incidentLinkSelect(record bool, source string) string {
	return "SELECT json_extract(value,'$.kind') AS target_kind,json_extract(value,'$.id') AS target_id FROM json_each(" + incidentLinkJSON(record, source) + ")"
}

func incidentLinkJSON(record bool, source string) string {
	return "agentos_incident_links_v1(" + incidentLinkArguments(record, source) + ")"
}

// All arguments are internal SQL expressions, not caller supplied identifiers.
func incidentLinkMatch(record bool, source, targetKind, targetID string) string {
	return "agentos_incident_link_match_v1(" + incidentLinkArguments(record, source) + "," + targetKind + "," + targetID + ")"
}

// Invalid documents and fields derive no links, so maintenance cannot reject a
// malformed raw source write. Exact admission validation still rejects selected
// malformed evidence. A source-sized parse is required; there is no cross-read
// cache or truncation that could hide a derivable incoming reference.
func visitIncidentLinks(args []driver.Value, target string, visit func(string, string) bool) bool {
	var body []byte
	switch value := args[2].(type) {
	case string:
		body = []byte(value)
	case []byte:
		body = value
	default:
		return false
	}
	if !json.Valid(body) {
		return false
	}
	projection := incidentLinkObject(body)
	if projection == nil {
		return false
	}
	kind, _ := args[1].(string)
	if args[0] == int64(0) {
		projection = incidentLinkObject(projection["projection"])
		kind = incidentLinkString(projection["projection_kind"])
		id := incidentLinkString(projection["record_id"])
		if kind != "" && id != "" && (target == "" || target == kind) && visit(kind, id) {
			return true
		}
	}
	values := incidentLinkObject(projection["value"])
	for _, rule := range incidentLinkRules {
		if target != "" && rule.target != target {
			continue
		}
		if !strings.Contains(","+rule.sources+",", ","+kind+",") {
			continue
		}
		if rule.discriminator != "" && incidentLinkString(values[rule.discriminator]) != rule.equals {
			continue
		}
		fields := strings.Split(rule.field, ".")
		value := values[fields[0]]
		for _, field := range fields[1:] {
			value = incidentLinkObject(value)[field]
		}
		if !rule.array {
			if id := incidentLinkString(value); id != "" && visit(rule.target, id) {
				return true
			}
			continue
		}
		decoder := json.NewDecoder(bytes.NewReader(value))
		opening, err := decoder.Token()
		if err != nil || opening != json.Delim('[') {
			continue
		}
		for decoder.More() {
			var item json.RawMessage
			if decoder.Decode(&item) != nil {
				break
			}
			if rule.element != "" {
				item = incidentLinkObject(item)[rule.element]
			}
			if id := incidentLinkString(item); id != "" && visit(rule.target, id) {
				return true
			}
		}
	}
	return false
}

// SQLite json_extract uses the first occurrence of a duplicate object member.
// Preserve that discovery behavior; selected duplicates fail exact validation.
func incidentLinkObject(raw []byte) map[string]json.RawMessage {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return nil
	}
	fields := map[string]json.RawMessage{}
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return nil
		}
		var value json.RawMessage
		if decoder.Decode(&value) != nil {
			return nil
		}
		name, ok := key.(string)
		if !ok {
			return nil
		}
		if _, seen := fields[name]; !seen {
			fields[name] = value
		}
	}
	return fields
}

func incidentLinkString(raw []byte) string {
	var value string
	if json.Unmarshal(raw, &value) != nil {
		return ""
	}
	return value
}
