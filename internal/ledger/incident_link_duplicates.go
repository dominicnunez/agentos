package ledger

import (
	"bytes"
	"database/sql/driver"
	"encoding/json"
	"strings"

	"github.com/dominicnunez/agentos/internal/events"
)

// The current grammar retains every occurrence along a contract-owned path.
// Ambiguous documents remain candidates for exact validation; a duplicate
// discriminator cannot conceal a reference by preceding it with another value.
// The historical v1 visitor keeps its first-member rule. This v2 entry point
// also preserves its projection-dependent detail discovery for v14 verification.
func visitAllIncidentLinks(args []driver.Value, target string, rules []incidentLinkRule, visit func(string, string) bool) bool {
	return visitIncidentLinkSources(args, target, rules, false, "", visit)
}

func visitIncidentLifecycleLinks(args []driver.Value, target string, rules []incidentLinkRule, visit func(string, string) bool) bool {
	return visitIncidentLinkSources(args, target, rules, true, "", visit)
}

func visitIncidentLinkSources(args []driver.Value, target string, rules []incidentLinkRule, lifecycle bool, counterpart string, visit func(string, string) bool) bool {
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
	root := incidentLinkMembers(body)
	if root == nil {
		return false
	}
	kind, _ := args[1].(string)
	eventType := ""
	type source struct {
		kind   string
		values []map[string][]json.RawMessage
	}
	var sources []source
	if args[0] == int64(0) {
		eventType = kind
		if counterpart != "" {
			sources = append(sources, source{counterpart, nil})
		}
		for _, raw := range root["projection"] {
			projection := incidentLinkMembers(raw)
			values := incidentLinkObjectsAll(projection["value"])
			ids := incidentLinkStrings(projection["record_id"])
			kinds := incidentLinkStrings(projection["projection_kind"])
			if counterpart != "" {
				kinds = append(kinds, counterpart)
			}
			if lifecycle && projection != nil {
				for _, kind := range incidentLifecycleKinds[eventType] {
					present := false
					for _, existing := range kinds {
						present = present || existing == kind
					}
					if !present {
						kinds = append(kinds, kind)
					}
				}
			}
			for _, kind := range kinds {
				// Unsupported kinds cannot be admitted or selected typed projections.
				// The closed owner contract bounds ambiguous kind/ID combinations without
				// truncating any identity belonging to a supported kind.
				if !events.ProjectionKindRequiresAdmission(kind) {
					continue
				}
				for _, id := range ids {
					if id != "" && (target == "" || target == kind) && visit(kind, id) {
						return true
					}
				}
				sources = append(sources, source{kind, values})
			}
		}
	} else {
		sources = append(sources, source{kind, incidentLinkObjectsAll(root["value"])})
		if lifecycle && args[0] == int64(2) {
			for _, claimed := range incidentLinkStrings(root["projection_kind"]) {
				if claimed != kind && events.ProjectionKindRequiresAdmission(claimed) {
					sources = append(sources, source{claimed, incidentLinkObjectsAll(root["value"])})
				}
			}
		}
	}
	details := incidentLinkObjectsAll(root["detail"])
	for _, rule := range rules {
		if target != "" && rule.target != target {
			continue
		}
		if rule.eventTypes != "" && !incidentLinkSource(rule.eventTypes, eventType) {
			continue
		}
		var values []map[string][]json.RawMessage
		if rule.payload {
			values = []map[string][]json.RawMessage{root}
		} else if lifecycle && rule.detail && rule.eventTypes != "" && eventType != "" {
			// The reserved event label owns this detail contract. A damaged
			// projection must not conceal its claim from exact admission validation.
			values = details
		} else {
			for _, source := range sources {
				if !incidentLinkSource(rule.sources, source.kind) {
					continue
				}
				if rule.detail {
					values = details
					break
				} else {
					values = append(values, source.values...)
				}
			}
		}
		for _, fields := range values {
			if rule.discriminator != "" {
				matches := false
				for _, value := range fields[rule.discriminator] {
					if incidentLinkString(value) == rule.equals {
						matches = true
						break
					}
				}
				if !matches {
					continue
				}
			}
			if rule.requires != "" {
				if _, present := fields[rule.requires]; !present {
					continue
				}
			}
			for _, value := range incidentLinkObjectPath(fields, rule.field) {
				if rule.array {
					if visitAllIncidentLinkArray(value, rule, visit) {
						return true
					}
					continue
				}
				if id := incidentLinkID(value, rule.prefix); id != "" && visit(rule.target, id) {
					return true
				}
			}
		}
	}
	return false
}

// Lifecycle labels are an independent owner channel for each retained
// projection object. Kind and value corruption are validated after discovery.
var incidentLifecycleKinds = func() map[string][]string {
	owners := map[string][]string{}
	for _, kind := range projectionScopeKinds {
		for _, label := range events.ProjectionLifecycleEventTypes(kind) {
			owners[label] = append(owners[label], kind)
		}
	}
	return owners
}()

func incidentLinkSource(sources, kind string) bool {
	return kind != "" && strings.Contains(","+sources+",", ","+kind+",")
}

// Traverse named object members only. Each duplicate intermediate object and
// terminal member contributes independently; opaque nested note fields do not.
func incidentLinkObjectPath(fields map[string][]json.RawMessage, path string) []json.RawMessage {
	names := strings.Split(path, ".")
	values := fields[names[0]]
	for _, field := range names[1:] {
		var next []json.RawMessage
		for _, value := range values {
			next = append(next, incidentLinkMembers(value)[field]...)
		}
		values = next
	}
	return values
}

func incidentLinkObjectsAll(values []json.RawMessage) []map[string][]json.RawMessage {
	var objects []map[string][]json.RawMessage
	for _, value := range values {
		if fields := incidentLinkMembers(value); fields != nil {
			objects = append(objects, fields)
		}
	}
	return objects
}
func visitAllIncidentLinkArray(value json.RawMessage, rule incidentLinkRule, visit func(string, string) bool) bool {
	decoder := json.NewDecoder(bytes.NewReader(value))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('[') {
		return false
	}
	for decoder.More() {
		var item json.RawMessage
		if decoder.Decode(&item) != nil {
			return false
		}
		items := []json.RawMessage{item}
		if rule.element != "" {
			items = incidentLinkObjectPath(incidentLinkMembers(item), rule.element)
		}
		for _, item := range items {
			if rule.elementArray {
				if visitAllIncidentLinkArray(item, incidentLinkRule{target: rule.target, prefix: rule.prefix}, visit) {
					return true
				}
				continue
			}
			if id := incidentLinkID(item, rule.prefix); id != "" && visit(rule.target, id) {
				return true
			}
		}
	}
	return false
}

func incidentLinkMembers(raw []byte) map[string][]json.RawMessage {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return nil
	}
	fields := map[string][]json.RawMessage{}
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
		fields[name] = append(fields[name], value)
	}
	return fields
}

func incidentLinkStrings(values []json.RawMessage) []string {
	var result []string
	seen := map[string]bool{}
	for _, value := range values {
		id := incidentLinkString(value)
		if id != "" && !seen[id] {
			seen[id] = true
			result = append(result, id)
		}
	}
	return result
}
