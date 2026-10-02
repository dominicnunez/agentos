package ledger

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"testing"
)

// Reserved lifecycle labels own these detail fields even when their admission
// envelope is malformed. Discovery must retain the claim for exact validation.
func TestIncidentDetailWithoutProjection(t *testing.T) {
	cases := []struct {
		event, detail, target string
		want                  bool
	}{
		{"WORK_COMPLETED", `{"evidence_event_ref":"selected"}`, "event", true},
		{"GOAL_ACHIEVED", `{"evidence_event_ref":"selected"}`, "event", true},
		{"TASK_VERIFIED_COMPLETE", `{"outcome_event_ref":"selected"}`, "event", true},
		{"TASK_VERIFIED_COMPLETE", `{"submission_event_ref":"selected"}`, "event", true},
		{"TASK_VERIFIED_COMPLETE", `{"judgment_ref":"selected"}`, "event", true},
		{"EXECUTION_STARTED", `{"input_event_ref":"selected"}`, "event", true},
		{"EXECUTION_STARTED", `{"strategic_event_refs":["selected"]}`, "event", true},
		{"EXECUTION_STARTED", `{"dispatch_binding":{"agent_event_ref":"selected"}}`, "event", true},
		{"EXECUTION_STARTED", `{"dispatch_binding":{"blueprint_event_ref":"selected"}}`, "event", true},
		{"EXECUTION_STARTED", `{"dispatch_binding":{"execution_profile_event_ref":"selected"}}`, "event", true},
		{"TASK_EXECUTION_SUSPENDED", `{"stop_request_ref":"selected"}`, "event", true},
		{"TASK_EXECUTION_SUSPENDED", `{"stop_request_ref":null,"execution_start_ref":"selected"}`, "event", true},
		{"TASK_EXECUTION_SUSPENDED", `{"execution_start_ref":"selected"}`, "event", false},
		{"EXECUTION_STARTED", `{"strategic_context_refs":[{"id":"mission/selected"}]}`, "mission", true},
		{"EXECUTION_STARTED", `{"strategic_context_refs":[{"id":"goal/selected"}]}`, "goal", true},
		{"AUDIT_NOTE", `{"evidence_event_ref":"selected"}`, "event", false},
		{"WORK_CREATED", `{"evidence_event_ref":"selected"}`, "event", false},
		{"WORK_COMPLETED", `{"untrusted":{"evidence_event_ref":"selected"}}`, "event", false},
	}
	projections := []string{
		``,
		`"projection":null,`,
		`"projection":[],`,
		`"projection":{},`,
		`"projection":{"projection_kind":7},`,
		`"projection":{"projection_kind":"unsupported"},`,
		`"projection":{"projection_kind":"goal"},`,
	}
	rules := append(append([]incidentLinkRule(nil), incidentDetailLinkRules...), incidentManifestRecordLinks...)
	for index, tc := range cases {
		for shape, projection := range projections {
			t.Run(fmt.Sprintf("%s/%d/%d", tc.event, index, shape), func(t *testing.T) {
				body := `{` + projection + `"detail":` + tc.detail + `}`
				found := visitIncidentLifecycleLinks([]driver.Value{int64(0), tc.event, body}, tc.target, rules, func(kind, id string) bool {
					return kind == tc.target && id == "selected"
				})
				if found != tc.want {
					t.Fatalf("lifecycle detail discovery=%v want %v", found, tc.want)
				}
			})
		}
	}
}

func TestIncidentRecordOwnerChannels(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	body := `{"projection_kind":"knowledge","value":{"provenance_event_refs":["selected"]}}`
	for _, flag := range []int{1, 2} {
		t.Run(fmt.Sprint(flag), func(t *testing.T) {
			var legacy, current, match int
			if err := store.db.QueryRowContext(t.Context(), `SELECT json_array_length(agentos_incident_links_v2(?,'authorization_trace',?)),json_array_length(agentos_incident_links_v3(?,'authorization_trace',?)),agentos_incident_link_match_v3(?,'authorization_trace',?,'event','selected')`, flag, body, flag, body, flag, body).Scan(&legacy, &current, &match); err != nil {
				t.Fatal(err)
			}
			want := flag - 1
			if legacy != 0 || current != want || match != want {
				t.Fatalf("record ownership flag=%d legacy=%d links=%d match=%d want=%d", flag, legacy, current, match, want)
			}
		})
	}
}

func changeIncidentSourceKind(t *testing.T, store *SQLite, id, mode string, removeBacking bool) {
	t.Helper()
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		event, found, err := eventByID(t.Context(), tx, id)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("fixture event is missing")
		}
		var root, projection map[string]json.RawMessage
		if err := json.Unmarshal(event.Payload, &root); err != nil {
			return err
		}
		if err := json.Unmarshal(root["projection"], &projection); err != nil {
			return err
		}
		switch mode {
		case "missing-kind", "counterpart-kind":
			delete(projection, "projection_kind")
		case "nontext-kind":
			projection["projection_kind"] = json.RawMessage(`7`)
		case "wrong-kind":
			projection["projection_kind"] = json.RawMessage(`"organization"`)
		default:
			return fmt.Errorf("unknown source mutation %q", mode)
		}
		root["projection"], err = json.Marshal(projection)
		if err != nil {
			return err
		}
		body, err := json.Marshal(root)
		if err != nil {
			return err
		}
		if removeBacking {
			if _, err := tx.ExecContext(t.Context(), `DELETE FROM records WHERE admission_event_id=?`, id); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, id); err != nil {
			return err
		}
		if mode == "counterpart-kind" {
			if _, err := tx.ExecContext(t.Context(), `UPDATE events SET event_type='AUDIT_NOTE' WHERE event_id=?`, id); err != nil {
				return err
			}
		}
		if err := validateIncidentLinkContents(t.Context(), tx); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
			return err
		}
		return rebuildEventIntegrity(t.Context(), tx)
	}); err != nil {
		t.Fatal(err)
	}
}

func ChangeIncidentSourceKindForTest(t *testing.T, store *SQLite, id, mode string) {
	t.Helper()
	changeIncidentSourceKind(t, store, id, mode, true)
}

func ChangeIncidentRecordKindForTest(t *testing.T, store *SQLite, id string, removeDiscriminator bool) {
	t.Helper()
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		var body []byte
		if err := tx.QueryRowContext(t.Context(), `SELECT body FROM records WHERE admission_event_id=?`, id).Scan(&body); err != nil {
			return err
		}
		if removeDiscriminator {
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(body, &fields); err != nil {
				return err
			}
			delete(fields, "projection_kind")
			var err error
			body, err = json.Marshal(fields)
			if err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(t.Context(), `UPDATE records SET kind='authorization_trace',body=? WHERE admission_event_id=?`, body, id); err != nil {
			return err
		}
		return validateIncidentLinkContents(t.Context(), tx)
	}); err != nil {
		t.Fatal(err)
	}
}

// RemoveIncidentAdmissionForTest models a resealed local history whose reserved
// lifecycle label and detail survive but whose admission and record do not.
func RemoveIncidentAdmissionForTest(t *testing.T, store *SQLite, id string) {
	t.Helper()
	if err := store.withTx(t.Context(), func(tx *sql.Tx) error {
		event, found, err := eventByID(t.Context(), tx, id)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("fixture event is missing")
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(event.Payload, &fields); err != nil {
			return err
		}
		delete(fields, "projection")
		delete(fields, "admission")
		body, err := json.Marshal(fields)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM records WHERE admission_event_id=?`, id); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `UPDATE events SET payload=? WHERE event_id=?`, body, id); err != nil {
			return err
		}
		if err := validateIncidentLinkContents(t.Context(), tx); err != nil {
			return err
		}
		if _, err := tx.ExecContext(t.Context(), `DELETE FROM event_integrity`); err != nil {
			return err
		}
		return rebuildEventIntegrity(t.Context(), tx)
	}); err != nil {
		t.Fatal(err)
	}
}
