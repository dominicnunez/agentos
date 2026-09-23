package events

import (
	"strings"
	"testing"
)

func TestIncidentAdmissionBounds(t *testing.T) {
	snapshot := IncidentSnapshot{Admissions: make([]IncidentAdmission, MaximumIncidentEvidence)}
	if err := ValidateIncidentBounds(snapshot); err != nil {
		t.Fatalf("exact admission item limit rejected: %v", err)
	}
	snapshot.AuthorityRecords = []AuthorityRecord{{}}
	if err := ValidateIncidentBounds(snapshot); err == nil {
		t.Error("admissions did not share the aggregate item budget")
	}
	snapshot.AuthorityRecords = nil
	snapshot.Admissions = append(snapshot.Admissions, IncidentAdmission{})
	if err := ValidateIncidentBounds(snapshot); err == nil {
		t.Error("oversized admission list accepted")
	}
	large := strings.Repeat("x", MaximumIncidentEvidenceBytes)
	for _, field := range []string{"event", "kind", "task", "execution"} {
		t.Run(field, func(t *testing.T) {
			admission := IncidentAdmission{}
			switch field {
			case "event":
				admission.EventRef = large
			case "kind":
				admission.Kind = large
			case "task":
				admission.TaskID = large
			case "execution":
				admission.ExecutionID = large
			}
			snapshot := IncidentSnapshot{Admissions: []IncidentAdmission{admission}}
			if err := ValidateIncidentBounds(snapshot); err != nil {
				t.Fatalf("exact byte limit rejected: %v", err)
			}
			snapshot.DependencyEvents = []Event{{EventID: "x"}}
			if err := ValidateIncidentBounds(snapshot); err == nil {
				t.Fatal("admission field did not share the aggregate byte budget")
			}
		})
	}
}
