package events

import (
	"strings"
	"testing"
)

func TestIncidentNestedItemBounds(t *testing.T) {
	for _, kind := range []string{"authorization", "artifact", "inbox"} {
		t.Run(kind, func(t *testing.T) {
			snapshot := IncidentSnapshot{DependencyEvents: []Event{{}, {}}}
			switch kind {
			case "authorization":
				snapshot.DependencyEvents[0].AuthorizationRefs = make([]string, MaximumIncidentEvidence-3)
				snapshot.DependencyEvents[1].AuthorizationRefs = []string{""}
			case "artifact":
				snapshot.DependencyEvents[0].ArtifactRefs = make([]string, MaximumIncidentEvidence-3)
				snapshot.DependencyEvents[1].ArtifactRefs = []string{""}
			case "inbox":
				snapshot.InboxObservations = map[string]InboxObservationBinding{"first": {EventIDs: make([]string, MaximumIncidentEvidence-5)}, "second": {EventIDs: []string{""}}}
			}
			if err := ValidateIncidentBounds(snapshot); err != nil {
				t.Fatalf("exact aggregate bound rejected: %v", err)
			}
			snapshot.Admissions = []IncidentAdmission{{}}
			if err := ValidateIncidentBounds(snapshot); err == nil {
				t.Fatal("nested references escaped aggregate item budget")
			}
		})
	}
}

func TestIncidentMixedReferenceBound(t *testing.T) {
	snapshot := IncidentSnapshot{
		Work:              VerifiedEventSnapshot{Events: []Event{{AuthorizationRefs: make([]string, 1000)}}},
		RelatedEvents:     []Event{{ArtifactRefs: make([]string, 1000)}},
		DependencyEvents:  []Event{{AuthorizationRefs: make([]string, 1000)}},
		InboxObservations: map[string]InboxObservationBinding{"observed": {EventIDs: make([]string, MaximumIncidentEvidence-3002)}},
	}
	if err := ValidateIncidentBounds(snapshot); err != nil {
		t.Fatalf("exact mixed reference bound rejected: %v", err)
	}
	snapshot.Work.Events[0].ArtifactRefs = []string{""}
	if err := ValidateIncidentBounds(snapshot); err == nil {
		t.Fatal("public and private nested references did not share one item budget")
	}
}

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

func TestIncidentSeparateByteBounds(t *testing.T) {
	snapshot := IncidentSnapshot{
		Work:             VerifiedEventSnapshot{Events: []Event{{Payload: []byte(strings.Repeat("p", 2<<20))}}},
		DependencyEvents: []Event{{Payload: []byte(strings.Repeat("d", MaximumIncidentEvidenceBytes))}},
	}
	if err := ValidateIncidentBounds(snapshot); err != nil {
		t.Fatalf("independent exact public and support bounds rejected: %v", err)
	}
	snapshot.RelatedEvents = []Event{{EventID: "x"}}
	if err := ValidateIncidentBounds(snapshot); err == nil {
		t.Fatal("related event escaped the combined public byte bound")
	}
	snapshot.RelatedEvents = nil
	snapshot.Admissions = []IncidentAdmission{{EventRef: "x"}}
	if err := ValidateIncidentBounds(snapshot); err == nil {
		t.Fatal("annotation escaped the independent support byte bound")
	}
}

func TestIncidentPublicReferenceByteBounds(t *testing.T) {
	snapshot := IncidentSnapshot{
		Work:             VerifiedEventSnapshot{Events: []Event{{ArtifactRefs: []string{strings.Repeat("r", 2<<20)}}}},
		DependencyEvents: []Event{{Payload: []byte(strings.Repeat("d", MaximumIncidentEvidenceBytes-(2<<20)))}},
	}
	if err := ValidateIncidentBounds(snapshot); err != nil {
		t.Fatalf("exact shared reference bytes rejected: %v", err)
	}
	snapshot.Admissions = []IncidentAdmission{{Kind: "x"}}
	if err := ValidateIncidentBounds(snapshot); err == nil {
		t.Fatal("public reference bytes did not consume support")
	}
	snapshot.Admissions = nil
	snapshot.Work.Events[0].Payload = []byte("x")
	if err := ValidateIncidentBounds(snapshot); err == nil {
		t.Fatal("public reference bytes did not consume public allowance")
	}
}

func TestIncidentInboxStartItemBound(t *testing.T) {
	snapshot := IncidentSnapshot{InboxObservations: map[string]InboxObservationBinding{
		"observed": {ExecutionStartEventRef: "start", EventIDs: make([]string, MaximumIncidentEvidence-2)},
	}}
	if err := ValidateIncidentBounds(snapshot); err != nil {
		t.Fatalf("exact inbox binding/reference item bound rejected: %v", err)
	}
	snapshot.Admissions = []IncidentAdmission{{}}
	if err := ValidateIncidentBounds(snapshot); err == nil {
		t.Fatal("inbox start reference did not consume its support item")
	}
}
