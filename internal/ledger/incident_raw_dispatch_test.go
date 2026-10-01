package ledger

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
)

// Retain the prior boolean expression as an independent equivalence oracle.
// The consumer/path implementation is unchanged by the dispatch optimization.
func incidentRawApplicableBeforeDispatch(source string) string {
	proposal := incidentKnowledgeConsumerFor(source, "provenance_event_refs", func(body, path string) string {
		return incidentScalarClaim(body, path+".created_by_kind", `='AGENT'`)
	}, false)
	validation := incidentKnowledgeConsumerFor(source, "validation_refs", func(body, path string) string {
		return incidentScalarClaim(body, path+".validation_method", `='`+string(core.KnowledgeValidationDeterministic)+`'`)
	}, true)
	judgment := incidentKnowledgeConsumerFor(source, "validation_refs", func(body, path string) string {
		return `(` + incidentScalarClaim(body, path+".validation_method", ` IN ('`+string(core.KnowledgeValidationHuman)+`','`+string(core.KnowledgeValidationIndependentAgent)+`')`) + ` OR ` + incidentScalarClaim(body, path+".validated_by_kind", ` IN ('HUMAN','AGENT','EXTERNAL_AGENT')`) + `)`
	}, true)
	knowledge := `(` + source + `.event_type NOT IN ('KNOWLEDGE_PROPOSED','KNOWLEDGE_VALIDATION_RECORDED','KNOWLEDGE_JUDGMENT_PUBLISHED','HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED','A2A_KNOWLEDGE_JUDGMENT_RECEIVED') OR ` + incidentScalarClaim(source+".payload", "$.projection.projection_kind", `='knowledge'`) + ` OR
(` + source + `.event_type='KNOWLEDGE_PROPOSED' AND (` + proposal + `)) OR
(` + source + `.event_type='KNOWLEDGE_VALIDATION_RECORDED' AND (` + validation + `)) OR
(` + source + `.event_type IN ('KNOWLEDGE_JUDGMENT_PUBLISHED','HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED','A2A_KNOWLEDGE_JUDGMENT_RECEIVED') AND (` + judgment + `)))`
	return `(` + incidentAggregateIncoming(source) + ` AND ` + knowledge + `)`
}

func TestIncidentRawDispatchSkipsOrdinaryPayload(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	// A failing SQL payload expression makes unnecessary document evaluation
	// observable without timing thresholds or inspecting implementation text.
	query := func(expression string) string {
		return `SELECT ` + strings.ReplaceAll(expression, "s.payload", `json('not-json')`) + ` FROM (SELECT 'AUDIT_NOTE' AS event_type,'ordinary' AS event_id) s`
	}
	var applicable bool
	if err := store.db.QueryRowContext(t.Context(), query(incidentRawApplicableBeforeDispatch("s"))).Scan(&applicable); err == nil {
		t.Fatal("prior expression did not exercise the payload evaluation sentinel")
	}
	if err := store.db.QueryRowContext(t.Context(), query(incidentRawClaimApplicable("s"))).Scan(&applicable); err != nil {
		t.Fatalf("ordinary eligibility evaluated its unused payload: %v", err)
	}
	if !applicable {
		t.Fatal("ordinary source lost applicability")
	}
}

func TestIncidentRawDispatchEquivalence(t *testing.T) {
	for _, family := range []string{"proposal", "validation", "judgment"} {
		t.Run(family, func(t *testing.T) {
			store, err := Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			switch family {
			case "proposal":
				appendIncidentAgentCandidate(t, store, true)
			case "validation":
				appendIncidentDeterministicKnowledge(t, store, true)
			case "judgment":
				appendDerivedIncidentChain(t, store, 1)
			}
			stream, err := store.Events(t.Context(), "")
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range stream {
				var before, after bool
				if err := store.db.QueryRowContext(t.Context(), `SELECT `+incidentRawApplicableBeforeDispatch("s")+`,`+incidentRawClaimApplicable("s")+` FROM events s WHERE s.event_id=?`, event.EventID).Scan(&before, &after); err != nil {
					t.Fatal(err)
				}
				if before != after {
					t.Fatalf("actual writer source %s changed applicability", event.EventType)
				}
			}
			// Compare every family, including an unused foreign source and
			// malformed/duplicate/escaped projection-wrapper claims. These SQL
			// variants test eligibility only; they do not claim valid admissions.
			for _, kind := range []string{"AUDIT_NOTE", "WORK_COMPLETION_EVALUATED", "GOAL_PROGRESS_EVALUATED", "KNOWLEDGE_PROPOSED", "KNOWLEDGE_VALIDATION_RECORDED", "KNOWLEDGE_JUDGMENT_PUBLISHED", "HUMAN_KNOWLEDGE_JUDGMENT_RECEIVED", "A2A_KNOWLEDGE_JUDGMENT_RECEIVED"} {
				for _, payload := range []string{`{}`, `{`, `{"projection":{"projection_kind":"unrelated"},"projection":{"projection_kind":"knowledge"}}`, `{"\u0070rojection":{"projection_\u006bind":"knowledge"}}`} {
					var before, after bool
					if !json.Valid([]byte(payload)) && payload != "{" {
						t.Fatal("invalid differential fixture")
					}
					if err := store.db.QueryRowContext(t.Context(), `SELECT `+incidentRawApplicableBeforeDispatch("s")+`,`+incidentRawClaimApplicable("s")+` FROM (SELECT 'unused-foreign' AS event_id,? AS event_type,? AS payload) s`, kind, payload).Scan(&before, &after); err != nil {
						t.Fatal(err)
					}
					if before != after {
						t.Fatalf("%s %q changed eligibility: before=%v after=%v", kind, payload, before, after)
					}
				}
			}
		})
	}
}
