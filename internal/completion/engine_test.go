package completion

import (
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestCompletionRequiresVerification(t *testing.T) {
	c := core.CompletionContract{TaskID: "t", TaskVersion: 1, Criteria: []core.CompletionCriterion{{ID: "c", Assurance: core.AssuranceDeterministic, Required: true}}}
	if (Engine{}).Evaluate(c, core.ToolOutcome{Status: core.OutcomeSucceeded}).Complete {
		t.Fatal("unverified outcome completed")
	}
	if !(Engine{}).Evaluate(c, core.ToolOutcome{Status: core.OutcomeSucceeded, PostconditionStatus: core.PostconditionVerified}).Complete {
		t.Fatal("verified success did not complete")
	}
}

func TestHumanJudgmentDoesNotMasqueradeAsDeterministicEvidence(t *testing.T) {
	t.Parallel()
	contract := core.CompletionContract{TaskID: "t", TaskVersion: 1, Criteria: []core.CompletionCriterion{{ID: "review", Assurance: core.AssuranceHumanJudgment, Required: true}}}
	outcome := core.ToolOutcome{Status: core.OutcomeSucceeded, PostconditionStatus: core.PostconditionNotChecked}
	if result := (Engine{}).Evaluate(contract, outcome); result.Complete || len(result.Reasons) == 0 {
		t.Fatalf("unreviewed judgment completed: %+v", result)
	}
	if result := (Engine{}).EvaluateHuman(contract, outcome, true); !result.Complete {
		t.Fatalf("approved human judgment was rejected: %+v", result)
	}
	if result := (Engine{}).EvaluateHuman(contract, outcome, false); result.Complete || len(result.Reasons) == 0 {
		t.Fatalf("rejected human judgment completed: %+v", result)
	}
	if outcome.PostconditionStatus != core.PostconditionNotChecked {
		t.Fatalf("human judgment rewrote runtime evidence: %+v", outcome)
	}
}

func TestHumanTaskRequiresEveryStructuredFieldAndArtifact(t *testing.T) {
	contract := core.CompletionContract{
		TaskID: "task-1", TaskVersion: 1,
		RequiredFields:       []core.CompletionFieldRequirement{{Name: "summary", MinBytes: 4, MaxBytes: 100}},
		ArtifactRequirements: []core.ArtifactRequirement{{Role: "signed-form", MediaTypes: []string{"application/pdf"}, MinCount: 1, MaxCount: 1}},
	}
	submission := core.HumanTaskSubmission{MessageID: "message-1", Fields: map[string]string{"summary": "done"}, Artifacts: []core.ArtifactEvidence{{
		Ref: "artifact/sha256/" + strings.Repeat("a", 64), Role: "signed-form", Name: "form.pdf", MediaType: "application/pdf",
		SHA256: strings.Repeat("a", 64), Size: 10, Origin: "local-uid-1000", Trust: "UNTRUSTED_USER_ARTIFACT",
	}}}
	if result := (Engine{}).EvaluateHumanTask(contract, submission); !result.Complete {
		t.Fatalf("valid submission rejected: %+v", result)
	}
	missing := submission
	missing.Artifacts = nil
	if result := (Engine{}).EvaluateHumanTask(contract, missing); result.Complete {
		t.Fatal("missing required artifact was accepted")
	}
	unexpected := submission
	unexpected.Fields = map[string]string{"summary": "done", "self_report": "yes"}
	if result := (Engine{}).EvaluateHumanTask(contract, unexpected); result.Complete {
		t.Fatal("unexpected self-report field was accepted")
	}
	duplicate := submission
	duplicate.Artifacts = append(append([]core.ArtifactEvidence{}, submission.Artifacts...), submission.Artifacts[0])
	if result := (Engine{}).EvaluateHumanTask(contract, duplicate); result.Complete || !strings.Contains(strings.Join(result.Reasons, "; "), "duplicated") {
		t.Fatalf("duplicate artifact evidence was accepted: %+v", result)
	}
	forged := submission
	forged.Artifacts = append([]core.ArtifactEvidence(nil), submission.Artifacts...)
	forged.Artifacts[0].Ref = "other/" + forged.Artifacts[0].SHA256
	if result := (Engine{}).EvaluateHumanTask(contract, forged); result.Complete {
		t.Fatal("noncanonical artifact reference was accepted")
	}
}
