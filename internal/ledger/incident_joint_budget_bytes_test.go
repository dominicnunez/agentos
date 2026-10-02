package ledger

import (
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

func TestIncidentJointSupportBytes(t *testing.T) {
	parallelIncidentTest(t)
	for _, target := range []int{15 << 20, (15 << 20) + (768 << 10)} {
		t.Run(fmt.Sprint(target), func(t *testing.T) {
			store, err := Open(filepath.Join(t.TempDir(), "joint-bytes.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = store.Close() }()
			// This is the existing distinct-policy writer lifecycle from
			// TestIncidentInferenceSupportBudget. No application manifest is
			// invented: the supported library auxiliary-context owner permits
			// no manifest when ExecutionManifestRef is empty.
			policy := testInferencePolicy(time.Now().UTC())
			policy.MaxConcurrentRequests, policy.MaxTokensPerWindow = 2, 1000
			policy.Pricing.MaxCostNanoUSDPerWindow = 2000000
			if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
				t.Fatal(err)
			}
			first, err := store.ReserveInference(t.Context(), testInferenceRequest("first"))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.ReconcileInference(t.Context(), first, nil, inference.ReconciliationUncertain); err != nil {
				t.Fatal(err)
			}
			policy.AuthorizedBy = "other-owner"
			policy.AuthorizedAt = policy.AuthorizedAt.Add(time.Second)
			if err := store.ActivateInferencePolicy(t.Context(), policy); err != nil {
				t.Fatal(err)
			}
			if _, err := store.ReserveInference(t.Context(), testInferenceRequest("later")); err != nil {
				t.Fatal(err)
			}
			// Each policy stays below boundaryjson.MaximumBytes (16MiB).
			// Whitespace preserves its decoded policy and owner fingerprint.
			rows, err := store.db.QueryContext(t.Context(), `SELECT policy_fingerprint,length(CAST(body AS BLOB)) FROM inference_policies ORDER BY policy_fingerprint`)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = rows.Close() }()
			type stored struct {
				fingerprint string
				size        int
			}
			var policies []stored
			for rows.Next() {
				var p stored
				if err := rows.Scan(&p.fingerprint, &p.size); err != nil {
					t.Fatal(err)
				}
				policies = append(policies, p)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if err := rows.Close(); err != nil {
				t.Fatal(err)
			}
			if len(policies) != 2 {
				t.Fatalf("policy count=%d want2", len(policies))
			}
			for _, p := range policies {
				if p.size >= target {
					t.Fatal("policy already too large for target")
				}
				if _, err := store.db.ExecContext(t.Context(), `UPDATE inference_policies SET body=CAST(body || ? AS BLOB) WHERE policy_fingerprint=?`, strings.Repeat(" ", target-p.size), p.fingerprint); err != nil {
					t.Fatal(err)
				}
			}
			refs := make([]string, 1000)
			refBytes := 0
			for i := range refs {
				prefix := fmt.Sprintf("joint-artifact-%04d-", i)
				refs[i] = prefix + strings.Repeat("x", 1024-len(prefix))
				refBytes += len(refs[i])
			}
			if _, err := store.Append(t.Context(), events.TrustedDraft{OrganizationID: "organization-1", CorrelationID: "work-1", EventType: "AUDIT_NOTE", ArtifactRefs: refs, Payload: map[string]string{"text": "public retained evidence"}}); err != nil {
				t.Fatal(err)
			}
			// These are required preconditions, not reasons to overlook a failing
			// owner. If one fails, this does not establish the joint-budget defect.
			if err := store.ValidateInferenceAdmissions(t.Context()); err != nil {
				t.Fatalf("full inference owner rejected healthy history: %v", err)
			}
			if _, err := ValidateEventIntegrity(t.Context(), store.db); err != nil {
				t.Fatal(err)
			}
			if err := validateIncidentLinkContents(t.Context(), store.db); err != nil {
				t.Fatal(err)
			}
			var policyBytes, policiesCount int
			if err := store.db.QueryRowContext(t.Context(), `SELECT COUNT(*),SUM(length(CAST(body AS BLOB))) FROM inference_policies`).Scan(&policiesCount, &policyBytes); err != nil {
				t.Fatal(err)
			}
			if policiesCount != 2 || policyBytes != 2*target {
				t.Fatalf("policy bytes=%d count=%d", policyBytes, policiesCount)
			}
			// This lower bound excludes public event own items/payload bytes.
			// It includes only selected supporting policy bodies and public
			// nested ArtifactRef bytes, which the documented support union owns.
			over := policyBytes+refBytes > events.MaximumIncidentEvidenceBytes
			if over != (target > 15<<20) {
				t.Fatal("fixture does not discriminate joint byte bound")
			}
			snapshot, err := store.VerifiedIncidentEvents(t.Context(), "organization-1", "work-1", 256)
			if over {
				if err == nil {
					t.Fatalf("accepted joint support above32MiB: policyBodies=%d publicReferenceBytes=%d", policyBytes, refBytes)
				}
				if !reflect.DeepEqual(snapshot, events.IncidentSnapshot{}) {
					t.Fatal("overbudget read returned partial evidence")
				}
				return
			}
			if err != nil {
				t.Fatalf("below-bound healthy control rejected: %v", err)
			}
			if len(snapshot.Work.Events) != 4 || len(snapshot.Admissions) != 2 {
				t.Fatalf("public=%d admissions=%d want4/2", len(snapshot.Work.Events), len(snapshot.Admissions))
			}
			if _, err := events.ValidateIncidentHistory(snapshot); err != nil {
				t.Fatal(err)
			}
		})
	}
}
