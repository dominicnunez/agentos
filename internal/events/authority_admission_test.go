package events

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/core"
)

func TestResolveAuthorityAdmissionsKeepsLeaseRevisionsInGrantOrganization(t *testing.T) {
	grantedAt := time.Unix(1, 0).UTC()
	revokedAt := grantedAt.Add(time.Second)
	lease := core.CapabilityLease{ID: "lease-1", ActorID: "agent-1", Action: "write", Resource: "record-1", Scope: "org-1", OriginTaskID: "task-1"}
	grantBody, err := json.Marshal(lease)
	if err != nil {
		t.Fatal(err)
	}
	lease.RevokedAt = &revokedAt
	revocationBody, err := json.Marshal(lease)
	if err != nil {
		t.Fatal(err)
	}
	records := []AuthorityRecord{
		{Kind: authorityKindLease, RecordID: "lease-1", Version: 1, Body: grantBody, AdmissionEventID: "grant-1"},
		{Kind: authorityKindLease, RecordID: "lease-1", Version: 2, Body: revocationBody, AdmissionEventID: "revoke-1"},
	}
	stream := []Event{
		{EventID: "grant-1", Sequence: 1, OrganizationID: "org-1", EventType: "CAPABILITY_GRANTED", SourceActorID: "user-1", TaskID: "task-1", Payload: grantBody, CreatedAt: grantedAt, SchemaVersion: SchemaVersion},
		{EventID: "revoke-1", Sequence: 2, OrganizationID: "org-2", EventType: "CAPABILITY_REVOKED", SourceActorID: "user-1", TaskID: "task-1", Payload: revocationBody, CreatedAt: revokedAt, SchemaVersion: SchemaVersion},
	}
	if _, _, err := ResolveAuthorityAdmissions(stream, records); err == nil {
		t.Fatal("cross-organization lease revocation was accepted")
	}
	stream[1].OrganizationID = "org-1"
	admissions, _, err := ResolveAuthorityAdmissions(stream, records)
	if err != nil {
		t.Fatal(err)
	}
	if len(admissions) != 2 || admissions[0].OrganizationID != "org-1" || admissions[1].OrganizationID != "org-1" || admissions[1].Lease.RevokedAt == nil {
		t.Fatalf("same-organization authority history was not preserved: %+v", admissions)
	}
}

func TestValidateAuthorityRecordTransitionPreservesActorKindAtRevocation(t *testing.T) {
	grantedAt := time.Unix(1, 0).UTC()
	revokedAt := grantedAt.Add(time.Second)
	lease := core.CapabilityLease{ID: "lease-1", ActorID: "agent-1", ActorKind: core.PrincipalAgent, Action: "write", Resource: "record-1", Scope: "org-1", OriginTaskID: "task-1"}
	grantBody, err := json.Marshal(lease)
	if err != nil {
		t.Fatal(err)
	}
	lease.ActorKind = core.PrincipalExternalAgent
	lease.RevokedAt = &revokedAt
	revocationBody, err := json.Marshal(lease)
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateAuthorityRecordTransition(authorityKindLease, "lease-1", 2, revocationBody, grantBody); err == nil {
		t.Fatal("lease revocation changed the durable principal kind")
	}
}

func TestResolveAuthorityAdmissionsHasNoLifetimeEventLimit(t *testing.T) {
	const count = 4097
	stream := make([]Event, 0, count)
	records := make([]AuthorityRecord, 0, count)
	created := time.Unix(1, 0).UTC()
	for index := 0; index < count; index++ {
		leaseID := fmt.Sprintf("lease-%d", index)
		taskID := fmt.Sprintf("task-%d", index)
		body, err := json.Marshal(core.CapabilityLease{ID: core.ID(leaseID), ActorID: "agent-1", Action: "read", Resource: "record-1", Scope: "org-1", OriginTaskID: core.ID(taskID)})
		if err != nil {
			t.Fatal(err)
		}
		sequence := int64(index + 1)
		stream = append(stream, Event{EventID: fmt.Sprintf("event-%d", index), Sequence: sequence, OrganizationID: "org-1", EventType: "CAPABILITY_GRANTED", SourceActorID: "user-1", TaskID: taskID, Payload: body, CreatedAt: created.Add(time.Duration(index) * time.Second), SchemaVersion: SchemaVersion})
		records = append(records, AuthorityRecord{Kind: authorityKindLease, RecordID: leaseID, Version: 1, Body: body, AdmissionEventID: fmt.Sprintf("event-%d", index)})
	}
	admissions, freezes, err := ResolveAuthorityAdmissions(stream, records)
	if err != nil {
		t.Fatal(err)
	}
	if len(admissions) != count || len(freezes) != 0 {
		t.Fatalf("complete authority history was truncated: leases=%d freezes=%d", len(admissions), len(freezes))
	}
}
