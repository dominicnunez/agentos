package events

import (
	"encoding/json"
	"testing"
)

func TestReservedProjectionAliasesCannotBeOrdinaryContent(t *testing.T) {
	for _, key := range []string{"projection", "admission", "PROJECTION", "Projection", "ADMISSION", "admiſsion"} {
		t.Run(key, func(t *testing.T) {
			body, err := json.Marshal(map[string]any{key: map[string]any{"record_id": "task-1"}})
			if err != nil {
				t.Fatal(err)
			}
			if err := ValidateOrdinaryEventPayload(json.RawMessage(body)); err == nil {
				t.Error("reserved alias accepted as ordinary content")
			}
			if _, _, err := AdmittedProjection(Event{Payload: body}); err == nil {
				t.Error("reserved alias classified without an admission error")
			}
		})
	}
}

func TestNonreservedProjectionLikeContentRemainsOrdinary(t *testing.T) {
	body := json.RawMessage(`{"body":"message","projection_note":"text","admissions":[],"content":{"PROJECTION":{"record_id":"task-1"}}}`)
	if err := ValidateOrdinaryEventPayload(body); err != nil {
		t.Fatal(err)
	}
	if _, present, err := AdmittedProjection(Event{Payload: body}); err != nil || present {
		t.Fatalf("ordinary content classified as projection: present=%v err=%v", present, err)
	}
}
