package trustconfig

import (
	"strings"
	"testing"
	"time"
)

func TestDecodeObjectIsStrict(t *testing.T) {
	t.Parallel()

	var target struct {
		Name string `json:"name"`
	}
	if err := DecodeObject(strings.NewReader(`{"name":"trusted"}`), "registry", &target); err != nil {
		t.Fatalf("decode object: %v", err)
	}
	if target.Name != "trusted" {
		t.Fatalf("name = %q, want trusted", target.Name)
	}

	for _, input := range []string{
		`{"name":"trusted","name":"replacement"}`,
		`{"NAME":"replacement"}`,
		`{"name":"trusted","unknown":true}`,
		`{"name":"trusted"} {}`,
	} {
		if err := DecodeObject(strings.NewReader(input), "registry", &target); err == nil {
			t.Fatalf("DecodeObject(%q) succeeded, want error", input)
		}
	}
	if err := DecodeObject(strings.NewReader(`{"name":"trusted"}`+strings.Repeat(" ", registryLimit)), "registry", &target); err == nil {
		t.Fatal("DecodeObject accepted content beyond the registry limit")
	}
}

func TestDecodeEntriesRequiresContent(t *testing.T) {
	t.Parallel()

	type registry struct {
		Entries []string `json:"entries"`
	}
	var populated registry
	entries, err := DecodeEntries(strings.NewReader(`{"entries":["trusted"]}`), "registry", "entry", &populated, &populated.Entries)
	if err != nil || len(entries) != 1 || entries[0] != "trusted" {
		t.Fatalf("entries=%v err=%v", entries, err)
	}
	var empty registry
	if _, err := DecodeEntries(strings.NewReader(`{"entries":[]}`), "registry", "entry", &empty, &empty.Entries); err == nil {
		t.Fatal("DecodeEntries accepted an empty registry")
	}
}

func TestValidateCredentialLifecycle(t *testing.T) {
	t.Parallel()

	expiresAt := time.Now().Add(time.Hour)
	credential := strings.Repeat("s", 32)
	if err := ValidateCredentialLifecycle("ACTIVE", credential, &expiresAt); err != nil {
		t.Fatalf("validate lifecycle: %v", err)
	}

	tests := []struct {
		name       string
		status     string
		credential string
		expiresAt  *time.Time
	}{
		{name: "invalid status", status: "ADMIN", credential: credential, expiresAt: &expiresAt},
		{name: "short credential", status: "ACTIVE", credential: "short", expiresAt: &expiresAt},
		{name: "missing expiry", status: "ACTIVE", credential: credential},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if err := ValidateCredentialLifecycle(test.status, test.credential, test.expiresAt); err == nil {
				t.Fatal("ValidateCredentialLifecycle succeeded, want error")
			}
		})
	}
}
