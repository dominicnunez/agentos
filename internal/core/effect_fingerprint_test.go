package core

import "testing"

func TestEffectFingerprintBindsEveryImmutableEffectField(t *testing.T) {
	base := EffectObligation{
		ID: "effect-1", OrganizationID: "org-1", TaskID: "task-1", ActorID: "actor-1", ActorKind: PrincipalAgent,
		Action: "send", Resource: "customer-1", Scope: "org-1", ConsequenceBoundary: BoundaryPublicExternal,
		Descriptor: "send exact message", AuthorizationRefs: []string{"lease-1"}, ApprovalRef: "approval-1",
		IdempotencyKey: "effect-key-1", ReplayContext: map[string]string{"body": "hello", "format": "plain"},
	}
	want, err := FingerprintEffect(base)
	if err != nil {
		t.Fatal(err)
	}
	mutations := map[string]func(*EffectObligation){
		"effect id":               func(effect *EffectObligation) { effect.ID = "effect-2" },
		"organization":            func(effect *EffectObligation) { effect.OrganizationID = "org-2" },
		"task":                    func(effect *EffectObligation) { effect.TaskID = "task-2" },
		"actor":                   func(effect *EffectObligation) { effect.ActorID = "actor-2" },
		"actor kind":              func(effect *EffectObligation) { effect.ActorKind = PrincipalExternalAgent },
		"action":                  func(effect *EffectObligation) { effect.Action = "publish" },
		"resource":                func(effect *EffectObligation) { effect.Resource = "customer-2" },
		"scope":                   func(effect *EffectObligation) { effect.Scope = "expanded" },
		"consequence boundary":    func(effect *EffectObligation) { effect.ConsequenceBoundary = BoundaryLegalBinding },
		"descriptor":              func(effect *EffectObligation) { effect.Descriptor = "changed" },
		"authorization reference": func(effect *EffectObligation) { effect.AuthorizationRefs = []string{"lease-2"} },
		"approval reference":      func(effect *EffectObligation) { effect.ApprovalRef = "approval-2" },
		"idempotency key":         func(effect *EffectObligation) { effect.IdempotencyKey = "effect-key-2" },
		"replay argument": func(effect *EffectObligation) {
			effect.ReplayContext = map[string]string{"body": "changed", "format": "plain"}
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			changed := base
			mutate(&changed)
			got, err := FingerprintEffect(changed)
			if err != nil {
				t.Fatal(err)
			}
			if got == want {
				t.Fatal("authority-bearing effect mutation retained its fingerprint")
			}
		})
	}

	reordered := base
	reordered.ReplayContext = map[string]string{"format": "plain", "body": "hello"}
	got, err := FingerprintEffect(reordered)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatal("map insertion order changed the canonical fingerprint")
	}
}
