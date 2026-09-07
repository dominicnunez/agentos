package modelinput

import "testing"

func TestRoutingSignalConstraintsPreserveLegacyAndCannotWeaken(t *testing.T) {
	base := RouteRequirements{OrganizationID: "org", Capabilities: []Capability{Text}, InputTokens: 1, OutputTokens: 1, Locality: LocalOnly, DataClass: "internal"}
	body, err := base.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != `{"organization_id":"org","capabilities":["text"],"input_tokens":1,"output_tokens":1,"locality":"LOCAL_ONLY","data_class":"internal"}` {
		t.Fatal("legacy requirements representation changed", string(body))
	}
	base.RequireHealthy = true
	base.MaxLatencyMilliseconds = 100
	base.Evaluation = &EvaluationRequirement{Metric: "classification-v1", MinimumScoreBasisPoints: 9000}
	for _, bound := range []int64{0, 200, 50} {
		rule := base.Clone()
		rule.RequireHealthy = false
		rule.MaxLatencyMilliseconds = bound
		rule.Evaluation = nil
		got, err := IntersectRouteRequirements(base, rule)
		if err != nil {
			t.Fatal(err)
		}
		expected := int64(100)
		if bound == 50 {
			expected = 50
		}
		if !got.RequireHealthy || got.MaxLatencyMilliseconds != expected || got.Evaluation == nil || got.Evaluation.MinimumScoreBasisPoints != 9000 {
			t.Fatal("key weakened evidence constraints", got)
		}
		got.Evaluation.Metric = "changed"
		if base.Evaluation.Metric != "classification-v1" {
			t.Fatal("evaluation aliased")
		}
	}
	rule := base.Clone()
	rule.Evaluation.Metric = "another-metric"
	if _, err := IntersectRouteRequirements(base, rule); err == nil {
		t.Fatal("incomparable metric replaced default")
	}
	rule = base.Clone()
	rule.Evaluation.MinimumScoreBasisPoints = 9500
	got, err := IntersectRouteRequirements(base, rule)
	if err != nil || got.Evaluation.MinimumScoreBasisPoints != 9500 {
		t.Fatal("stricter evaluation was lost", err)
	}
	for _, mutate := range []func(*RouteRequirements){
		func(r *RouteRequirements) { r.MaxLatencyMilliseconds = -1 }, func(r *RouteRequirements) { r.MaxLatencyMilliseconds = 86400001 },
		func(r *RouteRequirements) { r.Evaluation.MinimumScoreBasisPoints = -1 }, func(r *RouteRequirements) { r.Evaluation.MinimumScoreBasisPoints = 10001 },
		func(r *RouteRequirements) { r.Evaluation.Metric = "invalid metric" },
	} {
		r := base.Clone()
		mutate(&r)
		if _, err := r.Canonical(); err == nil {
			t.Fatal("invalid signal requirement accepted")
		}
	}
	old, _ := base.Fingerprint()
	rule = base.Clone()
	rule.Evaluation.MinimumScoreBasisPoints++
	changed, _ := rule.Fingerprint()
	if old == changed {
		t.Fatal("evaluation threshold not fingerprinted")
	}
}
