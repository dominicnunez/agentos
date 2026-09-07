package inference

import (
	"github.com/dominicnunez/agentos/internal/modelinput"
	"testing"
	"time"
)

func signalFixture(now time.Time) *RoutingSignals {
	return &RoutingSignals{Health: "READY", ObservedAt: now.Add(-time.Minute), ValidUntil: now.Add(time.Minute), EvidenceRef: "operator-observation-1", ExpectedLatencyMilliseconds: 100, Evaluation: &RoutingEvaluation{Metric: "classification-v1", ScoreBasisPoints: 9000, EvaluatorID: "independent-evaluator", EvidenceRef: "evaluation-1"}}
}
func bindSignals(b *Broker, index int, s *RoutingSignals) {
	b.Routes[index].Signals = cloneRoutingSignals(s)
	b.Manager.Pools[index].Policy.Catalog.Signals = cloneRoutingSignals(s)
}

func TestBrokerSignalEligibilityAndLocalOnly(t *testing.T) {
	now := time.Date(2026, 9, 7, 1, 0, 0, 0, time.UTC)
	for name, mutate := range map[string]func(**RoutingSignals){
		"missing":            func(s **RoutingSignals) { *s = nil },
		"unavailable":        func(s **RoutingSignals) { (*s).Health = "UNAVAILABLE" },
		"expired":            func(s **RoutingSignals) { (*s).ValidUntil = now },
		"future":             func(s **RoutingSignals) { (*s).ObservedAt = now.Add(time.Nanosecond) },
		"unknown-latency":    func(s **RoutingSignals) { (*s).ExpectedLatencyMilliseconds = 0 },
		"slow":               func(s **RoutingSignals) { (*s).ExpectedLatencyMilliseconds = 101 },
		"missing-evaluation": func(s **RoutingSignals) { (*s).Evaluation = nil },
		"wrong-metric":       func(s **RoutingSignals) { (*s).Evaluation.Metric = "another-v1" },
		"low-score":          func(s **RoutingSignals) { (*s).Evaluation.ScoreBasisPoints = 8999 },
	} {
		t.Run(name, func(t *testing.T) {
			b, r, p := brokerFixture(now)
			r.RequireHealthy = true
			r.MaxLatencyMilliseconds = 100
			r.Evaluation = &modelinput.EvaluationRequirement{Metric: "classification-v1", MinimumScoreBasisPoints: 9000}
			r.Locality = LocalOnly
			signals := signalFixture(now)
			bindSignals(&b, 0, signals)
			bindSignals(&b, 1, signals)
			if got, err := b.Select(now, r, p); err != nil || got.ConnectionID != "local" {
				t.Fatal("exact evidence boundary rejected", err)
			}
			mutate(&signals)
			bindSignals(&b, 1, signals)
			if _, err := b.Select(now, r, p); err == nil {
				t.Fatal("invalid local evidence fell back to cloud")
			}
			r.Locality = CloudAllowed
			if got, err := b.Select(now, r, p); err != nil || got.ConnectionID != "cloud" {
				t.Fatal("healthy authorized alternative rejected", err)
			}
			r.ConnectionID = "local"
			if _, err := b.Select(now, r, p); err == nil {
				t.Fatal("explicit selection bypassed evidence constraints")
			}
		})
	}
	b, r, p := brokerFixture(now)
	if _, err := b.Select(now, r, p); err != nil {
		t.Fatal("legacy catalog requires new signals", err)
	}
	signals := signalFixture(now)
	signals.Health = "UNAVAILABLE"
	bindSignals(&b, 0, signals)
	bindSignals(&b, 1, signals)
	if _, err := b.Select(now, r, p); err == nil {
		t.Fatal("declared unavailability ignored without caller opt-in")
	}
}

func TestSignalsBoundToPolicyAndRegistryCopies(t *testing.T) {
	now := time.Date(2026, 9, 7, 1, 0, 0, 0, time.UTC)
	b, r, p := brokerFixture(now)
	signals := signalFixture(now)
	bindSignals(&b, 0, signals)
	r.ConnectionID = "cloud"
	r.RequireHealthy = true
	before, _ := b.Manager.Pools[0].Policy.Fingerprint()
	b.Routes[0].Signals.Evaluation.ScoreBasisPoints++
	if _, err := b.Select(now, r, p); err == nil {
		t.Fatal("registry signal not bound to policy")
	}
	b.Manager.Pools[0].Policy.Catalog.Signals.Evaluation.ScoreBasisPoints++
	after, _ := b.Manager.Pools[0].Policy.Fingerprint()
	if before == after {
		t.Fatal("policy did not bind evaluation")
	}
	if _, err := b.Select(now, r, p); err != nil {
		t.Fatal("matching evidence rejected", err)
	}
	model := &guardModel{}
	metadata := b.Routes[0]
	metadata.Descriptor = model.Descriptor()
	registry, err := NewConnectionRegistry(&guardStore{}, []Connection{{ID: metadata.ConnectionID, Adapter: model, Metadata: &metadata}})
	if err != nil {
		t.Fatal(err)
	}
	metadata.Signals.Evaluation.ScoreBasisPoints = 0
	copy := registry.Catalog()
	copy[0].Signals.Evaluation.ScoreBasisPoints = 1
	if registry.Catalog()[0].Signals.Evaluation.ScoreBasisPoints != 9001 {
		t.Fatal("caller mutated registry evidence")
	}
	for _, mutate := range []func(*RoutingSignals){func(s *RoutingSignals) { s.Health = "unknown" }, func(s *RoutingSignals) { s.EvidenceRef = "" }, func(s *RoutingSignals) { s.Evaluation.ScoreBasisPoints = 10001 }, func(s *RoutingSignals) { s.Evaluation.EvaluatorID = "" }, func(s *RoutingSignals) { s.ExpectedLatencyMilliseconds = -1 }, func(s *RoutingSignals) { s.ValidUntil = s.ObservedAt }} {
		s := signalFixture(now)
		mutate(s)
		if s.Validate() == nil {
			t.Fatal("invalid evidence admitted")
		}
	}
}
