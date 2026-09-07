package inference

import (
	"fmt"
	"reflect"
	"time"
)

// RoutingSignals are operator-reviewed evidence bound by the account policy.
// They are never accepted from model output or inferred from a model name.
// Estimates are eligibility evidence, not a runtime latency guarantee.
type RoutingSignals struct {
	Health                      string             `json:"health"`
	ObservedAt                  time.Time          `json:"observed_at"`
	ValidUntil                  time.Time          `json:"valid_until"`
	EvidenceRef                 string             `json:"evidence_ref"`
	ExpectedLatencyMilliseconds int64              `json:"expected_latency_milliseconds,omitempty"`
	Evaluation                  *RoutingEvaluation `json:"evaluation,omitempty"`
}

type RoutingEvaluation struct {
	Metric           string `json:"metric"`
	ScoreBasisPoints int    `json:"score_basis_points"`
	EvaluatorID      string `json:"evaluator_id"`
	EvidenceRef      string `json:"evidence_ref"`
}

func (s RoutingSignals) Validate() error {
	if s.Health != "READY" && s.Health != "UNAVAILABLE" || s.ObservedAt.IsZero() || s.ValidUntil.IsZero() || s.ObservedAt.Location() != time.UTC || s.ValidUntil.Location() != time.UTC || !s.ValidUntil.After(s.ObservedAt) || !ValidConnectionID(s.EvidenceRef) || s.ExpectedLatencyMilliseconds < 0 || s.ExpectedLatencyMilliseconds > 86400000 {
		return fmt.Errorf("invalid reviewed routing signals")
	}
	if e := s.Evaluation; e != nil && (!ValidConnectionID(e.Metric) || e.ScoreBasisPoints < 0 || e.ScoreBasisPoints > 10000 || !ValidConnectionID(e.EvaluatorID) || !ValidConnectionID(e.EvidenceRef)) {
		return fmt.Errorf("invalid reviewed routing evaluation")
	}
	return nil
}

func cloneRoutingSignals(s *RoutingSignals) *RoutingSignals {
	if s == nil {
		return nil
	}
	result := *s
	if s.Evaluation != nil {
		evaluation := *s.Evaluation
		result.Evaluation = &evaluation
	}
	return &result
}
func sameRoutingSignals(a, b *RoutingSignals) bool { return reflect.DeepEqual(a, b) }

func routingSignalsAllow(now time.Time, signals *RoutingSignals, request RouteRequirements) bool {
	if signals == nil {
		return !request.RequireHealthy && request.MaxLatencyMilliseconds == 0 && request.Evaluation == nil
	}
	if signals.Validate() != nil || signals.Health != "READY" || signals.ObservedAt.After(now) || !now.Before(signals.ValidUntil) {
		return false
	}
	if request.MaxLatencyMilliseconds > 0 && (signals.ExpectedLatencyMilliseconds == 0 || signals.ExpectedLatencyMilliseconds > request.MaxLatencyMilliseconds) {
		return false
	}
	if requirement := request.Evaluation; requirement != nil {
		evaluation := signals.Evaluation
		if evaluation == nil || evaluation.Metric != requirement.Metric || evaluation.ScoreBasisPoints < requirement.MinimumScoreBasisPoints {
			return false
		}
	}
	return true
}
