package modelinput

import "fmt"

// EvaluationRequirement names a reviewed metric; scores from different metrics
// are not interchangeable. Scores use integer basis points from 0 through 10000.
type EvaluationRequirement struct {
	Metric                  string `json:"metric"`
	MinimumScoreBasisPoints int    `json:"minimum_score_basis_points"`
}

func (r RouteRequirements) validateSignals() error {
	if r.MaxLatencyMilliseconds < 0 || r.MaxLatencyMilliseconds > 86400000 {
		return fmt.Errorf("routing latency bound is invalid")
	}
	if r.Evaluation != nil && (!validRoutingConnection(r.Evaluation.Metric) || r.Evaluation.MinimumScoreBasisPoints < 0 || r.Evaluation.MinimumScoreBasisPoints > 10000) {
		return fmt.Errorf("routing evaluation requirement is invalid")
	}
	return nil
}

func intersectRoutingSignals(base, rule RouteRequirements, result *RouteRequirements) error {
	result.RequireHealthy = base.RequireHealthy || rule.RequireHealthy
	if base.MaxLatencyMilliseconds > 0 && (result.MaxLatencyMilliseconds == 0 || base.MaxLatencyMilliseconds < result.MaxLatencyMilliseconds) {
		result.MaxLatencyMilliseconds = base.MaxLatencyMilliseconds
	}
	if base.Evaluation != nil {
		if rule.Evaluation != nil && base.Evaluation.Metric != rule.Evaluation.Metric {
			return fmt.Errorf("task rule cannot change evaluation metric")
		}
		requirement := *base.Evaluation
		if rule.Evaluation != nil {
			requirement.MinimumScoreBasisPoints = max(requirement.MinimumScoreBasisPoints, rule.Evaluation.MinimumScoreBasisPoints)
		}
		result.Evaluation = &requirement
	}
	return nil
}
