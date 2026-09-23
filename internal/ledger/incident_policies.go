package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"github.com/dominicnunez/agentos/internal/events"
	"github.com/dominicnunez/agentos/internal/inference"
)

// Load complete selected connection histories, including inverse activation
// references. A missing replacement row must not make an old policy look active.
// All rows and events consume the caller's aggregate support budget before load.
func loadIncidentPolicyHistory(ctx context.Context, tx *sql.Tx, organization string, fingerprints []string, budget *incidentBudget, support *incidentInferenceSupport) error {
	connections := map[string]bool{}
	for _, row := range support.reservations {
		if row.connectionID != "" {
			connections[row.connectionID] = true
		}
	}
	ids := make([]string, 0, len(connections))
	for id := range connections {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	args := []any{organization}
	for _, fingerprint := range fingerprints {
		args = append(args, fingerprint)
	}
	where := `p.organization_id=? AND (p.policy_fingerprint IN (` + incidentMarks(len(fingerprints)) + `)`
	if len(ids) != 0 {
		where += ` OR p.connection_id IN (` + incidentMarks(len(ids)) + `) OR CASE WHEN json_valid(p.body) THEN json_extract(p.body,'$.connection_id') END IN (` + incidentMarks(len(ids)) + `)`
		for range 2 {
			for _, id := range ids {
				args = append(args, id)
			}
		}
	}
	where += `)`
	var count int
	var size int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(bytes),0) FROM (SELECT `+incidentInferencePolicyBytes+`+length(CAST(p.policy_fingerprint AS BLOB)) AS bytes FROM inference_policies p WHERE `+where+` LIMIT ?)`, append(args, budget.events+1)...).Scan(&count, &size); err != nil {
		return err
	}
	if count > budget.events || size > budget.bytes {
		return fmt.Errorf("incident inference policy history exceeds support limit")
	}
	budget.events -= count
	budget.bytes -= size
	type storedPolicy struct {
		fingerprint, activation, connection string
		body                                []byte
		active                              int
	}
	stored := make([]storedPolicy, 0, count)
	rows, err := tx.QueryContext(ctx, `SELECT p.policy_fingerprint,p.body,p.activation_event_id,p.connection_id,p.active FROM inference_policies p WHERE `+where+` ORDER BY p.policy_fingerprint`, args...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var policy storedPolicy
		if err := rows.Scan(&policy.fingerprint, &policy.body, &policy.activation, &policy.connection, &policy.active); err != nil {
			return err
		}
		stored = append(stored, policy)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	activationArgs := []any{organization}
	activationWhere := `organization_id=? AND (event_id IN (SELECT p.activation_event_id FROM inference_policies p WHERE ` + where + `)`
	activationArgs = append(activationArgs, args...)
	if len(ids) != 0 {
		activationWhere += ` OR (event_type='INFERENCE_POLICY_ACTIVATED' AND CASE WHEN json_valid(payload) THEN json_extract(payload,'$.connection_id') END IN (` + incidentMarks(len(ids)) + `))`
		for _, id := range ids {
			activationArgs = append(activationArgs, id)
		}
	}
	activationWhere += `)`
	activations, err := incidentEvents(ctx, tx, budget, activationWhere, activationArgs...)
	if err != nil {
		return fmt.Errorf("incident inference supporting evidence exceeds byte limit or lacks activation: %w", err)
	}
	if len(activations) != len(stored) {
		return fmt.Errorf("incident inference policy history lacks exact activation backing")
	}
	byID := make(map[string]events.Event, len(activations))
	for _, event := range activations {
		byID[event.EventID] = event
	}
	support.histories = map[string][]inferencePolicyRevision{}
	activeSequence := map[string]int64{}
	activeCount := map[string]int{}
	for _, record := range stored {
		activation, found := byID[record.activation]
		if !found {
			return fmt.Errorf("incident inference policy history lacks exact activation backing")
		}
		delete(byID, record.activation)
		policy, err := validateIncidentInferencePolicy(organization, record.fingerprint, record.connection, record.body, activation)
		if err != nil {
			return err
		}
		support.policies[[2]string{organization, record.fingerprint}] = incidentPolicy{value: policy, sequence: activation.Sequence}
		if policy.Version != inference.ConnectionPolicyVersion {
			continue
		}
		if record.active != 0 && record.active != 1 {
			return fmt.Errorf("incident inference policy active state is invalid")
		}
		support.histories[record.connection] = append(support.histories[record.connection], inferencePolicyRevision{fingerprint: record.fingerprint, sequence: activation.Sequence, authorizedAt: policy.AuthorizedAt})
		if record.active == 1 {
			activeCount[record.connection]++
			activeSequence[record.connection] = activation.Sequence
		}
	}
	if len(byID) != 0 {
		return fmt.Errorf("incident inference activation lacks its policy record")
	}
	for connection, history := range support.histories {
		sort.Slice(history, func(i, j int) bool { return history[i].sequence < history[j].sequence })
		for i, revision := range history {
			if revision.sequence < 1 || i > 0 && (history[i-1].sequence >= revision.sequence || !revision.authorizedAt.After(history[i-1].authorizedAt)) {
				return fmt.Errorf("incident connection policy replacement history is invalid")
			}
		}
		if activeCount[connection] != 1 || activeSequence[connection] != history[len(history)-1].sequence {
			return fmt.Errorf("incident connection policy history has no unique current active revision")
		}
	}
	return nil
}
