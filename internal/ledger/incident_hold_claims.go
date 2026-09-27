package ledger

// A shared hold does not connect otherwise independent executions in its own
// organization. A foreign execution claiming a selected hold is conflicting
// evidence, however, even if its claimed hold organization also lies.
// Requests own hold directly. Tool outcomes own interruption holds under the
// legacy security_hold contract or the modern runtime-containment contract.
func incidentForeignHoldClaim(refs int) string {
	return `organization_id<>? AND event_type IN ('EXECUTION_STOP_REQUESTED','MODEL_STOP_REQUESTED','TOOL_OUTCOME_RECORDED') AND
EXISTS (WITH nodes AS MATERIALIZED (SELECT id,parent,key,value FROM json_tree(CASE WHEN json_valid(payload) THEN payload ELSE '{}' END))
SELECT 1 FROM nodes leaf JOIN nodes hold ON hold.id=leaf.parent
LEFT JOIN nodes effect ON effect.id=hold.parent AND effect.parent=0
WHERE leaf.key='event_ref' AND leaf.value IN (` + incidentMarks(refs) + `) AND hold.key='hold' AND (
(event_type IN ('EXECUTION_STOP_REQUESTED','MODEL_STOP_REQUESTED') AND hold.parent=0) OR
(event_type='TOOL_OUTCOME_RECORDED' AND effect.key='observed_effect' AND (
EXISTS (SELECT 1 FROM nodes WHERE parent=0 AND key='error_class' AND value='security_hold') OR
(EXISTS (SELECT 1 FROM nodes WHERE parent=0 AND key='tool_id' AND value='runtime-containment') AND
EXISTS (SELECT 1 FROM nodes WHERE parent=effect.id AND key IN ('stop_request_ref','provider_stop')))))))`
}
