package events

// IncidentTaskAdmissions validates a self-contained Work history. Readers with
// private dependencies use ValidateIncidentHistory instead.
func IncidentTaskAdmissions(stream []Event) (map[string]int64, error) {
	snapshot := IncidentSnapshot{Work: VerifiedEventSnapshot{Events: stream}}
	if len(stream) != 0 {
		snapshot.Work.OrganizationID = stream[0].OrganizationID
		snapshot.Work.CorrelationID = stream[0].CorrelationID
	}
	return ValidateIncidentHistory(snapshot)
}
