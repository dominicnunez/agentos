// Package replay deterministically reconstructs a payload-free incident
// timeline from a verified Event Contract snapshot. It never executes work,
// republishes an event, calls a model, or grants authority.
package replay

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/dominicnunez/agentos/internal/events"
)

const (
	SchemaVersion             = 1
	MaximumEvents             = 256
	MaximumReferencesPerEvent = 1024
	MaximumEnvelopeFieldBytes = 4096
	MaximumReportBytes        = 2 << 20
)

type Integrity struct {
	Algorithm    string `json:"algorithm"`
	Verification string `json:"verification"`
}

type Entry struct {
	Index             int       `json:"index"`
	EventID           string    `json:"event_id"`
	EventType         string    `json:"event_type"`
	RecordedAt        time.Time `json:"recorded_at"`
	SourceActorID     string    `json:"source_actor_id,omitempty"`
	SourceExecutionID string    `json:"source_execution_id,omitempty"`
	RecipientScope    string    `json:"recipient_scope,omitempty"`
	RecipientID       string    `json:"recipient_id,omitempty"`
	TaskID            string    `json:"task_id,omitempty"`
	AuthorizationRefs []string  `json:"authorization_refs"`
	ArtifactRefs      []string  `json:"artifact_refs"`
	PayloadSHA256     string    `json:"payload_sha256"`
	SchemaVersion     int       `json:"event_schema_version"`
}

type Link struct {
	FromEventID string `json:"from_event_id"`
	ToEventID   string `json:"to_event_id"`
	Kind        string `json:"kind"`
}

type Report struct {
	SchemaVersion  int       `json:"schema_version"`
	OrganizationID string    `json:"organization_id"`
	ConversationID string    `json:"conversation_id"`
	CorrelationID  string    `json:"-"`
	Integrity      Integrity `json:"integrity"`
	EventCount     int       `json:"event_count"`
	Entries        []Entry   `json:"entries"`
	Links          []Link    `json:"links"`
}

// Project returns the same report for the same verified snapshot. Links state
// only explicit order within the selected stream, Task, or execution; they are
// not a judgment about root cause.
func Project(snapshot events.VerifiedEventSnapshot, conversationID string) (Report, error) {
	if !validRequiredField(snapshot.OrganizationID) || !validRequiredField(snapshot.CorrelationID) || !validRequiredField(conversationID) {
		return Report{}, fmt.Errorf("incident replay requires organization and correlation identity")
	}
	if len(snapshot.Events) == 0 || len(snapshot.Events) > MaximumEvents {
		return Report{}, fmt.Errorf("incident replay requires 1 to %d events", MaximumEvents)
	}
	if snapshot.Algorithm != "SHA-256" || snapshot.LedgerEvents < int64(len(snapshot.Events)) || snapshot.LedgerSequence < 1 || !validRequiredField(snapshot.LedgerEventID) || !validSHA256(snapshot.LedgerSHA256) {
		return Report{}, fmt.Errorf("incident replay integrity head is invalid")
	}

	report := Report{
		SchemaVersion:  SchemaVersion,
		OrganizationID: snapshot.OrganizationID,
		ConversationID: conversationID,
		CorrelationID:  snapshot.CorrelationID,
		Integrity: Integrity{
			Algorithm:    snapshot.Algorithm,
			Verification: "COMPLETE_LEDGER_CHAIN",
		},
		EventCount: len(snapshot.Events),
		Entries:    make([]Entry, 0, len(snapshot.Events)),
		Links:      make([]Link, 0, len(snapshot.Events)*2),
	}
	seen := make(map[string]struct{}, len(snapshot.Events))
	lastTask := make(map[string]string)
	lastExecution := make(map[string]string)
	metadataBytes := len(snapshot.OrganizationID) + len(conversationID)
	var previous events.Event
	for index, event := range snapshot.Events {
		if err := validateEvent(snapshot, previous, event, seen); err != nil {
			return Report{}, err
		}
		metadataBytes += eventMetadataBytes(event)
		if metadataBytes > MaximumReportBytes {
			return Report{}, fmt.Errorf("incident replay metadata exceeds %d bytes", MaximumReportBytes)
		}
		seen[event.EventID] = struct{}{}
		digest := sha256.Sum256(event.Payload)
		report.Entries = append(report.Entries, Entry{
			Index:             index + 1,
			EventID:           event.EventID,
			EventType:         event.EventType,
			RecordedAt:        event.CreatedAt.UTC(),
			SourceActorID:     event.SourceActorID,
			SourceExecutionID: event.SourceExecutionID,
			RecipientScope:    event.RecipientScope,
			RecipientID:       event.RecipientID,
			TaskID:            event.TaskID,
			AuthorizationRefs: cloneStrings(event.AuthorizationRefs),
			ArtifactRefs:      cloneStrings(event.ArtifactRefs),
			PayloadSHA256:     hex.EncodeToString(digest[:]),
			SchemaVersion:     event.SchemaVersion,
		})
		if previous.EventID != "" {
			report.Links = append(report.Links, Link{FromEventID: previous.EventID, ToEventID: event.EventID, Kind: "STREAM_PREDECESSOR"})
		}
		if prior := lastTask[event.TaskID]; event.TaskID != "" && prior != "" {
			report.Links = append(report.Links, Link{FromEventID: prior, ToEventID: event.EventID, Kind: "TASK_PREDECESSOR"})
		}
		if prior := lastExecution[event.SourceExecutionID]; event.SourceExecutionID != "" && prior != "" {
			report.Links = append(report.Links, Link{FromEventID: prior, ToEventID: event.EventID, Kind: "EXECUTION_PREDECESSOR"})
		}
		if event.TaskID != "" {
			lastTask[event.TaskID] = event.EventID
		}
		if event.SourceExecutionID != "" {
			lastExecution[event.SourceExecutionID] = event.EventID
		}
		previous = event
	}
	if snapshot.Events[len(snapshot.Events)-1].Sequence > snapshot.LedgerSequence {
		return Report{}, fmt.Errorf("incident replay extends beyond its verified ledger head")
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		return Report{}, fmt.Errorf("encode incident replay: %w", err)
	}
	if len(encoded)+1 > MaximumReportBytes {
		return Report{}, fmt.Errorf("incident replay response exceeds %d bytes", MaximumReportBytes)
	}
	return report, nil
}

func validateEvent(snapshot events.VerifiedEventSnapshot, previous, event events.Event, seen map[string]struct{}) error {
	if event.EventID == "" || event.Sequence < 1 || event.EventType == "" || event.CreatedAt.IsZero() || event.SchemaVersion != events.SchemaVersion {
		return fmt.Errorf("incident replay event has an incomplete envelope")
	}
	if !validRequiredField(event.EventID) || !validRequiredField(event.EventType) ||
		!validOptionalField(event.SourceActorID) || !validOptionalField(event.SourceExecutionID) ||
		!validOptionalField(event.RecipientScope) || !validOptionalField(event.RecipientID) || !validOptionalField(event.TaskID) ||
		!validReferences(event.AuthorizationRefs) || !validReferences(event.ArtifactRefs) {
		return fmt.Errorf("incident replay event envelope exceeds its public bounds")
	}
	if event.OrganizationID != snapshot.OrganizationID || event.CorrelationID != snapshot.CorrelationID {
		return fmt.Errorf("incident replay crosses its organization or correlation boundary")
	}
	if previous.Sequence != 0 && event.Sequence <= previous.Sequence {
		return fmt.Errorf("incident replay event sequence is not strictly increasing")
	}
	if _, duplicate := seen[event.EventID]; duplicate {
		return fmt.Errorf("incident replay contains duplicate event %s", event.EventID)
	}
	return nil
}

func validRequiredField(value string) bool {
	return value != "" && len(value) <= MaximumEnvelopeFieldBytes && utf8.ValidString(value)
}

func validOptionalField(value string) bool {
	return value == "" || validRequiredField(value)
}

func validReferences(values []string) bool {
	if len(values) > MaximumReferencesPerEvent {
		return false
	}
	for _, value := range values {
		if !validRequiredField(value) {
			return false
		}
	}
	return true
}

func eventMetadataBytes(event events.Event) int {
	total := len(event.EventID) + len(event.EventType) + len(event.SourceActorID) + len(event.SourceExecutionID) +
		len(event.RecipientScope) + len(event.RecipientID) + len(event.TaskID)
	for _, value := range event.AuthorizationRefs {
		total += len(value)
	}
	for _, value := range event.ArtifactRefs {
		total += len(value)
	}
	return total
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func cloneStrings(values []string) []string {
	cloned := make([]string, len(values))
	copy(cloned, values)
	return cloned
}
