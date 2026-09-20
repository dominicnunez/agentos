package replay

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/dominicnunez/agentos/internal/events"
)

// Containment is observational evidence, not permission to retry work or proof
// of when a remote action occurred. Admission is a durable local boundary.
type Containment struct {
	Holds               []HoldBoundary  `json:"holds"`
	Stops               []StopEvidence  `json:"stops"`
	Effects             []EffectHistory `json:"effects"`
	AdmissionScope      string          `json:"admission_scope"`
	UnclassifiedActions []string        `json:"unclassified_actions"`
}

type HoldBoundary struct {
	EventRef       string      `json:"event_ref"`
	Frozen         bool        `json:"frozen"`
	PriorEventRef  string      `json:"prior_event_ref,omitempty"`
	Control        string      `json:"control"`
	LastAdmissions []Admission `json:"last_admissions"`
}

type Admission struct {
	EventRef    string `json:"event_ref"`
	Kind        string `json:"kind"`
	TaskID      string `json:"task_id,omitempty"`
	ExecutionID string `json:"execution_id,omitempty"`
}

type StopEvidence struct {
	RequestRef  string       `json:"request_ref"`
	TaskID      string       `json:"task_id"`
	ExecutionID string       `json:"execution_id"`
	StartRef    string       `json:"start_ref"`
	Reason      string       `json:"reason"`
	HoldRef     string       `json:"hold_ref,omitempty"`
	Results     []StopResult `json:"results"`
}

type StopResult struct {
	EventRef   string            `json:"event_ref"`
	LocalState string            `json:"local_state"`
	UsageRef   string            `json:"usage_ref,omitempty"`
	OutcomeRef string            `json:"outcome_ref,omitempty"`
	FinishRef  string            `json:"finish_ref,omitempty"`
	Provider   *ProviderEvidence `json:"provider,omitempty"`
}

type ProviderEvidence struct {
	LocalTurnStopped          bool   `json:"local_turn_stopped"`
	LocalProcessStopAttempted bool   `json:"local_process_stop_attempted"`
	LocalProcessStopped       bool   `json:"local_process_stopped"`
	RemoteStatus              string `json:"remote_status"`
}

type EffectHistory struct {
	EffectID  string        `json:"effect_id"`
	TaskID    string        `json:"task_id"`
	LinkScope string        `json:"link_scope"`
	States    []EffectState `json:"states"`
}

type EffectState struct {
	EventRef           string   `json:"event_ref"`
	Status             string   `json:"status"`
	AttemptCount       int      `json:"attempt_count"`
	ConfirmationRefs   []string `json:"confirmation_refs"`
	ReconciliationRefs []string `json:"reconciliation_refs"`
}

// ProjectIncident keeps the ordinary Work boundary intact while admitting only
// the explicitly related tenant hold and task-effect evidence of this snapshot.
func ProjectIncident(snapshot events.IncidentSnapshot, conversationID string) (Report, error) {
	if _, err := Project(snapshot.Work, conversationID); err != nil {
		return Report{}, err
	}
	combined := snapshot.Work
	combined.Events = append([]events.Event(nil), snapshot.Work.Events...)
	related := make(map[string]bool, len(snapshot.RelatedEvents))
	tasks, err := events.ValidateIncidentHistory(snapshot)
	if err != nil {
		return Report{}, err
	}
	for _, event := range snapshot.RelatedEvents {
		if event.EventType != "FREEZE_SET" && (event.EventType != "EFFECT_OBLIGATION_TRANSITIONED" || (tasks[event.TaskID] == 0 || event.Sequence <= tasks[event.TaskID])) {
			return Report{}, fmt.Errorf("incident has unrelated evidence")
		}
		related[event.EventID] = true
		combined.Events = append(combined.Events, event)
	}
	sort.Slice(combined.Events, func(i, j int) bool { return combined.Events[i].Sequence < combined.Events[j].Sequence })
	report, err := project(combined, conversationID, related)
	if err != nil {
		return Report{}, err
	}
	_, freezes, err := events.ResolveFreezeHistory(combined.Events, snapshot.FreezeRecords)
	if err != nil {
		return Report{}, err
	}
	if err := events.ValidateModelStops(combined.Events, freezes); err != nil {
		return Report{}, err
	}
	if err := events.ValidateExecutionStops(combined.Events, freezes); err != nil {
		return Report{}, err
	}
	if err := events.ValidateSecurityHoldOutcomes(combined.Events, freezes); err != nil {
		return Report{}, err
	}
	if _, err := events.IncidentAdmissions(combined.Events, nil, freezes); err != nil {
		return Report{}, err
	}
	view := &Containment{Holds: []HoldBoundary{}, Stops: []StopEvidence{}, Effects: []EffectHistory{}, AdmissionScope: "CONTAINMENT_BOUNDARIES", UnclassifiedActions: []string{"COORDINATION", "OUTPUT"}}
	for _, freeze := range freezes {
		hold := HoldBoundary{EventRef: freeze.EventRef, Frozen: freeze.Frozen, Control: "LEGACY", LastAdmissions: []Admission{}}
		if freeze.Control != nil {
			hold.Control = "EXPLICIT_OWNER"
			hold.PriorEventRef = freeze.Control.PriorEventRef
		}
		view.Holds = append(view.Holds, hold)
	}
	if err := collectAdmissions(view, snapshot.Admissions, combined.Events); err != nil {
		return Report{}, err
	}
	if err := collectStops(view, combined.Events); err != nil {
		return Report{}, err
	}
	if err := collectEffects(view, combined.Events, tasks); err != nil {
		return Report{}, err
	}
	report.Containment = view
	body, err := json.Marshal(report)
	if err != nil || len(body)+1 > MaximumReportBytes {
		return Report{}, fmt.Errorf("incident containment response exceeds its bounds")
	}
	return report, nil
}

func collectAdmissions(view *Containment, admissions []events.IncidentAdmission, stream []events.Event) error {
	byRef := make(map[string]events.IncidentAdmission, len(admissions))
	for _, admission := range admissions {
		if _, exists := byRef[admission.EventRef]; exists {
			return fmt.Errorf("incident repeats an admission boundary")
		}
		byRef[admission.EventRef] = admission
	}
	latest := map[string]Admission{}
	holds := map[string]int{}
	for i, hold := range view.Holds {
		holds[hold.EventRef] = i
	}
	for _, event := range stream {
		if index, found := holds[event.EventID]; found && view.Holds[index].Frozen {
			for _, kind := range []string{"EXECUTION_START", "INFERENCE_RESERVATION", "EFFECT_ATTEMPT"} {
				if admission, found := latest[kind]; found {
					view.Holds[index].LastAdmissions = append(view.Holds[index].LastAdmissions, admission)
				}
			}
		}
		admission, found := byRef[event.EventID]
		derived, valid, err := events.AdmissionForIncident(event)
		if err != nil {
			return err
		}
		if found != valid {
			return fmt.Errorf("incident admission set differs from its durable events")
		}
		if !valid {
			continue
		}
		if admission != derived || !validOptionalField(admission.ExecutionID) {
			return fmt.Errorf("incident admission crosses its recorded boundary")
		}
		latest[admission.Kind] = Admission{EventRef: event.EventID, Kind: admission.Kind, TaskID: admission.TaskID, ExecutionID: admission.ExecutionID}
		delete(byRef, event.EventID)
	}
	if len(byRef) != 0 {
		return fmt.Errorf("incident admission refers outside its snapshot")
	}
	return nil
}

func collectStops(view *Containment, stream []events.Event) error {
	requests := map[string]int{}
	byID := make(map[string]events.Event, len(stream))
	for _, event := range stream {
		byID[event.EventID] = event
		switch event.EventType {
		case "MODEL_STOP_REQUESTED", "EXECUTION_STOP_REQUESTED":
			item := StopEvidence{RequestRef: event.EventID, TaskID: event.TaskID, ExecutionID: event.SourceExecutionID, Results: []StopResult{}}
			if event.EventType == "MODEL_STOP_REQUESTED" {
				var detail events.ModelStopRequest
				if err := json.Unmarshal(event.Payload, &detail); err != nil {
					return err
				}
				item.StartRef, item.Reason = detail.ContextEventRef, detail.ReasonClass
				if detail.Hold != nil {
					item.HoldRef = detail.Hold.EventRef
				}
			} else {
				var detail events.ExecutionStopRequest
				if err := json.Unmarshal(event.Payload, &detail); err != nil {
					return err
				}
				item.StartRef, item.Reason = detail.ExecutionStartRef, detail.ReasonClass
				if detail.Hold != nil {
					item.HoldRef = detail.Hold.EventRef
				}
			}
			requests[event.EventID] = len(view.Stops)
			view.Stops = append(view.Stops, item)
		case "MODEL_STOP_UNCERTAIN", "MODEL_STOP_CONFIRMED", "EXECUTION_STOP_UNCERTAIN", "EXECUTION_STOP_CONFIRMED":
			var detail struct {
				StopRequestRef  string            `json:"stop_request_ref"`
				LocalState      string            `json:"local_state"`
				UsageEventRef   string            `json:"usage_event_ref"`
				OutcomeEventRef string            `json:"outcome_event_ref"`
				FinishEventRef  string            `json:"finish_event_ref"`
				ProviderStop    *ProviderEvidence `json:"provider_stop"`
			}
			if err := json.Unmarshal(event.Payload, &detail); err != nil {
				return err
			}
			index, ok := requests[detail.StopRequestRef]
			if !ok {
				return fmt.Errorf("incident stop result has no prior request")
			}
			state := detail.LocalState
			if event.EventType == "MODEL_STOP_UNCERTAIN" || event.EventType == "EXECUTION_STOP_UNCERTAIN" {
				state = "UNCERTAIN"
			}
			if event.EventType == "EXECUTION_STOP_CONFIRMED" {
				state = "RETURNED"
				var outcome struct {
					ObservedEffect struct {
						ProviderStop *ProviderEvidence `json:"provider_stop"`
					} `json:"observed_effect"`
				}
				if err := json.Unmarshal(byID[detail.OutcomeEventRef].Payload, &outcome); err != nil {
					return err
				}
				detail.ProviderStop = outcome.ObservedEffect.ProviderStop
			}
			view.Stops[index].Results = append(view.Stops[index].Results, StopResult{EventRef: event.EventID, LocalState: state, UsageRef: detail.UsageEventRef, OutcomeRef: detail.OutcomeEventRef, FinishRef: detail.FinishEventRef, Provider: detail.ProviderStop})
		}
	}
	return nil
}

func collectEffects(view *Containment, stream []events.Event, tasks map[string]int64) error {
	effects, err := events.IncidentEffects(stream, tasks)
	if err != nil {
		return err
	}
	indices := map[string]int{}
	for _, effect := range effects {
		if !validRequiredField(effect.EffectID) || !validReferences(effect.ConfirmationRefs) || !validReferences(effect.ReconciliationRefs) {
			return fmt.Errorf("incident effect evidence has invalid public bounds")
		}
		index, found := indices[effect.EffectID]
		if !found {
			index = len(view.Effects)
			indices[effect.EffectID] = index
			view.Effects = append(view.Effects, EffectHistory{EffectID: effect.EffectID, TaskID: effect.TaskID, LinkScope: "TASK", States: []EffectState{}})
		}
		view.Effects[index].States = append(view.Effects[index].States, EffectState{EventRef: effect.EventRef, Status: effect.Status, AttemptCount: effect.AttemptCount, ConfirmationRefs: cloneStrings(effect.ConfirmationRefs), ReconciliationRefs: cloneStrings(effect.ReconciliationRefs)})
	}
	return nil
}
