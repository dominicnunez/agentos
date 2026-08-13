package core

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"
)

// ValidGoalReferenceID keeps Goal identifiers admitted through untrusted
// natural-language intake unambiguous and safe to match as exact tokens.
func ValidGoalReferenceID(value string) bool {
	if value == "" || len(value) > 256 {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '-' || character == '_' || character == '.' || character == ':' {
			continue
		}
		return false
	}
	return true
}

// FingerprintIntentDraft binds confirmation to the complete canonical draft,
// including its version and creation time, while excluding the fingerprint
// field itself.
func FingerprintIntentDraft(draft IntentDraft) (string, error) {
	draft.Fingerprint = ""
	draft.CreatedAt = draft.CreatedAt.UTC()
	body, err := json.Marshal(draft)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(body)
	return hex.EncodeToString(hash[:]), nil
}

// ValidateAcceptedIntentDraft enforces the domain invariants shared by the
// application service and durable confirmation admission. It does not grant
// authority: the caller must separately authenticate and authorize the actor.
func ValidateAcceptedIntentDraft(draft IntentDraft, organizationID ID, kind ExecutionKind) error {
	if draft.ID == "" || draft.OrganizationID != organizationID || draft.Version < 1 || draft.Status != IntentStatusReadyForReview || draft.RequestedExecutionKind != kind {
		return fmt.Errorf("identity, version, review state, and requested execution kind must match")
	}
	if strings.TrimSpace(draft.Objective) == "" || len(draft.Deliverables) == 0 || len(draft.CompletionCriteria) == 0 || len(draft.MissingUserInputs) != 0 {
		return fmt.Errorf("objective, deliverables, and completion criteria are required and missing user inputs are forbidden")
	}
	groups := [][]IntentValue{draft.Context, draft.Deliverables, draft.CompletionCriteria, draft.Constraints}
	if draft.Goal != nil {
		if _, err := AcceptedIntentGoalID(draft); err != nil {
			return err
		}
		groups = append(groups, []IntentValue{*draft.Goal})
	}
	for _, group := range groups {
		for _, value := range group {
			if strings.TrimSpace(value.Value) == "" || strings.TrimSpace(value.Origin) == "" {
				return fmt.Errorf("accepted intent values require content and provenance")
			}
		}
	}
	expected, err := FingerprintIntentDraft(draft)
	if err != nil {
		return err
	}
	if draft.Fingerprint == "" || draft.Fingerprint != expected {
		return fmt.Errorf("fingerprint does not bind the accepted intent")
	}
	return nil
}

// AcceptedIntentGoalID validates the Goal identity and provenance shape in an
// accepted Intent. Source-message content is checked at durable admission.
func AcceptedIntentGoalID(draft IntentDraft) (ID, error) {
	if draft.Goal == nil {
		return "", nil
	}
	value := draft.Goal.Value
	if !ValidGoalReferenceID(value) {
		return "", fmt.Errorf("accepted Intent Goal identity is invalid")
	}
	switch draft.Goal.Origin {
	case "EXPLICIT", "CONFIRMED":
		if draft.Goal.SourceMessageID == "" {
			return "", fmt.Errorf("accepted Intent Goal requires source-message provenance")
		}
	case "POLICY":
		if draft.Goal.SourceMessageID != "" {
			return "", fmt.Errorf("policy-selected Goal cannot claim operator-message provenance")
		}
	default:
		return "", fmt.Errorf("accepted Intent Goal provenance is invalid")
	}
	return ID(value), nil
}

// ContainsExactGoalReference recognizes a Goal ID only as a complete token.
// This prevents one Goal from being selected by a prefix or substring match.
func ContainsExactGoalReference(text, goalID string) bool {
	for _, field := range strings.Fields(text) {
		token := field
		for {
			if goalTokenMatches(token, goalID) {
				return true
			}
			leading, size := utf8.DecodeRuneInString(token)
			if !isGoalLeadingDelimiter(leading) {
				break
			}
			token = token[size:]
		}
	}
	return false
}

func goalTokenMatches(token, goalID string) bool {
	if !strings.HasPrefix(token, goalID) {
		return false
	}
	remainder := token[len(goalID):]
	if remainder == "" {
		return true
	}
	boundary, _ := utf8.DecodeRuneInString(remainder)
	return !isGoalIDCharacter(boundary) && isGoalTrailingDelimiter(boundary)
}

func isGoalIDCharacter(character rune) bool {
	return character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
		character >= '0' && character <= '9' || character == '-' || character == '_' || character == '.' || character == ':'
}

func isGoalLeadingDelimiter(character rune) bool {
	return strings.ContainsRune("([{<\"'“‘", character)
}

func isGoalTrailingDelimiter(character rune) bool {
	return strings.ContainsRune(".,:;!?…)]}>\"'”’", character)
}
