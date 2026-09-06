package modelinput

import (
	"context"
	"encoding/json"
	"unicode/utf8"
)

type invocationKey struct{}

// InvocationScope derives a replayable, unambiguous namespace from admitted
// runtime identities. It conveys no authority by itself.
func InvocationScope(organizationID, executionID string) (string, error) {
	if organizationID == "" || executionID == "" || len(organizationID) > 512 || len(executionID) > 512 || !utf8.ValidString(organizationID) || !utf8.ValidString(executionID) {
		return "", ErrInvalid
	}
	body, err := json.Marshal([]string{organizationID, executionID})
	if err != nil {
		return "", ErrInvalid
	}
	return TextDigest(string(body)), nil
}

func WithInvocation(ctx context.Context, organizationID, executionID string) (context.Context, error) {
	if ctx == nil {
		return nil, ErrInvalid
	}
	scope, err := InvocationScope(organizationID, executionID)
	if err != nil {
		return nil, err
	}
	return context.WithValue(ctx, invocationKey{}, scope), nil
}

func Invocation(ctx context.Context) (string, error) {
	if ctx == nil {
		return "", ErrInvalid
	}
	scope, ok := ctx.Value(invocationKey{}).(string)
	if !ok || !validReference(scope) {
		return "", ErrInvalid
	}
	return scope, nil
}

func BindContext(ctx context.Context, request Request) (*Binding, error) {
	scope, err := Invocation(ctx)
	if err != nil {
		return nil, err
	}
	return Bind(scope, request)
}

// ValidateBinding rejects a request whose handles were minted for other input
// or another admitted invocation. Missing handles are not a compatibility mode.
func ValidateBinding(scope string, request Request) error {
	if len(request.Messages) > MaximumMessages {
		return ErrLimit
	}
	if err := request.Validate(); err != nil {
		return err
	}
	unbound := request
	unbound.Messages = append([]Message(nil), request.Messages...)
	for i := range unbound.Messages {
		unbound.Messages[i].Source.Handle = ""
	}
	binding, err := Bind(scope, unbound)
	if err != nil {
		return err
	}
	for i, message := range request.Messages {
		if message.Source.Handle != binding.request.Messages[i].Source.Handle {
			return ErrReference
		}
	}
	return nil
}
