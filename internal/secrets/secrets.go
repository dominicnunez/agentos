// Package secrets keeps credential resolution behind runtime adapters.
package secrets

import (
	"context"
	"fmt"
	"os"
)

type Ref string
type Value string
type Source interface {
	Resolve(context.Context, Ref) (Value, error)
}
type Environment struct{ Prefix string }

func (e Environment) Resolve(_ context.Context, ref Ref) (Value, error) {
	v, ok := os.LookupEnv(e.Prefix + string(ref))
	if !ok {
		return "", fmt.Errorf("secret %q is unavailable", ref)
	}
	return Value(v), nil
}
