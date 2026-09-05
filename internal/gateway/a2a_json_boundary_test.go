package gateway

import "testing"

func TestA2ARejectsAmbiguousEnvelopeAndNestedParameters(t *testing.T) {
	for _, input := range []string{
		`{"jsonrpc":"2.0","id":1,"method":"GetTask","method":"SendMessage","params":{"id":"task"}}`,
		`{"JSONRPC":"2.0","id":1,"method":"GetTask","params":{"id":"task"}}`,
		`{"jsonrpc":"2.0","id":1,"method":"GetTask","params":{"id":"first","id":"second"}}`,
		`{"jsonrpc":"2.0","id":1,"method":"GetTask","params":{"ID":"task"}}`,
	} {
		if validateA2ARequest([]byte(input)) == nil {
			t.Fatal("ambiguous A2A request accepted")
		}
	}
	if err := validateA2ARequest([]byte(`{"jsonrpc":"2.0","id":9007199254740993,"method":"GetTask","params":{"id":"task"}}`)); err != nil {
		t.Fatalf("valid exact request rejected: %v", err)
	}
}
