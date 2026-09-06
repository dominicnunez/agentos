package gateway

import (
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/boundaryjson"
)

func TestWorkContentValidatesStructureBeforeAuthorityInspection(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want error
	}{
		{"duplicate", `{"approval":{},"approval":{}}`, boundaryjson.ErrInvalid},
		{"invalid UTF-8", "{\"approval\":\"\xff\"}", boundaryjson.ErrInvalid},
		{"depth", `{"approval":` + strings.Repeat("[", boundaryjson.MaximumDepth+1) + "0" + strings.Repeat("]", boundaryjson.MaximumDepth+1) + "}", boundaryjson.ErrLimit},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequestWithContext(t.Context(), "POST", "/", strings.NewReader(tc.body))
			var target any
			err := decodeWorkContent(httptest.NewRecorder(), request, &target)
			if !errors.Is(err, tc.want) {
				t.Fatalf("structure must fail before authority inspection: got %v want %v", err, tc.want)
			}
		})
	}
}
