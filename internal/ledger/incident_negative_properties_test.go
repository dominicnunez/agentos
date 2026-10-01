package ledger

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"testing"

	"github.com/dominicnunez/agentos/internal/boundaryjson"
	"github.com/dominicnunez/agentos/internal/events"
)

// Independent owner oracle: every exact AdmittedProjection error must remain a
// SQL candidate. Separately generated valid ordinary ASCII objects must be
// excluded, so an always-true discovery predicate cannot satisfy this test.
func TestIncidentNegativeDiscoveryProperty(t *testing.T) {
	store := projectionScopeFixture(t)
	stream, err := store.Events(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	var source events.Event
	for _, event := range stream {
		if event.EventType == "TEAM_CREATED" {
			source = event
		}
	}
	source.EventType = "AUDIT_NOTE"
	check := func(body []byte, exclude bool) error {
		source.Payload = body
		_, _, ownerErr := events.AdmittedProjection(source)
		if _, err := store.db.ExecContext(t.Context(), `UPDATE events SET event_type='AUDIT_NOTE',payload=? WHERE event_id=?`, body, source.EventID); err != nil {
			return err
		}
		var candidate bool
		if err := store.db.QueryRowContext(t.Context(), `SELECT `+incidentNegativeJSON+` FROM events WHERE event_id=?`, source.EventID).Scan(&candidate); err != nil {
			return err
		}
		if ownerErr != nil && !candidate {
			return fmt.Errorf("exact owner rejects an excluded source %q: %w", body, ownerErr)
		}
		if exclude && (ownerErr != nil || candidate) {
			if ownerErr != nil {
				return fmt.Errorf("valid ordinary source was not excluded %q: %w", body, ownerErr)
			}
			return fmt.Errorf("valid ordinary source was not excluded %q: candidate=%v", body, candidate)
		}
		return nil
	}
	t.Run("valid-ordinary", func(t *testing.T) {
		random := rand.New(rand.NewSource(217))
		for range 300 {
			body, err := json.Marshal(negativeASCIIObject(random, 0))
			if err != nil {
				t.Fatal(err)
			}
			if err := check(body, true); err != nil {
				t.Fatal(err)
			}
		}
	})
	t.Run("raw-controls-and-trailing", func(t *testing.T) {
		for control := 0; control < 128; control++ {
			for _, body := range [][]byte{
				append(append([]byte(`{"text":"`), byte(control)), []byte(`"}`)...),
				append([]byte(`{"text":"safe"}`), byte(control)),
				append([]byte{byte(control)}, []byte(`{"text":"safe"}`)...),
				append(append([]byte(`{"text":"safe"}`), byte(control)), []byte(`garbage`)...),
			} {
				if err := check(body, false); err != nil {
					t.Fatal(err)
				}
			}
		}
	})
	t.Run("reserved-ascii-casings", func(t *testing.T) {
		for _, field := range []string{"projection", "admission"} {
			for mask := 0; mask < 1<<len(field); mask++ {
				spelling := []byte(field)
				for i := range spelling {
					if mask&(1<<i) != 0 {
						spelling[i] -= 'a' - 'A'
					}
				}
				if err := check([]byte(`{"`+string(spelling)+`":{}}`), false); err != nil {
					t.Fatal(err)
				}
			}
		}
	})
	t.Run("invalid-bytes-escapes-and-depth", func(t *testing.T) {
		for _, body := range [][]byte{
			append(append([]byte(`{"text":"`), 0xff, 0xc0, 0x80), []byte(`"}`)...),
			[]byte(`{"\ud800":1,"\ud900":2}`), []byte(`{"id":1,"i\u0064":2}`),
			[]byte(`{"pr\u006fjection":{}}`), []byte(`{"text":{"key":1,"key":2}}`),
			[]byte(`{} {}`), []byte(`{}true`), []byte(`{"number":1e9999}`),
		} {
			if err := check(body, false); err != nil {
				t.Fatal(err)
			}
		}
		for depth := 60; depth <= 67; depth++ {
			body := []byte(`{"value":` + strings.Repeat(`[`, depth) + `0` + strings.Repeat(`]`, depth) + `}`)
			if err := check(body, false); err != nil {
				t.Fatal(err)
			}
		}
	})
	t.Run("bounded-byte-mutations", func(t *testing.T) {
		random := rand.New(rand.NewSource(4154025621))
		for range 500 {
			body, err := json.Marshal(negativeASCIIObject(random, 0))
			if err != nil {
				t.Fatal(err)
			}
			for range 1 + random.Intn(4) {
				body[random.Intn(len(body))] = byte(random.Intn(256))
			}
			if err := check(body, false); err != nil {
				t.Fatal(err)
			}
		}
	})
	t.Run("byte-boundary", func(t *testing.T) {
		for _, size := range []int{boundaryjson.MaximumBytes, boundaryjson.MaximumBytes + 1} {
			body := []byte(`{"text":"` + strings.Repeat("x", size-len(`{"text":""}`)) + `"}`)
			if err := check(body, size == boundaryjson.MaximumBytes); err != nil {
				t.Fatal(err)
			}
		}
	})
}

func negativeASCIIObject(random *rand.Rand, depth int) map[string]any {
	value := map[string]any{}
	for n := 0; n < 1+random.Intn(4); n++ {
		key := fmt.Sprintf("field%d", n)
		switch random.Intn(6) {
		case 0:
			value[key] = random.Intn(100000) - 50000
		case 1:
			value[key] = random.Intn(2) == 0
		case 2:
			value[key] = nil
		case 3:
			value[key] = []any{random.Intn(100), "ASCII", false, nil}
		case 4:
			if depth < 4 {
				value[key] = negativeASCIIObject(random, depth+1)
			} else {
				value[key] = "leaf"
			}
		default:
			chars := []byte(" letters0123456789\\\"\n\t\r\b\f")
			text := make([]byte, random.Intn(20))
			for i := range text {
				text[i] = chars[random.Intn(len(chars))]
			}
			value[key] = string(text)
		}
	}
	return value
}

func TestIncidentNegativeWindowNormalizationProperty(t *testing.T) {
	random := rand.New(rand.NewSource(2174154025621))
	covered := func(windows []incidentNegativeWindow, organization string, sequence int64) bool {
		for _, window := range windows {
			if (window.Organization == "" || window.Organization == organization) && sequence > window.After && sequence < window.Before {
				return true
			}
		}
		return false
	}
	for sample := 0; sample < 500; sample++ {
		windows := []incidentNegativeWindow{{After: 1, Before: 5}, {After: 5, Before: 9}}
		for range random.Intn(40) {
			windows = append(windows, incidentNegativeWindow{After: int64(random.Intn(40)), Before: int64(random.Intn(40)), Organization: []string{"", "org-1", "org-2"}[random.Intn(3)]})
		}
		before := append([]incidentNegativeWindow(nil), windows...)
		after := normalizeNegativeWindows(windows)
		for _, organization := range []string{"org-1", "org-2", "foreign"} {
			for sequence := int64(0); sequence <= 40; sequence++ {
				if covered(before, organization, sequence) != covered(after, organization, sequence) {
					t.Fatalf("normalization changed coverage sample=%d organization=%s sequence=%d: before=%v after=%v", sample, organization, sequence, before, after)
				}
			}
		}
	}
	// Touching open intervals must not acquire their shared excluded endpoint.
	touching := normalizeNegativeWindows([]incidentNegativeWindow{{After: 1, Before: 5}, {After: 5, Before: 9}})
	if len(touching) != 2 || covered(touching, "org-1", 5) {
		t.Fatal("touching intervals acquired their shared endpoint")
	}
	// Many starts pinned to the same roster must not multiply SQL window work.
	repeated := make([]incidentNegativeWindow, 16)
	for i := range repeated {
		repeated[i] = incidentNegativeWindow{After: 1, Before: 20 + int64(i)}
	}
	if normalized := normalizeNegativeWindows(repeated); len(normalized) != 1 {
		t.Fatalf("redundant roster windows retained: %v", normalized)
	}
}
