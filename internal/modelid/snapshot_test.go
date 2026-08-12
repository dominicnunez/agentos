package modelid

import "testing"

func TestHasDatedSnapshot(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		{name: "suffix", value: "gpt-5-2026-08-07", want: true},
		{name: "colon delimited", value: "provider:2026-08-07:profile", want: true},
		{name: "date only", value: "2026-08-07", want: true},
		{name: "invalid calendar date", value: "gpt-5-2026-02-31"},
		{name: "missing leading delimiter", value: "gpt2026-08-07"},
		{name: "missing trailing delimiter", value: "gpt-5-2026-08-07-preview"},
		{name: "short", value: "gpt-5"},
		{name: "empty"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := HasDatedSnapshot(test.value); got != test.want {
				t.Fatalf("HasDatedSnapshot(%q)=%t want=%t", test.value, got, test.want)
			}
		})
	}
}
