// Package modelid owns provider-model identifier primitives shared by setup
// validation and runtime adapters.
package modelid

import "time"

// HasDatedSnapshot reports whether value contains a complete YYYY-MM-DD
// snapshot segment. A segment may begin the identifier or follow '-' or ':',
// and it must end the identifier or precede ':'.
func HasDatedSnapshot(value string) bool {
	const dateLength = len("2006-01-02")
	for start := 0; start+dateLength <= len(value); start++ {
		end := start + dateLength
		if start > 0 && value[start-1] != '-' && value[start-1] != ':' {
			continue
		}
		if end < len(value) && value[end] != ':' {
			continue
		}
		if _, err := time.Parse("2006-01-02", value[start:end]); err == nil {
			return true
		}
	}
	return false
}
