package core

// ValidInferenceConnectionID accepts a bounded configuration identifier, never
// a URL, credential, path, or provider-returned model name.
func ValidInferenceConnectionID(value string) bool {
	if len(value) == 0 || len(value) > 128 {
		return false
	}
	for _, c := range value {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' && c != '_' {
			return false
		}
	}
	return true
}
