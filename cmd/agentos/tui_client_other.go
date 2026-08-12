//go:build !linux

package main

import (
	"fmt"
	"net/http"
)

func localHTTPClient(_ string) (*http.Client, error) {
	return nil, fmt.Errorf("Agent OS V1 local user access is supported on Linux")
}
