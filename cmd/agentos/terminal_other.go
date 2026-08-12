//go:build !linux

package main

import (
	"fmt"
	"os"
)

type terminalState struct{}

func readTerminalEscape(_ *os.File) ([2]byte, bool, error) { return [2]byte{}, false, nil }

func isTerminal(_ int) bool { return false }
func makeTerminalRaw(_ int) (*terminalState, error) {
	return nil, fmt.Errorf("Linux terminal required")
}
func restoreTerminal(_ int, _ *terminalState) error { return nil }
func readTerminalSecret(_ int) ([]byte, error)      { return nil, fmt.Errorf("Linux terminal required") }
