package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestSafeTerminalTextRemovesControlAndDirectionOverrides(t *testing.T) {
	input := "candidate\x1b[31m\u202Etxt\nsecond\tcolumn"
	output := safeTerminalText(input)
	if strings.ContainsAny(output, "\x1b\u202E") || output != "candidate[31mtxt\nsecond\tcolumn" {
		t.Fatalf("sanitized terminal output=%q", output)
	}
}

func TestSafeTerminalLineCannotForgeAdditionalFields(t *testing.T) {
	got := safeTerminalLine("approved\nFingerprint: forged\tvalue")
	if strings.ContainsAny(got, "\r\n\t") || got != "approved Fingerprint: forged value" {
		t.Fatalf("unsafe single-line rendering %q", got)
	}
}

func TestDrawOptionsUsesBoundedScrollingWindow(t *testing.T) {
	options := make([]string, 30)
	visible := make([]int, 30)
	for index := range options {
		options[index] = fmt.Sprintf("choice-%02d", index)
		visible[index] = index
	}
	var output strings.Builder
	if err := drawOptions(&output, "Select:", options, visible, 20, ""); err != nil {
		t.Fatal(err)
	}
	if strings.Count(output.String(), "choice-") != 12 || !strings.Contains(output.String(), "Showing 10-21 of 30") {
		t.Fatalf("menu was not bounded:\n%s", output.String())
	}
}
