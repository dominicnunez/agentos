package main

import (
	"fmt"
	"slices"
	"testing"
)

type pathSetupUITestDouble struct {
	selected int
	entered  string
	label    string
	options  []string
	lineUsed bool
}

func (u *pathSetupUITestDouble) selectOne(label string, options []string) (int, error) {
	u.label = label
	u.options = append([]string{}, options...)
	return u.selected, nil
}

func (u *pathSetupUITestDouble) line(_ string, _ bool) (string, error) {
	u.lineUsed = true
	if u.entered == "" {
		return "", fmt.Errorf("no test input")
	}
	return u.entered, nil
}

func TestSelectDetectedPath(t *testing.T) {
	t.Run("uses detected choice", func(t *testing.T) {
		detected := []string{"/usr/bin/codex", "/opt/codex"}
		ui := &pathSetupUITestDouble{selected: 1}
		got, err := selectDetectedPath(ui, "Select Codex:", "Codex path:", detected)
		if err != nil || got != detected[1] || ui.lineUsed {
			t.Fatalf("path=%q lineUsed=%t err=%v", got, ui.lineUsed, err)
		}
		wantOptions := []string{"/usr/bin/codex", "/opt/codex", "Enter another path..."}
		if ui.label != "Select Codex:" || !slices.Equal(ui.options, wantOptions) || len(detected) != 2 {
			t.Fatalf("label=%q options=%v detected=%v", ui.label, ui.options, detected)
		}
	})

	t.Run("accepts manual alternative", func(t *testing.T) {
		ui := &pathSetupUITestDouble{selected: 1, entered: "/custom/codex"}
		got, err := selectDetectedPath(ui, "Select Codex:", "Codex path:", []string{"/usr/bin/codex"})
		if err != nil || got != ui.entered || !ui.lineUsed {
			t.Fatalf("path=%q lineUsed=%t err=%v", got, ui.lineUsed, err)
		}
	})

	t.Run("requests input when none detected", func(t *testing.T) {
		ui := &pathSetupUITestDouble{entered: "/custom/codex"}
		got, err := selectDetectedPath(ui, "Select Codex:", "Codex path:", nil)
		if err != nil || got != ui.entered || !ui.lineUsed || ui.options != nil {
			t.Fatalf("path=%q lineUsed=%t options=%v err=%v", got, ui.lineUsed, ui.options, err)
		}
	})

	t.Run("rejects invalid selection", func(t *testing.T) {
		ui := &pathSetupUITestDouble{selected: 3}
		if _, err := selectDetectedPath(ui, "Select Codex:", "Codex path:", []string{"/usr/bin/codex"}); err == nil {
			t.Fatal("invalid selection was accepted")
		}
	})
}
