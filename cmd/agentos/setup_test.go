package main

import (
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/bootstrap"
	"github.com/dominicnunez/agentos/internal/inference"
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

func TestLoadOrBeginInitPersistsOneWayVersion1Upgrade(t *testing.T) {
	now := time.Now().UTC()
	paths, err := bootstrap.UserPaths(filepath.Join(t.TempDir(), "home"), filepath.Join(t.TempDir(), "run"), 1000)
	if err != nil {
		t.Fatal(err)
	}
	owner := bootstrap.Owner{Username: "alice", UID: 1000, GID: 1000}
	config := bootstrap.NewConfig(bootstrap.ModeUser, owner, paths, now)
	config.Version = 1
	provider := testOpenAIProvider(config, "gpt-test-2026-01-01", "openai-api-key")
	provider.InferencePolicy = inference.Policy{}
	config.Providers = []bootstrap.Provider{provider}
	state := bootstrap.State{Version: 1, Mode: bootstrap.ModeUser, Stage: bootstrap.StageReady, UpdatedAt: now}
	configPath := bootstrap.ConfigPath(paths)
	statePath := bootstrap.StatePath(paths)
	if err := bootstrap.SaveConfig(configPath, config); err != nil {
		t.Fatal(err)
	}
	if err := bootstrap.SaveState(statePath, state); err != nil {
		t.Fatal(err)
	}

	upgraded, upgradedState, err := loadOrBeginInit(bootstrap.ModeUser, owner, paths, configPath, statePath)
	if err != nil {
		t.Fatal(err)
	}
	if upgraded.Version != bootstrap.ConfigVersion || upgradedState.Version != bootstrap.ConfigVersion || upgradedState.Stage != bootstrap.StageProvider || len(upgraded.Providers) != 0 {
		t.Fatalf("version-1 checkpoint was not safely upgraded: config=%+v state=%+v", upgraded, upgradedState)
	}
	reloaded, err := bootstrap.LoadConfig(configPath)
	if err != nil || reloaded.Version != bootstrap.ConfigVersion || len(reloaded.Providers) != 0 {
		t.Fatalf("upgraded checkpoint was not persisted: config=%+v err=%v", reloaded, err)
	}
}
