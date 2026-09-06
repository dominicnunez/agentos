//go:build linux

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dominicnunez/agentos/internal/bootstrap"
	"github.com/dominicnunez/agentos/internal/execution"
	"github.com/dominicnunez/agentos/internal/inference"
	"github.com/dominicnunez/agentos/internal/ledger"
	"github.com/dominicnunez/agentos/internal/secrets"
)

type compositionModel struct {
	execution.FakeModel
	descriptor execution.ModelDescriptor
}

func (m compositionModel) Descriptor() execution.ModelDescriptor { return m.descriptor }
func compositionProviders(t *testing.T) []bootstrap.Provider {
	t.Helper()
	now := time.Now().UTC()
	root := t.TempDir()
	budget := &inference.OrganizationBudget{WindowDurationSeconds: 3600, MaxTokensPerWindow: 1000, MaxConcurrentRequests: 2}
	providers := make([]bootstrap.Provider, 0, 2)
	for _, id := range []string{"first", "second"} {
		policy := inference.Policy{Version: inference.ConnectionPolicyVersion, ConnectionID: id, OrganizationBudget: budget, OrganizationID: "org", Provider: "codex-subscription", Model: "model", ExecutionProfileVersion: "v1-codex-subscription-restricted", Mode: inference.Subscription, MaxInputTokensPerRequest: 100, MaxOutputTokensPerRequest: 20, MaxTokensPerWindow: 240, WindowDurationSeconds: 3600, MaxConcurrentRequests: 1, MaxAttemptsPerRequest: 1, AuthorizedBy: "operator", AuthorizedAt: now, AuthorizationExpiresAt: now.Add(time.Hour)}
		providers = append(providers, bootstrap.Provider{Kind: bootstrap.ProviderCodexSubscription, Model: "model", SecretRef: id, CodexBinary: filepath.Join(root, "codex"), CodexCredential: filepath.Join(root, id+".enc"), InferencePolicy: policy})
	}
	return providers
}
func TestComposeConnectionsIsolatesCredentialsAndCleansUp(t *testing.T) {
	for _, failSecond := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "partial-failure"}[failSecond], func(t *testing.T) {
			store, err := ledger.Open(":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			runtimeDir := t.TempDir()
			if err := os.Chmod(runtimeDir, 0o700); err != nil {
				t.Fatal(err)
			}
			var closed, directories []string
			sentinel := errors.New("factory failed")
			factory := func(ctx context.Context, provider bootstrap.Provider, directory string, source secrets.Source) (execution.ModelAdapter, func() error, error) {
				id := provider.InferencePolicy.ConnectionID
				directories = append(directories, directory)
				value, err := source.Resolve(ctx, secrets.Ref(id))
				if err != nil || string(value) != "secret-"+id {
					t.Fatalf("wrong own credential: %v", err)
				}
				other := "first"
				if id == other {
					other = "second"
				}
				if _, err := source.Resolve(ctx, secrets.Ref(other)); err == nil {
					t.Fatal("adapter resolved another account's credential")
				}
				closeAdapter := func() error { closed = append(closed, id); return nil }
				if id == "second" && failSecond {
					return nil, closeAdapter, sentinel
				}
				policy := provider.InferencePolicy
				return compositionModel{descriptor: execution.ModelDescriptor{Provider: policy.Provider, Model: policy.Model, ExecutionProfileVersion: policy.ExecutionProfileVersion}}, closeAdapter, nil
			}
			registry, closeAll, err := composeProviderConnections(t.Context(), compositionProviders(t), runtimeDir, secrets.Values{"first": "secret-first", "second": "secret-second"}, store, factory)
			if failSecond {
				if !errors.Is(err, sentinel) || registry != nil || closeAll != nil {
					t.Fatalf("failed composition escaped: %v", err)
				}
			} else {
				if err != nil || !reflect.DeepEqual(registry.Connections(), []string{"first", "second"}) {
					t.Fatalf("composition failed: %v", err)
				}
				if err := closeAll(); err != nil {
					t.Fatal(err)
				}
				if err := closeAll(); err != nil {
					t.Fatal(err)
				}
			}
			if !reflect.DeepEqual(closed, []string{"second", "first"}) {
				t.Fatalf("cleanup=%v", closed)
			}
			if !reflect.DeepEqual(directories, []string{filepath.Join(runtimeDir, "connections", "first"), filepath.Join(runtimeDir, "connections", "second")}) {
				t.Fatalf("directories=%v", directories)
			}
		})
	}
}
func TestComposeConnectionsRejectsLinkedRuntimeDirectory(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	runtimeDir := t.TempDir()
	if err := os.Chmod(runtimeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	parent := filepath.Join(runtimeDir, "connections")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(parent, "first")); err != nil {
		t.Fatal(err)
	}
	called := false
	factory := func(context.Context, bootstrap.Provider, string, secrets.Source) (execution.ModelAdapter, func() error, error) {
		called = true
		return nil, nil, nil
	}
	if _, _, err := composeProviderConnections(t.Context(), compositionProviders(t), runtimeDir, secrets.Values{}, store, factory); err == nil || called {
		t.Fatal("linked directory reached provider factory")
	}
}

func TestRuntimeModelsSelectPurposeConnections(t *testing.T) {
	store, err := ledger.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	runtimeDir := t.TempDir()
	if err := os.Chmod(runtimeDir, 0o700); err != nil {
		t.Fatal(err)
	}
	config := bootstrap.Config{Mode: bootstrap.ModeUser, Paths: bootstrap.Paths{RuntimeDir: runtimeDir}, Providers: compositionProviders(t), Routing: &bootstrap.ProviderRouting{TaskDefault: "first", Planning: "second", Normalization: "first", TaskConnections: map[string]string{"research": "second"}}}
	calls := 0
	factory := func(_ context.Context, p bootstrap.Provider, _ string, _ secrets.Source) (execution.ModelAdapter, func() error, error) {
		calls++
		return compositionModel{descriptor: execution.ModelDescriptor{Provider: p.InferencePolicy.Provider, Model: p.Model, ExecutionProfileVersion: p.InferencePolicy.ExecutionProfileVersion}}, func() error { return nil }, nil
	}
	models, err := composeRuntimeModels(t.Context(), config, secrets.Values{}, store, factory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := models.close(); err != nil {
			t.Error(err)
		}
	}()
	if calls != 2 || modelConnectionID(models.task) != "first" || (planningModel{adapter: models.planning}).Descriptor().ConnectionID != "second" || (intakeModel{adapter: models.normalization}).Descriptor().ConnectionID != "first" {
		t.Fatal("purpose selection used a global provider")
	}
	for _, invalid := range []*bootstrap.ProviderRouting{
		nil,
		{TaskDefault: "first", Planning: "absent", Normalization: "second"},
		{TaskDefault: "first", Planning: "second", Normalization: ""},
		{TaskDefault: "first", Planning: "second", Normalization: "first", TaskConnections: map[string]string{"bad key": "second"}},
		{TaskDefault: "first", Planning: "second", Normalization: "first", TaskConnections: map[string]string{"research": "absent"}},
	} {
		config.Routing = invalid
		if _, err := composeRuntimeModels(t.Context(), config, secrets.Values{}, store, factory); err == nil || calls != 2 {
			t.Fatal("invalid routes initialized providers")
		}
	}
}

func TestRoutedInstallationIncludesEveryCredentialAndPolicy(t *testing.T) {
	providers := compositionProviders(t)
	config := bootstrap.Config{Providers: providers, Paths: bootstrap.Paths{ConfigDir: t.TempDir()}, Routing: &bootstrap.ProviderRouting{TaskDefault: "first", Planning: "second", Normalization: "first"}}
	credentialDir := filepath.Join(config.Paths.ConfigDir, "credentials")
	if err := os.Mkdir(credentialDir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, provider := range providers {
		if err := os.WriteFile(filepath.Join(credentialDir, provider.SecretRef+".cred"), []byte("synthetic encrypted credential"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(provider.CodexCredential, []byte("synthetic sealed store"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := doctorProviderCredential(config); err != nil {
		t.Fatal(err)
	}
	if err := doctorInferencePolicy(config, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	directives, err := serviceCredentialDirectives(config)
	if err != nil || strings.Count(directives, "LoadCredentialEncrypted=") != 2 || !strings.Contains(directives, "first.cred") || !strings.Contains(directives, "second.cred") {
		t.Fatalf("credentials=%q err=%v", directives, err)
	}
	if err := os.Remove(filepath.Join(credentialDir, "second.cred")); err != nil {
		t.Fatal(err)
	}
	if err := doctorProviderCredential(config); err == nil {
		t.Fatal("missing second account credential reported ready")
	}
	config.Providers[1].InferencePolicy.AuthorizationExpiresAt = time.Now().Add(-time.Minute)
	if err := doctorInferencePolicy(config, time.Now().UTC()); err == nil {
		t.Fatal("expired second account policy reported ready")
	}
}
