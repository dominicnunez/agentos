//go:build !linux

package main

import (
	"context"
	"fmt"

	"github.com/dominicnunez/agentos/internal/bootstrap"
)

func ensureInitPrivileges(_ context.Context, _ bootstrap.Mode, _ *terminalUI) (bool, error) {
	return false, fmt.Errorf("Agent OS V1 setup is supported on Linux")
}
func ensureProviderSetupPrivileges(_ context.Context, _ bootstrap.Config, _ *terminalUI) (bool, error) {
	return false, fmt.Errorf("Agent OS V1 setup is supported on Linux")
}
func ensureIntegrityMaintenancePrivileges(_ context.Context, _ bootstrap.Config, _ *terminalUI, _, _ string) (bool, error) {
	return false, fmt.Errorf("Agent OS V1 integrity maintenance is supported on Linux")
}
func integrityMaintenanceAuthority(_ context.Context, _ bootstrap.Config) (string, error) {
	return "", fmt.Errorf("Agent OS V1 integrity maintenance is supported on Linux")
}
func requireIntegrityServiceStopped(_ context.Context, _ bootstrap.Config) error {
	return fmt.Errorf("Agent OS V1 integrity maintenance is supported on Linux")
}
func beginIntegrityMaintenance(_ context.Context, _ bootstrap.Config) (func() error, error) {
	return nil, fmt.Errorf("Agent OS V1 integrity maintenance is supported on Linux")
}
func prepareIntegrityCheckpointAccess(_ context.Context, _ bootstrap.Config) error {
	return fmt.Errorf("Agent OS V1 integrity maintenance is supported on Linux")
}
func prepareLedgerDatabaseAccess(_ context.Context, _ bootstrap.Config) error {
	return fmt.Errorf("Agent OS V1 database access is supported on Linux")
}
func doctorIntegrityCheckpointAccess(_ context.Context, _ bootstrap.Config) error {
	return fmt.Errorf("Agent OS V1 ledger checkpoint inspection is supported on Linux")
}
func invokingSystemOwner(_ context.Context) (bootstrap.Owner, error) {
	return bootstrap.Owner{}, fmt.Errorf("Agent OS V1 setup is supported on Linux")
}
func canonicalCodexBinary(_ bootstrap.Mode, _ string) (string, error) {
	return "", fmt.Errorf("Agent OS V1 setup is supported on Linux")
}
func readSetupCredential(_ string, _ int) ([]byte, error) {
	return nil, fmt.Errorf("Agent OS V1 setup is supported on Linux")
}
func storeEncryptedCredential(_ context.Context, _ bootstrap.Config, _ string, _ []byte) error {
	return fmt.Errorf("Agent OS V1 setup is supported on Linux")
}
func storeEncryptedCredentialNew(_ context.Context, _ bootstrap.Config, _ string, _ []byte) error {
	return fmt.Errorf("Agent OS V1 setup is supported on Linux")
}
func installRuntime(_ context.Context, _ bootstrap.Config, _ int) error {
	return fmt.Errorf("Agent OS V1 setup is supported on Linux")
}
func activateInstalledRuntime(_ context.Context, _ bootstrap.Config, _ int) error {
	return fmt.Errorf("Agent OS V1 setup is supported on Linux")
}
func applyProviderRuntime(_ context.Context, _ bootstrap.Config) error {
	return fmt.Errorf("Agent OS V1 setup is supported on Linux")
}
func serviceCredentialDirectives(_ bootstrap.Config) (string, error) {
	return "", fmt.Errorf("Agent OS V1 service credentials are supported on Linux")
}
func decryptProviderCredential(_ context.Context, _ bootstrap.Mode, _, _ string) ([]byte, error) {
	return nil, fmt.Errorf("Agent OS V1 setup is supported on Linux")
}
func doctorUserSocket(_ bootstrap.Config) (string, string) {
	return "BLOCKED", "Agent OS V1 local user access is supported on Linux"
}
func doctorService(_ context.Context, _ bootstrap.Config) (string, string) {
	return "INFO", "Agent OS V1 service management is supported on Linux"
}
