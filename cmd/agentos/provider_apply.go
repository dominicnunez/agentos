package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/dominicnunez/agentos/internal/bootstrap"
)

func runProviderApply(ctx context.Context, input *os.File, output io.Writer) error {
	_, config, state, err := discoverInstallation()
	if err != nil {
		return fmt.Errorf("load installed provider configuration: %w", err)
	}
	if err := config.ValidateReady(); err != nil {
		return err
	}
	completed, err := ensureProviderApplyPrivileges(ctx, config, newTerminalUI(input, output))
	if err != nil || completed {
		return err
	}
	if err := applyReviewedProviderConfig(ctx, config, state, effectiveUID(), applyProviderRuntime); err != nil {
		return err
	}
	_, err = fmt.Fprintln(output, "Applied provider configuration to the installed service.")
	return err
}

// applyReviewedProviderConfig preflights the entire installed set before service
// changes. It does not collect credentials, modify configuration, or probe models.
func applyReviewedProviderConfig(ctx context.Context, config bootstrap.Config, state bootstrap.State, uid int, apply func(context.Context, bootstrap.Config) error) error {
	if ctx == nil || apply == nil {
		return fmt.Errorf("provider application dependencies are required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if state.Version != bootstrap.ConfigVersion || state.Mode != config.Mode || state.Stage != bootstrap.StageReady {
		return fmt.Errorf("provider application requires a ready installation")
	}
	if err := config.ValidateReady(); err != nil {
		return err
	}
	if (config.Mode == bootstrap.ModeSystem && uid != 0) || (config.Mode == bootstrap.ModeUser && uid != config.Owner.UID) {
		return fmt.Errorf("provider application requires the installation authority")
	}
	if err := doctorInferencePolicy(config, nowUTC()); err != nil {
		return err
	}
	if err := doctorProviderCredential(config); err != nil {
		return err
	}
	return apply(ctx, config)
}
