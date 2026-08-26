package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/dominicnunez/agentos/internal/bootstrap"
	"github.com/dominicnunez/agentos/internal/ledger/recovery"
)

var version = "1.0.0-dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, args []string, output io.Writer) error {
	if ctx == nil || output == nil {
		return fmt.Errorf("context and output are required")
	}
	if len(args) == 0 {
		return fmt.Errorf("command is required: backup, restore, verify, or version")
	}
	if len(args) == 1 && (args[0] == "--version" || args[0] == "version") {
		_, err := fmt.Fprintln(output, version)
		return err
	}
	var result recovery.Result
	var err error
	switch args[0] {
	case "backup":
		flags := flag.NewFlagSet("backup", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		configPath := flags.String("config", "", "Agent OS installation configuration")
		destination := flags.String("output", "", "new backup file")
		if err := parseFlags(flags, args[1:]); err != nil {
			return err
		}
		config, configErr := recoveryConfig(*configPath)
		if configErr != nil {
			return configErr
		}
		result, err = recovery.BackupAnchored(ctx, config.Paths.Database, *destination, config.Integrity.CheckpointFile, *destination+".anchor.json", config.Integrity.InstallationID, config.Integrity.PublicKey)
	case "restore":
		flags := flag.NewFlagSet("restore", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		backup := flags.String("backup", "", "verified Agent OS backup")
		backupAnchor := flags.String("backup-anchor", "", "signed checkpoint for the backup")
		configPath := flags.String("config", "", "Agent OS installation configuration")
		destination := flags.String("output", "", "new restored database file")
		destinationAnchor := flags.String("output-anchor", "", "new restored checkpoint file")
		if err := parseFlags(flags, args[1:]); err != nil {
			return err
		}
		config, configErr := recoveryConfig(*configPath)
		if configErr != nil {
			return configErr
		}
		if *backupAnchor == "" {
			*backupAnchor = *backup + ".anchor.json"
		}
		if *destinationAnchor == "" {
			*destinationAnchor = *destination + ".anchor.json"
		}
		result, err = recovery.RestoreAnchored(ctx, *backup, *destination, *backupAnchor, *destinationAnchor, config.Integrity.InstallationID, config.Integrity.PublicKey)
	case "verify":
		flags := flag.NewFlagSet("verify", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		database := flags.String("database", "", "offline backup or restore candidate")
		checkpoint := flags.String("anchor", "", "signed checkpoint for the database")
		configPath := flags.String("config", "", "Agent OS installation configuration")
		if err := parseFlags(flags, args[1:]); err != nil {
			return err
		}
		config, configErr := recoveryConfig(*configPath)
		if configErr != nil {
			return configErr
		}
		if *checkpoint == "" {
			if *database == config.Paths.Database {
				*checkpoint = config.Integrity.CheckpointFile
			} else {
				*checkpoint = *database + ".anchor.json"
			}
		}
		result, err = recovery.VerifyAnchored(ctx, *database, *checkpoint, config.Integrity.InstallationID, config.Integrity.PublicKey)
	default:
		return fmt.Errorf("unsupported command %q: use backup, restore, verify, or version", args[0])
	}
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(result)
}

func recoveryConfig(path string) (bootstrap.Config, error) {
	if path == "" {
		return bootstrap.Config{}, fmt.Errorf("--config is required for anchored recovery")
	}
	config, err := bootstrap.LoadConfig(path)
	if err != nil {
		return bootstrap.Config{}, err
	}
	if err := config.ValidateReady(); err != nil {
		return bootstrap.Config{}, fmt.Errorf("recovery configuration is invalid: %w", err)
	}
	return config, nil
}

func parseFlags(flags *flag.FlagSet, args []string) error {
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	return nil
}
