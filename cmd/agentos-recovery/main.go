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

	"github.com/dominicnunez/agentos/internal/ledger/recovery"
)

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
		return fmt.Errorf("command is required: backup, restore, or verify")
	}
	var result recovery.Result
	var err error
	switch args[0] {
	case "backup":
		flags := flag.NewFlagSet("backup", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		database := flags.String("database", "", "source Agent OS SQLite database")
		destination := flags.String("output", "", "new backup file")
		if err := parseFlags(flags, args[1:]); err != nil {
			return err
		}
		result, err = recovery.Backup(ctx, *database, *destination)
	case "restore":
		flags := flag.NewFlagSet("restore", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		backup := flags.String("backup", "", "verified Agent OS backup")
		destination := flags.String("output", "", "new restored database file")
		if err := parseFlags(flags, args[1:]); err != nil {
			return err
		}
		result, err = recovery.Restore(ctx, *backup, *destination)
	case "verify":
		flags := flag.NewFlagSet("verify", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		database := flags.String("database", "", "offline backup or restore candidate")
		if err := parseFlags(flags, args[1:]); err != nil {
			return err
		}
		result, err = recovery.Verify(ctx, *database)
	default:
		return fmt.Errorf("unsupported command %q: use backup, restore, or verify", args[0])
	}
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(result)
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
