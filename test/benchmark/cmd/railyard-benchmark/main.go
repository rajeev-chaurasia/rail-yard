package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := runCommand(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "railyard-benchmark:", err)
		os.Exit(1)
	}
}

func runCommand(ctx context.Context, arguments []string) error {
	if len(arguments) == 0 {
		return usageError()
	}
	switch arguments[0] {
	case "run":
		return runWorkload(ctx, arguments[1:])
	case "reconcile":
		return reconcileRun(ctx, arguments[1:])
	case "summarize":
		return summarizeRuns(arguments[1:])
	default:
		return fmt.Errorf("unknown command %q\n\n%s", arguments[0], usageText)
	}
}

func usageError() error {
	return fmt.Errorf("a command is required\n\n%s", usageText)
}

const usageText = `usage:
  railyard-benchmark run [flags]
  railyard-benchmark reconcile [flags]
  railyard-benchmark summarize [flags]

Run "railyard-benchmark <command> -help" for command flags.`
