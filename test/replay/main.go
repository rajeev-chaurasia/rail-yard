package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	flags := flag.NewFlagSet("railyard-replay-qualification", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var inputPath string
	var outputPath string
	flags.StringVar(&inputPath, "input", "", "checked decision log input; generated when omitted")
	flags.StringVar(&outputPath, "output", "", "new replay evidence directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected arguments: %v", flags.Args())
	}
	if outputPath == "" {
		return errors.New("--output is required")
	}

	result, err := qualify(qualificationConfig{
		InputPath:  inputPath,
		OutputPath: outputPath,
	})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(
		stdout,
		"replay qualification passed: decisions=%d processes=%d sha256=%s evidence=%s\n",
		result.Decisions,
		len(result.Runs),
		result.CanonicalSHA256,
		result.OutputPath,
	)
	return err
}
