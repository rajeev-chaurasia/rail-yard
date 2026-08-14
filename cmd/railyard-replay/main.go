package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/rajeev-chaurasia/rail-yard/internal/replay"
	sqlitestore "github.com/rajeev-chaurasia/rail-yard/internal/store/sqlite"
)

func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) (runErr error) {
	flags := flag.NewFlagSet("railyard-replay", flag.ContinueOnError)
	flags.SetOutput(stderr)
	inputPath := flags.String("input", "-", "decision JSONL input path, or - for stdin")
	databasePath := flags.String("db", "", "SQLite database containing the decision log")
	outputPath := flags.String("output", "-", "canonical replay output path, or - for stdout")
	if err := flags.Parse(args); err != nil {
		return err
	}

	var input io.Reader
	var err error
	closeInput := func() {}
	if *databasePath != "" {
		if *inputPath != "-" {
			return fmt.Errorf("--db and --input cannot be used together")
		}
		jobStore, err := sqlitestore.Open(*databasePath)
		if err != nil {
			return err
		}
		defer func() {
			_ = jobStore.Close()
		}()
		var records bytes.Buffer
		if err := jobStore.ExportDecisionLog(context.Background(), &records); err != nil {
			return err
		}
		input = &records
	} else {
		input, closeInput, err = openInput(*inputPath, stdin)
		if err != nil {
			return err
		}
	}
	defer closeInput()

	output, closeOutput, err := openOutput(*outputPath, stdout)
	if err != nil {
		return err
	}
	defer func() {
		if err := closeOutput(); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("close replay output: %w", err))
		}
	}()

	result, err := replay.Run(input, output)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal replay summary: %w", err)
	}
	if _, err := fmt.Fprintln(stderr, string(encoded)); err != nil {
		return fmt.Errorf("write replay summary: %w", err)
	}
	return nil
}

func openInput(path string, fallback io.Reader) (io.Reader, func(), error) {
	if path == "-" {
		return fallback, func() {}, nil
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, func() {}, fmt.Errorf("open replay input: %w", err)
	}
	return file, func() { _ = file.Close() }, nil
}

func openOutput(path string, fallback io.Writer) (io.Writer, func() error, error) {
	if path == "-" {
		return fallback, func() error { return nil }, nil
	}
	file, err := os.Create(path)
	if err != nil {
		return nil, func() error { return nil }, fmt.Errorf("create replay output: %w", err)
	}
	return file, file.Close, nil
}
