package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

const defaultComposeFile = "deploy/compose.yaml"
const golangCILintVersion = "v2.3.0"

var composeProjectPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return usageError()
	}

	switch args[0] {
	case "format":
		if err := requireNoArgs(args[0], args[1:]); err != nil {
			return err
		}
		return checkFormatting(ctx)
	case "vet":
		if err := requireNoArgs(args[0], args[1:]); err != nil {
			return err
		}
		return execute(ctx, "go", "vet", "-mod=readonly", "./...")
	case "test":
		if err := requireNoArgs(args[0], args[1:]); err != nil {
			return err
		}
		return execute(ctx, "go", "test", "-mod=readonly", "-count=1", "./...")
	case "race":
		if err := requireNoArgs(args[0], args[1:]); err != nil {
			return err
		}
		return execute(ctx, "go", "test", "-mod=readonly", "-race", "-count=1", "./...")
	case "lint":
		if err := requireNoArgs(args[0], args[1:]); err != nil {
			return err
		}
		linter := "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@" +
			golangCILintVersion
		return execute(
			ctx,
			"go",
			"run",
			"-mod=readonly",
			linter,
			"run",
			"--config=.golangci.yml",
			"./...",
		)
	case "replay-smoke":
		if err := requireNoArgs(args[0], args[1:]); err != nil {
			return err
		}
		return execute(
			ctx,
			"go",
			"test",
			"-mod=readonly",
			"-count=1",
			"-run=^TestRunReproducesCanonicalDecisions$",
			"./internal/replay",
		)
	case "compose-integration":
		return runIntegration(ctx, args[1:])
	case "compose-chaos":
		return runChaos(ctx, args[1:])
	case "compose-benchmark":
		return runBenchmark(ctx, args[1:])
	default:
		return fmt.Errorf("unknown command %q\n\n%w", args[0], usageError())
	}
}

func usageError() error {
	return errors.New(`usage: go run ./hack/ci <command> [flags]

commands:
  format              check all repository Go files with gofmt
  vet                 run go vet across the module
  test                run the complete Go test suite once
  race                run the complete Go test suite with the race detector
  lint                run the pinned golangci-lint release
  replay-smoke        run the deterministic replay smoke test
  compose-integration run the intended Docker Compose integration harness
  compose-chaos       run the intended seeded Docker Compose chaos harness
  compose-benchmark   run the intended Docker Compose benchmark harness`)
}

func requireNoArgs(command string, args []string) error {
	if len(args) != 0 {
		return fmt.Errorf("%s does not accept arguments", command)
	}
	return nil
}

func checkFormatting(ctx context.Context) error {
	var goFiles []string
	err := filepath.WalkDir(".", func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "vendor":
				if path != "." {
					return filepath.SkipDir
				}
			}
			return nil
		}
		if strings.EqualFold(filepath.Ext(entry.Name()), ".go") {
			goFiles = append(goFiles, path)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("discover Go files: %w", err)
	}
	sort.Strings(goFiles)

	var unformatted []string
	const batchSize = 100
	for start := 0; start < len(goFiles); start += batchSize {
		end := min(start+batchSize, len(goFiles))
		command := exec.CommandContext(ctx, "gofmt", append([]string{"-l"}, goFiles[start:end]...)...)
		output, err := command.CombinedOutput()
		if err != nil {
			return fmt.Errorf("gofmt: %w\n%s", err, strings.TrimSpace(string(output)))
		}
		for _, path := range strings.Split(strings.TrimSpace(string(output)), "\n") {
			if path = strings.TrimSpace(path); path != "" {
				unformatted = append(unformatted, path)
			}
		}
	}
	if len(unformatted) != 0 {
		return fmt.Errorf(
			"gofmt is required for:\n  %s",
			strings.Join(unformatted, "\n  "),
		)
	}
	return nil
}

func runIntegration(ctx context.Context, args []string) error {
	flags := newFlagSet("compose-integration")
	composeFile := flags.String(
		"compose-file",
		envOrDefault("RAILYARD_COMPOSE_FILE", defaultComposeFile),
		"Docker Compose file",
	)
	project := flags.String(
		"project",
		envOrDefault("RAILYARD_COMPOSE_PROJECT", "rail-yard-integration"),
		"isolated Compose project name",
	)
	output := flags.String("output", "artifacts/integration", "artifact output directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("compose-integration received unexpected arguments: %v", flags.Args())
	}
	if err := validateHarnessInputs(*composeFile, *project, *output); err != nil {
		return err
	}
	if err := validateCompose(ctx, *composeFile); err != nil {
		return err
	}
	return execute(
		ctx,
		"go",
		"run",
		"-mod=readonly",
		"./test/integration",
		"--compose-file="+filepath.Clean(*composeFile),
		"--project="+*project,
		"--output="+filepath.Clean(*output),
	)
}

func runChaos(ctx context.Context, args []string) error {
	flags := newFlagSet("compose-chaos")
	composeFile := flags.String(
		"compose-file",
		envOrDefault("RAILYARD_COMPOSE_FILE", defaultComposeFile),
		"Docker Compose file",
	)
	project := flags.String(
		"project-prefix",
		envOrDefault("RAILYARD_COMPOSE_PROJECT", "rail-yard-chaos"),
		"isolated Compose project prefix",
	)
	output := flags.String("output", "artifacts/chaos", "artifact output directory")
	runs := flags.Int("runs", 1, "number of fresh-volume runs")
	jobs := flags.Int("jobs", 1000, "accepted jobs per run")
	seed := flags.Int64("seed", 1, "deterministic base seed")
	workerKills := flags.Int("worker-kills", 4, "worker SIGKILL actions per run")
	jobDuration := flags.Duration("job-duration", 10*time.Second, "no-op duration")
	actionMinimum := flags.Duration("action-min", 25*time.Millisecond, "minimum kill interval")
	actionMaximum := flags.Duration("action-max", 75*time.Millisecond, "maximum kill interval")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("compose-chaos received unexpected arguments: %v", flags.Args())
	}
	if err := validateHarnessInputs(*composeFile, *project, *output); err != nil {
		return err
	}
	if err := requirePositive("runs", *runs); err != nil {
		return err
	}
	if err := requirePositive("jobs", *jobs); err != nil {
		return err
	}
	if err := requirePositive("worker-kills", *workerKills); err != nil {
		return err
	}
	if *jobDuration < 0 || *actionMinimum < 0 || *actionMaximum < *actionMinimum {
		return errors.New("chaos durations are invalid")
	}
	if err := validateCompose(ctx, *composeFile); err != nil {
		return err
	}
	return execute(
		ctx,
		"go",
		"run",
		"-mod=readonly",
		"./test/chaos",
		"--compose-file="+filepath.Clean(*composeFile),
		"--project-prefix="+*project,
		"--runs="+strconv.Itoa(*runs),
		"--jobs="+strconv.Itoa(*jobs),
		"--seed="+strconv.FormatInt(*seed, 10),
		"--worker-kills="+strconv.Itoa(*workerKills),
		"--job-duration="+jobDuration.String(),
		"--action-min="+actionMinimum.String(),
		"--action-max="+actionMaximum.String(),
		"--output="+filepath.Clean(*output),
	)
}

func runBenchmark(ctx context.Context, args []string) error {
	flags := newFlagSet("compose-benchmark")
	composeFile := flags.String(
		"compose-file",
		envOrDefault("RAILYARD_COMPOSE_FILE", defaultComposeFile),
		"Docker Compose file",
	)
	project := flags.String(
		"project-prefix",
		envOrDefault("RAILYARD_COMPOSE_PROJECT", "rail-yard-benchmark"),
		"isolated Compose project prefix",
	)
	output := flags.String("output", "artifacts/benchmark", "artifact output directory")
	runs := flags.Int("runs", 1, "number of measured fresh-volume runs")
	jobs := flags.Int("jobs", 100, "accepted jobs per run")
	workers := flags.Int("workers", 1, "worker process count")
	workerSlots := flags.Int("worker-slots", 16, "slots per worker")
	hostPort := flags.Int("host-port", 0, "server host port; zero selects an available port")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("compose-benchmark received unexpected arguments: %v", flags.Args())
	}
	if err := validateHarnessInputs(*composeFile, *project, *output); err != nil {
		return err
	}
	if err := requirePositive("runs", *runs); err != nil {
		return err
	}
	if err := requirePositive("jobs", *jobs); err != nil {
		return err
	}
	if err := requirePositive("workers", *workers); err != nil {
		return err
	}
	if err := requirePositive("worker-slots", *workerSlots); err != nil {
		return err
	}
	if *hostPort < 0 || *hostPort > 65535 {
		return fmt.Errorf("host-port must be between 0 and 65535, got %d", *hostPort)
	}
	if err := validateCompose(ctx, *composeFile); err != nil {
		return err
	}
	return execute(
		ctx,
		"go",
		"run",
		"-mod=readonly",
		"./test/benchmark",
		"--compose-file="+filepath.Clean(*composeFile),
		"--project-prefix="+*project,
		"--runs="+strconv.Itoa(*runs),
		"--jobs="+strconv.Itoa(*jobs),
		"--workers="+strconv.Itoa(*workers),
		"--worker-slots="+strconv.Itoa(*workerSlots),
		"--host-port="+strconv.Itoa(*hostPort),
		"--output="+filepath.Clean(*output),
	)
}

func newFlagSet(name string) *flag.FlagSet {
	flags := flag.NewFlagSet(name, flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	return flags
}

func validateHarnessInputs(composeFile, project, output string) error {
	if strings.TrimSpace(composeFile) == "" {
		return errors.New("compose-file must not be empty")
	}
	info, err := os.Stat(composeFile)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf(
				"compose file %q does not exist; the production Compose topology must provide it",
				composeFile,
			)
		}
		return fmt.Errorf("inspect compose file %q: %w", composeFile, err)
	}
	if info.IsDir() {
		return fmt.Errorf("compose-file %q is a directory", composeFile)
	}
	if !composeProjectPattern.MatchString(project) {
		return fmt.Errorf(
			"compose project %q must match %s",
			project,
			composeProjectPattern.String(),
		)
	}
	if strings.TrimSpace(output) == "" {
		return errors.New("output must not be empty")
	}
	if err := os.MkdirAll(output, 0o755); err != nil {
		return fmt.Errorf("create artifact directory %q: %w", output, err)
	}
	return nil
}

func requirePositive(name string, value int) error {
	if value < 1 {
		return fmt.Errorf("%s must be positive, got %d", name, value)
	}
	return nil
}

func validateCompose(ctx context.Context, composeFile string) error {
	if err := execute(
		ctx,
		"docker",
		"compose",
		"--file",
		filepath.Clean(composeFile),
		"config",
		"--quiet",
	); err != nil {
		return fmt.Errorf("validate Docker Compose topology: %w", err)
	}
	return nil
}

func envOrDefault(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func execute(ctx context.Context, name string, args ...string) error {
	fmt.Fprintf(os.Stderr, "+ %s\n", renderCommand(name, args))
	command := exec.CommandContext(ctx, name, args...)
	command.Stdin = os.Stdin
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Run(); err != nil {
		return fmt.Errorf("%s failed: %w", name, err)
	}
	return nil
}

func renderCommand(name string, args []string) string {
	parts := make([]string, 0, len(args)+1)
	parts = append(parts, quoteCommandArgument(name))
	for _, arg := range args {
		parts = append(parts, quoteCommandArgument(arg))
	}
	return strings.Join(parts, " ")
}

func quoteCommandArgument(value string) string {
	if value != "" && !strings.ContainsAny(value, " \t\r\n\"'") {
		return value
	}
	if runtime.GOOS == "windows" {
		return strconv.Quote(value)
	}
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}
