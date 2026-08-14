package executor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
)

func TestNoopUsesInjectedWait(t *testing.T) {
	var got time.Duration
	executor := &Noop{
		Wait: func(_ context.Context, duration time.Duration) error {
			got = duration
			return nil
		},
	}

	result := executor.Execute(context.Background(), Request{
		Payload: domain.Payload{Type: domain.PayloadNoop, DurationMS: 125},
	})

	if !result.Success {
		t.Fatalf("result = %+v, want success", result)
	}
	if got != 125*time.Millisecond {
		t.Fatalf("wait duration = %s, want 125ms", got)
	}
	if result.OutputDigest != emptyDigest() {
		t.Fatalf("digest = %q, want empty output digest", result.OutputDigest)
	}
}

func TestNoopHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := NewNoop().Execute(ctx, Request{
		Payload: domain.Payload{Type: domain.PayloadNoop, DurationMS: 1},
	})

	if result.Success || result.Failure == nil || result.Failure.Class != "canceled" {
		t.Fatalf("result = %+v, want canceled failure", result)
	}
}

func TestNoopRejectsDurationOverflow(t *testing.T) {
	executor := &Noop{
		Wait: func(context.Context, time.Duration) error {
			t.Fatal("wait called for invalid duration")
			return nil
		},
	}
	result := executor.Execute(context.Background(), Request{
		Payload: domain.Payload{Type: domain.PayloadNoop, DurationMS: maxDurationMillis + 1},
	})

	if result.Success || result.Failure == nil || result.Failure.Class != "invalid_payload" {
		t.Fatalf("result = %+v, want invalid duration failure", result)
	}
}

func TestShellRequiresExplicitGate(t *testing.T) {
	result := NewShell(false, 16).Execute(context.Background(), Request{
		Payload: domain.Payload{Type: domain.PayloadShell, Args: helperArgs("stdout", "ignored")},
	})

	if result.Success || result.Failure == nil || result.Failure.Class != "invalid_payload" {
		t.Fatalf("result = %+v, want invalid payload failure", result)
	}
}

func TestShellBoundsOutputAndHashesFullStream(t *testing.T) {
	const output = "abcdef"
	result := NewShell(true, 3).Execute(context.Background(), Request{
		Payload: domain.Payload{Type: domain.PayloadShell, Args: helperArgs("stdout", output)},
	})

	if !result.Success {
		t.Fatalf("result = %+v, want success", result)
	}
	if result.Stdout != "abc" || !result.StdoutTruncated {
		t.Fatalf("stdout = %q, truncated = %t; want %q, true", result.Stdout, result.StdoutTruncated, "abc")
	}
	sum := sha256.Sum256([]byte(output))
	if result.OutputDigest != hex.EncodeToString(sum[:]) {
		t.Fatalf("digest = %q, want full output digest %q", result.OutputDigest, hex.EncodeToString(sum[:]))
	}
}

func TestShellPassesIdempotencyKey(t *testing.T) {
	const key = "job/attempt/generation"
	result := NewShell(true, 128).Execute(context.Background(), Request{
		Payload:        domain.Payload{Type: domain.PayloadShell, Args: helperArgs("idempotency")},
		IdempotencyKey: key,
	})

	if !result.Success {
		t.Fatalf("result = %+v, want success", result)
	}
	if result.Stdout != key {
		t.Fatalf("stdout = %q, want idempotency key %q", result.Stdout, key)
	}
}

func TestShellReportsExitFailure(t *testing.T) {
	result := NewShell(true, 128).Execute(context.Background(), Request{
		Payload: domain.Payload{Type: domain.PayloadShell, Args: helperArgs("exit", "7")},
	})

	if result.Success || !result.Retryable {
		t.Fatalf("result = %+v, want retryable failure", result)
	}
	if result.ExitCode != 7 || result.Failure == nil || result.Failure.Class != "exit_nonzero" {
		t.Fatalf("result = %+v, want exit code 7", result)
	}
}

func TestShellHonorsCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := NewShell(true, 128).Execute(ctx, Request{
		Payload: domain.Payload{Type: domain.PayloadShell, Args: helperArgs("stdout", "ignored")},
	})

	if result.Success || result.Failure == nil || result.Failure.Class != "canceled" {
		t.Fatalf("result = %+v, want canceled failure", result)
	}
}

func helperArgs(operation string, values ...string) []string {
	args := []string{os.Args[0], "-test.run=^TestExecutorHelperProcess$", "--", operation}
	return append(args, values...)
}

func TestExecutorHelperProcess(t *testing.T) {
	separator := -1
	for index, arg := range os.Args {
		if arg == "--" {
			separator = index
			break
		}
	}
	if separator < 0 || separator+1 >= len(os.Args) {
		return
	}

	operation := os.Args[separator+1]
	values := os.Args[separator+2:]
	switch operation {
	case "stdout":
		if len(values) != 1 {
			os.Exit(2)
		}
		_, _ = fmt.Fprint(os.Stdout, values[0])
	case "idempotency":
		_, _ = fmt.Fprint(os.Stdout, os.Getenv("RAILYARD_IDEMPOTENCY_KEY"))
	case "exit":
		if len(values) != 1 {
			os.Exit(2)
		}
		code, err := strconv.Atoi(values[0])
		if err != nil {
			os.Exit(2)
		}
		_, _ = fmt.Fprint(os.Stderr, strings.Repeat("failure", 2))
		os.Exit(code)
	default:
		os.Exit(2)
	}
	os.Exit(0)
}
