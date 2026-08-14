package executor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
)

const (
	DefaultOutputLimit = 64 << 10
	DefaultWaitDelay   = 2 * time.Second
	maxDurationMillis  = int64(^uint64(0)>>1) / int64(time.Millisecond)
)

type Request struct {
	Payload        domain.Payload
	IdempotencyKey string
}

type Result struct {
	Success         bool
	Retryable       bool
	ExitCode        int
	Stdout          string
	Stderr          string
	StdoutTruncated bool
	StderrTruncated bool
	OutputDigest    string
	Failure         *domain.Failure
}

type Executor interface {
	Execute(context.Context, Request) Result
}

type WaitFunc func(context.Context, time.Duration) error

func Wait(ctx context.Context, duration time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if duration <= 0 {
		return nil
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type Noop struct {
	Wait WaitFunc
}

func NewNoop() *Noop {
	return &Noop{Wait: Wait}
}

func (e *Noop) Execute(ctx context.Context, request Request) Result {
	if err := request.Payload.Validate(false); err != nil {
		return failedResult("invalid_payload", err, false, -1, "", "", false, false, emptyDigest())
	}
	if request.Payload.Type != domain.PayloadNoop {
		err := fmt.Errorf("noop executor cannot execute payload type %q", request.Payload.Type)
		return failedResult("invalid_payload", err, false, -1, "", "", false, false, emptyDigest())
	}
	if request.Payload.DurationMS > maxDurationMillis {
		err := errors.New("duration_ms exceeds the supported duration")
		return failedResult("invalid_payload", err, false, -1, "", "", false, false, emptyDigest())
	}

	wait := e.Wait
	if wait == nil {
		wait = Wait
	}
	if err := wait(ctx, time.Duration(request.Payload.DurationMS)*time.Millisecond); err != nil {
		return failedResult("canceled", err, true, -1, "", "", false, false, emptyDigest())
	}

	return Result{
		Success:      true,
		ExitCode:     0,
		OutputDigest: emptyDigest(),
	}
}

type Shell struct {
	Allow       bool
	OutputLimit int
	WaitDelay   time.Duration
}

func NewShell(allow bool, outputLimit int) *Shell {
	if outputLimit <= 0 {
		outputLimit = DefaultOutputLimit
	}
	return &Shell{
		Allow:       allow,
		OutputLimit: outputLimit,
		WaitDelay:   DefaultWaitDelay,
	}
}

func (e *Shell) Execute(ctx context.Context, request Request) Result {
	if request.Payload.Type != domain.PayloadShell {
		err := fmt.Errorf("shell executor cannot execute payload type %q", request.Payload.Type)
		return failedResult("invalid_payload", err, false, -1, "", "", false, false, emptyDigest())
	}
	if err := request.Payload.Validate(e.Allow); err != nil {
		return failedResult("invalid_payload", err, false, -1, "", "", false, false, emptyDigest())
	}

	limit := e.OutputLimit
	if limit <= 0 {
		limit = DefaultOutputLimit
	}
	stdout := newBoundedBuffer(limit)
	stderr := newBoundedBuffer(limit)
	digest := newLockedHash()

	args := request.Payload.Args
	command := exec.CommandContext(ctx, args[0], args[1:]...)
	command.Env = withIdempotencyKey(os.Environ(), request.IdempotencyKey)
	command.Stdout = io.MultiWriter(digest, stdout)
	command.Stderr = io.MultiWriter(digest, stderr)
	if e.WaitDelay > 0 {
		command.WaitDelay = e.WaitDelay
	} else {
		command.WaitDelay = DefaultWaitDelay
	}

	runErr := command.Run()
	outputDigest := digest.SumHex()
	if runErr == nil {
		return Result{
			Success:         true,
			ExitCode:        0,
			Stdout:          stdout.String(),
			Stderr:          stderr.String(),
			StdoutTruncated: stdout.Truncated(),
			StderrTruncated: stderr.Truncated(),
			OutputDigest:    outputDigest,
		}
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return failedResult(
			"canceled",
			ctxErr,
			true,
			-1,
			stdout.String(),
			stderr.String(),
			stdout.Truncated(),
			stderr.Truncated(),
			outputDigest,
		)
	}

	var exitErr *exec.ExitError
	if errors.As(runErr, &exitErr) {
		return failedResult(
			"exit_nonzero",
			runErr,
			true,
			exitErr.ExitCode(),
			stdout.String(),
			stderr.String(),
			stdout.Truncated(),
			stderr.Truncated(),
			outputDigest,
		)
	}

	return failedResult(
		"start_failed",
		runErr,
		false,
		-1,
		stdout.String(),
		stderr.String(),
		stdout.Truncated(),
		stderr.Truncated(),
		outputDigest,
	)
}

type Dispatch struct {
	Noop  Executor
	Shell Executor
}

func New(allowShell bool, outputLimit int) *Dispatch {
	return &Dispatch{
		Noop:  NewNoop(),
		Shell: NewShell(allowShell, outputLimit),
	}
}

func (e *Dispatch) Execute(ctx context.Context, request Request) Result {
	switch request.Payload.Type {
	case domain.PayloadNoop:
		if e.Noop == nil {
			return failedResult("executor_unavailable", errors.New("noop executor is unavailable"), false, -1, "", "", false, false, emptyDigest())
		}
		return e.Noop.Execute(ctx, request)
	case domain.PayloadShell:
		if e.Shell == nil {
			return failedResult("executor_unavailable", errors.New("shell executor is unavailable"), false, -1, "", "", false, false, emptyDigest())
		}
		return e.Shell.Execute(ctx, request)
	default:
		err := fmt.Errorf("unsupported payload type %q", request.Payload.Type)
		return failedResult("invalid_payload", err, false, -1, "", "", false, false, emptyDigest())
	}
}

func failedResult(
	class string,
	err error,
	retryable bool,
	exitCode int,
	stdout string,
	stderr string,
	stdoutTruncated bool,
	stderrTruncated bool,
	outputDigest string,
) Result {
	return Result{
		Retryable:       retryable,
		ExitCode:        exitCode,
		Stdout:          stdout,
		Stderr:          stderr,
		StdoutTruncated: stdoutTruncated,
		StderrTruncated: stderrTruncated,
		OutputDigest:    outputDigest,
		Failure: &domain.Failure{
			Class:        class,
			Message:      err.Error(),
			ExitCode:     exitCode,
			OutputDigest: outputDigest,
			Stderr:       stderr,
		},
	}
}

func withIdempotencyKey(environment []string, key string) []string {
	const name = "RAILYARD_IDEMPOTENCY_KEY"
	result := make([]string, 0, len(environment)+1)
	for _, variable := range environment {
		separator := strings.IndexByte(variable, '=')
		sameName := separator >= 0 && variable[:separator] == name
		if separator >= 0 && runtime.GOOS == "windows" {
			sameName = strings.EqualFold(variable[:separator], name)
		}
		if sameName {
			continue
		}
		result = append(result, variable)
	}
	return append(result, name+"="+key)
}

type boundedBuffer struct {
	limit     int
	buffer    bytes.Buffer
	truncated bool
}

func newBoundedBuffer(limit int) *boundedBuffer {
	return &boundedBuffer{limit: limit}
}

func (b *boundedBuffer) Write(data []byte) (int, error) {
	remaining := b.limit - b.buffer.Len()
	if remaining > 0 {
		if remaining > len(data) {
			remaining = len(data)
		}
		_, _ = b.buffer.Write(data[:remaining])
	}
	if remaining < len(data) {
		b.truncated = true
	}
	return len(data), nil
}

func (b *boundedBuffer) String() string {
	return b.buffer.String()
}

func (b *boundedBuffer) Truncated() bool {
	return b.truncated
}

type lockedHash struct {
	mu     sync.Mutex
	hasher hash.Hash
}

func newLockedHash() *lockedHash {
	return &lockedHash{hasher: sha256.New()}
}

func (h *lockedHash) Write(data []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.hasher.Write(data)
}

func (h *lockedHash) SumHex() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return hex.EncodeToString(h.hasher.Sum(nil))
}

func emptyDigest() string {
	sum := sha256.Sum256(nil)
	return hex.EncodeToString(sum[:])
}
