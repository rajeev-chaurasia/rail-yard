package p5

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
)

var workerServices = []string{
	"worker-1",
	"worker-2",
	"worker-3",
	"worker-4",
	"worker-5",
	"worker-6",
	"worker-7",
	"worker-8",
}

type Compose struct {
	WorkingDirectory string
	File             string
	Project          string
}

func (c Compose) Start(ctx context.Context, services ...string) error {
	args := []string{"up", "-d"}
	args = append(args, services...)
	return c.run(ctx, args...)
}

func (c Compose) Stop(ctx context.Context, services ...string) error {
	args := []string{"stop"}
	args = append(args, services...)
	return c.run(ctx, args...)
}

func (c Compose) Kill(ctx context.Context, service string) error {
	return c.run(ctx, "kill", "-s", "SIGKILL", service)
}

func (c Compose) StartAllWorkers(ctx context.Context) error {
	return c.Start(ctx, workerServices...)
}

func (c Compose) StopAllWorkers(ctx context.Context) error {
	return c.Stop(ctx, workerServices...)
}

func (c Compose) KeepOnlyWorker(ctx context.Context, service string) error {
	toStop := make([]string, 0, len(workerServices)-1)
	for _, candidate := range workerServices {
		if candidate != service {
			toStop = append(toStop, candidate)
		}
	}
	if err := c.Stop(ctx, toStop...); err != nil {
		return err
	}
	return c.Start(ctx, service)
}

func (c Compose) run(ctx context.Context, args ...string) error {
	commandArgs := []string{"compose", "-f", c.File, "-p", c.Project}
	commandArgs = append(commandArgs, args...)
	command := exec.CommandContext(ctx, "docker", commandArgs...)
	command.Dir = c.WorkingDirectory
	output, err := command.CombinedOutput()
	if err != nil {
		return fmt.Errorf(
			"docker compose %s: %w: %s",
			strings.Join(args, " "),
			err,
			strings.TrimSpace(string(output)),
		)
	}
	return nil
}
