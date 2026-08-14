package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/rajeev-chaurasia/rail-yard/internal/config"
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	"github.com/rajeev-chaurasia/rail-yard/internal/executor"
	"github.com/rajeev-chaurasia/rail-yard/internal/worker"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	workerConfig, err := config.ParseWorker(args, os.Stderr)
	if err != nil {
		return err
	}
	if workerConfig.WorkerID == "" {
		workerConfig.WorkerID, err = domain.NewID()
		if err != nil {
			return err
		}
	}

	client, err := worker.NewHTTPClient(workerConfig.ServerURL, &http.Client{
		Timeout: workerConfig.RequestTimeout,
	})
	if err != nil {
		return err
	}
	runner, err := worker.New(
		client,
		executor.New(workerConfig.AllowShell, executor.DefaultOutputLimit),
		worker.Config{
			WorkerID:          workerConfig.WorkerID,
			Slots:             workerConfig.Slots,
			LeaseBatch:        workerConfig.Slots,
			HeartbeatInterval: worker.DefaultHeartbeatInterval,
		},
	)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return runner.Run(ctx)
}
