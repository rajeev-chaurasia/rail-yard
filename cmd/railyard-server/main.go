package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/rajeev-chaurasia/rail-yard/internal/config"
	"github.com/rajeev-chaurasia/rail-yard/internal/control"
	"github.com/rajeev-chaurasia/rail-yard/internal/dashboard"
	"github.com/rajeev-chaurasia/rail-yard/internal/operations"
	"github.com/rajeev-chaurasia/rail-yard/internal/server"
	sqlitestore "github.com/rajeev-chaurasia/rail-yard/internal/store/sqlite"
	"github.com/rajeev-chaurasia/rail-yard/internal/telemetry"
	"github.com/rajeev-chaurasia/rail-yard/internal/trigger"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	serverConfig, err := config.ParseServer(args, os.Stderr)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(serverConfig.DatabasePath), 0o750); err != nil {
		return fmt.Errorf("create database directory: %w", err)
	}

	jobStore, err := sqlitestore.OpenWithOptions(
		serverConfig.DatabasePath,
		sqlitestore.Options{
			DefaultTenantDepth: serverConfig.DefaultQueueDepth,
			MaxSlotCost:        serverConfig.MaxSlotCost,
			AllowShell:         serverConfig.AllowShell,
		},
	)
	if err != nil {
		return err
	}
	controlAdapter := control.New(jobStore)
	operationsHandler, err := control.NewHandler(controlAdapter, operations.DefaultConfig())
	if err != nil {
		_ = jobStore.Close()
		return fmt.Errorf("create operations handler: %w", err)
	}
	dashboardHandler, err := dashboard.New(controlAdapter, dashboard.Config{})
	if err != nil {
		_ = jobStore.Close()
		return fmt.Errorf("create dashboard handler: %w", err)
	}

	metrics, err := telemetry.New()
	if err != nil {
		_ = jobStore.Close()
		return err
	}
	observedStore := telemetry.ObserveStore(jobStore, metrics)
	if err := observedStore.RefreshDLQDepth(context.Background()); err != nil {
		_ = jobStore.Close()
		return err
	}
	handlerConfig := server.DefaultConfig()
	handlerConfig.HeartbeatEvery = serverConfig.HeartbeatEvery
	handlerConfig.LeaseTTL = serverConfig.LeaseTTL
	handlerConfig.ReaperInterval = serverConfig.ReaperEvery
	handlerConfig.MaxSlotCost = serverConfig.MaxSlotCost
	handlerConfig.AllowShell = serverConfig.AllowShell
	handlerConfig.TriggerStore = jobStore
	handlerConfig.OnError = func(err error) {
		log.Printf("background operation failed: %v", err)
	}
	app, err := server.New(observedStore, handlerConfig)
	if err != nil {
		_ = jobStore.Close()
		return err
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	refreshingOperations := withDLQRefresh(operationsHandler, observedStore)
	refreshingDashboard := withDLQRefresh(dashboardHandler, observedStore)
	mux.Handle("/v1/operations/", refreshingOperations)
	mux.Handle("/ops", refreshingDashboard)
	mux.Handle("/ops/", refreshingDashboard)
	mux.Handle("/", app.Handler())

	triggerContext, stopTriggers := context.WithCancel(context.Background())
	defer stopTriggers()
	triggerResult := make(chan error, 1)
	var triggerWait sync.WaitGroup
	var redisClient *redis.Client
	if serverConfig.RedisURL != "" {
		options, err := redis.ParseURL(serverConfig.RedisURL)
		if err != nil {
			_ = app.Shutdown(context.Background())
			return fmt.Errorf("parse Redis URL: %w", err)
		}
		options.ContextTimeoutEnabled = true
		redisClient = redis.NewClient(options)
		if err := redisClient.Ping(triggerContext).Err(); err != nil {
			_ = redisClient.Close()
			_ = app.Shutdown(context.Background())
			return fmt.Errorf("connect to Redis: %w", err)
		}
		consumer, err := trigger.NewRedisConsumer(redisClient, jobStore, trigger.RedisConsumerConfig{
			TriggerID: "default",
			Stream:    serverConfig.RedisStream,
			Group:     serverConfig.RedisGroup,
			Consumer:  serverConfig.RedisConsumer,
			BatchSize: serverConfig.RedisBatchSize,
			Block:     serverConfig.RedisBlock,
			ClaimIdle: serverConfig.RedisClaimIdle,
		})
		if err != nil {
			_ = redisClient.Close()
			_ = app.Shutdown(context.Background())
			return err
		}
		triggerWait.Add(1)
		go func() {
			defer triggerWait.Done()
			triggerResult <- consumer.Run(triggerContext)
		}()
	}

	httpServer := app.HTTPServer(serverConfig.HTTPAddr)
	httpServer.Handler = mux
	listenResult := make(chan error, 1)
	go func() {
		listenResult <- httpServer.ListenAndServe()
	}()

	signalContext, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var runErr error
	select {
	case err := <-listenResult:
		if !errors.Is(err, http.ErrServerClosed) {
			runErr = fmt.Errorf("serve HTTP: %w", err)
		}
	case err := <-triggerResult:
		if err != nil {
			runErr = fmt.Errorf("consume Redis stream: %w", err)
		}
	case <-signalContext.Done():
	}

	stopTriggers()
	shutdownContext, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var redisErr error
	if redisClient != nil {
		redisErr = redisClient.Close()
	}
	triggerWait.Wait()
	appErr := app.Shutdown(shutdownContext)
	httpErr := httpServer.Shutdown(shutdownContext)
	return errors.Join(runErr, appErr, httpErr, redisErr)
}

type dlqRefresher interface {
	RefreshDLQDepth(context.Context) error
}

func withDLQRefresh(next http.Handler, refresher dlqRefresher) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r)
		if err := refresher.RefreshDLQDepth(r.Context()); err != nil {
			log.Printf("refresh DLQ depth: %v", err)
		}
	})
}
