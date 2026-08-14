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

const telemetryRefreshInterval = 5 * time.Second

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
	metrics, err := telemetry.New()
	if err != nil {
		_ = jobStore.Close()
		return err
	}
	observedStore := telemetry.ObserveStore(jobStore, metrics)
	backgroundContext, stopBackground := context.WithCancel(context.Background())
	var backgroundWait sync.WaitGroup
	defer func() {
		stopBackground()
		backgroundWait.Wait()
	}()
	collector := telemetry.NewCollector(jobStore, metrics)
	if err := collector.Refresh(backgroundContext); err != nil {
		_ = jobStore.Close()
		return fmt.Errorf("initialize telemetry: %w", err)
	}
	backgroundWait.Add(1)
	go func() {
		defer backgroundWait.Done()
		collector.Run(backgroundContext, telemetryRefreshInterval, func(err error) {
			log.Printf("refresh durable telemetry: %v", err)
		})
	}()

	controlAdapter := control.New(jobStore)
	operationsHandler, err := control.NewHandler(controlAdapter, operations.DefaultConfig())
	if err != nil {
		stopBackground()
		backgroundWait.Wait()
		_ = jobStore.Close()
		return fmt.Errorf("create operations handler: %w", err)
	}
	dashboardHandler, err := dashboard.New(controlAdapter, dashboard.Config{})
	if err != nil {
		stopBackground()
		backgroundWait.Wait()
		_ = jobStore.Close()
		return fmt.Errorf("create dashboard handler: %w", err)
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
		stopBackground()
		backgroundWait.Wait()
		_ = jobStore.Close()
		return err
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.Handle("/v1/operations/", operationsHandler)
	mux.Handle("/ops", dashboardHandler)
	mux.Handle("/ops/", dashboardHandler)
	mux.Handle("/", app.Handler())

	triggerResult := make(chan error, 1)
	var redisClient *redis.Client
	if serverConfig.RedisURL != "" {
		options, err := redis.ParseURL(serverConfig.RedisURL)
		if err != nil {
			stopBackground()
			backgroundWait.Wait()
			_ = app.Shutdown(context.Background())
			return fmt.Errorf("parse Redis URL: %w", err)
		}
		options.ContextTimeoutEnabled = true
		redisClient = redis.NewClient(options)
		if err := redisClient.Ping(backgroundContext).Err(); err != nil {
			stopBackground()
			backgroundWait.Wait()
			_ = redisClient.Close()
			_ = app.Shutdown(context.Background())
			return fmt.Errorf("connect to Redis: %w", err)
		}
		consumer, err := trigger.NewRedisConsumer(redisClient, observedStore, trigger.RedisConsumerConfig{
			TriggerID: "default",
			Stream:    serverConfig.RedisStream,
			Group:     serverConfig.RedisGroup,
			Consumer:  serverConfig.RedisConsumer,
			BatchSize: serverConfig.RedisBatchSize,
			Block:     serverConfig.RedisBlock,
			ClaimIdle: serverConfig.RedisClaimIdle,
		})
		if err != nil {
			stopBackground()
			backgroundWait.Wait()
			_ = redisClient.Close()
			_ = app.Shutdown(context.Background())
			return err
		}
		if err := consumer.EnsureGroup(backgroundContext); err != nil {
			stopBackground()
			backgroundWait.Wait()
			_ = redisClient.Close()
			_ = app.Shutdown(context.Background())
			return err
		}
		refreshRedisStreamState(
			backgroundContext,
			redisClient,
			metrics,
			serverConfig.RedisStream,
			serverConfig.RedisGroup,
		)
		backgroundWait.Add(2)
		go func() {
			defer backgroundWait.Done()
			triggerResult <- consumer.Run(backgroundContext)
		}()
		go func() {
			defer backgroundWait.Done()
			collectRedisStreamState(
				backgroundContext,
				redisClient,
				metrics,
				serverConfig.RedisStream,
				serverConfig.RedisGroup,
				telemetryRefreshInterval,
			)
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

	stopBackground()
	shutdownContext, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var redisErr error
	if redisClient != nil {
		redisErr = redisClient.Close()
	}
	backgroundWait.Wait()
	appErr := app.Shutdown(shutdownContext)
	httpErr := httpServer.Shutdown(shutdownContext)
	return errors.Join(runErr, appErr, httpErr, redisErr)
}

func collectRedisStreamState(
	ctx context.Context,
	client *redis.Client,
	metrics *telemetry.Metrics,
	stream string,
	group string,
	interval time.Duration,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refreshRedisStreamState(ctx, client, metrics, stream, group)
		}
	}
}

func refreshRedisStreamState(
	ctx context.Context,
	client *redis.Client,
	metrics *telemetry.Metrics,
	stream string,
	group string,
) {
	groups, err := client.XInfoGroups(ctx, stream).Result()
	if err != nil {
		metrics.ClearRedisStreamState()
		if ctx.Err() == nil {
			log.Printf("refresh Redis telemetry: %v", err)
		}
		return
	}
	for _, value := range groups {
		if value.Name == group {
			metrics.SetRedisStreamState(value.Lag, value.Pending)
			return
		}
	}
	metrics.ClearRedisStreamState()
	if ctx.Err() == nil {
		log.Printf("refresh Redis telemetry: consumer group %q not found", group)
	}
}
