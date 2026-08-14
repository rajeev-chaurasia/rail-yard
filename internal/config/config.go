package config

import (
	"errors"
	"flag"
	"io"
	"os"
	"strconv"
	"time"
)

type Server struct {
	HTTPAddr          string
	DatabasePath      string
	LeaseTTL          time.Duration
	HeartbeatEvery    time.Duration
	ReaperEvery       time.Duration
	DefaultQueueDepth int
	MaxSlotCost       int
	AllowShell        bool
	RedisURL          string
	RedisStream       string
	RedisGroup        string
	RedisConsumer     string
	RedisBlock        time.Duration
	RedisClaimIdle    time.Duration
	RedisBatchSize    int64
}

func ParseServer(args []string, stderr io.Writer) (Server, error) {
	defaults := Server{
		HTTPAddr:          envString("RAILYARD_HTTP_ADDR", ":8080"),
		DatabasePath:      envString("RAILYARD_DB_PATH", "data/railyard.db"),
		LeaseTTL:          envDuration("RAILYARD_LEASE_TTL", 2500*time.Millisecond),
		HeartbeatEvery:    envDuration("RAILYARD_HEARTBEAT_EVERY", time.Second),
		ReaperEvery:       envDuration("RAILYARD_REAPER_EVERY", 250*time.Millisecond),
		DefaultQueueDepth: envInt("RAILYARD_QUEUE_DEPTH", 100_000),
		MaxSlotCost:       envInt("RAILYARD_MAX_SLOT_COST", 64),
		AllowShell:        envBool("RAILYARD_ALLOW_SHELL", false),
		RedisURL:          envString("RAILYARD_REDIS_URL", ""),
		RedisStream:       envString("RAILYARD_REDIS_STREAM", "railyard:events"),
		RedisGroup:        envString("RAILYARD_REDIS_GROUP", "railyard"),
		RedisConsumer:     envString("RAILYARD_REDIS_CONSUMER", "railyard-server"),
		RedisBlock:        envDuration("RAILYARD_REDIS_BLOCK", time.Second),
		RedisClaimIdle:    envDuration("RAILYARD_REDIS_CLAIM_IDLE", 5*time.Second),
		RedisBatchSize:    int64(envInt("RAILYARD_REDIS_BATCH_SIZE", 64)),
	}

	flags := flag.NewFlagSet("railyard-server", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&defaults.HTTPAddr, "listen", defaults.HTTPAddr, "HTTP listen address")
	flags.StringVar(&defaults.DatabasePath, "db-path", defaults.DatabasePath, "SQLite database path")
	flags.DurationVar(&defaults.LeaseTTL, "lease-ttl", defaults.LeaseTTL, "worker lease duration")
	flags.DurationVar(&defaults.HeartbeatEvery, "heartbeat-every", defaults.HeartbeatEvery, "worker heartbeat interval")
	flags.DurationVar(&defaults.ReaperEvery, "reaper-every", defaults.ReaperEvery, "expired lease scan interval")
	flags.IntVar(&defaults.DefaultQueueDepth, "queue-depth", defaults.DefaultQueueDepth, "default per-tenant active job limit")
	flags.IntVar(&defaults.MaxSlotCost, "max-slot-cost", defaults.MaxSlotCost, "maximum accepted job slot cost")
	flags.BoolVar(&defaults.AllowShell, "allow-shell", defaults.AllowShell, "allow argv command payloads")
	flags.StringVar(&defaults.RedisURL, "redis-url", defaults.RedisURL, "Redis URL; empty disables Redis triggers")
	flags.StringVar(&defaults.RedisStream, "redis-stream", defaults.RedisStream, "Redis event stream")
	flags.StringVar(&defaults.RedisGroup, "redis-group", defaults.RedisGroup, "Redis consumer group")
	flags.StringVar(&defaults.RedisConsumer, "redis-consumer", defaults.RedisConsumer, "Redis consumer name")
	if err := flags.Parse(args); err != nil {
		return Server{}, err
	}
	if err := defaults.Validate(); err != nil {
		return Server{}, err
	}
	return defaults, nil
}

func (config Server) Validate() error {
	if config.HTTPAddr == "" {
		return errors.New("http address is required")
	}
	if config.DatabasePath == "" {
		return errors.New("database path is required")
	}
	if config.HeartbeatEvery <= 0 {
		return errors.New("heartbeat interval must be positive")
	}
	if config.LeaseTTL <= config.HeartbeatEvery {
		return errors.New("lease TTL must exceed heartbeat interval")
	}
	if config.ReaperEvery <= 0 || config.ReaperEvery >= config.LeaseTTL {
		return errors.New("reaper interval must be positive and shorter than lease TTL")
	}
	if config.DefaultQueueDepth < 1 {
		return errors.New("queue depth must be positive")
	}
	if config.MaxSlotCost < 1 {
		return errors.New("maximum slot cost must be positive")
	}
	if config.RedisURL != "" {
		if config.RedisStream == "" || config.RedisGroup == "" || config.RedisConsumer == "" {
			return errors.New("redis stream, group, and consumer are required")
		}
		if config.RedisBlock <= 0 || config.RedisClaimIdle <= 0 || config.RedisBatchSize < 1 {
			return errors.New("redis timing and batch settings must be positive")
		}
	}
	return nil
}

type Worker struct {
	ServerURL      string
	WorkerID       string
	Slots          int
	AllowShell     bool
	RequestTimeout time.Duration
}

func ParseWorker(args []string, stderr io.Writer) (Worker, error) {
	defaults := Worker{
		ServerURL:      envString("RAILYARD_SERVER_URL", "http://127.0.0.1:8080"),
		WorkerID:       envString("RAILYARD_WORKER_ID", ""),
		Slots:          envInt("RAILYARD_WORKER_SLOTS", 1),
		AllowShell:     envBool("RAILYARD_ALLOW_SHELL", false),
		RequestTimeout: envDuration("RAILYARD_REQUEST_TIMEOUT", 30*time.Second),
	}

	flags := flag.NewFlagSet("railyard-worker", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&defaults.ServerURL, "server-url", defaults.ServerURL, "Rail Yard server base URL")
	flags.StringVar(&defaults.WorkerID, "worker-id", defaults.WorkerID, "stable worker ID; generated when empty")
	flags.IntVar(&defaults.Slots, "slots", defaults.Slots, "worker execution slots")
	flags.BoolVar(&defaults.AllowShell, "allow-shell", defaults.AllowShell, "allow argv command payloads")
	flags.DurationVar(&defaults.RequestTimeout, "request-timeout", defaults.RequestTimeout, "HTTP request timeout")
	if err := flags.Parse(args); err != nil {
		return Worker{}, err
	}
	if defaults.ServerURL == "" {
		return Worker{}, errors.New("server URL is required")
	}
	if defaults.Slots < 1 {
		return Worker{}, errors.New("worker slots must be positive")
	}
	if defaults.RequestTimeout <= 0 {
		return Worker{}, errors.New("request timeout must be positive")
	}
	return defaults, nil
}

func envString(name, fallback string) string {
	if value, ok := os.LookupEnv(name); ok {
		return value
	}
	return fallback
}

func envInt(name string, fallback int) int {
	value, ok := os.LookupEnv(name)
	if !ok {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envDuration(name string, fallback time.Duration) time.Duration {
	value, ok := os.LookupEnv(name)
	if !ok {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}

func envBool(name string, fallback bool) bool {
	value, ok := os.LookupEnv(name)
	if !ok {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}
