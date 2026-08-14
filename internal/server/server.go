package server

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/store"
)

type Server struct {
	store  store.Store
	config Config

	backgroundContext context.Context
	stopBackground    context.CancelFunc
	background        sync.WaitGroup

	requestsMu sync.Mutex
	draining   bool
	requests   sync.WaitGroup

	workersMu sync.RWMutex
	workers   map[string]int

	ready atomic.Bool

	shutdownOnce sync.Once
	shutdownDone chan struct{}
	shutdownErr  error
}

func New(jobStore store.Store, config Config) (*Server, error) {
	if jobStore == nil {
		return nil, fmt.Errorf("create server: store is required")
	}

	normalized, err := config.normalized()
	if err != nil {
		return nil, fmt.Errorf("create server: %w", err)
	}

	backgroundContext, stopBackground := context.WithCancel(context.Background())
	server := &Server{
		store:             jobStore,
		config:            normalized,
		backgroundContext: backgroundContext,
		stopBackground:    stopBackground,
		workers:           make(map[string]int),
		shutdownDone:      make(chan struct{}),
	}
	server.ready.Store(true)
	server.startBackgroundLoops()
	return server, nil
}

func (s *Server) Handler() http.Handler {
	return s
}

func (s *Server) HTTPServer(address string) *http.Server {
	writeTimeout := s.config.WriteTimeout
	minimumWriteTimeout := s.config.LongPollTimeout + s.config.RequestTimeout + time.Second
	if writeTimeout < minimumWriteTimeout {
		writeTimeout = minimumWriteTimeout
	}

	return &http.Server{
		Addr:              address,
		Handler:           s,
		ReadTimeout:       s.config.ReadTimeout,
		ReadHeaderTimeout: s.config.ReadHeaderTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       s.config.IdleTimeout,
		MaxHeaderBytes:    s.config.MaxHeaderBytes,
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.shutdownOnce.Do(func() {
		s.requestsMu.Lock()
		s.draining = true
		s.ready.Store(false)
		s.requestsMu.Unlock()

		s.stopBackground()
		go func() {
			s.background.Wait()
			s.requests.Wait()
			s.shutdownErr = s.store.Close()
			close(s.shutdownDone)
		}()
	})

	select {
	case <-s.shutdownDone:
		return s.shutdownErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Server) startBackgroundLoops() {
	s.startLoop("reap expired leases", s.config.ReaperInterval, func(ctx context.Context) error {
		_, err := s.store.ReapExpired(ctx, s.config.Now().UTC(), s.config.ReaperBatchSize)
		return err
	})
	s.startLoop("promote due jobs", s.config.DuePromotionInterval, func(ctx context.Context) error {
		_, err := s.store.PromoteDue(ctx, s.config.Now().UTC(), s.config.PromotionBatchSize)
		return err
	})
	if s.config.TriggerStore != nil {
		s.startLoop("fire due cron triggers", s.config.CronInterval, func(ctx context.Context) error {
			_, err := s.config.TriggerStore.FireDueCron(
				ctx,
				s.config.Now().UTC(),
				s.config.CronBatchSize,
			)
			return err
		})
	}
}

func (s *Server) startLoop(name string, interval time.Duration, operation func(context.Context) error) {
	s.background.Add(1)
	go func() {
		defer s.background.Done()

		run := func() {
			operationContext, cancel := context.WithTimeout(
				s.backgroundContext,
				s.config.BackgroundOperationTimeout,
			)
			defer cancel()

			if err := operation(operationContext); err != nil &&
				s.backgroundContext.Err() == nil {
				s.config.OnError(fmt.Errorf("%s: %w", name, err))
			}
		}

		select {
		case <-s.backgroundContext.Done():
			return
		default:
			run()
		}

		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.backgroundContext.Done():
				return
			case <-ticker.C:
				run()
			}
		}
	}()
}

func (s *Server) beginRequest() bool {
	s.requestsMu.Lock()
	defer s.requestsMu.Unlock()
	if s.draining {
		return false
	}
	s.requests.Add(1)
	return true
}

func (s *Server) endRequest() {
	s.requests.Done()
}

func (s *Server) isDraining() bool {
	s.requestsMu.Lock()
	defer s.requestsMu.Unlock()
	return s.draining
}
