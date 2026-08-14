package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/rajeev-chaurasia/rail-yard/internal/api"
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
)

const maxIdempotencyKeyBytes = 256

type httpError struct {
	status     int
	code       string
	message    string
	retryAfter int
}

func (e *httpError) Error() string {
	return e.message
}

func invalidRequest(message string) *httpError {
	return &httpError{
		status:  http.StatusBadRequest,
		code:    "invalid_request",
		message: message,
	}
}

func (s *Server) decodeJSON(w http.ResponseWriter, r *http.Request, target any) *httpError {
	body := http.MaxBytesReader(w, r.Body, s.config.MaxBodyBytes)
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(target); err != nil {
		return decodeError(err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err != nil {
			return decodeError(err)
		}
		return invalidRequest("request body must contain exactly one JSON value")
	}
	return nil
}

func decodeError(err error) *httpError {
	var maxBytesError *http.MaxBytesError
	if errors.As(err, &maxBytesError) {
		return &httpError{
			status:  http.StatusRequestEntityTooLarge,
			code:    "request_too_large",
			message: "request body exceeds the configured limit",
		}
	}

	if errors.Is(err, io.EOF) {
		return invalidRequest("request body is required")
	}
	if strings.HasPrefix(err.Error(), "json: unknown field ") {
		return invalidRequest("request body contains an unknown field")
	}
	return invalidRequest("request body contains malformed JSON")
}

func idempotencyKey(r *http.Request) (string, *httpError) {
	values := r.Header.Values("Idempotency-Key")
	if len(values) != 1 {
		return "", invalidRequest("exactly one Idempotency-Key header is required")
	}

	key := strings.TrimSpace(values[0])
	if key == "" {
		return "", invalidRequest("Idempotency-Key must not be empty")
	}
	if len(key) > maxIdempotencyKeyBytes {
		return "", invalidRequest("Idempotency-Key exceeds 256 bytes")
	}
	for _, character := range key {
		if character < 0x21 || character > 0x7e {
			return "", invalidRequest("Idempotency-Key must contain printable ASCII without spaces")
		}
	}
	return key, nil
}

func stableDigest(value any) (string, error) {
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:]), nil
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeHTTPError(w http.ResponseWriter, err *httpError) {
	if err.retryAfter > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(err.retryAfter))
	}
	writeJSON(w, err.status, api.ErrorResponse{
		Code:       err.code,
		Message:    err.message,
		RetryAfter: err.retryAfter,
	})
}

func writeStoreError(w http.ResponseWriter, err error) {
	writeHTTPError(w, mapStoreError(err))
}

func mapStoreError(err error) *httpError {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		return &httpError{status: http.StatusNotFound, code: "not_found", message: "resource not found"}
	case errors.Is(err, domain.ErrQueueFull):
		return &httpError{
			status:     http.StatusTooManyRequests,
			code:       "queue_full",
			message:    "tenant queue is full",
			retryAfter: 1,
		}
	case errors.Is(err, domain.ErrIdempotencyConflict):
		return &httpError{
			status:  http.StatusConflict,
			code:    "idempotency_conflict",
			message: "idempotency key conflicts with an existing request",
		}
	case errors.Is(err, domain.ErrCycleDetected):
		return &httpError{
			status:  http.StatusConflict,
			code:    "cycle_detected",
			message: "workflow contains a dependency cycle",
		}
	case errors.Is(err, domain.ErrStaleLease):
		return &httpError{
			status:  http.StatusConflict,
			code:    "stale_lease",
			message: "lease is stale",
		}
	case errors.Is(err, domain.ErrTerminalJob):
		return &httpError{
			status:  http.StatusConflict,
			code:    "terminal_job",
			message: "job is already terminal",
		}
	case errors.Is(err, domain.ErrDeadLetterRedriven):
		return &httpError{
			status:  http.StatusConflict,
			code:    "dead_letter_redriven",
			message: "dead letter was already redriven",
		}
	case errors.Is(err, context.DeadlineExceeded):
		return &httpError{
			status:  http.StatusGatewayTimeout,
			code:    "request_timeout",
			message: "request deadline exceeded",
		}
	case errors.Is(err, context.Canceled):
		return &httpError{
			status:  http.StatusRequestTimeout,
			code:    "request_canceled",
			message: "request was canceled",
		}
	default:
		return &httpError{
			status:  http.StatusInternalServerError,
			code:    "internal_error",
			message: "internal server error",
		}
	}
}

func methodNotAllowed(w http.ResponseWriter, allowed string) {
	w.Header().Set("Allow", allowed)
	writeHTTPError(w, &httpError{
		status:  http.StatusMethodNotAllowed,
		code:    "method_not_allowed",
		message: "method not allowed",
	})
}
