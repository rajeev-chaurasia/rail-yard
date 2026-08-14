package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/api"
	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	"github.com/rajeev-chaurasia/rail-yard/internal/operations"
	"github.com/rajeev-chaurasia/rail-yard/internal/store/sqlite"
)

const actorHeader = "X-Rail-Yard-Actor"

type actorContextKey struct{}

type Handler struct {
	adapter    *Adapter
	operations http.Handler
}

type operatorActionRequest struct {
	Action     string            `json:"action"`
	TargetType string            `json:"target_type"`
	TargetID   string            `json:"target_id"`
	Details    map[string]string `json:"details,omitempty"`
}

func NewHandler(adapter *Adapter, config operations.Config) (*Handler, error) {
	if adapter == nil || adapter.store == nil {
		return nil, errors.New("control adapter is required")
	}
	operationsHandler, err := operations.New(adapter.Repositories(), config)
	if err != nil {
		return nil, err
	}
	return &Handler{adapter: adapter, operations: operationsHandler}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/v1/operations/operator-actions":
		h.serveOperatorAction(w, r)
	case "/v1/operations/audit-events":
		h.serveAuditEvents(w, r)
	default:
		actor := strings.TrimSpace(r.Header.Get(actorHeader))
		if actor == "" {
			actor = "api"
		}
		ctx := context.WithValue(r.Context(), actorContextKey{}, actor)
		h.operations.ServeHTTP(w, r.WithContext(ctx))
	}
}

func (h *Handler) serveOperatorAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	key, err := requiredHeader(r, "Idempotency-Key")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	actor, err := requiredHeader(r, actorHeader)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	var request operatorActionRequest
	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if err := validateOperatorAction(request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	requestDigest, err := digest(struct {
		Actor string `json:"actor"`
		operatorActionRequest
	}{Actor: actor, operatorActionRequest: request})
	if err != nil {
		writeControlError(w, err)
		return
	}
	event, duplicate, err := h.adapter.store.RecordOperatorAction(
		r.Context(),
		sqlite.ControlAction{
			IdempotencyKey: key,
			Action:         request.Action,
			Actor:          actor,
			RequestDigest:  requestDigest,
			CommittedAt:    h.adapter.now().UTC(),
			TargetType:     request.TargetType,
			TargetID:       request.TargetID,
			Details:        request.Details,
		},
	)
	if err != nil {
		writeControlError(w, err)
		return
	}
	status := http.StatusCreated
	if duplicate {
		status = http.StatusOK
	}
	writeJSON(w, status, struct {
		Event sqlite.AuditEvent `json:"event"`
	}{Event: event})
}

func (h *Handler) serveAuditEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	since := time.Unix(0, 0).UTC()
	if raw := r.URL.Query().Get("since"); raw != "" {
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "since must be an RFC3339 timestamp")
			return
		}
		since = parsed.UTC()
	}
	actor := r.URL.Query().Get("actor")
	if actor != strings.TrimSpace(actor) {
		writeError(w, http.StatusBadRequest, "invalid_request", "actor must not have surrounding whitespace")
		return
	}
	events, err := h.adapter.store.ListAuditEvents(r.Context(), since, actor)
	if err != nil {
		writeControlError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, struct {
		Events []sqlite.AuditEvent `json:"events"`
	}{Events: events})
}

func requestActor(ctx context.Context) string {
	actor, _ := ctx.Value(actorContextKey{}).(string)
	if actor == "" {
		return "api"
	}
	return actor
}

func requiredHeader(r *http.Request, name string) (string, error) {
	values := r.Header.Values(name)
	if len(values) != 1 {
		return "", fmt.Errorf("exactly one %s header is required", name)
	}
	value := strings.TrimSpace(values[0])
	if value == "" {
		return "", fmt.Errorf("%s must not be empty", name)
	}
	if len(value) > 256 {
		return "", fmt.Errorf("%s exceeds 256 bytes", name)
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e {
			return "", fmt.Errorf("%s must contain printable ASCII without spaces", name)
		}
	}
	return value, nil
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) error {
	body := http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		if errors.Is(err, io.EOF) {
			return errors.New("request body is required")
		}
		return errors.New("request body contains malformed JSON")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain exactly one JSON value")
	}
	return nil
}

func validateOperatorAction(request operatorActionRequest) error {
	if err := validateText("action", request.Action, 128); err != nil {
		return err
	}
	if err := validateText("target_type", request.TargetType, 128); err != nil {
		return err
	}
	if err := validateText("target_id", request.TargetID, 128); err != nil {
		return err
	}
	if len(request.Details) > 64 {
		return errors.New("details exceeds 64 entries")
	}
	for key, value := range request.Details {
		if err := validateText("details key", key, 128); err != nil {
			return err
		}
		if len(value) > 2048 {
			return errors.New("details value exceeds 2048 bytes")
		}
	}
	return nil
}

func validateText(name, value string, limit int) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", name)
	}
	if value != strings.TrimSpace(value) {
		return fmt.Errorf("%s must not have surrounding whitespace", name)
	}
	if len(value) > limit {
		return fmt.Errorf("%s exceeds %d bytes", name, limit)
	}
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return fmt.Errorf("%s contains a control character", name)
		}
	}
	return nil
}

func writeControlError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "resource not found")
	case errors.Is(err, domain.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, "idempotency_conflict", "idempotency key conflicts with an existing request")
	case errors.Is(err, domain.ErrTerminalJob), errors.Is(err, operations.ErrConflict):
		writeError(w, http.StatusConflict, "state_conflict", "operation conflicts with the current resource state")
	default:
		writeError(w, http.StatusInternalServerError, "internal_error", "internal server error")
	}
}

func methodNotAllowed(w http.ResponseWriter, method string) {
	w.Header().Set("Allow", method)
	writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, api.ErrorResponse{Code: code, Message: message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
