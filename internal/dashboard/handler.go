package dashboard

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/rajeev-chaurasia/rail-yard/internal/domain"
	"github.com/rajeev-chaurasia/rail-yard/web"
)

const (
	csrfCookieName   = "railyard_ops_csrf"
	maxActionBytes   = 8 << 10
	deadLetterLimit  = 100
	defaultBasePath  = "/ops/"
	defaultPollEvery = 5 * time.Second
	defaultTimeout   = 5 * time.Second
)

type Config struct {
	BasePath       string
	PollInterval   time.Duration
	RequestTimeout time.Duration
}

type handler struct {
	client         Client
	basePath       string
	pollInterval   time.Duration
	requestTimeout time.Duration
	page           *template.Template
	stylesheet     []byte
	script         []byte
}

type pageData struct {
	BasePath       string
	CSRFToken      string
	PollIntervalMS int64
}

type errorResponse struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type actionRequest struct {
	Action      Action      `json:"action"`
	ForceAction ForceAction `json:"force_action,omitempty"`
	JobID       string      `json:"job_id"`
	Actor       string      `json:"actor"`
	Reason      string      `json:"reason,omitempty"`
}

func New(client Client, config Config) (http.Handler, error) {
	if client == nil {
		return nil, errors.New("dashboard client is required")
	}

	basePath, err := normalizeBasePath(config.BasePath)
	if err != nil {
		return nil, err
	}
	pollInterval := config.PollInterval
	if pollInterval == 0 {
		pollInterval = defaultPollEvery
	}
	if pollInterval < 2*time.Second || pollInterval > time.Minute {
		return nil, errors.New("poll interval must be between 2 seconds and 1 minute")
	}
	requestTimeout := config.RequestTimeout
	if requestTimeout == 0 {
		requestTimeout = defaultTimeout
	}
	if requestTimeout < time.Second || requestTimeout > 30*time.Second {
		return nil, errors.New("request timeout must be between 1 and 30 seconds")
	}

	pageBytes, err := web.Files.ReadFile("templates/dashboard.html")
	if err != nil {
		return nil, fmt.Errorf("read dashboard template: %w", err)
	}
	page, err := template.New("dashboard").Parse(string(pageBytes))
	if err != nil {
		return nil, fmt.Errorf("parse dashboard template: %w", err)
	}
	stylesheet, err := web.Files.ReadFile("assets/app.css")
	if err != nil {
		return nil, fmt.Errorf("read dashboard stylesheet: %w", err)
	}
	script, err := web.Files.ReadFile("assets/app.js")
	if err != nil {
		return nil, fmt.Errorf("read dashboard script: %w", err)
	}

	dashboard := &handler{
		client:         client,
		basePath:       basePath,
		pollInterval:   pollInterval,
		requestTimeout: requestTimeout,
		page:           page,
		stylesheet:     stylesheet,
		script:         script,
	}
	protection := http.NewCrossOriginProtection()
	protection.SetDenyHandler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusForbidden, "cross_origin_denied", "cross-origin mutation denied")
	}))
	return protection.Handler(dashboard), nil
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	trimmedBasePath := strings.TrimSuffix(h.basePath, "/")
	if r.URL.Path == trimmedBasePath {
		if r.Method != http.MethodGet {
			methodNotAllowed(w, http.MethodGet)
			return
		}
		http.Redirect(w, r, h.basePath, http.StatusPermanentRedirect)
		return
	}
	if !strings.HasPrefix(r.URL.Path, h.basePath) {
		writeError(w, http.StatusNotFound, "not_found", "route not found")
		return
	}

	relativePath := strings.TrimPrefix(r.URL.Path, h.basePath)
	switch {
	case relativePath == "":
		h.servePage(w, r)
	case relativePath == "assets/app.css":
		h.serveAsset(w, r, "text/css; charset=utf-8", h.stylesheet)
	case relativePath == "assets/app.js":
		h.serveAsset(w, r, "text/javascript; charset=utf-8", h.script)
	case relativePath == "api/snapshot":
		h.serveSnapshot(w, r)
	case relativePath == "api/dead-letters":
		h.serveDeadLetters(w, r)
	case strings.HasPrefix(relativePath, "api/runs/"):
		h.serveRun(w, r, strings.TrimPrefix(relativePath, "api/runs/"))
	case relativePath == "api/actions":
		h.serveAction(w, r)
	default:
		writeError(w, http.StatusNotFound, "not_found", "route not found")
	}
}

func (h *handler) servePage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	token := ""
	if cookie, err := r.Cookie(csrfCookieName); err == nil && validToken(cookie.Value) {
		token = cookie.Value
	}
	if token == "" {
		var err error
		token, err = randomToken()
		if err != nil {
			writeError(w, http.StatusInternalServerError, "internal_error", "dashboard could not initialize")
			return
		}
	}
	http.SetCookie(w, &http.Cookie{
		Name:     csrfCookieName,
		Value:    token,
		Path:     h.basePath,
		MaxAge:   8 * 60 * 60,
		Secure:   r.TLS != nil,
		HttpOnly: false,
		SameSite: http.SameSiteStrictMode,
	})
	setSecurityHeaders(w)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.page.Execute(w, pageData{
		BasePath:       h.basePath,
		CSRFToken:      token,
		PollIntervalMS: h.pollInterval.Milliseconds(),
	}); err != nil {
		return
	}
}

func (h *handler) serveAsset(w http.ResponseWriter, r *http.Request, contentType string, content []byte) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	setSecurityHeaders(w)
	w.Header().Set("Cache-Control", "public, max-age=3600")
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	_, _ = w.Write(content)
}

func (h *handler) serveSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.requestTimeout)
	defer cancel()
	snapshot, err := h.client.Snapshot(ctx)
	if err != nil {
		writeClientError(w, err)
		return
	}
	if snapshot.QueueDepths == nil {
		snapshot.QueueDepths = []QueueDepth{}
	}
	if snapshot.RunningJobs == nil {
		snapshot.RunningJobs = []JobSummary{}
	}
	if snapshot.FailedJobs == nil {
		snapshot.FailedJobs = []JobSummary{}
	}
	if snapshot.Workers == nil {
		snapshot.Workers = []WorkerSummary{}
	}
	writeJSON(w, http.StatusOK, snapshot)
}

func (h *handler) serveDeadLetters(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.requestTimeout)
	defer cancel()
	deadLetters, err := h.client.DeadLetters(ctx, deadLetterLimit)
	if err != nil {
		writeClientError(w, err)
		return
	}
	if deadLetters == nil {
		deadLetters = []domain.DeadLetter{}
	}
	writeJSON(w, http.StatusOK, struct {
		DeadLetters []domain.DeadLetter `json:"dead_letters"`
	}{DeadLetters: deadLetters})
}

func (h *handler) serveRun(w http.ResponseWriter, r *http.Request, runID string) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	if err := validateIdentifier("run_id", runID); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.requestTimeout)
	defer cancel()
	run, err := h.client.Run(ctx, runID)
	if err != nil {
		writeClientError(w, err)
		return
	}
	if run.Nodes == nil {
		run.Nodes = []RunNode{}
	}
	writeJSON(w, http.StatusOK, run)
}

func (h *handler) serveAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	if !strings.HasPrefix(strings.ToLower(r.Header.Get("Content-Type")), "application/json") {
		writeError(w, http.StatusUnsupportedMediaType, "unsupported_media_type", "Content-Type must be application/json")
		return
	}
	if !h.validCSRF(r) {
		writeError(w, http.StatusForbidden, "csrf_failed", "CSRF validation failed")
		return
	}

	var request actionRequest
	if err := decodeAction(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if err := validateAction(request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	requestID, err := randomToken()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal_error", "operation could not be initialized")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.requestTimeout)
	defer cancel()
	result, err := h.client.Operate(ctx, Operation{
		Action:      request.Action,
		ForceAction: request.ForceAction,
		JobID:       request.JobID,
		Actor:       request.Actor,
		Reason:      request.Reason,
		RequestID:   requestID,
	})
	if err != nil {
		writeClientError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *handler) validCSRF(r *http.Request) bool {
	cookie, err := r.Cookie(csrfCookieName)
	if err != nil || !validToken(cookie.Value) {
		return false
	}
	header := r.Header.Get("X-CSRF-Token")
	return len(header) == len(cookie.Value) &&
		subtle.ConstantTimeCompare([]byte(header), []byte(cookie.Value)) == 1
}

func decodeAction(w http.ResponseWriter, r *http.Request, target *actionRequest) error {
	body := http.MaxBytesReader(w, r.Body, maxActionBytes)
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			return errors.New("request body exceeds 8 KiB")
		}
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

func validateAction(request actionRequest) error {
	switch request.Action {
	case ActionCancel, ActionRetry, ActionForce, ActionRedrive:
	default:
		return errors.New("action must be cancel, retry, force, or redrive")
	}
	if request.Action == ActionForce {
		switch request.ForceAction {
		case ForceRelease, ForceFail, ForceDeadLetter:
		default:
			return errors.New("force_action must be release, fail, or dead_letter")
		}
	} else if request.ForceAction != "" {
		return errors.New("force_action is only valid for force operations")
	}
	if err := validateIdentifier("job_id", request.JobID); err != nil {
		return err
	}
	if strings.TrimSpace(request.Actor) == "" {
		return errors.New("actor is required")
	}
	if strings.TrimSpace(request.Actor) != request.Actor {
		return errors.New("actor must not have leading or trailing whitespace")
	}
	if len(request.Actor) > 128 {
		return errors.New("actor exceeds 128 bytes")
	}
	for _, character := range request.Actor {
		if character < 0x20 || character == 0x7f {
			return errors.New("actor contains a control character")
		}
	}
	if request.Action == ActionCancel || request.Action == ActionForce {
		if strings.TrimSpace(request.Reason) == "" {
			return errors.New("reason is required for cancel and force operations")
		}
		if strings.TrimSpace(request.Reason) != request.Reason {
			return errors.New("reason must not have leading or trailing whitespace")
		}
		if len(request.Reason) > 1024 {
			return errors.New("reason exceeds 1024 bytes")
		}
		for _, character := range request.Reason {
			if character < 0x20 || character == 0x7f {
				return errors.New("reason contains a control character")
			}
		}
	} else if request.Reason != "" {
		return errors.New("reason is only valid for cancel and force operations")
	}
	return nil
}

func validateIdentifier(name, value string) error {
	if strings.TrimSpace(value) == "" {
		return fmt.Errorf("%s is required", name)
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("%s must not have leading or trailing whitespace", name)
	}
	if len(value) > 128 {
		return fmt.Errorf("%s exceeds 128 bytes", name)
	}
	for _, character := range value {
		if character < 0x21 || character == 0x7f || character == '/' || character == '\\' {
			return fmt.Errorf("%s contains an unsupported character", name)
		}
	}
	return nil
}

func normalizeBasePath(basePath string) (string, error) {
	if basePath == "" {
		return defaultBasePath, nil
	}
	if !strings.HasPrefix(basePath, "/") || !strings.HasSuffix(basePath, "/") {
		return "", errors.New("base path must start and end with a slash")
	}
	if strings.Contains(basePath, "//") || strings.ContainsAny(basePath, "?#\\") {
		return "", errors.New("base path contains an unsupported character")
	}
	for _, segment := range strings.Split(strings.Trim(basePath, "/"), "/") {
		if segment == "" || segment == "." || segment == ".." {
			return "", errors.New("base path contains an invalid segment")
		}
		for _, character := range segment {
			if character >= 'a' && character <= 'z' ||
				character >= 'A' && character <= 'Z' ||
				character >= '0' && character <= '9' ||
				character == '-' ||
				character == '_' {
				continue
			}
			return "", errors.New("base path contains an unsupported character")
		}
	}
	return basePath, nil
}

func randomToken() (string, error) {
	var value [32]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func validToken(token string) bool {
	if len(token) != 64 {
		return false
	}
	_, err := hex.DecodeString(token)
	return err == nil
}

func writeClientError(w http.ResponseWriter, err error) {
	var clientError *ClientError
	if errors.As(err, &clientError) {
		normalized := clientError.normalized()
		writeError(w, normalized.Status, normalized.Code, normalized.Message)
		return
	}
	writeError(w, http.StatusServiceUnavailable, "backend_unavailable", "dashboard data is unavailable")
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	setSecurityHeaders(w)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	response := errorResponse{}
	response.Error.Code = code
	response.Error.Message = message
	writeJSON(w, status, response)
}

func methodNotAllowed(w http.ResponseWriter, allowed string) {
	w.Header().Set("Allow", allowed)
	writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
}

func setSecurityHeaders(w http.ResponseWriter) {
	w.Header().Set(
		"Content-Security-Policy",
		"default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; "+
			"base-uri 'none'; form-action 'self'; frame-ancestors 'none'",
	)
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
}
