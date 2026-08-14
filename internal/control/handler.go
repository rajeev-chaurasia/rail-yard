package control

import (
	"errors"
	"net/http"

	"github.com/rajeev-chaurasia/rail-yard/internal/operations"
)

const actorHeader = "X-Rail-Yard-Actor"

type Handler struct {
	operations http.Handler
}

func NewHandler(adapter *Adapter, config operations.Config) (*Handler, error) {
	if adapter == nil || adapter.store == nil {
		return nil, errors.New("control adapter is required")
	}
	operationsHandler, err := operations.New(adapter.Repositories(), config)
	if err != nil {
		return nil, err
	}
	return &Handler{operations: operationsHandler}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.operations.ServeHTTP(w, r)
}
