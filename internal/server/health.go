package server

import (
	"context"
	"encoding/json"
	"net/http"

	"mo11y/internal/storage"
)

// HealthResponse is the JSON response for health checks.
type HealthResponse struct {
	Status   string `json:"status"`
	Database string `json:"database"`
	Message  string `json:"message,omitempty"`
}

// handleHealth returns the health status as JSON.
func handleHealth(store *storage.Storage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		resp := HealthResponse{
			Status:  "healthy",
			Message: "mo11y is running",
		}

		if err := store.Health(context.Background()); err != nil {
			resp.Status = "unhealthy"
			resp.Database = "disconnected"
			resp.Message = err.Error()
		} else {
			resp.Database = "connected"
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}
