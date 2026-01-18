package server

import (
	"fmt"
	"net/http"
	"time"

	"mo11y/internal/storage"
)

// Config holds server configuration.
type Config struct {
	Port int
}

// New creates a new HTTP server with OTLP endpoints.
func New(cfg Config, store *storage.Storage) *http.Server {
	mux := http.NewServeMux()

	// Health endpoint (JSON)
	mux.HandleFunc("/health", handleHealth(store))

	// Query endpoint (JSON)
	mux.HandleFunc("/query", handleQuery(store))

	// OTLP endpoints (protobuf)
	mux.HandleFunc("/v1/traces", handleTraces(store))
	mux.HandleFunc("/v1/metrics", handleMetrics(store))
	mux.HandleFunc("/v1/logs", handleLogs(store))

	// Middleware execution order (request path):
	// requestID -> recovery -> sizeLimit -> gzip -> handler
	handler := chain(mux,
		requestIDMiddleware,
		recoveryMiddleware,
		sizeLimitMiddleware,
		gzipMiddleware,
	)

	return &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      handler,
		IdleTimeout:  time.Minute,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
}
