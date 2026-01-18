package server

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"mo11y/internal/auth"
	"mo11y/internal/storage"
)

// Config holds server configuration.
type Config struct {
	Port               int
	RetentionCfg       storage.CleanupConfig
	MaxConcurrentIngest int
	MaxConcurrentQuery  int
}

// New creates a new HTTP server with OTLP endpoints.
func New(cfg Config, store *storage.Storage, authProvider *auth.Auth) *http.Server {
	mux := http.NewServeMux()

	// Semaphores for backpressure
	ingestSem := NewSemaphore(cfg.MaxConcurrentIngest)
	querySem := NewSemaphore(cfg.MaxConcurrentQuery)

	// Health endpoint (always public)
	mux.HandleFunc("/health", handleHealth(store))

	if authProvider == nil {
		// Auth disabled - mount handlers directly
		mux.HandleFunc("/stats", handleStats(store, cfg.RetentionCfg))
		mux.Handle("/query", querySem.Middleware(http.HandlerFunc(handleQuery(store))))
		mux.Handle("/v1/traces", ingestSem.Middleware(http.HandlerFunc(handleTraces(store))))
		mux.Handle("/v1/metrics", ingestSem.Middleware(http.HandlerFunc(handleMetrics(store))))
		mux.Handle("/v1/logs", ingestSem.Middleware(http.HandlerFunc(handleLogs(store))))
	} else {
		// Auth enabled - wrap with auth middleware
		// Read endpoints
		mux.Handle("/stats", authProvider.Middleware(
			auth.RequireScope(auth.ScopeRead)(http.HandlerFunc(handleStats(store, cfg.RetentionCfg)))))
		mux.Handle("/query", authProvider.Middleware(
			auth.RequireScope(auth.ScopeRead)(querySem.Middleware(http.HandlerFunc(handleQuery(store))))))

		// Ingest endpoints
		mux.Handle("/v1/traces", authProvider.Middleware(
			auth.RequireScope(auth.ScopeIngest)(ingestSem.Middleware(http.HandlerFunc(handleTraces(store))))))
		mux.Handle("/v1/metrics", authProvider.Middleware(
			auth.RequireScope(auth.ScopeIngest)(ingestSem.Middleware(http.HandlerFunc(handleMetrics(store))))))
		mux.Handle("/v1/logs", authProvider.Middleware(
			auth.RequireScope(auth.ScopeIngest)(ingestSem.Middleware(http.HandlerFunc(handleLogs(store))))))

		// Admin endpoints
		mux.Handle("/admin/keys", authProvider.Middleware(
			auth.RequireScope(auth.ScopeAdmin)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.Method {
				case http.MethodGet:
					handleListKeys(authProvider)(w, r)
				case http.MethodPost:
					handleCreateKey(authProvider)(w, r)
				default:
					w.WriteHeader(http.StatusMethodNotAllowed)
				}
			}))))
		mux.Handle("/admin/keys/", authProvider.Middleware(
			auth.RequireScope(auth.ScopeAdmin)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/admin/keys/") {
					handleRevokeKey(authProvider)(w, r)
				} else {
					w.WriteHeader(http.StatusMethodNotAllowed)
				}
			}))))
	}

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
