package server

import (
	"compress/gzip"
	"context"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/google/uuid"
)

const maxRequestSize = 10 * 1024 * 1024 // 10MB

// requestIDKey is the context key for request ID.
type requestIDKey struct{}

// RequestID returns the request ID from context, or empty string if not set.
func RequestID(ctx context.Context) string {
	if id, ok := ctx.Value(requestIDKey{}).(string); ok {
		return id
	}
	return ""
}

// chain applies middleware in the order they execute (first to last).
// Given: chain(handler, A, B, C)
// Execution order: A -> B -> C -> handler -> C -> B -> A
//
// The middlewares are applied by wrapping from right to left,
// so the first middleware in the list executes first on the request path.
func chain(h http.Handler, middlewares ...func(http.Handler) http.Handler) http.Handler {
	for i := len(middlewares) - 1; i >= 0; i-- {
		h = middlewares[i](h)
	}
	return h
}

// requestIDMiddleware assigns a UUID to each request and stores it in context.
func requestIDMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := uuid.New().String()
		ctx := context.WithValue(r.Context(), requestIDKey{}, id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// recoveryMiddleware catches panics and returns 503.
func recoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				reqID := RequestID(r.Context())
				log.Printf("[%s] panic recovered: %v", reqID, err)
				w.WriteHeader(http.StatusServiceUnavailable)
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// sizeLimitMiddleware enforces max request body size.
func sizeLimitMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestSize)
		next.ServeHTTP(w, r)
	})
}

// gzipMiddleware decompresses gzip-encoded request bodies.
// Rejects unsupported Content-Encoding values with 415.
// Removes Content-Encoding header after successful decompression.
func gzipMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		encoding := r.Header.Get("Content-Encoding")
		if encoding == "" {
			next.ServeHTTP(w, r)
			return
		}

		if !strings.EqualFold(encoding, "gzip") {
			reqID := RequestID(r.Context())
			log.Printf("[%s] unsupported Content-Encoding: %s", reqID, encoding)
			w.WriteHeader(http.StatusUnsupportedMediaType)
			return
		}

		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			reqID := RequestID(r.Context())
			log.Printf("[%s] gzip decompression failed: %v", reqID, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		defer gz.Close()

		r.Body = io.NopCloser(gz)
		r.Header.Del("Content-Encoding")
		next.ServeHTTP(w, r)
	})
}
