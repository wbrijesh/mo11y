package auth

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
)

type keyInfoKey struct{}

// KeyFromContext returns the KeyInfo from context.
func KeyFromContext(ctx context.Context) *KeyInfo {
	if v, ok := ctx.Value(keyInfoKey{}).(*KeyInfo); ok {
		return v
	}
	return nil
}

// Middleware returns an auth middleware that validates API keys.
func (a *Auth) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := extractKey(r)
		if key == "" {
			authError(w, "missing authorization", http.StatusUnauthorized)
			return
		}

		if !strings.HasPrefix(key, "mo11y_") {
			authError(w, "invalid key format", http.StatusUnauthorized)
			return
		}

		info, err := a.ValidateKey(r.Context(), key)
		if err != nil {
			reqID := r.Context().Value(requestIDKey{})
			log.Printf("[%v] auth failed: %v", reqID, err)

			switch err {
			case ErrKeyRevoked:
				authError(w, "key revoked", http.StatusUnauthorized)
			case ErrKeyExpired:
				authError(w, "key expired", http.StatusUnauthorized)
			default:
				authError(w, "invalid key", http.StatusUnauthorized)
			}
			return
		}

		ctx := context.WithValue(r.Context(), keyInfoKey{}, info)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RequireScope returns middleware that checks for required scope.
func RequireScope(scope Scope) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			info := KeyFromContext(r.Context())
			if info == nil {
				authError(w, "missing authorization", http.StatusUnauthorized)
				return
			}

			if !info.Scopes.Has(scope) {
				authError(w, "insufficient permissions", http.StatusForbidden)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func extractKey(r *http.Request) string {
	// Try Authorization: Bearer <key>
	if auth := r.Header.Get("Authorization"); auth != "" {
		if strings.HasPrefix(auth, "Bearer ") {
			return strings.TrimPrefix(auth, "Bearer ")
		}
	}

	// Fallback: X-API-Key header
	if key := r.Header.Get("X-API-Key"); key != "" {
		return key
	}

	return ""
}

func authError(w http.ResponseWriter, msg string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// requestIDKey matches the one in server/middleware.go
type requestIDKey struct{}
