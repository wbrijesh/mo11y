package server

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"mo11y/internal/auth"
)

func handleListKeys(a *auth.Auth) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		keys, err := a.ListKeys(r.Context())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		type keyResponse struct {
			ID         string  `json:"id"`
			Name       string  `json:"name"`
			Prefix     string  `json:"prefix"`
			Scopes     string  `json:"scopes"`
			CreatedAt  string  `json:"created_at"`
			ExpiresAt  *string `json:"expires_at,omitempty"`
			LastUsedAt *string `json:"last_used_at,omitempty"`
			Revoked    bool    `json:"revoked"`
		}

		resp := make([]keyResponse, len(keys))
		for i, k := range keys {
			resp[i] = keyResponse{
				ID:        k.ID,
				Name:      k.Name,
				Prefix:    k.Prefix,
				Scopes:    k.Scopes.String(),
				CreatedAt: k.CreatedAt.Format(time.RFC3339),
				Revoked:   k.Revoked,
			}
			if k.ExpiresAt != nil {
				s := k.ExpiresAt.Format(time.RFC3339)
				resp[i].ExpiresAt = &s
			}
			if k.LastUsedAt != nil {
				s := k.LastUsedAt.Format(time.RFC3339)
				resp[i].LastUsedAt = &s
			}
		}

		json.NewEncoder(w).Encode(resp)
	}
}

func handleCreateKey(a *auth.Auth) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			Name   string `json:"name"`
			Scopes string `json:"scopes"` // comma-separated: "ingest,read"
		}

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "invalid request body"})
			return
		}

		if req.Name == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "name is required"})
			return
		}

		scopes := auth.ParseScopes(req.Scopes)
		if scopes == 0 {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "at least one scope required (ingest, read, admin)"})
			return
		}

		createdBy := ""
		if info := auth.KeyFromContext(r.Context()); info != nil {
			createdBy = info.ID
		}

		key, info, err := a.CreateKey(r.Context(), req.Name, scopes, nil, createdBy)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(map[string]any{
			"id":     info.ID,
			"name":   info.Name,
			"key":    key, // Only time the full key is returned
			"scopes": info.Scopes.String(),
		})
	}
}

func handleRevokeKey(a *auth.Auth) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method != http.MethodDelete {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		// Extract key ID from path: /admin/keys/{id}
		path := strings.TrimPrefix(r.URL.Path, "/admin/keys/")
		if path == "" || path == r.URL.Path {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "key id required"})
			return
		}

		err := a.RevokeKey(r.Context(), path)
		if err == auth.ErrKeyNotFound {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]string{"error": "key not found"})
			return
		}
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		json.NewEncoder(w).Encode(map[string]string{"status": "revoked"})
	}
}
