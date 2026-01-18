package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"mo11y/internal/storage"
)

const (
	queryTimeout  = 5 * time.Second
	queryRowLimit = 1000
)

func handleQuery(store *storage.Storage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		reqID := RequestID(r.Context())
		sql := strings.TrimSpace(r.FormValue("sql"))

		// Validate: non-empty
		if sql == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "missing sql parameter"})
			return
		}

		// Validate: SELECT only
		if !strings.HasPrefix(strings.ToUpper(sql), "SELECT") {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "only SELECT queries allowed"})
			return
		}

		// Validate: no multi-statement
		if strings.Contains(sql, ";") {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "multi-statement queries not allowed"})
			return
		}

		// Add LIMIT if not present
		if !strings.Contains(strings.ToUpper(sql), "LIMIT") {
			sql = fmt.Sprintf("%s LIMIT %d", sql, queryRowLimit)
		}

		// Context with timeout
		ctx, cancel := context.WithTimeout(r.Context(), queryTimeout)
		defer cancel()

		rows, err := store.DB().QueryContext(ctx, sql)
		if err != nil {
			log.Printf("[%s] query error: %v", reqID, err)
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		defer rows.Close()

		cols, _ := rows.Columns()
		var results []map[string]any

		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				log.Printf("[%s] scan error: %v", reqID, err)
				w.WriteHeader(http.StatusInternalServerError)
				json.NewEncoder(w).Encode(map[string]string{"error": "failed to scan row"})
				return
			}

			row := make(map[string]any)
			for i, col := range cols {
				row[col] = vals[i]
			}
			results = append(results, row)
		}

		if err := rows.Err(); err != nil {
			log.Printf("[%s] rows error: %v", reqID, err)
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": "error reading results"})
			return
		}

		json.NewEncoder(w).Encode(map[string]any{
			"columns": cols,
			"rows":    results,
			"count":   len(results),
		})
	}
}
