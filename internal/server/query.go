package server

import (
	"encoding/json"
	"log"
	"net/http"

	"mo11y/internal/storage"
)

func handleQuery(store *storage.Storage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		reqID := RequestID(r.Context())
		sql := r.FormValue("sql")
		if sql == "" {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]string{"error": "missing sql parameter"})
			return
		}

		rows, err := store.DB().QueryContext(r.Context(), sql)
		if err != nil {
			log.Printf("[%s] query error: %v", reqID, err)
			w.Header().Set("Content-Type", "application/json")
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
			rows.Scan(ptrs...)

			row := make(map[string]any)
			for i, col := range cols {
				row[col] = vals[i]
			}
			results = append(results, row)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"columns": cols,
			"rows":    results,
			"count":   len(results),
		})
	}
}
