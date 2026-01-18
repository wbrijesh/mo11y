package server

import (
	"encoding/json"
	"net/http"

	"mo11y/internal/storage"
)

// StatsResponse is the JSON response for storage stats.
type StatsResponse struct {
	Database  DatabaseStats  `json:"database"`
	Tables    TableStats     `json:"tables"`
	Retention RetentionStats `json:"retention"`
	Cleanup   *CleanupStats  `json:"cleanup,omitempty"`
}

type DatabaseStats struct {
	Path         string `json:"path"`
	SizeBytes    int64  `json:"size_bytes"`
	WALSizeBytes int64  `json:"wal_size_bytes"`
}

type TableStats struct {
	Spans      int64 `json:"spans"`
	SpanEvents int64 `json:"span_events"`
	SpanLinks  int64 `json:"span_links"`
	Logs       int64 `json:"logs"`
	Metrics    int64 `json:"metrics"`
}

type RetentionStats struct {
	Enabled             bool `json:"enabled"`
	Hours               int  `json:"hours"`
	CleanupIntervalMins int  `json:"cleanup_interval_mins"`
}

type CleanupStats struct {
	LastRun        string        `json:"last_run"`
	LastDurationMs int64         `json:"last_duration_ms"`
	LastResult     CleanupCounts `json:"last_result"`
}

type CleanupCounts struct {
	SpansDeleted      int64 `json:"spans_deleted"`
	SpanEventsDeleted int64 `json:"span_events_deleted"`
	SpanLinksDeleted  int64 `json:"span_links_deleted"`
	LogsDeleted       int64 `json:"logs_deleted"`
	MetricsDeleted    int64 `json:"metrics_deleted"`
}

func handleStats(store *storage.Storage, retentionCfg storage.CleanupConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		stats, err := store.Stats(r.Context())
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}

		resp := StatsResponse{
			Database: DatabaseStats{
				Path:         stats.DBPath,
				SizeBytes:    stats.DBSizeBytes,
				WALSizeBytes: stats.WALSizeBytes,
			},
			Tables: TableStats{
				Spans:      stats.Tables.Spans,
				SpanEvents: stats.Tables.SpanEvents,
				SpanLinks:  stats.Tables.SpanLinks,
				Logs:       stats.Tables.Logs,
				Metrics:    stats.Tables.Metrics,
			},
			Retention: RetentionStats{
				Enabled:             retentionCfg.RetentionHours > 0,
				Hours:               retentionCfg.RetentionHours,
				CleanupIntervalMins: retentionCfg.CleanupIntervalMins,
			},
		}

		if stats.LastCleanup != nil {
			resp.Cleanup = &CleanupStats{
				LastRun:        stats.LastCleanup.Timestamp.Format("2006-01-02T15:04:05Z07:00"),
				LastDurationMs: stats.LastCleanup.Duration.Milliseconds(),
				LastResult: CleanupCounts{
					SpansDeleted:      stats.LastCleanup.SpansDeleted,
					SpanEventsDeleted: stats.LastCleanup.SpanEventsDeleted,
					SpanLinksDeleted:  stats.LastCleanup.SpanLinksDeleted,
					LogsDeleted:       stats.LastCleanup.LogsDeleted,
					MetricsDeleted:    stats.LastCleanup.MetricsDeleted,
				},
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}
}
