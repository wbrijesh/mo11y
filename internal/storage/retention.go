package storage

import (
	"context"
	"log"
	"time"
)

// CleanupConfig holds retention configuration.
type CleanupConfig struct {
	RetentionHours      int
	CleanupIntervalMins int
}

// CleanupResult contains the outcome of a cleanup run.
type CleanupResult struct {
	Timestamp         time.Time
	Duration          time.Duration
	SpansDeleted      int64
	SpanEventsDeleted int64
	SpanLinksDeleted  int64
	LogsDeleted       int64
	MetricsDeleted    int64
}

// StartCleanupWorker starts the periodic cleanup goroutine.
// Returns immediately if retention is disabled (hours=0).
// Stops when ctx is cancelled.
func (s *Storage) StartCleanupWorker(ctx context.Context, cfg CleanupConfig) {
	if cfg.RetentionHours == 0 {
		log.Println("Retention disabled, cleanup worker not started")
		return
	}

	if cfg.CleanupIntervalMins < 1 {
		cfg.CleanupIntervalMins = 1
	}

	log.Printf("Cleanup worker started: retention=%dh, interval=%dm", cfg.RetentionHours, cfg.CleanupIntervalMins)

	// Run once at startup
	s.runCleanup(ctx, cfg.RetentionHours)

	ticker := time.NewTicker(time.Duration(cfg.CleanupIntervalMins) * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Cleanup worker stopped")
			return
		case <-ticker.C:
			s.runCleanup(ctx, cfg.RetentionHours)
		}
	}
}

// runCleanup executes a single cleanup cycle.
func (s *Storage) runCleanup(ctx context.Context, retentionHours int) {
	// Try to acquire semaphore (non-blocking)
	select {
	case s.cleanupRunning <- struct{}{}:
		defer func() { <-s.cleanupRunning }()
	default:
		log.Println("Cleanup already in progress, skipping")
		return
	}

	start := time.Now()
	cutoff := start.Add(-time.Duration(retentionHours) * time.Hour)

	result := &CleanupResult{Timestamp: start}

	// Run deletes in a transaction
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		log.Printf("Cleanup failed to start transaction: %v", err)
		return
	}

	// Delete from each table
	tables := []struct {
		name    string
		deleted *int64
	}{
		{"span_events", &result.SpanEventsDeleted},
		{"span_links", &result.SpanLinksDeleted},
		{"spans", &result.SpansDeleted},
		{"logs", &result.LogsDeleted},
		{"metrics", &result.MetricsDeleted},
	}

	for _, t := range tables {
		res, err := tx.ExecContext(ctx, "DELETE FROM "+t.name+" WHERE ingested_at < ?", cutoff)
		if err != nil {
			log.Printf("Cleanup failed to delete from %s: %v", t.name, err)
			tx.Rollback()
			return
		}
		*t.deleted, _ = res.RowsAffected()
	}

	if err := tx.Commit(); err != nil {
		log.Printf("Cleanup failed to commit: %v", err)
		return
	}

	// Checkpoint to flush WAL
	if _, err := s.db.ExecContext(ctx, "CHECKPOINT"); err != nil {
		log.Printf("Cleanup checkpoint failed: %v", err)
	}

	result.Duration = time.Since(start)
	s.lastCleanup = result

	total := result.SpansDeleted + result.SpanEventsDeleted + result.SpanLinksDeleted +
		result.LogsDeleted + result.MetricsDeleted

	if total > 0 {
		log.Printf("Cleanup completed in %v: spans=%d, events=%d, links=%d, logs=%d, metrics=%d",
			result.Duration.Round(time.Millisecond),
			result.SpansDeleted, result.SpanEventsDeleted, result.SpanLinksDeleted,
			result.LogsDeleted, result.MetricsDeleted)
	} else {
		log.Printf("Cleanup completed in %v: no old data to delete", result.Duration.Round(time.Millisecond))
	}
}
