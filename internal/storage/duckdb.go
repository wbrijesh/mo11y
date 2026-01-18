package storage

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

// Storage provides database operations.
type Storage struct {
	db     *sql.DB
	dbPath string

	// Cleanup state
	cleanupRunning chan struct{}
	lastCleanup    *CleanupResult
}

// New creates a new Storage instance connected to DuckDB.
// If dbPath is empty, uses an in-memory database.
func New(dbPath string) (*Storage, error) {
	if dbPath == "" {
		dbPath = ":memory:"
	}

	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}

	// Verify connection works
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping duckdb: %w", err)
	}

	s := &Storage{
		db:             db,
		dbPath:         dbPath,
		cleanupRunning: make(chan struct{}, 1),
	}

	// Initialize schema
	if err := s.initSchema(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return s, nil
}

// initSchema creates the database tables if they don't exist.
func (s *Storage) initSchema(ctx context.Context) error {
	statements := []string{
		spansSchema, spansIndexes,
		spanEventsSchema, spanEventsIndexes,
		spanLinksSchema, spanLinksIndexes,
		logsSchema, logsIndexes,
		metricsSchema, metricsIndexes,
	}
	for _, stmt := range statements {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// Health checks if the database connection is healthy.
func (s *Storage) Health(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// Close closes the database connection.
func (s *Storage) Close() error {
	return s.db.Close()
}

// DB returns the underlying sql.DB for direct queries.
func (s *Storage) DB() *sql.DB {
	return s.db
}

// DBPath returns the database file path.
func (s *Storage) DBPath() string {
	return s.dbPath
}

// Stats returns storage statistics.
func (s *Storage) Stats(ctx context.Context) (*StorageStats, error) {
	stats := &StorageStats{
		DBPath:      s.dbPath,
		LastCleanup: s.lastCleanup,
	}

	// File sizes
	if s.dbPath != ":memory:" {
		if info, err := os.Stat(s.dbPath); err == nil {
			stats.DBSizeBytes = info.Size()
		}
		if info, err := os.Stat(s.dbPath + ".wal"); err == nil {
			stats.WALSizeBytes = info.Size()
		}
	}

	// Row counts
	row := s.db.QueryRowContext(ctx, `
		SELECT 
			(SELECT COUNT(*) FROM spans) as spans,
			(SELECT COUNT(*) FROM span_events) as span_events,
			(SELECT COUNT(*) FROM span_links) as span_links,
			(SELECT COUNT(*) FROM logs) as logs,
			(SELECT COUNT(*) FROM metrics) as metrics
	`)
	err := row.Scan(
		&stats.Tables.Spans,
		&stats.Tables.SpanEvents,
		&stats.Tables.SpanLinks,
		&stats.Tables.Logs,
		&stats.Tables.Metrics,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get row counts: %w", err)
	}

	return stats, nil
}

// StorageStats contains storage statistics.
type StorageStats struct {
	DBPath       string
	DBSizeBytes  int64
	WALSizeBytes int64
	Tables       TableCounts
	LastCleanup  *CleanupResult
}

// TableCounts contains row counts for each table.
type TableCounts struct {
	Spans      int64
	SpanEvents int64
	SpanLinks  int64
	Logs       int64
	Metrics    int64
}
