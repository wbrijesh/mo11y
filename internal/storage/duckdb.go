package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

// Storage provides database operations.
type Storage struct {
	db *sql.DB
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

	s := &Storage{db: db}

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
