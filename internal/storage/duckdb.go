package storage

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	collectorlogsv1 "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetricsv1 "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
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

	return &Storage{db: db}, nil
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

// StoreTraces stores trace data from OTLP request.
// TODO: Implement actual storage with flattening.
func (s *Storage) StoreTraces(ctx context.Context, req *collectortracev1.ExportTraceServiceRequest) error {
	// Stub: count spans for logging
	var spanCount int
	for _, rs := range req.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			spanCount += len(ss.GetSpans())
		}
	}
	if spanCount > 0 {
		log.Printf("Received %d spans", spanCount)
	}
	return nil
}

// StoreMetrics stores metrics data from OTLP request.
// TODO: Implement actual storage with flattening.
func (s *Storage) StoreMetrics(ctx context.Context, req *collectormetricsv1.ExportMetricsServiceRequest) error {
	// Stub: count metrics for logging
	var metricCount int
	for _, rm := range req.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			metricCount += len(sm.GetMetrics())
		}
	}
	if metricCount > 0 {
		log.Printf("Received %d metrics", metricCount)
	}
	return nil
}

// StoreLogs stores log data from OTLP request.
// TODO: Implement actual storage with flattening.
func (s *Storage) StoreLogs(ctx context.Context, req *collectorlogsv1.ExportLogsServiceRequest) error {
	// Stub: count logs for logging
	var logCount int
	for _, rl := range req.GetResourceLogs() {
		for _, sl := range rl.GetScopeLogs() {
			logCount += len(sl.GetLogRecords())
		}
	}
	if logCount > 0 {
		log.Printf("Received %d logs", logCount)
	}
	return nil
}
