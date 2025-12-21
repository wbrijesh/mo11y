package storage

// SQL schemas for OTLP data storage.
// Based on docs/design/duckdb-schema-design.adoc
// 5 tables: spans, span_events, span_links, logs, metrics

const spansSchema = `
CREATE TABLE IF NOT EXISTS spans (
    -- Identity
    trace_id VARCHAR NOT NULL,
    span_id VARCHAR NOT NULL,
    parent_span_id VARCHAR,
    
    -- Timing (microsecond precision, converted from OTLP nanoseconds)
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    duration_ns BIGINT NOT NULL,
    
    -- Span metadata
    name VARCHAR NOT NULL,
    kind TINYINT NOT NULL,
    status_code TINYINT,
    status_message VARCHAR,
    
    -- Resource attributes (denormalized)
    resource_attrs MAP(VARCHAR, VARCHAR),
    resource_schema_url VARCHAR,
    
    -- Scope attributes (denormalized)
    scope_name VARCHAR,
    scope_version VARCHAR,
    scope_attrs MAP(VARCHAR, VARCHAR),
    scope_schema_url VARCHAR,
    
    -- Span attributes
    attrs MAP(VARCHAR, VARCHAR),
    dropped_attrs_count INTEGER,
    
    -- Ingestion metadata
    ingested_at TIMESTAMP NOT NULL
);
`

const spansIndexes = `
CREATE INDEX IF NOT EXISTS idx_spans_trace_id ON spans(trace_id);
CREATE INDEX IF NOT EXISTS idx_spans_start_time ON spans(start_time);
CREATE INDEX IF NOT EXISTS idx_spans_name ON spans(name);
`

const spanEventsSchema = `
CREATE TABLE IF NOT EXISTS span_events (
    -- Parent span reference
    trace_id VARCHAR NOT NULL,
    span_id VARCHAR NOT NULL,
    
    -- Event data
    event_time TIMESTAMP NOT NULL,
    event_name VARCHAR NOT NULL,
    event_attrs MAP(VARCHAR, VARCHAR),
    dropped_attrs_count INTEGER,
    
    -- Ingestion metadata
    ingested_at TIMESTAMP NOT NULL
);
`

const spanEventsIndexes = `
CREATE INDEX IF NOT EXISTS idx_span_events_trace_span ON span_events(trace_id, span_id);
CREATE INDEX IF NOT EXISTS idx_span_events_name ON span_events(event_name);
CREATE INDEX IF NOT EXISTS idx_span_events_time ON span_events(event_time);
`

const spanLinksSchema = `
CREATE TABLE IF NOT EXISTS span_links (
    -- Source span reference
    trace_id VARCHAR NOT NULL,
    span_id VARCHAR NOT NULL,
    
    -- Linked span reference
    linked_trace_id VARCHAR NOT NULL,
    linked_span_id VARCHAR NOT NULL,
    trace_state VARCHAR,
    
    -- Link attributes
    link_attrs MAP(VARCHAR, VARCHAR),
    dropped_attrs_count INTEGER,
    
    -- Ingestion metadata
    ingested_at TIMESTAMP NOT NULL
);
`

const spanLinksIndexes = `
CREATE INDEX IF NOT EXISTS idx_span_links_trace_span ON span_links(trace_id, span_id);
CREATE INDEX IF NOT EXISTS idx_span_links_linked ON span_links(linked_trace_id, linked_span_id);
`

const logsSchema = `
CREATE TABLE IF NOT EXISTS logs (
    -- Identity
    log_id VARCHAR NOT NULL,
    trace_id VARCHAR,
    span_id VARCHAR,
    
    -- Timing (microsecond precision)
    timestamp TIMESTAMP NOT NULL,
    observed_timestamp TIMESTAMP,
    
    -- Log content
    severity_number TINYINT,
    severity_text VARCHAR,
    body VARCHAR,
    body_fields MAP(VARCHAR, VARCHAR),
    
    -- Resource attributes (denormalized)
    resource_attrs MAP(VARCHAR, VARCHAR),
    resource_schema_url VARCHAR,
    
    -- Scope attributes (denormalized)
    scope_name VARCHAR,
    scope_version VARCHAR,
    scope_attrs MAP(VARCHAR, VARCHAR),
    scope_schema_url VARCHAR,
    
    -- Log attributes
    attrs MAP(VARCHAR, VARCHAR),
    dropped_attrs_count INTEGER,
    
    -- Flags
    flags INTEGER,
    
    -- Ingestion metadata
    ingested_at TIMESTAMP NOT NULL
);
`

const logsIndexes = `
CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_logs_trace_id ON logs(trace_id);
CREATE INDEX IF NOT EXISTS idx_logs_severity ON logs(severity_number);
`

const metricsSchema = `
CREATE TABLE IF NOT EXISTS metrics (
    -- Identity
    metric_id VARCHAR NOT NULL,
    
    -- Timing (microsecond precision)
    timestamp TIMESTAMP NOT NULL,
    
    -- Metric metadata
    name VARCHAR NOT NULL,
    description VARCHAR,
    unit VARCHAR,
    type TINYINT NOT NULL,
    
    -- Unified metric value
    value DOUBLE,
    
    -- Sum-specific fields
    is_monotonic BOOLEAN,
    
    -- Histogram-specific fields (JSON - queried as complete units)
    histogram_json VARCHAR,
    
    -- Resource attributes (denormalized)
    resource_attrs MAP(VARCHAR, VARCHAR),
    resource_schema_url VARCHAR,
    
    -- Scope attributes (denormalized)
    scope_name VARCHAR,
    scope_version VARCHAR,
    scope_attrs MAP(VARCHAR, VARCHAR),
    scope_schema_url VARCHAR,
    
    -- Data point attributes
    attrs MAP(VARCHAR, VARCHAR),
    
    -- Ingestion metadata
    ingested_at TIMESTAMP NOT NULL
);
`

const metricsIndexes = `
CREATE INDEX IF NOT EXISTS idx_metrics_timestamp ON metrics(timestamp);
CREATE INDEX IF NOT EXISTS idx_metrics_name ON metrics(name);
CREATE INDEX IF NOT EXISTS idx_metrics_type ON metrics(type);
`
