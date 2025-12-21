package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/marcboeker/go-duckdb"
	collectortracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

// StoreTraces stores trace data from OTLP request using multiple DuckDB Appenders.
// Returns StoreResult with accepted/rejected counts for partial success.
func (s *Storage) StoreTraces(ctx context.Context, req *collectortracev1.ExportTraceServiceRequest) (*StoreResult, error) {
	if len(req.GetResourceSpans()) == 0 {
		return &StoreResult{}, nil
	}

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, NewInfrastructureError("failed to get connection", err)
	}
	defer conn.Close()

	// Create appenders for all three tables
	var spanAppender, eventAppender, linkAppender *duckdb.Appender
	err = conn.Raw(func(driverConn any) error {
		duckConn, ok := driverConn.(*duckdb.Conn)
		if !ok {
			return fmt.Errorf("unexpected connection type: %T", driverConn)
		}

		var appErr error
		spanAppender, appErr = duckdb.NewAppenderFromConn(duckConn, "", "spans")
		if appErr != nil {
			return appErr
		}

		eventAppender, appErr = duckdb.NewAppenderFromConn(duckConn, "", "span_events")
		if appErr != nil {
			spanAppender.Close()
			return appErr
		}

		linkAppender, appErr = duckdb.NewAppenderFromConn(duckConn, "", "span_links")
		if appErr != nil {
			spanAppender.Close()
			eventAppender.Close()
			return appErr
		}

		return nil
	})
	if err != nil {
		return nil, NewInfrastructureError("failed to create appenders", err)
	}
	defer spanAppender.Close()
	defer eventAppender.Close()
	defer linkAppender.Close()

	result := &StoreResult{}
	now := time.Now()

	// Flatten OTLP hierarchy and append rows
	for _, rs := range req.GetResourceSpans() {
		var resourceAttrs duckdb.Map
		var resourceSchemaURL string
		if rs.Resource != nil {
			resourceAttrs = flattenAttributes(rs.Resource.Attributes)
		}
		resourceSchemaURL = rs.SchemaUrl

		for _, ss := range rs.GetScopeSpans() {
			var scopeName, scopeVersion string
			var scopeAttrs duckdb.Map
			var scopeSchemaURL string
			if ss.Scope != nil {
				scopeName = ss.Scope.Name
				scopeVersion = ss.Scope.Version
				scopeAttrs = flattenAttributes(ss.Scope.Attributes)
			}
			scopeSchemaURL = ss.SchemaUrl

			for _, span := range ss.GetSpans() {
				traceID := hexEncode(span.TraceId)
				spanID := hexEncode(span.SpanId)

				// Extract status
				var statusCode int8
				var statusMessage string
				if span.Status != nil {
					statusCode = int8(span.Status.Code)
					statusMessage = span.Status.Message
				}

				// Calculate duration
				durationNs := int64(span.EndTimeUnixNano - span.StartTimeUnixNano)

				// Append span
				err := spanAppender.AppendRow(
					traceID,
					spanID,
					hexEncode(span.ParentSpanId),
					unixNanoToTime(span.StartTimeUnixNano),
					unixNanoToTime(span.EndTimeUnixNano),
					durationNs,
					span.Name,
					int8(span.Kind),
					statusCode,
					statusMessage,
					resourceAttrs,
					resourceSchemaURL,
					scopeName,
					scopeVersion,
					scopeAttrs,
					scopeSchemaURL,
					flattenAttributes(span.Attributes),
					int32(span.DroppedAttributesCount),
					now,
				)
				if err != nil {
					result.AddError(fmt.Sprintf("span %s: %v", spanID, err))
					continue
				}
				result.Accepted++

				// Append events for this span
				for _, event := range span.Events {
					err := eventAppender.AppendRow(
						traceID,
						spanID,
						unixNanoToTime(event.TimeUnixNano),
						event.Name,
						flattenAttributes(event.Attributes),
						int32(event.DroppedAttributesCount),
						now,
					)
					if err != nil {
						// Log but don't fail the span for event errors
						result.Errors = append(result.Errors, fmt.Sprintf("event %s/%s: %v", spanID, event.Name, err))
					}
				}

				// Append links for this span
				for _, link := range span.Links {
					err := linkAppender.AppendRow(
						traceID,
						spanID,
						hexEncode(link.TraceId),
						hexEncode(link.SpanId),
						link.TraceState,
						flattenAttributes(link.Attributes),
						int32(link.DroppedAttributesCount),
						now,
					)
					if err != nil {
						// Log but don't fail the span for link errors
						result.Errors = append(result.Errors, fmt.Sprintf("link %s: %v", spanID, err))
					}
				}
			}
		}
	}

	// Flush all appenders
	if err := spanAppender.Flush(); err != nil {
		return nil, NewInfrastructureError("failed to flush spans", err)
	}
	if err := eventAppender.Flush(); err != nil {
		return nil, NewInfrastructureError("failed to flush events", err)
	}
	if err := linkAppender.Flush(); err != nil {
		return nil, NewInfrastructureError("failed to flush links", err)
	}

	return result, nil
}
