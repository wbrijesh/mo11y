package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/marcboeker/go-duckdb"
	collectorlogsv1 "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
)

// StoreLogs stores log data from OTLP request using DuckDB Appender.
// Returns StoreResult with accepted/rejected counts for partial success.
func (s *Storage) StoreLogs(ctx context.Context, req *collectorlogsv1.ExportLogsServiceRequest) (*StoreResult, error) {
	if len(req.GetResourceLogs()) == 0 {
		return &StoreResult{}, nil
	}

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return nil, NewInfrastructureError("failed to get connection", err)
	}
	defer conn.Close()

	var appender *duckdb.Appender
	err = conn.Raw(func(driverConn any) error {
		duckConn, ok := driverConn.(*duckdb.Conn)
		if !ok {
			return fmt.Errorf("unexpected connection type: %T", driverConn)
		}
		var appErr error
		appender, appErr = duckdb.NewAppenderFromConn(duckConn, "", "logs")
		return appErr
	})
	if err != nil {
		return nil, NewInfrastructureError("failed to create appender", err)
	}
	defer appender.Close()

	result := &StoreResult{}
	now := time.Now()

	// Flatten OTLP hierarchy and append rows
	for _, rl := range req.GetResourceLogs() {
		var resourceAttrs duckdb.Map
		var resourceSchemaURL string
		if rl.Resource != nil {
			resourceAttrs = flattenAttributes(rl.Resource.Attributes)
		}
		resourceSchemaURL = rl.SchemaUrl

		for _, sl := range rl.GetScopeLogs() {
			var scopeName, scopeVersion string
			var scopeAttrs duckdb.Map
			var scopeSchemaURL string
			if sl.Scope != nil {
				scopeName = sl.Scope.Name
				scopeVersion = sl.Scope.Version
				scopeAttrs = flattenAttributes(sl.Scope.Attributes)
			}
			scopeSchemaURL = sl.SchemaUrl

			for _, lr := range sl.GetLogRecords() {
				logID := uuid.New().String()
				body, bodyFields := extractLogBody(lr.Body)

				err := appender.AppendRow(
					logID,
					hexEncode(lr.TraceId),
					hexEncode(lr.SpanId),
					unixNanoToTime(lr.TimeUnixNano),
					unixNanoToTime(lr.ObservedTimeUnixNano),
					int8(lr.SeverityNumber),
					lr.SeverityText,
					body,
					bodyFields,
					resourceAttrs,
					resourceSchemaURL,
					scopeName,
					scopeVersion,
					scopeAttrs,
					scopeSchemaURL,
					flattenAttributes(lr.Attributes),
					int32(lr.DroppedAttributesCount),
					int32(lr.Flags),
					now,
				)
				if err != nil {
					result.AddError(fmt.Sprintf("log %s: %v", logID, err))
					continue
				}
				result.Accepted++
			}
		}
	}

	if err := appender.Flush(); err != nil {
		return nil, NewInfrastructureError("failed to flush logs", err)
	}

	return result, nil
}

// extractLogBody extracts body content from OTLP AnyValue.
// Returns (body string, body_fields map) - one or the other will be populated.
func extractLogBody(v *commonv1.AnyValue) (string, duckdb.Map) {
	if v == nil {
		return "", nil
	}

	// If it's a simple string, return as body
	if sv, ok := v.Value.(*commonv1.AnyValue_StringValue); ok {
		return sv.StringValue, nil
	}

	// If it's a kvlist (structured log), return as body_fields
	if kvl, ok := v.Value.(*commonv1.AnyValue_KvlistValue); ok && kvl.KvlistValue != nil {
		fields := make(duckdb.Map, len(kvl.KvlistValue.Values))
		for _, kv := range kvl.KvlistValue.Values {
			if kv != nil {
				fields[kv.Key] = anyValueToString(kv.Value)
			}
		}
		return "", fields
	}

	// For other types (int, double, bool, array, bytes), stringify to body
	return anyValueToString(v), nil
}
