package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/marcboeker/go-duckdb"
	collectormetricsv1 "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricsv1 "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// MetricType constants matching the schema.
const (
	MetricTypeGauge     int8 = 1
	MetricTypeSum       int8 = 2
	MetricTypeHistogram int8 = 3
	MetricTypeSummary   int8 = 4 // Not fully supported yet
)

// Histogram represents histogram data for JSON serialization.
type Histogram struct {
	Count          int64     `json:"count"`
	Sum            float64   `json:"sum"`
	BucketCounts   []int64   `json:"bucket_counts"`
	ExplicitBounds []float64 `json:"explicit_bounds"`
}

// StoreMetrics stores metric data from OTLP request using DuckDB Appender.
// Returns StoreResult with accepted/rejected counts for partial success.
func (s *Storage) StoreMetrics(ctx context.Context, req *collectormetricsv1.ExportMetricsServiceRequest) (*StoreResult, error) {
	if len(req.GetResourceMetrics()) == 0 {
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
		appender, appErr = duckdb.NewAppenderFromConn(duckConn, "", "metrics")
		return appErr
	})
	if err != nil {
		return nil, NewInfrastructureError("failed to create appender", err)
	}
	defer appender.Close()

	result := &StoreResult{}
	now := time.Now()

	// Flatten OTLP hierarchy and append rows
	for _, rm := range req.GetResourceMetrics() {
		var resourceAttrs duckdb.Map
		var resourceSchemaURL string
		if rm.Resource != nil {
			resourceAttrs = flattenAttributes(rm.Resource.Attributes)
		}
		resourceSchemaURL = rm.SchemaUrl

		for _, sm := range rm.GetScopeMetrics() {
			var scopeName, scopeVersion string
			var scopeAttrs duckdb.Map
			var scopeSchemaURL string
			if sm.Scope != nil {
				scopeName = sm.Scope.Name
				scopeVersion = sm.Scope.Version
				scopeAttrs = flattenAttributes(sm.Scope.Attributes)
			}
			scopeSchemaURL = sm.SchemaUrl

			for _, m := range sm.GetMetrics() {
				s.appendMetricDataPoints(appender, m, resourceAttrs, resourceSchemaURL,
					scopeName, scopeVersion, scopeAttrs, scopeSchemaURL, now, result)
			}
		}
	}

	if err := appender.Flush(); err != nil {
		return nil, NewInfrastructureError("failed to flush metrics", err)
	}

	return result, nil
}

// appendMetricDataPoints extracts data points from a metric and appends them.
func (s *Storage) appendMetricDataPoints(
	appender *duckdb.Appender,
	m *metricsv1.Metric,
	resourceAttrs duckdb.Map,
	resourceSchemaURL string,
	scopeName, scopeVersion string,
	scopeAttrs duckdb.Map,
	scopeSchemaURL string,
	now time.Time,
	result *StoreResult,
) {
	switch data := m.Data.(type) {
	case *metricsv1.Metric_Gauge:
		s.appendNumberDataPoints(appender, m, data.Gauge.DataPoints, MetricTypeGauge, false,
			resourceAttrs, resourceSchemaURL, scopeName, scopeVersion, scopeAttrs, scopeSchemaURL, now, result)

	case *metricsv1.Metric_Sum:
		s.appendNumberDataPoints(appender, m, data.Sum.DataPoints, MetricTypeSum, data.Sum.IsMonotonic,
			resourceAttrs, resourceSchemaURL, scopeName, scopeVersion, scopeAttrs, scopeSchemaURL, now, result)

	case *metricsv1.Metric_Histogram:
		s.appendHistogramDataPoints(appender, m, data.Histogram.DataPoints,
			resourceAttrs, resourceSchemaURL, scopeName, scopeVersion, scopeAttrs, scopeSchemaURL, now, result)
	}
}

// appendNumberDataPoints appends Gauge or Sum data points.
func (s *Storage) appendNumberDataPoints(
	appender *duckdb.Appender,
	m *metricsv1.Metric,
	dataPoints []*metricsv1.NumberDataPoint,
	metricType int8,
	isMonotonic bool,
	resourceAttrs duckdb.Map,
	resourceSchemaURL string,
	scopeName, scopeVersion string,
	scopeAttrs duckdb.Map,
	scopeSchemaURL string,
	now time.Time,
	result *StoreResult,
) {
	for _, dp := range dataPoints {
		metricID := uuid.New().String()

		var value float64
		switch v := dp.Value.(type) {
		case *metricsv1.NumberDataPoint_AsDouble:
			value = v.AsDouble
		case *metricsv1.NumberDataPoint_AsInt:
			value = float64(v.AsInt)
		}

		err := appender.AppendRow(
			metricID,
			unixNanoToTime(dp.TimeUnixNano),
			m.Name,
			m.Description,
			m.Unit,
			metricType,
			value,
			isMonotonic,
			"", // No histogram JSON for Gauge/Sum
			resourceAttrs,
			resourceSchemaURL,
			scopeName,
			scopeVersion,
			scopeAttrs,
			scopeSchemaURL,
			flattenAttributes(dp.Attributes),
			now,
		)
		if err != nil {
			result.AddError(fmt.Sprintf("metric %s/%s: %v", m.Name, metricID, err))
			continue
		}
		result.Accepted++
	}
}

// appendHistogramDataPoints appends Histogram data points.
func (s *Storage) appendHistogramDataPoints(
	appender *duckdb.Appender,
	m *metricsv1.Metric,
	dataPoints []*metricsv1.HistogramDataPoint,
	resourceAttrs duckdb.Map,
	resourceSchemaURL string,
	scopeName, scopeVersion string,
	scopeAttrs duckdb.Map,
	scopeSchemaURL string,
	now time.Time,
	result *StoreResult,
) {
	for _, dp := range dataPoints {
		metricID := uuid.New().String()

		bucketCounts := make([]int64, len(dp.BucketCounts))
		for i, c := range dp.BucketCounts {
			bucketCounts[i] = int64(c)
		}

		histogram := Histogram{
			Count:          int64(dp.Count),
			Sum:            dp.GetSum(),
			BucketCounts:   bucketCounts,
			ExplicitBounds: dp.ExplicitBounds,
		}
		histogramJSON, _ := json.Marshal(histogram)

		err := appender.AppendRow(
			metricID,
			unixNanoToTime(dp.TimeUnixNano),
			m.Name,
			m.Description,
			m.Unit,
			MetricTypeHistogram,
			float64(0),
			false,
			string(histogramJSON),
			resourceAttrs,
			resourceSchemaURL,
			scopeName,
			scopeVersion,
			scopeAttrs,
			scopeSchemaURL,
			flattenAttributes(dp.Attributes),
			now,
		)
		if err != nil {
			result.AddError(fmt.Sprintf("histogram %s/%s: %v", m.Name, metricID, err))
			continue
		}
		result.Accepted++
	}
}
