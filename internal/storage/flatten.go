package storage

import (
	"encoding/hex"
	"encoding/json"
	"strconv"
	"time"

	"github.com/marcboeker/go-duckdb"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
)

// unixNanoToTime converts OTLP nanoseconds to Go time.Time.
// Precision loss (nano → micro) is acceptable for mo11y.
func unixNanoToTime(nanos uint64) time.Time {
	if nanos == 0 {
		return time.Time{}
	}
	return time.Unix(0, int64(nanos))
}

// hexEncode converts a byte slice to hex string.
// Used for trace_id (16 bytes → 32 chars) and span_id (8 bytes → 16 chars).
func hexEncode(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return hex.EncodeToString(b)
}

// flattenAttributes converts OTLP KeyValue slice to a DuckDB Map.
// Complex values (arrays, kvlists) are serialized to JSON.
func flattenAttributes(kvs []*commonv1.KeyValue) duckdb.Map {
	if len(kvs) == 0 {
		return nil
	}
	result := make(duckdb.Map, len(kvs))
	for _, kv := range kvs {
		if kv != nil && kv.Key != "" {
			result[kv.Key] = anyValueToString(kv.Value)
		}
	}
	return result
}

// anyValueToString converts an OTLP AnyValue to string representation.
func anyValueToString(v *commonv1.AnyValue) string {
	if v == nil {
		return ""
	}
	switch val := v.Value.(type) {
	case *commonv1.AnyValue_StringValue:
		return val.StringValue
	case *commonv1.AnyValue_IntValue:
		return strconv.FormatInt(val.IntValue, 10)
	case *commonv1.AnyValue_DoubleValue:
		return strconv.FormatFloat(val.DoubleValue, 'f', -1, 64)
	case *commonv1.AnyValue_BoolValue:
		return strconv.FormatBool(val.BoolValue)
	case *commonv1.AnyValue_BytesValue:
		return hex.EncodeToString(val.BytesValue)
	case *commonv1.AnyValue_ArrayValue:
		return anyValueArrayToJSON(val.ArrayValue)
	case *commonv1.AnyValue_KvlistValue:
		return anyValueKvlistToJSON(val.KvlistValue)
	default:
		return ""
	}
}

// anyValueArrayToJSON serializes an ArrayValue to JSON string.
func anyValueArrayToJSON(arr *commonv1.ArrayValue) string {
	if arr == nil || len(arr.Values) == 0 {
		return "[]"
	}
	values := make([]any, len(arr.Values))
	for i, v := range arr.Values {
		values[i] = anyValueToInterface(v)
	}
	b, _ := json.Marshal(values)
	return string(b)
}

// anyValueKvlistToJSON serializes a KeyValueList to JSON string.
func anyValueKvlistToJSON(kvl *commonv1.KeyValueList) string {
	if kvl == nil || len(kvl.Values) == 0 {
		return "{}"
	}
	m := make(map[string]any, len(kvl.Values))
	for _, kv := range kvl.Values {
		if kv != nil {
			m[kv.Key] = anyValueToInterface(kv.Value)
		}
	}
	b, _ := json.Marshal(m)
	return string(b)
}

// anyValueToInterface converts AnyValue to a Go interface{} for JSON marshaling.
func anyValueToInterface(v *commonv1.AnyValue) any {
	if v == nil {
		return nil
	}
	switch val := v.Value.(type) {
	case *commonv1.AnyValue_StringValue:
		return val.StringValue
	case *commonv1.AnyValue_IntValue:
		return val.IntValue
	case *commonv1.AnyValue_DoubleValue:
		return val.DoubleValue
	case *commonv1.AnyValue_BoolValue:
		return val.BoolValue
	case *commonv1.AnyValue_BytesValue:
		return hex.EncodeToString(val.BytesValue)
	case *commonv1.AnyValue_ArrayValue:
		if val.ArrayValue == nil {
			return []any{}
		}
		arr := make([]any, len(val.ArrayValue.Values))
		for i, v := range val.ArrayValue.Values {
			arr[i] = anyValueToInterface(v)
		}
		return arr
	case *commonv1.AnyValue_KvlistValue:
		if val.KvlistValue == nil {
			return map[string]any{}
		}
		m := make(map[string]any, len(val.KvlistValue.Values))
		for _, kv := range val.KvlistValue.Values {
			if kv != nil {
				m[kv.Key] = anyValueToInterface(kv.Value)
			}
		}
		return m
	default:
		return nil
	}
}
