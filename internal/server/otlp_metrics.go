package server

import (
	"io"
	"log"
	"net/http"
	"strings"

	collectormetricsv1 "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/protobuf/proto"

	"mo11y/internal/storage"
)

// handleMetrics handles POST /v1/metrics.
func handleMetrics(store *storage.Storage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reqID := RequestID(r.Context())

		// 1. Validate method
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		// 2. Validate content-type
		if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/x-protobuf") {
			log.Printf("[%s] metrics: unsupported Content-Type: %s", reqID, r.Header.Get("Content-Type"))
			w.WriteHeader(http.StatusUnsupportedMediaType)
			return
		}

		// 3. Read body
		body, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("[%s] metrics: failed to read body: %v", reqID, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// 4. Unmarshal protobuf request
		req := &collectormetricsv1.ExportMetricsServiceRequest{}
		if err := proto.Unmarshal(body, req); err != nil {
			log.Printf("[%s] metrics: failed to unmarshal protobuf: %v", reqID, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// 5. Store data
		if err := store.StoreMetrics(r.Context(), req); err != nil {
			log.Printf("[%s] metrics: storage failed: %v", reqID, err)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		// 6. Marshal and write protobuf response
		resp := &collectormetricsv1.ExportMetricsServiceResponse{}
		respBytes, err := proto.Marshal(resp)
		if err != nil {
			log.Printf("[%s] BUG: metrics: failed to marshal response: %v", reqID, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		w.Write(respBytes)
	}
}
