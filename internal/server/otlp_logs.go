package server

import (
	"errors"
	"io"
	"log"
	"net/http"
	"strings"

	collectorlogsv1 "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/protobuf/proto"

	"mo11y/internal/storage"
)

// handleLogs handles POST /v1/logs.
func handleLogs(store *storage.Storage) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reqID := RequestID(r.Context())

		// 1. Validate method
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		// 2. Validate content-type
		if !strings.HasPrefix(r.Header.Get("Content-Type"), "application/x-protobuf") {
			log.Printf("[%s] logs: unsupported Content-Type: %s", reqID, r.Header.Get("Content-Type"))
			w.WriteHeader(http.StatusUnsupportedMediaType)
			return
		}

		// 3. Read body
		body, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("[%s] logs: failed to read body: %v", reqID, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// 4. Unmarshal protobuf request
		req := &collectorlogsv1.ExportLogsServiceRequest{}
		if err := proto.Unmarshal(body, req); err != nil {
			log.Printf("[%s] logs: failed to unmarshal protobuf: %v", reqID, err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		// 5. Store data
		result, err := store.StoreLogs(r.Context(), req)
		if err != nil {
			var storageErr *storage.StorageError
			if errors.As(err, &storageErr) {
				log.Printf("[%s] logs: storage unavailable: %v", reqID, err)
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			log.Printf("[%s] logs: unexpected error: %v", reqID, err)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}

		log.Printf("[%s] logs: accepted %d, rejected %d log records", reqID, result.Accepted, result.Rejected)

		// 6. Build response with partial success if needed
		resp := &collectorlogsv1.ExportLogsServiceResponse{}
		if result.HasRejections() {
			resp.PartialSuccess = &collectorlogsv1.ExportLogsPartialSuccess{
				RejectedLogRecords: int64(result.Rejected),
				ErrorMessage:       result.ErrorMessage(),
			}
		}

		respBytes, err := proto.Marshal(resp)
		if err != nil {
			log.Printf("[%s] BUG: logs: failed to marshal response: %v", reqID, err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/x-protobuf")
		w.WriteHeader(http.StatusOK)
		w.Write(respBytes)
	}
}
