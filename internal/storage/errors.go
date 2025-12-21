package storage

import "fmt"

// ErrorType distinguishes between retryable and non-retryable errors.
type ErrorType int

const (
	// ErrorTypeInfrastructure indicates DB/system errors (503 - retryable).
	ErrorTypeInfrastructure ErrorType = iota
	// ErrorTypeInvalidData indicates bad input data (handled via partial success).
	ErrorTypeInvalidData
)

// StorageError wraps storage layer errors with type information.
type StorageError struct {
	Type    ErrorType
	Message string
	Cause   error
}

func (e *StorageError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}

func (e *StorageError) Unwrap() error {
	return e.Cause
}

// NewInfrastructureError creates an infrastructure error (503 - retryable).
func NewInfrastructureError(message string, cause error) *StorageError {
	return &StorageError{
		Type:    ErrorTypeInfrastructure,
		Message: message,
		Cause:   cause,
	}
}

// StoreResult contains the outcome of a storage operation.
// Used for OTLP partial success responses.
type StoreResult struct {
	Accepted int      // Number of items successfully stored
	Rejected int      // Number of items that failed validation
	Errors   []string // Human-readable error messages for rejected items
}

// AddError records a rejected item with its error message.
func (r *StoreResult) AddError(msg string) {
	r.Rejected++
	r.Errors = append(r.Errors, msg)
}

// HasRejections returns true if any items were rejected.
func (r *StoreResult) HasRejections() bool {
	return r.Rejected > 0
}

// ErrorMessage returns a combined error message for partial success response.
func (r *StoreResult) ErrorMessage() string {
	if len(r.Errors) == 0 {
		return ""
	}
	if len(r.Errors) == 1 {
		return r.Errors[0]
	}
	return fmt.Sprintf("%d errors: %s", len(r.Errors), r.Errors[0])
}
