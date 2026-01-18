package auth

import (
	"crypto/rand"
	"encoding/hex"
	"time"

	"github.com/google/uuid"
)

// KeyInfo contains API key metadata (no secrets).
type KeyInfo struct {
	ID         string
	Name       string
	Prefix     string
	Scopes     Scope
	CreatedAt  time.Time
	ExpiresAt  *time.Time
	LastUsedAt *time.Time
	Revoked    bool
}

// generateKey creates a new API key: mo11y_<32 random hex chars>
func generateKey() string {
	b := make([]byte, 16)
	rand.Read(b)
	return "mo11y_" + hex.EncodeToString(b)
}

// generateID creates a new UUID.
func generateID() string {
	return uuid.New().String()
}
