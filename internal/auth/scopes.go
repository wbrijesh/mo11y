package auth

import "errors"

// Scope represents API key permissions as a bitmask.
type Scope int

const (
	ScopeIngest Scope = 1 << iota // 1
	ScopeRead                     // 2
	ScopeAdmin                    // 4
)

// Has checks if the scope includes the required scope.
func (s Scope) Has(required Scope) bool {
	// Admin has all permissions
	if s&ScopeAdmin != 0 {
		return true
	}
	return s&required != 0
}

// String returns a human-readable scope description.
func (s Scope) String() string {
	var scopes []string
	if s&ScopeIngest != 0 {
		scopes = append(scopes, "ingest")
	}
	if s&ScopeRead != 0 {
		scopes = append(scopes, "read")
	}
	if s&ScopeAdmin != 0 {
		scopes = append(scopes, "admin")
	}
	if len(scopes) == 0 {
		return "none"
	}
	result := scopes[0]
	for i := 1; i < len(scopes); i++ {
		result += "," + scopes[i]
	}
	return result
}

// ParseScopes parses a comma-separated scope string.
func ParseScopes(s string) Scope {
	var scope Scope
	for _, part := range splitScopes(s) {
		switch part {
		case "ingest":
			scope |= ScopeIngest
		case "read":
			scope |= ScopeRead
		case "admin":
			scope |= ScopeAdmin
		}
	}
	return scope
}

func splitScopes(s string) []string {
	var result []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == ',' {
			if i > start {
				result = append(result, s[start:i])
			}
			start = i + 1
		}
	}
	if start < len(s) {
		result = append(result, s[start:])
	}
	return result
}

// Errors
var (
	ErrInvalidKey  = errors.New("invalid key")
	ErrKeyRevoked  = errors.New("key revoked")
	ErrKeyExpired  = errors.New("key expired")
	ErrKeyNotFound = errors.New("key not found")
)
