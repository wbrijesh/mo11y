package auth

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// Auth manages API key authentication.
type Auth struct {
	db     *sql.DB
	pepper string
}

// New creates a new Auth instance.
func New(dbPath, pepper string) (*Auth, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open auth db: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping auth db: %w", err)
	}

	a := &Auth{db: db, pepper: pepper}

	if err := a.initSchema(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to init auth schema: %w", err)
	}

	return a, nil
}

// Close closes the auth database.
func (a *Auth) Close() error {
	return a.db.Close()
}

func (a *Auth) initSchema(ctx context.Context) error {
	_, err := a.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS api_keys (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			key_hash TEXT NOT NULL UNIQUE,
			key_prefix TEXT NOT NULL,
			scopes INTEGER NOT NULL,
			created_at TEXT NOT NULL,
			expires_at TEXT,
			revoked_at TEXT,
			last_used_at TEXT,
			created_by TEXT
		);
		CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash);
	`)
	return err
}

// hashKey computes SHA-256(key + pepper).
func (a *Auth) hashKey(key string) string {
	h := sha256.Sum256([]byte(key + a.pepper))
	return hex.EncodeToString(h[:])
}

// Bootstrap creates an admin key if no keys exist and bootstrapKey is provided.
func (a *Auth) Bootstrap(ctx context.Context, bootstrapKey string) error {
	if bootstrapKey == "" {
		return nil
	}

	var count int
	err := a.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM api_keys").Scan(&count)
	if err != nil {
		return err
	}

	if count > 0 {
		return nil
	}

	_, _, err = a.createKeyInternal(ctx, "bootstrap-admin", ScopeAdmin, nil, bootstrapKey, "system")
	if err != nil {
		return err
	}

	log.Println("Bootstrap admin key created. Unset MO11Y_BOOTSTRAP_KEY for security.")
	return nil
}

// ValidateKey validates an API key and returns its info.
func (a *Auth) ValidateKey(ctx context.Context, key string) (*KeyInfo, error) {
	hash := a.hashKey(key)

	var info KeyInfo
	var expiresAt, revokedAt sql.NullString

	err := a.db.QueryRowContext(ctx, `
		SELECT id, name, scopes, expires_at, revoked_at
		FROM api_keys WHERE key_hash = ?
	`, hash).Scan(&info.ID, &info.Name, &info.Scopes, &expiresAt, &revokedAt)

	if err == sql.ErrNoRows {
		return nil, ErrInvalidKey
	}
	if err != nil {
		return nil, err
	}

	if revokedAt.Valid {
		return nil, ErrKeyRevoked
	}

	if expiresAt.Valid {
		t, _ := time.Parse(time.RFC3339, expiresAt.String)
		if time.Now().After(t) {
			return nil, ErrKeyExpired
		}
	}

	// Update last_used_at (fire and forget)
	go func() {
		a.db.Exec("UPDATE api_keys SET last_used_at = ? WHERE id = ?",
			time.Now().Format(time.RFC3339), info.ID)
	}()

	return &info, nil
}

// CreateKey creates a new API key.
func (a *Auth) CreateKey(ctx context.Context, name string, scopes Scope, expiresAt *time.Time, createdBy string) (string, *KeyInfo, error) {
	key := generateKey()
	return a.createKeyInternal(ctx, name, scopes, expiresAt, key, createdBy)
}

func (a *Auth) createKeyInternal(ctx context.Context, name string, scopes Scope, expiresAt *time.Time, key, createdBy string) (string, *KeyInfo, error) {
	id := generateID()
	hash := a.hashKey(key)
	prefix := key[:12] // "mo11y_" + 6 chars

	var expiresAtStr *string
	if expiresAt != nil {
		s := expiresAt.Format(time.RFC3339)
		expiresAtStr = &s
	}

	_, err := a.db.ExecContext(ctx, `
		INSERT INTO api_keys (id, name, key_hash, key_prefix, scopes, created_at, expires_at, created_by)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, id, name, hash, prefix, scopes, time.Now().Format(time.RFC3339), expiresAtStr, createdBy)

	if err != nil {
		return "", nil, err
	}

	return key, &KeyInfo{ID: id, Name: name, Scopes: scopes, Prefix: prefix}, nil
}

// RevokeKey revokes an API key.
func (a *Auth) RevokeKey(ctx context.Context, keyID string) error {
	res, err := a.db.ExecContext(ctx, `
		UPDATE api_keys SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL
	`, time.Now().Format(time.RFC3339), keyID)
	if err != nil {
		return err
	}

	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrKeyNotFound
	}
	return nil
}

// ListKeys returns all API keys (without sensitive data).
func (a *Auth) ListKeys(ctx context.Context) ([]KeyInfo, error) {
	rows, err := a.db.QueryContext(ctx, `
		SELECT id, name, key_prefix, scopes, created_at, expires_at, revoked_at, last_used_at
		FROM api_keys ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []KeyInfo
	for rows.Next() {
		var k KeyInfo
		var createdAt string
		var expiresAt, revokedAt, lastUsedAt sql.NullString

		err := rows.Scan(&k.ID, &k.Name, &k.Prefix, &k.Scopes, &createdAt, &expiresAt, &revokedAt, &lastUsedAt)
		if err != nil {
			return nil, err
		}

		k.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
		if expiresAt.Valid {
			t, _ := time.Parse(time.RFC3339, expiresAt.String)
			k.ExpiresAt = &t
		}
		k.Revoked = revokedAt.Valid
		if lastUsedAt.Valid {
			t, _ := time.Parse(time.RFC3339, lastUsedAt.String)
			k.LastUsedAt = &t
		}

		keys = append(keys, k)
	}

	return keys, rows.Err()
}
