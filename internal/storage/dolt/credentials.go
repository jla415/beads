//go:build cgo

package dolt

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/steveyegge/beads/internal/storage"
)

// Credential storage and encryption for federation peers.
// Enables SQL user authentication when syncing with peer Gas Towns.

// federationEnvMutex protects DOLT_REMOTE_USER/PASSWORD env vars from concurrent access.
// Environment variables are process-global, so we need to serialize federation operations.
var federationEnvMutex sync.Mutex

// validPeerNameRegex matches valid peer names (alphanumeric, hyphens, underscores)
var validPeerNameRegex = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_-]*$`)

// validatePeerName checks that a peer name is safe for use as a Dolt remote name
func validatePeerName(name string) error {
	if name == "" {
		return fmt.Errorf("peer name cannot be empty")
	}
	if len(name) > 64 {
		return fmt.Errorf("peer name too long (max 64 characters)")
	}
	if !validPeerNameRegex.MatchString(name) {
		return fmt.Errorf("peer name must start with a letter and contain only alphanumeric characters, hyphens, and underscores")
	}
	return nil
}

// encryptionKey derives a key from the database path for credential encryption.
// This provides basic protection - credentials are not stored in plaintext.
// For production, consider using system keyring or external secret managers.
func (s *DoltStore) encryptionKey() []byte {
	// Use SHA-256 hash of the database path as the key (32 bytes for AES-256)
	// This ties credentials to this specific database location
	h := sha256.New()
	h.Write([]byte(s.dbPath + "beads-federation-key-v1"))
	return h.Sum(nil)
}

// federationMachineID returns a deterministic machine identifier for this
// local store path. Credentials are keyed by this value so peer auth rows
// are machine-scoped and do not overwrite each other across synced databases.
func (s *DoltStore) federationMachineID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown-host"
	}

	dbPath := s.dbPath
	if absPath, err := filepath.Abs(dbPath); err == nil {
		dbPath = absPath
	}

	h := sha256.New()
	h.Write([]byte(host))
	h.Write([]byte("|"))
	h.Write([]byte(dbPath))
	h.Write([]byte("|beads-federation-machine-v1"))
	sum := h.Sum(nil)
	return fmt.Sprintf("m-%x", sum[:16])
}

func (s *DoltStore) decryptPasswordBestEffort(encrypted []byte) (string, bool) {
	if len(encrypted) == 0 {
		return "", true
	}

	password, err := s.decryptPassword(encrypted)
	if err != nil {
		return "", false
	}
	return password, true
}

func (s *DoltStore) upsertFederationPeerAuth(
	ctx context.Context,
	peerName string,
	remoteURL string,
	username string,
	encryptedPwd []byte,
) error {
	var usernameValue any
	if username != "" {
		usernameValue = username
	}

	_, err := s.execContext(ctx, `
		INSERT INTO federation_peer_auth (peer_name, machine_id, remote_url, username, password_encrypted)
		VALUES (?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			remote_url = VALUES(remote_url),
			username = VALUES(username),
			password_encrypted = VALUES(password_encrypted),
			updated_at = CURRENT_TIMESTAMP
	`, peerName, s.federationMachineID(), remoteURL, usernameValue, encryptedPwd)
	return err
}

// encryptPassword encrypts a password using AES-GCM
func (s *DoltStore) encryptPassword(password string) ([]byte, error) {
	if password == "" {
		return nil, nil
	}

	block, err := aes.NewCipher(s.encryptionKey())
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	ciphertext := gcm.Seal(nonce, nonce, []byte(password), nil)
	return ciphertext, nil
}

// decryptPassword decrypts a password using AES-GCM
func (s *DoltStore) decryptPassword(encrypted []byte) (string, error) {
	if len(encrypted) == 0 {
		return "", nil
	}

	block, err := aes.NewCipher(s.encryptionKey())
	if err != nil {
		return "", fmt.Errorf("failed to create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("failed to create GCM: %w", err)
	}

	nonceSize := gcm.NonceSize()
	if len(encrypted) < nonceSize {
		return "", fmt.Errorf("ciphertext too short")
	}

	nonce, ciphertext := encrypted[:nonceSize], encrypted[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", fmt.Errorf("failed to decrypt: %w", err)
	}

	return string(plaintext), nil
}

// AddFederationPeer adds or updates a federation peer with credentials.
// This stores credentials in the database and also adds the Dolt remote.
func (s *DoltStore) AddFederationPeer(ctx context.Context, peer *storage.FederationPeer) error {
	// Validate peer name
	if err := validatePeerName(peer.Name); err != nil {
		return fmt.Errorf("invalid peer name: %w", err)
	}
	if strings.TrimSpace(peer.RemoteURL) == "" {
		return fmt.Errorf("remote URL cannot be empty")
	}

	// Encrypt password before storing
	var encryptedPwd []byte
	var err error
	if peer.Password != "" {
		encryptedPwd, err = s.encryptPassword(peer.Password)
		if err != nil {
			return fmt.Errorf("failed to encrypt password: %w", err)
		}
	}

	// Upsert shared peer metadata.
	// Credentials and machine-specific URLs are stored in federation_peer_auth.
	_, err = s.execContext(ctx, `
		INSERT INTO federation_peers (name, remote_url, sovereignty)
		VALUES (?, ?, ?)
		ON DUPLICATE KEY UPDATE
			sovereignty = VALUES(sovereignty),
			updated_at = CURRENT_TIMESTAMP
	`, peer.Name, peer.RemoteURL, peer.Sovereignty)

	if err != nil {
		return fmt.Errorf("failed to add federation peer: %w", err)
	}

	// Persist machine-scoped auth/URL for this local store.
	if err := s.upsertFederationPeerAuth(ctx, peer.Name, peer.RemoteURL, peer.Username, encryptedPwd); err != nil {
		return fmt.Errorf("failed to store peer credentials: %w", err)
	}

	// Also add the Dolt remote
	if err := s.AddRemote(ctx, peer.Name, peer.RemoteURL); err != nil {
		// Ignore "remote already exists" errors
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("failed to add dolt remote: %w", err)
		}
	}

	return nil
}

// GetFederationPeer retrieves a federation peer by name.
// Returns nil if peer doesn't exist.
func (s *DoltStore) GetFederationPeer(ctx context.Context, name string) (*storage.FederationPeer, error) {
	var peer storage.FederationPeer
	var sharedRemoteURL string
	var sharedUsername sql.NullString
	var sharedEncryptedPwd []byte
	var sharedLastSync sql.NullTime
	var localRemoteURL sql.NullString
	var localUsername sql.NullString
	var localEncryptedPwd []byte
	var localLastSync sql.NullTime

	err := s.db.QueryRowContext(ctx, `
		SELECT
			p.name,
			p.remote_url,
			p.username,
			p.password_encrypted,
			p.sovereignty,
			p.last_sync,
			p.created_at,
			p.updated_at,
			a.remote_url,
			a.username,
			a.password_encrypted,
			a.last_sync
		FROM federation_peers p
		LEFT JOIN federation_peer_auth a
			ON a.peer_name = p.name AND a.machine_id = ?
		WHERE p.name = ?
	`, s.federationMachineID(), name).Scan(
		&peer.Name,
		&sharedRemoteURL,
		&sharedUsername,
		&sharedEncryptedPwd,
		&peer.Sovereignty,
		&sharedLastSync,
		&peer.CreatedAt,
		&peer.UpdatedAt,
		&localRemoteURL,
		&localUsername,
		&localEncryptedPwd,
		&localLastSync,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get federation peer: %w", err)
	}

	peer.RemoteURL = sharedRemoteURL
	localAuthExists := localRemoteURL.Valid
	if localRemoteURL.Valid && localRemoteURL.String != "" {
		peer.RemoteURL = localRemoteURL.String
	}

	if localUsername.Valid {
		peer.Username = localUsername.String
	} else if sharedUsername.Valid {
		peer.Username = sharedUsername.String
	}

	if localLastSync.Valid {
		peer.LastSync = &localLastSync.Time
	} else if sharedLastSync.Valid {
		peer.LastSync = &sharedLastSync.Time
	}

	if localAuthExists {
		if password, ok := s.decryptPasswordBestEffort(localEncryptedPwd); ok {
			peer.Password = password
		}
		return &peer, nil
	}

	// Legacy fallback for existing rows that still store credentials in
	// federation_peers. If decrypt succeeds, backfill machine-scoped auth.
	if password, ok := s.decryptPasswordBestEffort(sharedEncryptedPwd); ok {
		peer.Password = password
		if sharedUsername.Valid || len(sharedEncryptedPwd) > 0 {
			_ = s.upsertFederationPeerAuth(ctx, peer.Name, peer.RemoteURL, peer.Username, sharedEncryptedPwd)
		}
	}

	return &peer, nil
}

// ListFederationPeers returns all configured federation peers.
func (s *DoltStore) ListFederationPeers(ctx context.Context) ([]*storage.FederationPeer, error) {
	rows, err := s.queryContext(ctx, `
		SELECT
			p.name,
			p.remote_url,
			p.username,
			p.password_encrypted,
			p.sovereignty,
			p.last_sync,
			p.created_at,
			p.updated_at,
			a.remote_url,
			a.username,
			a.password_encrypted,
			a.last_sync
		FROM federation_peers p
		LEFT JOIN federation_peer_auth a
			ON a.peer_name = p.name AND a.machine_id = ?
		ORDER BY p.name
	`, s.federationMachineID())
	if err != nil {
		return nil, fmt.Errorf("failed to list federation peers: %w", err)
	}
	defer rows.Close()

	var peers []*storage.FederationPeer
	for rows.Next() {
		var peer storage.FederationPeer
		var sharedRemoteURL string
		var sharedUsername sql.NullString
		var sharedEncryptedPwd []byte
		var sharedLastSync sql.NullTime
		var localRemoteURL sql.NullString
		var localUsername sql.NullString
		var localEncryptedPwd []byte
		var localLastSync sql.NullTime

		if err := rows.Scan(
			&peer.Name,
			&sharedRemoteURL,
			&sharedUsername,
			&sharedEncryptedPwd,
			&peer.Sovereignty,
			&sharedLastSync,
			&peer.CreatedAt,
			&peer.UpdatedAt,
			&localRemoteURL,
			&localUsername,
			&localEncryptedPwd,
			&localLastSync,
		); err != nil {
			return nil, fmt.Errorf("failed to scan federation peer: %w", err)
		}

		localAuthExists := localRemoteURL.Valid
		peer.RemoteURL = sharedRemoteURL
		if localRemoteURL.Valid && localRemoteURL.String != "" {
			peer.RemoteURL = localRemoteURL.String
		}

		if localUsername.Valid {
			peer.Username = localUsername.String
		} else if sharedUsername.Valid {
			peer.Username = sharedUsername.String
		}

		if localLastSync.Valid {
			peer.LastSync = &localLastSync.Time
		} else if sharedLastSync.Valid {
			peer.LastSync = &sharedLastSync.Time
		}

		if localAuthExists {
			if password, ok := s.decryptPasswordBestEffort(localEncryptedPwd); ok {
				peer.Password = password
			}
		} else if password, ok := s.decryptPasswordBestEffort(sharedEncryptedPwd); ok {
			peer.Password = password
		}

		peers = append(peers, &peer)
	}

	return peers, rows.Err()
}

// RemoveFederationPeer removes a federation peer and its credentials.
func (s *DoltStore) RemoveFederationPeer(ctx context.Context, name string) error {
	if _, err := s.execContext(ctx, "DELETE FROM federation_peer_auth WHERE peer_name = ?", name); err != nil {
		return fmt.Errorf("failed to remove federation peer auth: %w", err)
	}

	result, err := s.execContext(ctx, "DELETE FROM federation_peers WHERE name = ?", name)
	if err != nil {
		return fmt.Errorf("failed to remove federation peer: %w", err)
	}

	rows, _ := result.RowsAffected() // Best effort: rows affected is used only for logging
	if rows == 0 {
		// Peer not in credentials table, but might still be a Dolt remote
		// Continue to try removing the remote
	}

	// Also remove the Dolt remote (best-effort)
	_ = s.RemoveRemote(ctx, name) // Best effort cleanup before re-adding remote

	return nil
}

// updatePeerLastSync updates the last sync time for a peer.
func (s *DoltStore) updatePeerLastSync(ctx context.Context, name string) error {
	_, err := s.execContext(ctx, `
		UPDATE federation_peer_auth
		SET last_sync = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
		WHERE peer_name = ? AND machine_id = ?
	`, name, s.federationMachineID())
	return err
}

// setFederationCredentials sets DOLT_REMOTE_USER and DOLT_REMOTE_PASSWORD env vars.
// Returns a cleanup function that must be called (typically via defer) to unset them.
// The caller must hold federationEnvMutex.
func setFederationCredentials(username, password string) func() {
	if username != "" {
		// Best-effort: failures here should not crash the caller.
		_ = os.Setenv("DOLT_REMOTE_USER", username) // Best effort: Setenv failure is extremely rare in practice
	}
	if password != "" {
		// Best-effort: failures here should not crash the caller.
		_ = os.Setenv("DOLT_REMOTE_PASSWORD", password) // Best effort: Setenv failure is extremely rare in practice
	}
	return func() {
		// Best-effort cleanup.
		_ = os.Unsetenv("DOLT_REMOTE_USER")     // Best effort cleanup of auth env vars
		_ = os.Unsetenv("DOLT_REMOTE_PASSWORD") // Best effort cleanup of auth env vars
	}
}

// withPeerCredentials executes a function with peer credentials set in environment.
// If the peer has stored credentials, they are set as DOLT_REMOTE_USER/PASSWORD
// for the duration of the function call.
func (s *DoltStore) withPeerCredentials(ctx context.Context, peerName string, fn func() error) error {
	// Look up credentials for this peer
	peer, err := s.GetFederationPeer(ctx, peerName)
	if err != nil {
		return fmt.Errorf("failed to get peer credentials: %w", err)
	}

	if peer != nil && peer.Username != "" && peer.Password == "" {
		return fmt.Errorf(
			"peer %s has username %q but no local password; run 'bd federation add-peer %s <url> --user %s --password <password>' on this machine",
			peerName, peer.Username, peerName, peer.Username,
		)
	}

	// If we have credentials, set env vars with mutex protection
	if peer != nil && peer.Username != "" && peer.Password != "" {
		federationEnvMutex.Lock()
		cleanup := setFederationCredentials(peer.Username, peer.Password)
		defer func() {
			cleanup()
			federationEnvMutex.Unlock()
		}()
	}

	// Execute the function
	return fn()
}

// FederationPeer is an alias for storage.FederationPeer for convenience.
type FederationPeer = storage.FederationPeer
