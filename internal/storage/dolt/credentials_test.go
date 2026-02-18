//go:build cgo

package dolt

import (
	"database/sql"
	"strings"
	"testing"
)

func TestGetFederationPeerUsesMachineScopedAuth(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx, cancel := testContext(t)
	defer cancel()

	localEncrypted, err := store.encryptPassword("local-pass")
	if err != nil {
		t.Fatalf("failed to encrypt local password: %v", err)
	}

	_, err = store.execContext(ctx, `
		INSERT INTO federation_peers (name, remote_url, username, password_encrypted, sovereignty)
		VALUES (?, ?, ?, ?, ?)
	`, "beta-town", "http://100.64.0.10:50051/beads", "shared-user", []byte("invalid-ciphertext"), "T2")
	if err != nil {
		t.Fatalf("failed to seed shared peer row: %v", err)
	}

	if err := store.upsertFederationPeerAuth(ctx, "beta-town", "http://localhost:50051/beads", "local-user", localEncrypted); err != nil {
		t.Fatalf("failed to seed machine-scoped auth row: %v", err)
	}

	peer, err := store.GetFederationPeer(ctx, "beta-town")
	if err != nil {
		t.Fatalf("GetFederationPeer failed: %v", err)
	}
	if peer == nil {
		t.Fatal("expected peer, got nil")
	}
	if peer.RemoteURL != "http://localhost:50051/beads" {
		t.Fatalf("remote_url = %q, want machine-scoped URL", peer.RemoteURL)
	}
	if peer.Username != "local-user" {
		t.Fatalf("username = %q, want machine-scoped username", peer.Username)
	}
	if peer.Password != "local-pass" {
		t.Fatalf("password mismatch: got %q", peer.Password)
	}

	peers, err := store.ListFederationPeers(ctx)
	if err != nil {
		t.Fatalf("ListFederationPeers failed: %v", err)
	}
	if len(peers) != 1 {
		t.Fatalf("len(peers) = %d, want 1", len(peers))
	}
	if peers[0].Password != "local-pass" {
		t.Fatalf("listed password mismatch: got %q", peers[0].Password)
	}
}

func TestGetFederationPeerBackfillsLegacyCredentials(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx, cancel := testContext(t)
	defer cancel()

	legacyEncrypted, err := store.encryptPassword("legacy-pass")
	if err != nil {
		t.Fatalf("failed to encrypt legacy password: %v", err)
	}

	_, err = store.execContext(ctx, `
		INSERT INTO federation_peers (name, remote_url, username, password_encrypted, sovereignty)
		VALUES (?, ?, ?, ?, ?)
	`, "legacy-peer", "http://100.64.0.20:50051/beads", "legacy-user", legacyEncrypted, "T2")
	if err != nil {
		t.Fatalf("failed to seed legacy peer row: %v", err)
	}

	peer, err := store.GetFederationPeer(ctx, "legacy-peer")
	if err != nil {
		t.Fatalf("GetFederationPeer failed: %v", err)
	}
	if peer == nil {
		t.Fatal("expected peer, got nil")
	}
	if peer.Password != "legacy-pass" {
		t.Fatalf("password mismatch: got %q", peer.Password)
	}
	if peer.Username != "legacy-user" {
		t.Fatalf("username mismatch: got %q", peer.Username)
	}

	var count int
	err = store.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM federation_peer_auth
		WHERE peer_name = ? AND machine_id = ?
	`, "legacy-peer", store.federationMachineID()).Scan(&count)
	if err != nil {
		t.Fatalf("failed to query machine-scoped auth rows: %v", err)
	}
	if count != 1 {
		t.Fatalf("machine-scoped row count = %d, want 1", count)
	}
}

func TestUpdatePeerLastSyncIsMachineScoped(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx, cancel := testContext(t)
	defer cancel()

	_, err := store.execContext(ctx, `
		INSERT INTO federation_peers (name, remote_url, sovereignty)
		VALUES (?, ?, ?)
	`, "gamma-town", "http://localhost:50051/beads", "T2")
	if err != nil {
		t.Fatalf("failed to seed shared peer row: %v", err)
	}

	if err := store.upsertFederationPeerAuth(ctx, "gamma-town", "http://localhost:50051/beads", "", nil); err != nil {
		t.Fatalf("failed to seed machine-scoped row: %v", err)
	}

	if err := store.updatePeerLastSync(ctx, "gamma-town"); err != nil {
		t.Fatalf("updatePeerLastSync failed: %v", err)
	}

	var localLastSync sql.NullTime
	err = store.db.QueryRowContext(ctx, `
		SELECT last_sync
		FROM federation_peer_auth
		WHERE peer_name = ? AND machine_id = ?
	`, "gamma-town", store.federationMachineID()).Scan(&localLastSync)
	if err != nil {
		t.Fatalf("failed to query machine-scoped last_sync: %v", err)
	}
	if !localLastSync.Valid {
		t.Fatal("expected machine-scoped last_sync to be set")
	}

	var sharedLastSync sql.NullTime
	err = store.db.QueryRowContext(ctx, `
		SELECT last_sync
		FROM federation_peers
		WHERE name = ?
	`, "gamma-town").Scan(&sharedLastSync)
	if err != nil {
		t.Fatalf("failed to query shared last_sync: %v", err)
	}
	if sharedLastSync.Valid {
		t.Fatal("expected shared federation_peers.last_sync to remain unset")
	}
}

func TestWithPeerCredentialsRejectsMissingPassword(t *testing.T) {
	store, cleanup := setupTestStore(t)
	defer cleanup()

	ctx, cancel := testContext(t)
	defer cancel()

	_, err := store.execContext(ctx, `
		INSERT INTO federation_peers (name, remote_url, username, sovereignty)
		VALUES (?, ?, ?, ?)
	`, "hub", "http://beads-hub:50051/beads", "beads_sync", "T2")
	if err != nil {
		t.Fatalf("failed to seed shared peer row: %v", err)
	}

	err = store.withPeerCredentials(ctx, "hub", func() error { return nil })
	if err == nil {
		t.Fatal("expected error for missing local password")
	}
	if got := err.Error(); !strings.Contains(got, "no local password") {
		t.Fatalf("unexpected error: %v", err)
	}
}
