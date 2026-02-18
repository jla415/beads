//go:build cgo

package dolt

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/steveyegge/beads/internal/storage"
)

// FederatedStorage implementation for DoltStore
// These methods enable peer-to-peer synchronization between Gas Towns.

// PushTo pushes commits to a specific peer remote.
// If credentials are stored for this peer, they are passed via --user flag.
func (s *DoltStore) PushTo(ctx context.Context, peer string) error {
	return s.withPeerCredentials(ctx, peer, func() error {
		username := s.getPeerUsername(ctx, peer)
		var err error
		if username != "" {
			_, err = s.execContext(ctx, "CALL DOLT_PUSH('--user', ?, ?, ?)", username, peer, s.branch)
		} else {
			_, err = s.execContext(ctx, "CALL DOLT_PUSH(?, ?)", peer, s.branch)
		}
		if err != nil {
			return fmt.Errorf("failed to push to peer %s: %w", peer, err)
		}
		return nil
	})
}

// PullFrom pulls changes from a specific peer remote.
// If credentials are stored for this peer, they are used automatically.
// Returns any merge conflicts if present.
func (s *DoltStore) PullFrom(ctx context.Context, peer string) ([]storage.Conflict, error) {
	var conflicts []storage.Conflict
	err := s.withPeerCredentials(ctx, peer, func() error {
		username := s.getPeerUsername(ctx, peer)
		var pullErr error
		if username != "" {
			_, pullErr = s.execContext(ctx, "CALL DOLT_PULL('--user', ?, ?)", username, peer)
		} else {
			_, pullErr = s.execContext(ctx, "CALL DOLT_PULL(?)", peer)
		}
		if pullErr != nil {
			// Check if the error is due to merge conflicts
			c, conflictErr := s.GetConflicts(ctx)
			if conflictErr == nil && len(c) > 0 {
				conflicts = c
				return nil
			}
			return fmt.Errorf("failed to pull from peer %s: %w", peer, pullErr)
		}
		return nil
	})
	return conflicts, err
}

// Fetch fetches refs from a peer without merging.
// If credentials are stored for this peer, they are used automatically.
func (s *DoltStore) Fetch(ctx context.Context, peer string) error {
	return s.withPeerCredentials(ctx, peer, func() error {
		username := s.getPeerUsername(ctx, peer)
		var err error
		if username != "" {
			_, err = s.execContext(ctx, "CALL DOLT_FETCH('--user', ?, ?)", username, peer)
		} else {
			_, err = s.execContext(ctx, "CALL DOLT_FETCH(?)", peer)
		}
		if err != nil {
			return fmt.Errorf("failed to fetch from peer %s: %w", peer, err)
		}
		return nil
	})
}

// getPeerUsername returns the stored username for a federation peer, or "" if none.
func (s *DoltStore) getPeerUsername(ctx context.Context, peerName string) string {
	peer, err := s.GetFederationPeer(ctx, peerName)
	if err != nil || peer == nil {
		return ""
	}
	return peer.Username
}

// ListRemotes returns configured remote names and URLs.
func (s *DoltStore) ListRemotes(ctx context.Context) ([]storage.RemoteInfo, error) {
	rows, err := s.queryContext(ctx, "SELECT name, url FROM dolt_remotes")
	if err != nil {
		return nil, fmt.Errorf("failed to list remotes: %w", err)
	}
	defer rows.Close()

	var remotes []storage.RemoteInfo
	for rows.Next() {
		var r storage.RemoteInfo
		if err := rows.Scan(&r.Name, &r.URL); err != nil {
			return nil, fmt.Errorf("failed to scan remote: %w", err)
		}
		remotes = append(remotes, r)
	}
	return remotes, rows.Err()
}

// RemoveRemote removes a configured remote.
func (s *DoltStore) RemoveRemote(ctx context.Context, name string) error {
	_, err := s.execContext(ctx, "CALL DOLT_REMOTE('remove', ?)", name)
	if err != nil {
		return fmt.Errorf("failed to remove remote %s: %w", name, err)
	}
	return nil
}

// SyncStatus returns the sync status with a peer.
func (s *DoltStore) SyncStatus(ctx context.Context, peer string) (*storage.SyncStatus, error) {
	status := &storage.SyncStatus{
		Peer: peer,
	}

	// Get ahead/behind counts by comparing refs
	// This requires the peer to have been fetched first
	query := `
		SELECT
			(SELECT COUNT(*) FROM dolt_log WHERE commit_hash NOT IN
				(SELECT commit_hash FROM dolt_log AS OF CONCAT(?, '/', ?))) as ahead,
			(SELECT COUNT(*) FROM dolt_log AS OF CONCAT(?, '/', ?) WHERE commit_hash NOT IN
				(SELECT commit_hash FROM dolt_log)) as behind
	`

	err := s.db.QueryRowContext(ctx, query, peer, s.branch, peer, s.branch).
		Scan(&status.LocalAhead, &status.LocalBehind)
	if err != nil {
		// If we can't get the status, return a partial result
		// This happens when the remote branch doesn't exist locally yet
		status.LocalAhead = -1
		status.LocalBehind = -1
	}

	// Check for conflicts
	conflicts, err := s.GetConflicts(ctx)
	if err == nil && len(conflicts) > 0 {
		status.HasConflicts = true
	}

	// Get last sync time from metadata
	status.LastSync = s.getLastSyncTime(ctx, peer)

	return status, nil
}

// getLastSyncTime retrieves the last sync time for a peer from metadata.
func (s *DoltStore) getLastSyncTime(ctx context.Context, peer string) time.Time {
	key := "last_sync_" + peer
	var value string
	err := s.db.QueryRowContext(ctx, "SELECT value FROM metadata WHERE `key` = ?", key).Scan(&value)
	if err != nil {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}
	}
	return t
}

// setLastSyncTime records the last sync time for a peer in metadata.
func (s *DoltStore) setLastSyncTime(ctx context.Context, peer string) error {
	key := "last_sync_" + peer
	value := time.Now().Format(time.RFC3339)
	_, err := s.execContext(ctx,
		"REPLACE INTO metadata (`key`, value) VALUES (?, ?)", key, value)
	return err
}

// Sync performs a full bidirectional sync with a peer:
// 1. Fetch from peer
// 2. Merge peer's changes (handling conflicts per strategy)
// 3. Push local changes to peer
//
// Returns the sync result including any conflicts encountered.
func (s *DoltStore) Sync(ctx context.Context, peer string, strategy string) (*SyncResult, error) {
	result := &SyncResult{
		Peer:      peer,
		StartTime: time.Now(),
	}

	// Step 1: Fetch from peer
	if err := s.Fetch(ctx, peer); err != nil {
		result.Error = fmt.Errorf("fetch failed: %w", err)
		return result, result.Error
	}
	result.Fetched = true

	// Commit any working changes (e.g., federation_peers.last_sync update)
	// so they don't block the merge step.
	if err := s.Commit(ctx, fmt.Sprintf("Pre-merge commit for federation sync with %s", peer)); err != nil {
		// Ignore commit errors (e.g., nothing to commit)
		_ = err
	}

	// Step 2: Get status before merge
	beforeCommit, _ := s.GetCurrentCommit(ctx) // Best effort: empty commit hash means diff won't be logged

	// Step 3: Merge peer's branch
	remoteBranch := fmt.Sprintf("%s/%s", peer, s.branch)
	_, err := s.Merge(ctx, remoteBranch)
	if err != nil {
		// With autocommit ON, dolt rolls back on conflict, wiping dolt_conflicts.
		// Handle merge+resolve+commit in a single autocommit=0 transaction so the
		// result is a proper merge commit with both branches as ancestors.
		if strings.Contains(err.Error(), "Merge conflict detected") {
			resolved, resolveErr := s.mergeAndResolve(ctx, remoteBranch, strategy)
			if resolveErr != nil {
				result.Conflicts = resolved
				result.Error = resolveErr
				return result, result.Error
			}
			result.Conflicts = resolved
			result.ConflictsResolved = true
		} else {
			result.Error = fmt.Errorf("merge failed: %w", err)
			return result, result.Error
		}
	}
	result.Merged = true

	// Count pulled commits
	afterCommit, _ := s.GetCurrentCommit(ctx) // Best effort: empty commit hash means diff won't be logged
	if beforeCommit != afterCommit {
		result.PulledCommits = 1 // Simplified - could count actual commits
	}

	// Step 5: Push our changes to peer
	if err := s.PushTo(ctx, peer); err != nil {
		// Push failure is not fatal - peer may not accept pushes
		result.PushError = err
	} else {
		result.Pushed = true
	}

	// Record last sync time only when actual work was done
	if result.PulledCommits > 0 || result.ConflictsResolved {
		_ = s.setLastSyncTime(ctx, peer)
		_ = s.updatePeerLastSync(ctx, peer)
	}

	result.EndTime = time.Now()
	return result, nil
}

// SyncResult contains the outcome of a Sync operation.
type SyncResult struct {
	Peer              string
	StartTime         time.Time
	EndTime           time.Time
	Fetched           bool
	Merged            bool
	Pushed            bool
	PulledCommits     int
	PushedCommits     int
	Conflicts         []storage.Conflict
	ConflictsResolved bool
	Error             error
	PushError         error // Non-fatal push error
}
