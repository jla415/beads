//go:build cgo

package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/steveyegge/beads/internal/types"
)

func TestReadySuite(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	testDB := filepath.Join(tmpDir, ".beads", "beads.db")
	s := newTestStore(t, testDB)
	ctx := context.Background()

	// ========== Shared data setup ==========
	// All sub-tests share one DB. IDs are unique across all sub-tests.

	// --- Core ready work data ---
	coreIssues := []*types.Issue{
		{ID: "test-1", Title: "Ready task 1", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask, CreatedAt: time.Now()},
		{ID: "test-2", Title: "Ready task 2", Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask, CreatedAt: time.Now()},
		{ID: "test-3", Title: "Blocked task", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask, CreatedAt: time.Now()},
		{ID: "test-blocker", Title: "Blocking task", Status: types.StatusOpen, Priority: 0, IssueType: types.TypeTask, CreatedAt: time.Now()},
		{ID: "test-closed", Title: "Closed task", Status: types.StatusClosed, Priority: 2, IssueType: types.TypeTask, CreatedAt: time.Now(), ClosedAt: ptrTime(time.Now())},
	}
	for _, issue := range coreIssues {
		if err := s.CreateIssue(ctx, issue, "test"); err != nil {
			t.Fatal(err)
		}
	}
	// test-3 depends on test-blocker
	if err := s.AddDependency(ctx, &types.Dependency{
		IssueID: "test-3", DependsOnID: "test-blocker", Type: types.DepBlocks, CreatedAt: time.Now(),
	}, "test"); err != nil {
		t.Fatal(err)
	}

	// --- Assignee data ---
	assigneeIssues := []*types.Issue{
		{ID: "test-alice", Title: "Alice's task", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask, Assignee: "alice", CreatedAt: time.Now()},
		{ID: "test-bob", Title: "Bob's task", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask, Assignee: "bob", CreatedAt: time.Now()},
		{ID: "test-unassigned", Title: "Unassigned task", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask, CreatedAt: time.Now()},
	}
	for _, issue := range assigneeIssues {
		if err := s.CreateIssue(ctx, issue, "test"); err != nil {
			t.Fatal(err)
		}
	}

	// --- In-progress data ---
	if err := s.CreateIssue(ctx, &types.Issue{
		ID: "test-wip", Title: "Work in progress", Status: types.StatusInProgress, Priority: 1, IssueType: types.TypeTask, CreatedAt: time.Now(),
	}, "test"); err != nil {
		t.Fatal(err)
	}

	// --- Closed-blocker data ---
	closedBlockerIssues := []*types.Issue{
		{ID: "test-closed-blocker-1", Title: "Closed blocker 1", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask, CreatedAt: time.Now()},
		{ID: "test-closed-blocker-2", Title: "Closed blocker 2", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask, CreatedAt: time.Now()},
		{ID: "test-open-blocker", Title: "Open blocker", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask, CreatedAt: time.Now()},
		{ID: "test-ready-via-closed-blockers", Title: "Ready when all blockers are closed", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask, CreatedAt: time.Now()},
		{ID: "test-still-blocked", Title: "Still blocked by open blocker", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask, CreatedAt: time.Now()},
	}
	for _, issue := range closedBlockerIssues {
		if err := s.CreateIssue(ctx, issue, "test"); err != nil {
			t.Fatal(err)
		}
	}
	closedBlockerDeps := []*types.Dependency{
		{IssueID: "test-ready-via-closed-blockers", DependsOnID: "test-closed-blocker-1", Type: types.DepBlocks, CreatedAt: time.Now()},
		{IssueID: "test-ready-via-closed-blockers", DependsOnID: "test-closed-blocker-2", Type: types.DepBlocks, CreatedAt: time.Now()},
		{IssueID: "test-still-blocked", DependsOnID: "test-closed-blocker-1", Type: types.DepBlocks, CreatedAt: time.Now()},
		{IssueID: "test-still-blocked", DependsOnID: "test-open-blocker", Type: types.DepBlocks, CreatedAt: time.Now()},
	}
	for _, dep := range closedBlockerDeps {
		if err := s.AddDependency(ctx, dep, "test"); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.CloseIssue(ctx, "test-closed-blocker-1", "completed", "test", "session-ready-1"); err != nil {
		t.Fatal(err)
	}
	if err := s.CloseIssue(ctx, "test-closed-blocker-2", "completed", "test", "session-ready-2"); err != nil {
		t.Fatal(err)
	}

	// --- Epic/parent-child data (for buildParentEpicMap) ---
	epicIssues := []*types.Issue{
		{ID: "test-epic", Title: "Auth Overhaul", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeEpic, CreatedAt: time.Now()},
		{ID: "test-parent-task", Title: "Parent Task", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask, CreatedAt: time.Now()},
		{ID: "test-child-1", Title: "Implement login", Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask, CreatedAt: time.Now()},
		{ID: "test-child-2", Title: "Subtask of non-epic", Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask, CreatedAt: time.Now()},
		{ID: "test-orphan", Title: "Standalone task", Status: types.StatusOpen, Priority: 3, IssueType: types.TypeTask, CreatedAt: time.Now()},
	}
	for _, issue := range epicIssues {
		if err := s.CreateIssue(ctx, issue, "test"); err != nil {
			t.Fatal(err)
		}
	}
	epicDeps := []*types.Dependency{
		{IssueID: "test-child-1", DependsOnID: "test-epic", Type: types.DepParentChild, CreatedAt: time.Now()},
		{IssueID: "test-child-2", DependsOnID: "test-parent-task", Type: types.DepParentChild, CreatedAt: time.Now()},
	}
	for _, dep := range epicDeps {
		if err := s.AddDependency(ctx, dep, "test"); err != nil {
			t.Fatal(err)
		}
	}

	// --- Defer data ---
	futureDefer := time.Now().Add(24 * time.Hour)
	pastDefer := time.Now().Add(-1 * time.Hour)
	deferIssues := []*types.Issue{
		{ID: "test-future-defer", Title: "Future deferred task", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask, DeferUntil: &futureDefer, CreatedAt: time.Now()},
		{ID: "test-past-defer", Title: "Past deferred task", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask, DeferUntil: &pastDefer, CreatedAt: time.Now()},
		{ID: "test-no-defer", Title: "Normal task (no defer)", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask, CreatedAt: time.Now()},
	}
	for _, issue := range deferIssues {
		if err := s.CreateIssue(ctx, issue, "test"); err != nil {
			t.Fatal(err)
		}
	}

	// --- Unassigned-specific data ---
	unassignedIssues := []*types.Issue{
		{ID: "test-unassigned-1", Title: "Unassigned task 1", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask, Assignee: "", CreatedAt: time.Now()},
		{ID: "test-unassigned-2", Title: "Unassigned task 2", Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask, CreatedAt: time.Now()},
		{ID: "test-assigned-alice", Title: "Alice's task 2", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask, Assignee: "alice", CreatedAt: time.Now()},
		{ID: "test-assigned-bob", Title: "Bob's task 2", Status: types.StatusOpen, Priority: 1, IssueType: types.TypeTask, Assignee: "bob", CreatedAt: time.Now()},
	}
	for _, issue := range unassignedIssues {
		if err := s.CreateIssue(ctx, issue, "test"); err != nil {
			t.Fatal(err)
		}
	}

	// ========== Sub-tests ==========

	t.Run("ReadyWork", func(t *testing.T) {
		ready, err := s.GetReadyWork(ctx, types.WorkFilter{})
		if err != nil {
			t.Fatalf("GetReadyWork failed: %v", err)
		}

		readyIDs := make(map[string]bool)
		for _, issue := range ready {
			readyIDs[issue.ID] = true
		}

		// test-1, test-2, test-blocker should be in ready work
		for _, id := range []string{"test-1", "test-2", "test-blocker"} {
			if !readyIDs[id] {
				t.Errorf("Expected %s in ready work", id)
			}
		}

		// test-3 (blocked) and test-closed should NOT be in ready work
		if readyIDs["test-3"] {
			t.Error("test-3 should not be in ready work (it's blocked)")
		}
		if readyIDs["test-closed"] {
			t.Error("test-closed should not be in ready work (it's closed)")
		}

		// Priority filter
		priority1 := 1
		readyP1, err := s.GetReadyWork(ctx, types.WorkFilter{Priority: &priority1})
		if err != nil {
			t.Fatalf("GetReadyWork with priority filter failed: %v", err)
		}
		for _, issue := range readyP1 {
			if issue.Priority != 1 {
				t.Errorf("Expected priority 1, got %d for issue %s", issue.Priority, issue.ID)
			}
		}

		// Limit
		readyLimited, err := s.GetReadyWork(ctx, types.WorkFilter{Limit: 1})
		if err != nil {
			t.Fatalf("GetReadyWork with limit failed: %v", err)
		}
		if len(readyLimited) > 1 {
			t.Errorf("Expected at most 1 issue with limit=1, got %d", len(readyLimited))
		}
	})

	t.Run("ReadyWorkWithAssignee", func(t *testing.T) {
		alice := "alice"
		readyAlice, err := s.GetReadyWork(ctx, types.WorkFilter{Assignee: &alice})
		if err != nil {
			t.Fatalf("GetReadyWork with assignee filter failed: %v", err)
		}

		// All returned issues should be assigned to alice
		for _, issue := range readyAlice {
			if issue.Assignee != "alice" {
				t.Errorf("Expected assignee='alice', got %q for %s", issue.Assignee, issue.ID)
			}
		}

		// Should include test-alice
		found := false
		for _, issue := range readyAlice {
			if issue.ID == "test-alice" {
				found = true
				break
			}
		}
		if !found {
			t.Error("Expected test-alice in assignee-filtered results")
		}
	})

	t.Run("ReadyWorkUnassignedFilter", func(t *testing.T) {
		readyUnassigned, err := s.GetReadyWork(ctx, types.WorkFilter{Unassigned: true})
		if err != nil {
			t.Fatalf("GetReadyWork with unassigned filter failed: %v", err)
		}

		// All returned issues should have no assignee
		for _, issue := range readyUnassigned {
			if issue.Assignee != "" {
				t.Errorf("Expected empty assignee, got %q for issue %s", issue.Assignee, issue.ID)
			}
		}

		// Should include test-unassigned
		found := false
		for _, issue := range readyUnassigned {
			if issue.ID == "test-unassigned" {
				found = true
				break
			}
		}
		if !found {
			t.Error("Expected to find test-unassigned in unassigned results")
		}
	})

	t.Run("ReadyWorkInProgressWithEmptyFilter", func(t *testing.T) {
		ready, err := s.GetReadyWork(ctx, types.WorkFilter{})
		if err != nil {
			t.Fatalf("GetReadyWork failed: %v", err)
		}

		found := false
		for _, i := range ready {
			if i.ID == "test-wip" {
				found = true
				break
			}
		}
		if !found {
			t.Error("In-progress issue should appear when filter.Status is empty")
		}
	})

	t.Run("ReadyWorkExcludesInProgressWithOpenFilter", func(t *testing.T) {
		ready, err := s.GetReadyWork(ctx, types.WorkFilter{Status: "open"})
		if err != nil {
			t.Fatalf("GetReadyWork with Status=open failed: %v", err)
		}

		for _, i := range ready {
			if i.ID == "test-wip" {
				t.Error("In-progress issue should NOT appear when filter.Status='open'")
			}
		}
	})

	t.Run("ReadyWorkIncludesIssuesWhoseBlockersAreClosed", func(t *testing.T) {
		ready, err := s.GetReadyWork(ctx, types.WorkFilter{Status: "open"})
		if err != nil {
			t.Fatalf("GetReadyWork with Status=open failed: %v", err)
		}

		foundReadyViaClosed := false
		foundStillBlocked := false
		for _, issue := range ready {
			if issue.ID == "test-ready-via-closed-blockers" {
				foundReadyViaClosed = true
			}
			if issue.ID == "test-still-blocked" {
				foundStillBlocked = true
			}
		}

		if !foundReadyViaClosed {
			t.Error("Issue with only closed blockers should be in ready work")
		}
		if foundStillBlocked {
			t.Error("Issue with any open blocker should not be in ready work")
		}
	})

	// --- buildParentEpicMap tests (merged from TestBuildParentEpicMap) ---

	t.Run("BuildParentEpicMap_MapsChildToEpicParentOnly", func(t *testing.T) {
		issues := []*types.Issue{
			{ID: "test-child-1", Title: "Implement login", Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask},
			{ID: "test-child-2", Title: "Subtask of non-epic", Status: types.StatusOpen, Priority: 2, IssueType: types.TypeTask},
			{ID: "test-orphan", Title: "Standalone task", Status: types.StatusOpen, Priority: 3, IssueType: types.TypeTask},
		}
		result := buildParentEpicMap(ctx, s, issues)

		if result["test-child-1"] != "Auth Overhaul" {
			t.Errorf("Expected test-child-1 to map to 'Auth Overhaul', got %q", result["test-child-1"])
		}
		if _, ok := result["test-child-2"]; ok {
			t.Errorf("test-child-2 should not be in map (parent is not an epic), got %q", result["test-child-2"])
		}
		if _, ok := result["test-orphan"]; ok {
			t.Errorf("test-orphan should not be in map (no parent)")
		}
	})

	t.Run("BuildParentEpicMap_EmptyIssuesReturnsNil", func(t *testing.T) {
		result := buildParentEpicMap(ctx, s, nil)
		if result != nil {
			t.Errorf("Expected nil for empty issues, got %v", result)
		}
	})

	t.Run("BuildParentEpicMap_NoParentDepsReturnsNil", func(t *testing.T) {
		orphan := &types.Issue{ID: "test-orphan", Title: "Standalone task", Status: types.StatusOpen, Priority: 3, IssueType: types.TypeTask}
		result := buildParentEpicMap(ctx, s, []*types.Issue{orphan})
		if result != nil {
			t.Errorf("Expected nil when no parent deps exist, got %v", result)
		}
	})

	// --- Defer tests (merged from TestReadyWorkDeferUntil) ---

	t.Run("DeferUntil_ExcludesFutureDeferredByDefault", func(t *testing.T) {
		ready, err := s.GetReadyWork(ctx, types.WorkFilter{})
		if err != nil {
			t.Fatalf("GetReadyWork failed: %v", err)
		}

		for _, issue := range ready {
			if issue.ID == "test-future-defer" {
				t.Error("Future deferred issue should not appear in ready work by default")
			}
		}

		foundPast := false
		foundNoDefer := false
		for _, issue := range ready {
			if issue.ID == "test-past-defer" {
				foundPast = true
			}
			if issue.ID == "test-no-defer" {
				foundNoDefer = true
			}
		}
		if !foundPast {
			t.Error("Past deferred issue should appear in ready work")
		}
		if !foundNoDefer {
			t.Error("Issue without defer should appear in ready work")
		}
	})

	t.Run("DeferUntil_IncludeDeferredShowsAll", func(t *testing.T) {
		ready, err := s.GetReadyWork(ctx, types.WorkFilter{IncludeDeferred: true})
		if err != nil {
			t.Fatalf("GetReadyWork with IncludeDeferred failed: %v", err)
		}

		foundFuture := false
		for _, issue := range ready {
			if issue.ID == "test-future-defer" {
				foundFuture = true
				break
			}
		}
		if !foundFuture {
			t.Error("Future deferred issue should appear when IncludeDeferred=true")
		}
	})

	// --- Unassigned tests (merged from TestReadyWorkUnassigned) ---

	t.Run("Unassigned_FiltersCorrectly", func(t *testing.T) {
		readyUnassigned, err := s.GetReadyWork(ctx, types.WorkFilter{Unassigned: true})
		if err != nil {
			t.Fatalf("GetReadyWork with Unassigned filter failed: %v", err)
		}

		// All returned issues should have no assignee
		for _, issue := range readyUnassigned {
			if issue.Assignee != "" {
				t.Errorf("Expected no assignee, got %q for issue %s", issue.Assignee, issue.ID)
			}
		}

		// Should include test-unassigned-1 and test-unassigned-2
		unassignedIDs := make(map[string]bool)
		for _, issue := range readyUnassigned {
			unassignedIDs[issue.ID] = true
		}
		if !unassignedIDs["test-unassigned-1"] {
			t.Error("Expected test-unassigned-1 in unassigned results")
		}
		if !unassignedIDs["test-unassigned-2"] {
			t.Error("Expected test-unassigned-2 in unassigned results")
		}
	})

	t.Run("Unassigned_TakesPrecedenceOverAssignee", func(t *testing.T) {
		alice := "alice"
		readyConflict, err := s.GetReadyWork(ctx, types.WorkFilter{Unassigned: true, Assignee: &alice})
		if err != nil {
			t.Fatalf("GetReadyWork with conflicting filters failed: %v", err)
		}

		// Unassigned should win, returning only unassigned issues
		for _, issue := range readyConflict {
			if issue.Assignee != "" {
				t.Errorf("Unassigned should override Assignee filter, got %q for issue %s", issue.Assignee, issue.ID)
			}
		}
	})
}

func TestReadyCommandInit(t *testing.T) {
	t.Parallel()
	if readyCmd == nil {
		t.Fatal("readyCmd should be initialized")
	}

	if readyCmd.Use != "ready" {
		t.Errorf("Expected Use='ready', got %q", readyCmd.Use)
	}

	if len(readyCmd.Short) == 0 {
		t.Error("readyCmd should have Short description")
	}

	// Verify --pretty defaults to true
	prettyFlag := readyCmd.Flags().Lookup("pretty")
	if prettyFlag == nil {
		t.Fatal("--pretty flag should exist")
	}
	if prettyFlag.DefValue != "true" {
		t.Errorf("--pretty default should be 'true', got %q", prettyFlag.DefValue)
	}

	// Verify --plain flag exists and defaults to false
	plainFlag := readyCmd.Flags().Lookup("plain")
	if plainFlag == nil {
		t.Fatal("--plain flag should exist")
	}
	if plainFlag.DefValue != "false" {
		t.Errorf("--plain default should be 'false', got %q", plainFlag.DefValue)
	}

	// Verify --sort defaults to "priority"
	sortFlag := readyCmd.Flags().Lookup("sort")
	if sortFlag == nil {
		t.Fatal("--sort flag should exist")
	}
	if sortFlag.DefValue != "priority" {
		t.Errorf("--sort default should be 'priority', got %q", sortFlag.DefValue)
	}
}

func TestBuildParentEpicMap(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	testDB := filepath.Join(tmpDir, ".beads", "beads.db")
	s := newTestStore(t, testDB)
	ctx := context.Background()

	// Create an epic, a non-epic parent, and child tasks
	epic := &types.Issue{
		ID:        "test-epic",
		Title:     "Auth Overhaul",
		Status:    types.StatusOpen,
		Priority:  1,
		IssueType: types.TypeEpic,
		CreatedAt: time.Now(),
	}
	nonEpicParent := &types.Issue{
		ID:        "test-parent-task",
		Title:     "Parent Task",
		Status:    types.StatusOpen,
		Priority:  1,
		IssueType: types.TypeTask,
		CreatedAt: time.Now(),
	}
	childOfEpic := &types.Issue{
		ID:        "test-child-1",
		Title:     "Implement login",
		Status:    types.StatusOpen,
		Priority:  2,
		IssueType: types.TypeTask,
		CreatedAt: time.Now(),
	}
	childOfTask := &types.Issue{
		ID:        "test-child-2",
		Title:     "Subtask of non-epic",
		Status:    types.StatusOpen,
		Priority:  2,
		IssueType: types.TypeTask,
		CreatedAt: time.Now(),
	}
	orphan := &types.Issue{
		ID:        "test-orphan",
		Title:     "Standalone task",
		Status:    types.StatusOpen,
		Priority:  3,
		IssueType: types.TypeTask,
		CreatedAt: time.Now(),
	}

	for _, issue := range []*types.Issue{epic, nonEpicParent, childOfEpic, childOfTask, orphan} {
		if err := s.CreateIssue(ctx, issue, "test"); err != nil {
			t.Fatal(err)
		}
	}

	// Add parent-child dependencies
	deps := []*types.Dependency{
		{IssueID: "test-child-1", DependsOnID: "test-epic", Type: types.DepParentChild, CreatedAt: time.Now()},
		{IssueID: "test-child-2", DependsOnID: "test-parent-task", Type: types.DepParentChild, CreatedAt: time.Now()},
	}
	for _, dep := range deps {
		if err := s.AddDependency(ctx, dep, "test"); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("MapsChildToEpicParentOnly", func(t *testing.T) {
		issues := []*types.Issue{childOfEpic, childOfTask, orphan}
		result := buildParentEpicMap(ctx, s, issues)

		// child-1 should map to the epic
		if result["test-child-1"] != "Auth Overhaul" {
			t.Errorf("Expected test-child-1 to map to 'Auth Overhaul', got %q", result["test-child-1"])
		}

		// child-2 should NOT be in the map (parent is not an epic)
		if _, ok := result["test-child-2"]; ok {
			t.Errorf("test-child-2 should not be in map (parent is not an epic), got %q", result["test-child-2"])
		}

		// orphan should NOT be in the map
		if _, ok := result["test-orphan"]; ok {
			t.Errorf("test-orphan should not be in map (no parent)")
		}
	})

	t.Run("EmptyIssuesReturnsNil", func(t *testing.T) {
		result := buildParentEpicMap(ctx, s, nil)
		if result != nil {
			t.Errorf("Expected nil for empty issues, got %v", result)
		}
	})

	t.Run("NoParentDepsReturnsNil", func(t *testing.T) {
		// orphan has no parent-child deps
		result := buildParentEpicMap(ctx, s, []*types.Issue{orphan})
		if result != nil {
			t.Errorf("Expected nil when no parent deps exist, got %v", result)
		}
	})
}

// GH#820: Tests for defer_until filtering in ready work
func TestReadyWorkDeferUntil(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	testDB := filepath.Join(tmpDir, ".beads", "beads.db")
	s := newTestStore(t, testDB)
	ctx := context.Background()

	// Create issues with different defer_until values
	futureDefer := time.Now().Add(24 * time.Hour) // Deferred to future
	pastDefer := time.Now().Add(-1 * time.Hour)   // Deferred to past (should be visible)

	issues := []*types.Issue{
		{
			ID:         "test-future-defer",
			Title:      "Future deferred task",
			Status:     types.StatusOpen,
			Priority:   1,
			IssueType:  types.TypeTask,
			DeferUntil: &futureDefer,
			CreatedAt:  time.Now(),
		},
		{
			ID:         "test-past-defer",
			Title:      "Past deferred task",
			Status:     types.StatusOpen,
			Priority:   1,
			IssueType:  types.TypeTask,
			DeferUntil: &pastDefer,
			CreatedAt:  time.Now(),
		},
		{
			ID:        "test-no-defer",
			Title:     "Normal task (no defer)",
			Status:    types.StatusOpen,
			Priority:  1,
			IssueType: types.TypeTask,
			CreatedAt: time.Now(),
		},
	}

	for _, issue := range issues {
		if err := s.CreateIssue(ctx, issue, "test"); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("ExcludesFutureDeferredByDefault", func(t *testing.T) {
		// Default behavior: exclude issues with future defer_until
		ready, err := s.GetReadyWork(ctx, types.WorkFilter{})
		if err != nil {
			t.Fatalf("GetReadyWork failed: %v", err)
		}

		// Should NOT include test-future-defer
		for _, issue := range ready {
			if issue.ID == "test-future-defer" {
				t.Error("Future deferred issue should not appear in ready work by default")
			}
		}

		// Should include test-past-defer and test-no-defer
		foundPast := false
		foundNoDefer := false
		for _, issue := range ready {
			if issue.ID == "test-past-defer" {
				foundPast = true
			}
			if issue.ID == "test-no-defer" {
				foundNoDefer = true
			}
		}

		if !foundPast {
			t.Error("Past deferred issue should appear in ready work")
		}
		if !foundNoDefer {
			t.Error("Issue without defer should appear in ready work")
		}
	})

	t.Run("IncludeDeferredShowsAll", func(t *testing.T) {
		// With IncludeDeferred: show all issues including future deferred
		ready, err := s.GetReadyWork(ctx, types.WorkFilter{
			IncludeDeferred: true,
		})
		if err != nil {
			t.Fatalf("GetReadyWork with IncludeDeferred failed: %v", err)
		}

		// Should include test-future-defer
		foundFuture := false
		for _, issue := range ready {
			if issue.ID == "test-future-defer" {
				foundFuture = true
				break
			}
		}

		if !foundFuture {
			t.Error("Future deferred issue should appear when IncludeDeferred=true")
		}
	})
}

// Test that GetReadyWork returns results sorted by priority (P0 first, then P1, etc.)
func TestReadyWorkSortByPriority(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	testDB := filepath.Join(tmpDir, ".beads", "beads.db")
	s := newTestStore(t, testDB)
	ctx := context.Background()

	// Create issues with varying priorities, inserted in non-sorted order
	issues := []*types.Issue{
		{
			ID:        "sort-p3",
			Title:     "Low priority task",
			Status:    types.StatusOpen,
			Priority:  3,
			IssueType: types.TypeTask,
			CreatedAt: time.Now().Add(-3 * time.Hour),
		},
		{
			ID:        "sort-p0",
			Title:     "Critical task",
			Status:    types.StatusOpen,
			Priority:  0,
			IssueType: types.TypeTask,
			CreatedAt: time.Now().Add(-2 * time.Hour),
		},
		{
			ID:        "sort-p1",
			Title:     "High priority task",
			Status:    types.StatusOpen,
			Priority:  1,
			IssueType: types.TypeTask,
			CreatedAt: time.Now().Add(-1 * time.Hour),
		},
		{
			ID:        "sort-p2",
			Title:     "Medium priority task",
			Status:    types.StatusOpen,
			Priority:  2,
			IssueType: types.TypeTask,
			CreatedAt: time.Now(),
		},
	}

	for _, issue := range issues {
		if err := s.CreateIssue(ctx, issue, "test"); err != nil {
			t.Fatal(err)
		}
	}

	// Test SortPolicyPriority: should return P0, P1, P2, P3
	ready, err := s.GetReadyWork(ctx, types.WorkFilter{
		SortPolicy: types.SortPolicyPriority,
	})
	if err != nil {
		t.Fatalf("GetReadyWork with priority sort failed: %v", err)
	}

	if len(ready) < 4 {
		t.Fatalf("Expected at least 4 issues, got %d", len(ready))
	}

	// Verify priority ordering is preserved through the full query pipeline
	for i := 1; i < len(ready); i++ {
		if ready[i].Priority < ready[i-1].Priority {
			t.Errorf("Issues not sorted by priority: issue[%d] has P%d but issue[%d] has P%d",
				i-1, ready[i-1].Priority, i, ready[i].Priority)
		}
	}
}

// Test that SortPolicyOldest returns issues ordered by creation date (oldest first)
func TestReadyWorkSortByOldest(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	testDB := filepath.Join(tmpDir, ".beads", "beads.db")
	s := newTestStore(t, testDB)
	ctx := context.Background()

	baseTime := time.Now().Add(-10 * time.Hour)
	issues := []*types.Issue{
		{
			ID:        "oldest-new",
			Title:     "Newest task",
			Status:    types.StatusOpen,
			Priority:  0, // High priority but created last
			IssueType: types.TypeTask,
			CreatedAt: baseTime.Add(3 * time.Hour),
		},
		{
			ID:        "oldest-old",
			Title:     "Oldest task",
			Status:    types.StatusOpen,
			Priority:  3, // Low priority but created first
			IssueType: types.TypeTask,
			CreatedAt: baseTime,
		},
		{
			ID:        "oldest-mid",
			Title:     "Middle task",
			Status:    types.StatusOpen,
			Priority:  1,
			IssueType: types.TypeTask,
			CreatedAt: baseTime.Add(1 * time.Hour),
		},
	}

	for _, issue := range issues {
		if err := s.CreateIssue(ctx, issue, "test"); err != nil {
			t.Fatal(err)
		}
	}

	ready, err := s.GetReadyWork(ctx, types.WorkFilter{
		SortPolicy: types.SortPolicyOldest,
	})
	if err != nil {
		t.Fatalf("GetReadyWork with oldest sort failed: %v", err)
	}

	if len(ready) < 3 {
		t.Fatalf("Expected at least 3 issues, got %d", len(ready))
	}

	// Find positions of our test issues
	positions := map[string]int{}
	for i, issue := range ready {
		positions[issue.ID] = i
	}

	// oldest-old should come before oldest-mid, which should come before oldest-new
	if positions["oldest-old"] > positions["oldest-mid"] {
		t.Error("Oldest task should appear before middle task with SortPolicyOldest")
	}
	if positions["oldest-mid"] > positions["oldest-new"] {
		t.Error("Middle task should appear before newest task with SortPolicyOldest")
	}
}

func TestReadyWorkUnassigned(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	testDB := filepath.Join(tmpDir, ".beads", "beads.db")
	s := newTestStore(t, testDB)
	ctx := context.Background()

	// Create issues with different assignees
	issues := []*types.Issue{
		{
			ID:        "test-unassigned-1",
			Title:     "Unassigned task 1",
			Status:    types.StatusOpen,
			Priority:  1,
			IssueType: types.TypeTask,
			Assignee:  "",
			CreatedAt: time.Now(),
		},
		{
			ID:        "test-unassigned-2",
			Title:     "Unassigned task 2",
			Status:    types.StatusOpen,
			Priority:  2,
			IssueType: types.TypeTask,
			CreatedAt: time.Now(),
		},
		{
			ID:        "test-assigned-alice",
			Title:     "Alice's task",
			Status:    types.StatusOpen,
			Priority:  1,
			IssueType: types.TypeTask,
			Assignee:  "alice",
			CreatedAt: time.Now(),
		},
		{
			ID:        "test-assigned-bob",
			Title:     "Bob's task",
			Status:    types.StatusOpen,
			Priority:  1,
			IssueType: types.TypeTask,
			Assignee:  "bob",
			CreatedAt: time.Now(),
		},
	}

	for _, issue := range issues {
		if err := s.CreateIssue(ctx, issue, "test"); err != nil {
			t.Fatal(err)
		}
	}

	// Test filtering by --unassigned
	readyUnassigned, err := s.GetReadyWork(ctx, types.WorkFilter{
		Unassigned: true,
	})
	if err != nil {
		t.Fatalf("GetReadyWork with Unassigned filter failed: %v", err)
	}

	// Should only have unassigned issues
	if len(readyUnassigned) != 2 {
		t.Errorf("Expected 2 unassigned issues, got %d", len(readyUnassigned))
	}

	for _, issue := range readyUnassigned {
		if issue.Assignee != "" {
			t.Errorf("Expected no assignee, got %q for issue %s", issue.Assignee, issue.ID)
		}
	}

	// Test that Unassigned takes precedence over Assignee filter
	alice := "alice"
	readyConflict, err := s.GetReadyWork(ctx, types.WorkFilter{
		Unassigned: true,
		Assignee:   &alice,
	})
	if err != nil {
		t.Fatalf("GetReadyWork with conflicting filters failed: %v", err)
	}

	// Unassigned should win, returning only unassigned issues
	for _, issue := range readyConflict {
		if issue.Assignee != "" {
			t.Errorf("Unassigned should override Assignee filter, got %q for issue %s", issue.Assignee, issue.ID)
		}
	}
}
