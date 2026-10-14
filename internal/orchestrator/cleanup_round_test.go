package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/persistence"
)

type fakeRoundStore struct {
	rows    map[int64]*persistence.CleanupRound
	nextID  int64
	latest  map[string]persistence.LatestDispatch
	saveErr error // when non-nil, SaveCleanupRound fails
}

func newFakeRoundStore() *fakeRoundStore {
	return &fakeRoundStore{
		rows:   make(map[int64]*persistence.CleanupRound),
		latest: make(map[string]persistence.LatestDispatch),
	}
}

var _ CleanupRoundStore = (*fakeRoundStore)(nil)

func (s *fakeRoundStore) CreateCleanupRound(_ context.Context, r persistence.CleanupRound) (persistence.CleanupRound, error) {
	r.ID = s.nextID
	s.nextID++
	cp := r
	s.rows[r.ID] = &cp
	return r, nil
}

func (s *fakeRoundStore) SaveCleanupRound(_ context.Context, r persistence.CleanupRound) error {
	if s.saveErr != nil {
		return s.saveErr
	}
	cp := r
	s.rows[r.ID] = &cp
	return nil
}

func (s *fakeRoundStore) GetActiveCleanupRoundByPath(_ context.Context, path string) (*persistence.CleanupRound, error) {
	for _, r := range s.rows {
		if r.WorkspacePath == path && r.Status == persistence.CleanupRoundActive {
			cp := *r
			return &cp, nil
		}
	}
	return nil, nil
}

func (s *fakeRoundStore) ListCleanupRounds(_ context.Context, activeOnly bool) ([]persistence.CleanupRound, error) {
	out := make([]persistence.CleanupRound, 0, len(s.rows))
	for _, r := range s.rows {
		if activeOnly && r.Status != persistence.CleanupRoundActive {
			continue
		}
		out = append(out, *r)
	}
	return out, nil
}

func (s *fakeRoundStore) CompleteCleanupRound(_ context.Context, id int64, reason, at string) error {
	if r, ok := s.rows[id]; ok {
		r.Status = persistence.CleanupRoundCompleted
		r.CompletedReason = reason
		r.CompletedAt = at
	}
	return nil
}

func (s *fakeRoundStore) LatestAgentRunByIdentifier(_ context.Context, identifier string) (persistence.LatestDispatch, bool, error) {
	d, ok := s.latest[identifier]
	return d, ok, nil
}

func fixedRoundTime() time.Time { return time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC) }

// PopulateCleanupRounds builds the active-only gate indexes;
// completed rows do not gate.
func TestPopulateCleanupRounds_GateIndexes(t *testing.T) {
	t.Parallel()

	state := NewState(5000, 4, 0, nil, AgentTotals{})
	rows := []persistence.CleanupRound{
		{ID: 1, Status: persistence.CleanupRoundActive, IssueID: "ISS-1", WorkspaceKey: "PROJ-1", WorkspacePath: "/ws/PROJ-1"},
		{ID: 2, Status: persistence.CleanupRoundActive, WorkspaceKey: "PROJ-2", WorkspacePath: "/ws/PROJ-2"}, // ownerless
		{ID: 3, Status: persistence.CleanupRoundCompleted, IssueID: "ISS-3", WorkspaceKey: "PROJ-3", WorkspacePath: "/ws/PROJ-3"},
	}
	PopulateCleanupRounds(state, rows)

	if !cleanupRoundBlocksIssue(state, "ISS-1", "other") {
		t.Error("active round by issue id must block")
	}
	if !cleanupRoundBlocksIssue(state, "", "PROJ-1") {
		t.Error("active round by sanitized key must block")
	}
	// Keys are case-sensitive directory names: a different spelling is a
	// different directory and must not match.
	if cleanupRoundBlocksIssue(state, "", "proj-1") {
		t.Error("different-case key must not match the index")
	}
	if !cleanupRoundBlocksIssue(state, "", "PROJ-2") {
		t.Error("ownerless active round must block by key")
	}
	if cleanupRoundBlocksIssue(state, "ISS-3", "PROJ-3") {
		t.Error("completed round must not block")
	}
	if cleanupRoundBlocksIssue(state, "ISS-X", "PROJ-X") {
		t.Error("unrelated identity must not block")
	}
	if _, ok := state.cleanupRoundKeyIdx["PROJ-1"]; !ok {
		t.Error("key index misses PROJ-1")
	}
}

// repeated Ensure creates one active row; after completion a new
// episode gets a new row.
func TestEnsureCleanupRound_IdempotentAndNewEpisode(t *testing.T) {
	t.Parallel()

	store := newFakeRoundStore()
	state := NewState(5000, 4, 0, nil, AgentTotals{})
	params := EnsureCleanupRoundParams{
		Path:           "/ws/PROJ-1",
		IssueID:        "ISS-1",
		Identifier:     "PROJ-1",
		TerminalState:  "done",
		TerminalSource: persistence.CleanupSourceSweep,
		HookScript:     "echo bye",
		HookTimeoutMS:  60000,
		NowFunc:        fixedRoundTime,
	}

	first, created, err := EnsureCleanupRound(context.Background(), store, state, params)
	if err != nil || !created {
		t.Fatalf("first ensure: created=%v err=%v", created, err)
	}
	second, created, err := EnsureCleanupRound(context.Background(), store, state, params)
	if err != nil || created || second.ID != first.ID {
		t.Fatalf("second ensure: created=%v id=%d want %d err=%v", created, second.ID, first.ID, err)
	}
	if len(store.rows) != 1 {
		t.Fatalf("store rows = %d, want 1", len(store.rows))
	}

	if err := store.CompleteCleanupRound(context.Background(), first.ID, persistence.CleanupCompletedRemoved, "2026-10-14T11:00:00Z"); err != nil {
		t.Fatal(err)
	}
	// State gate indexes are rebuilt only on populate/engine completion;
	// emulate the post-completion state by repopulating.
	rows, _ := store.ListCleanupRounds(context.Background(), false)
	PopulateCleanupRounds(state, rows)

	third, created, err := EnsureCleanupRound(context.Background(), store, state, params)
	if err != nil || !created || third.ID == first.ID {
		t.Fatalf("new episode: created=%v id=%d err=%v", created, third.ID, err)
	}
}

// a sweep-style round pins observed facts, derives the root from
// the path, and marks hook skipped + gate confirmed at creation.
func TestEnsureCleanupRound_PinnedFacts(t *testing.T) {
	t.Parallel()

	store := newFakeRoundStore()
	state := NewState(5000, 4, 0, nil, AgentTotals{})
	r, created, err := EnsureCleanupRound(context.Background(), store, state, EnsureCleanupRoundParams{
		Path:            "/srv/ws/PROJ-7",
		Identifier:      "PROJ-7",
		TerminalState:   "closed",
		TerminalSource:  persistence.CleanupSourceStartup,
		AlreadyQuiesced: true,
		NowFunc:         fixedRoundTime,
	})
	if err != nil || !created {
		t.Fatalf("ensure: created=%v err=%v", created, err)
	}
	if r.WorkspaceRoot != "/srv/ws" || r.WorkspaceKey != "PROJ-7" {
		t.Errorf("root/key = %q/%q", r.WorkspaceRoot, r.WorkspaceKey)
	}
	if r.GateConfirmedAt == "" || r.QuiescedAt == "" {
		t.Error("gate and quiesce timestamps must be set")
	}
	if r.HookState != persistence.CleanupHookSkipped {
		t.Errorf("hook state = %q, want skipped", r.HookState)
	}
	if r.RemovalState != persistence.CleanupRemovalPending || r.Status != persistence.CleanupRoundActive {
		t.Errorf("initial stage states wrong: %+v", r)
	}
	if cleanupRoundBlocksIssue(state, "", "PROJ-7") != true {
		t.Error("created round must gate its key immediately")
	}
}

// disk observations backfill dispatch facts from run_history;
// absence of history yields a valid ownerless round.
func TestEnsureCleanupRoundFromDisk_DispatchBackfill(t *testing.T) {
	t.Parallel()

	t.Run("backfilled", func(t *testing.T) {
		t.Parallel()
		store := newFakeRoundStore()
		store.latest["PROJ-9"] = persistence.LatestDispatch{
			RunID: 77, IssueID: "ISS-9", DisplayID: "org/PROJ-9", Attempt: 2,
			Workspace: "/ws/PROJ-9", DispatchedAt: "2026-10-13T00:00:00Z",
			AgentKind: "claude", IssueTitle: "nine", IssueURL: "https://t/9",
		}
		state := NewState(5000, 4, 0, nil, AgentTotals{})
		r, created, err := EnsureCleanupRoundFromDisk(context.Background(), store, state, DiskObservationParams{
			Path:           "/ws/PROJ-9",
			Identifier:     "PROJ-9",
			TerminalState:  "done",
			TerminalSource: persistence.CleanupSourceSweep,
			HookScript:     "rm -rf /tmp/x",
			HookTimeoutMS:  30000,
			NowFunc:        fixedRoundTime,
		})
		if err != nil || !created {
			t.Fatalf("created=%v err=%v", created, err)
		}
		if r.IssueID != "ISS-9" || r.LastRunID != 77 || r.LastAttempt != 2 ||
			r.LastAgentKind != "claude" || r.LastDispatchedAt != "2026-10-13T00:00:00Z" ||
			r.DisplayIdentifier != "org/PROJ-9" || r.QuiescedAt == "" {
			t.Fatalf("backfill wrong: %+v", r)
		}
	})

	t.Run("ownerless without history", func(t *testing.T) {
		t.Parallel()
		store := newFakeRoundStore()
		state := NewState(5000, 4, 0, nil, AgentTotals{})
		r, created, err := EnsureCleanupRoundFromDisk(context.Background(), store, state, DiskObservationParams{
			Path:           "/ws/GHOST",
			Identifier:     "GHOST",
			TerminalState:  "done",
			TerminalSource: persistence.CleanupSourceStartup,
			NowFunc:        fixedRoundTime,
		})
		if err != nil || !created {
			t.Fatalf("created=%v err=%v", created, err)
		}
		if r.IssueID != "" || r.LastRunID != 0 || r.LastAttempt != 0 {
			t.Fatalf("ownerless round must carry no dispatch facts: %+v", r)
		}
	})
}

// the exit-source builder takes the worker's actual path and the
// running entry's dispatch identity, and refuses when no path is known.
func TestBuildExitRoundParams(t *testing.T) {
	t.Parallel()
	now := fixedRoundTime()
	firstAttempt := 0
	entry := &RunningEntry{
		Identifier:    "PROJ-4",
		Issue:         domain.Issue{ID: "ISS-4", DisplayID: "org/PROJ-4"},
		DispatchID:    "dispatch-4",
		SSHHost:       "builder.example",
		AgentKind:     "codex",
		StartedAt:     now.Add(-time.Minute),
		RetryAttempt:  &firstAttempt,
		WorkspacePath: "/old-root/PROJ-4",
	}

	t.Run("pins actual worker path and entry identity", func(t *testing.T) {
		t.Parallel()
		result := WorkerResult{
			IssueID:       "ISS-4",
			Identifier:    "PROJ-4",
			WorkspacePath: "/new-root/PROJ-4", // worker result is authoritative
			SSHHost:       "builder.example",
			AgentAdapter:  "codex",
		}
		params, ok := BuildExitRoundParams(entry, result, "done", persistence.CleanupSourceWorkerExit, "hook", 5000, 99, now)
		if !ok {
			t.Fatal("expected params")
		}
		if params.Path != "/new-root/PROJ-4" {
			t.Errorf("path = %q, want worker path", params.Path)
		}
		if params.LastDispatchID != "dispatch-4" || params.SSHHost != "builder.example" ||
			params.LastAgentKind != "codex" || params.LastRunID != 99 || params.LastAttempt != 1 ||
			params.DisplayID != "org/PROJ-4" || params.TerminalState != "done" ||
			params.HookScript != "hook" || !params.AlreadyQuiesced {
			t.Errorf("pinned facts wrong: %+v", params)
		}
	})

	t.Run("entry path fallback", func(t *testing.T) {
		t.Parallel()
		result := WorkerResult{IssueID: "ISS-4", Identifier: "PROJ-4"}
		params, ok := BuildExitRoundParams(entry, result, "done", persistence.CleanupSourceReconcile, "", 0, 0, now)
		if !ok || params.Path != "/old-root/PROJ-4" {
			t.Fatalf("ok=%v path=%q, want entry path", ok, params.Path)
		}
		if params.LastDispatchID != "dispatch-4" {
			t.Errorf("dispatch id = %q", params.LastDispatchID)
		}
	})

	t.Run("no pin when path and identifier are unknown", func(t *testing.T) {
		t.Parallel()
		_, ok := BuildExitRoundParams(&RunningEntry{}, WorkerResult{}, "done", persistence.CleanupSourceWorkerExit, "", 0, 0, now)
		if ok {
			t.Error("must refuse without a pinnable path")
		}
	})
}

// the poll-lane gate rejects a candidate whose issue or key
// is held by an active round, and the independent oracle agrees.
func TestShouldDispatch_CleanupRoundGate(t *testing.T) {
	t.Parallel()

	active := []string{"To Do"}
	terminal := []string{"Done"}
	issue := domain.Issue{ID: "ISS-1", Identifier: "PROJ-1", Title: "t", State: "To Do"}

	state := NewState(5000, 4, 0, nil, AgentTotals{})
	PopulateCleanupRounds(state, []persistence.CleanupRound{
		{ID: 1, Status: persistence.CleanupRoundActive, IssueID: "ISS-1", WorkspaceKey: "PROJ-1", WorkspacePath: "/ws/PROJ-1"},
	})
	if ShouldDispatch(issue, state, active, terminal) {
		t.Error("oracle must reject issue held by active round")
	}
	if ShouldDispatchWithSets(issue, state, stateSet(active), stateSet(terminal)) {
		t.Error("production gate must reject issue held by active round")
	}

	// A candidate colliding only on the exact sanitized key is also gated
	// (ownerless rounds carry no issue id). A different-case identifier is
	// a different directory and stays dispatchable.
	keyOnly := domain.Issue{ID: "ISS-OTHER", Identifier: "PROJ-1", Title: "t", State: "To Do"}
	if ShouldDispatchWithSets(keyOnly, state, stateSet(active), stateSet(terminal)) {
		t.Error("gate must match by workspace key")
	}

	rows := []persistence.CleanupRound{
		{ID: 1, Status: persistence.CleanupRoundCompleted, IssueID: "ISS-1", WorkspaceKey: "PROJ-1", WorkspacePath: "/ws/PROJ-1"},
	}
	PopulateCleanupRounds(state, rows)
	if !ShouldDispatch(issue, state, active, terminal) {
		t.Error("completed round must release the gate")
	}
	if !ShouldDispatchWithSets(issue, state, stateSet(active), stateSet(terminal)) {
		t.Error("oracle/production verdicts diverge after completion")
	}
}

// the retry lane releases the claim and deletes the persisted
// retry row when an active round owns the issue, without fetching the
// tracker.
func TestHandleRetryTimer_CleanupRoundGate(t *testing.T) {
	t.Parallel()

	const issueID = "ISS-CLN"
	store := &mockRetryStore{}
	tracker := &mockRetryTracker{}
	state := retryState(t, issueID, "PROJ-CLN", 1)
	PopulateCleanupRounds(state, []persistence.CleanupRound{
		{ID: 5, Status: persistence.CleanupRoundActive, IssueID: issueID, WorkspaceKey: "PROJ-CLN", WorkspacePath: "/ws/PROJ-CLN"},
	})

	HandleRetryTimer(state, issueID, defaultRetryParams(t, store, tracker))

	if _, claimed := state.Claimed[issueID]; claimed {
		t.Error("claim must be released for cleanup-gated retry")
	}
	if _, ok := state.RetryAttempts[issueID]; ok {
		t.Error("retry entry must be removed for cleanup-gated retry")
	}
	found := false
	for _, deleted := range store.deletedIssueID {
		if deleted == issueID {
			found = true
		}
	}
	if !found {
		t.Error("persisted retry row must be deleted")
	}
	if tracker.fetchCount != 0 {
		t.Error("cleanup gate must precede any tracker fetch")
	}
}
