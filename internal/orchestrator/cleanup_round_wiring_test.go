package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/persistence"
)

// a terminal reconcile observation of a running issue pins a
// round with the dispatch-time path and the config snapshot, leaves it
// waiting for the worker exit, and keeps the existing cancel/release
// behavior.
func TestReconcileTerminal_PinsRunningRoundBeforeRelease(t *testing.T) {
	t.Parallel()

	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wsPath := filepath.Join(root, "ISSUE-1-ident")
	if err := os.Mkdir(wsPath, 0o750); err != nil {
		t.Fatal(err)
	}

	rounds := newFakeRoundStore()
	store := &mockReconcileStore{}
	tracker := &mockReconcileTracker{states: map[string]string{"ISSUE-1": "Done"}}
	params := defaultReconcileParams(t, store, tracker)
	params.StallTimeoutMS = 0
	params.WorkspaceRoot = root
	params.BeforeRemoveHook = "echo pinned"
	params.HookTimeoutMS = 45000
	params.CleanupRounds = rounds

	state := NewState(5000, 4, 0, nil, AgentTotals{})
	cc := &cancelCounter{}
	state.Running["ISSUE-1"] = &RunningEntry{
		Identifier:    "ISSUE-1-ident",
		StartedAt:     reconcileBaseTime,
		CancelFunc:    cc.cancel,
		WorkspacePath: wsPath,
		Issue:         domain.Issue{ID: "ISSUE-1", State: "In Progress"},
	}
	state.Claimed["ISSUE-1"] = struct{}{}
	state.RetryAttempts["ISSUE-1"] = &RetryEntry{IssueID: "ISSUE-1", Identifier: "ISSUE-1-ident", Attempt: 1}

	ReconcileRunningIssues(state, params)

	if cc.count != 1 {
		t.Errorf("cancel count = %d, want 1", cc.count)
	}
	if !state.Running["ISSUE-1"].PendingCleanup {
		t.Error("PendingCleanup must still be set")
	}
	active, _ := rounds.GetActiveCleanupRoundByPath(context.Background(), wsPath)
	if active == nil {
		t.Fatal("expected a pinned active round")
	}
	if active.QuiescedAt != "" {
		t.Error("running-issue round must wait for the worker exit to quiesce")
	}
	if active.HookScript != "echo pinned" || active.HookTimeoutMS != 45000 {
		t.Errorf("hook snapshot = %q/%d", active.HookScript, active.HookTimeoutMS)
	}
	if active.TerminalState != "Done" || active.TerminalSource != persistence.CleanupSourceReconcile {
		t.Errorf("terminal observation = %q/%q", active.TerminalState, active.TerminalSource)
	}
	if active.IssueID != "ISSUE-1" || active.LastDispatchID != "" {
		t.Errorf("identity/dispatch pin wrong: %+v", active)
	}
	if !cleanupRoundBlocksIssue(state, "ISSUE-1", "ISSUE-1-ident") {
		t.Error("gate must be installed before release")
	}
}

// a terminal observation with no running entry (pending reaction
// holder) pins an already-quiesced round.
func TestReconcileTerminal_NonRunningRoundIsQuiesced(t *testing.T) {
	t.Parallel()

	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	wsPath := filepath.Join(root, "ISSUE-2-ident")
	if err := os.Mkdir(wsPath, 0o750); err != nil {
		t.Fatal(err)
	}

	rounds := newFakeRoundStore()
	store := &mockReconcileStore{}
	tracker := &mockReconcileTracker{states: map[string]string{"ISSUE-2": "Done"}}
	params := defaultReconcileParams(t, store, tracker)
	params.StallTimeoutMS = 0
	params.WorkspaceRoot = root
	params.CleanupRounds = rounds

	state := NewState(5000, 4, 0, nil, AgentTotals{})
	state.PendingReactions["ISSUE-2:review"] = &PendingReaction{
		IssueID:    "ISSUE-2",
		Identifier: "ISSUE-2-ident",
		Kind:       ReactionKindReview,
	}

	ReconcileRunningIssues(state, params)

	active, _ := rounds.GetActiveCleanupRoundByPath(context.Background(), wsPath)
	if active == nil {
		t.Fatal("expected round for non-running terminal issue")
	}
	if active.QuiescedAt == "" {
		t.Error("non-running round must be quiesced at creation")
	}
	if active.IssueID != "ISSUE-2" {
		t.Errorf("issue id = %q", active.IssueID)
	}
}

// when reconcile already pinned the round, worker exit only
// confirms quiescence; it never creates a second round.
func TestHandleWorkerExit_ReconcileRoundQuiescedNotDuplicated(t *testing.T) {
	t.Parallel()

	wsParent := t.TempDir()
	wsPath := filepath.Join(wsParent, "T-1-ident")
	if err := os.Mkdir(wsPath, 0o750); err != nil {
		t.Fatal(err)
	}

	rounds := newFakeRoundStore()
	seed := persistence.CleanupRound{
		Status:          persistence.CleanupRoundActive,
		IssueID:         "T-1",
		Identifier:      "T-1-ident",
		WorkspaceKey:    "T-1-ident",
		WorkspacePath:   wsPath,
		WorkspaceRoot:   wsParent,
		TerminalState:   "ai:cancelled",
		TerminalSource:  persistence.CleanupSourceReconcile,
		HookState:       persistence.CleanupHookPending,
		RemovalState:    persistence.CleanupRemovalPending,
		HookScript:      "echo bye",
		HookTimeoutMS:   60000,
		GateConfirmedAt: "2026-10-14T11:00:00Z",
	}
	created, err := rounds.CreateCleanupRound(context.Background(), seed)
	if err != nil {
		t.Fatal(err)
	}

	state := exitStateWithIssue(t, "T-1", "ai:in-progress")
	state.Running["T-1"].WorkspacePath = wsPath
	state.Running["T-1"].PendingCleanup = true
	state.Running["T-1"].ObservedTerminalState = "ai:cancelled"
	PopulateCleanupRounds(state, []persistence.CleanupRound{created})

	store := &mockExitStore{}
	params := defaultExitParams(t, store)
	params.CleanupRounds = rounds
	params.TerminalStates = []string{"ai:done", "ai:cancelled"}

	HandleWorkerExit(state, WorkerResult{
		IssueID:       "T-1",
		Identifier:    "T-1-ident",
		ExitKind:      WorkerExitCancelled,
		WorkspacePath: wsPath,
	}, params)

	stored := rounds.rows[created.ID]
	if stored.QuiescedAt == "" {
		t.Fatal("reconcile round must be quiesced at exit")
	}
	if len(rounds.rows) != 1 {
		t.Fatalf("rounds = %d, want exactly one", len(rounds.rows))
	}
}

// a terminal state seen only at the worker exit creates the
// round there, pinned to the worker's actual path, quiesced, and the
// engine completes it on the same pass without any re-dispatch.
func TestHandleWorkerExit_TerminalObservationCreatesAndCompletesRound(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	wsPath := filepath.Join(parent, "T-3-ident")
	if err := os.Mkdir(wsPath, 0o750); err != nil {
		t.Fatal(err)
	}
	writeSCMMetadata(t, wsPath, "feature/T-3", "sha3")

	rounds := newFakeRoundStore()
	store := &mockExitStore{}
	tracker := &mockTrackerAdapter{}
	state := exitStateWithIssue(t, "T-3", "ai:in-progress")
	state.Running["T-3"].WorkspacePath = wsPath
	params := defaultExitParams(t, store)
	params.CleanupRounds = rounds
	params.TrackerAdapter = tracker
	params.ActiveStates = []string{"ai:ready", "ai:in-progress"}
	params.TerminalStates = []string{"ai:done", "ai:cancelled"}
	params.HandoffState = "ai:in-review"
	params.CIProvider = &ciProviderStubExit{}

	HandleWorkerExit(state, WorkerResult{
		IssueID:            "T-3",
		Identifier:         "T-3-ident",
		ExitKind:           WorkerExitNormal,
		AgentAdapter:       "mock",
		WorkspacePath:      wsPath,
		ObservedIssueState: "ai:cancelled",
	}, params)

	// The exit handler only quiesces; the engine (same event-loop wake in
	// production) performs hook + removal. Run it explicitly here.
	res := ProcessCleanupRounds(context.Background(), state, ProcessCleanupRoundsParams{
		Store:          rounds,
		Ctx:            context.Background(),
		Logger:         discardLogger(),
		PollIntervalMS: 100,
		MaxBackoffMS:   5000,
	})
	if res.CompletedRemoved != 1 {
		t.Fatalf("engine result = %+v, want one removed", res)
	}

	if got := activeCleanupRoundForIssue(state, "T-3"); got != nil {
		t.Fatal("round should be completed, but an active round remains")
	}
	var completed persistence.CleanupRound
	found := false
	for _, r := range rounds.rows {
		if r.Status == persistence.CleanupRoundCompleted {
			completed = *r
			found = true
		}
	}
	if !found {
		t.Fatal("expected a completed round")
	}
	if completed.TerminalSource != persistence.CleanupSourceWorkerExit ||
		completed.TerminalState != "ai:cancelled" ||
		completed.CompletedReason != persistence.CleanupCompletedRemoved {
		t.Errorf("round wrong: %+v", completed)
	}
	if completed.HookState != persistence.CleanupHookSkipped {
		t.Errorf("no-hook round should be skipped, got %q", completed.HookState)
	}
	if _, statErr := os.Stat(wsPath); !os.IsNotExist(statErr) {
		t.Fatal("workspace tree should have been removed")
	}
}

// a non-terminal exit never touches the cleanup store.
func TestHandleWorkerExit_NonTerminalCreatesNoRound(t *testing.T) {
	t.Parallel()

	wsPath := t.TempDir()
	writeSCMMetadata(t, wsPath, "feature/T-4", "sha4")

	rounds := newFakeRoundStore()
	store := &mockExitStore{}
	tracker := &mockTrackerAdapter{}
	state := exitStateWithIssue(t, "T-4", "In Progress")
	params := defaultExitParams(t, store)
	params.CleanupRounds = rounds
	params.TrackerAdapter = tracker
	params.ActiveStates = []string{"In Progress"}
	params.TerminalStates = []string{"Done"}
	params.HandoffState = "Human Review"

	HandleWorkerExit(state, WorkerResult{
		IssueID:       "T-4",
		Identifier:    "T-4-ident",
		ExitKind:      WorkerExitNormal,
		AgentAdapter:  "mock",
		WorkspacePath: wsPath,
	}, params)

	if len(rounds.rows) != 0 {
		t.Fatalf("non-terminal exit created %d rounds", len(rounds.rows))
	}
}

// The dispatch-time path pin survives a later root reload so a
// reconcile round targets the directory the dispatch actually used.
func TestPinDispatchWorkspacePath(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	state := NewState(5000, 4, 0, nil, AgentTotals{})
	state.Running["ISS-9"] = &RunningEntry{Identifier: "ISS-9"}
	state.Claimed["ISS-9"] = struct{}{}

	PinDispatchWorkspacePath(state, "ISS-9", root, "ISS-9")
	want := filepath.Join(resolvedRoot, "ISS-9")
	if state.Running["ISS-9"].WorkspacePath != want {
		t.Fatalf("pinned path = %q, want %q", state.Running["ISS-9"].WorkspacePath, want)
	}

	// A later call with a changed root must not rewrite the pin.
	otherRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	PinDispatchWorkspacePath(state, "ISS-9", otherRoot, "ISS-9")
	if state.Running["ISS-9"].WorkspacePath != want {
		t.Fatalf("pin was rewritten by later root: %q", state.Running["ISS-9"].WorkspacePath)
	}

	// Unknown issue / bad root are no-ops.
	PinDispatchWorkspacePath(state, "MISSING", root, "X")
	PinDispatchWorkspacePath(state, "ISS-9", "", "ISS-9")
	if state.Running["ISS-9"].WorkspacePath != want {
		t.Fatal("no-op calls must not move the pin")
	}
}
