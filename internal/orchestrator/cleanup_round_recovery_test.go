package orchestrator

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/persistence"
)

var errSweepTrackerUnavailable = errors.New("tracker unavailable")

func recoverParams(t *testing.T, root string, store CleanupRoundStore, tracker domain.TrackerAdapter) RecoverCleanupRoundsParams {
	t.Helper()
	return RecoverCleanupRoundsParams{
		Store:          store,
		State:          NewState(5000, 4, 0, nil, AgentTotals{}),
		WorkspaceRoot:  root,
		TrackerAdapter: tracker,
		TerminalStates: []string{"Done"},
		PollIntervalMS: 100,
		MaxBackoffMS:   5000,
		Ctx:            context.Background(),
		Logger:         discardLogger(),
		Metrics:        &domain.NoopMetrics{},
	}
}

func insertRecoveryRound(t *testing.T, store CleanupRoundStore, r persistence.CleanupRound) persistence.CleanupRound {
	t.Helper()
	created, err := store.CreateCleanupRound(context.Background(), r)
	if err != nil {
		t.Fatalf("create round: %v", err)
	}
	return created
}

func baseRecoveryRound(id, root, key string) persistence.CleanupRound {
	return persistence.CleanupRound{
		Status:          persistence.CleanupRoundActive,
		IssueID:         id,
		Identifier:      key,
		WorkspaceKey:    key,
		WorkspacePath:   filepath.Join(root, key),
		WorkspaceRoot:   root,
		TerminalState:   "Done",
		TerminalSource:  persistence.CleanupSourceReconcile,
		HookState:       persistence.CleanupHookSkipped,
		RemovalState:    persistence.CleanupRemovalPending,
		GateConfirmedAt: "2026-10-14T11:00:00Z",
		QuiescedAt:      "2026-10-14T11:00:00Z",
		CreatedAt:       "2026-10-14T11:00:00Z",
	}
}

// an active round whose path is already absent completes
// already_gone without recreating the directory, and releases its gate.
func TestRecoverCleanupRounds_ActiveRoundAlreadyGone(t *testing.T) {
	t.Parallel()

	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	store := newFakeRoundStore()
	insertRecoveryRound(t, store, baseRecoveryRound("ISS-G", root, "PROJ-G"))
	tracker := &sweepTracker{statesByKey: map[string]string{}}

	params := recoverParams(t, root, store, tracker)
	RecoverCleanupRounds(params)

	rows, _ := store.ListCleanupRounds(context.Background(), false)
	if len(rows) != 1 || rows[0].Status != persistence.CleanupRoundCompleted ||
		rows[0].CompletedReason != persistence.CleanupCompletedAlreadyGone {
		t.Fatalf("round = %+v, want completed/already_gone", rows)
	}
	if cleanupRoundBlocksIssue(params.State, "ISS-G", "PROJ-G") {
		t.Error("gate must clear on already-gone completion")
	}
	if _, statErr := os.Stat(filepath.Join(root, "PROJ-G")); !os.IsNotExist(statErr) {
		t.Error("recovery must not recreate the absent workspace")
	}
}

// an existing active round on a
// present directory finishes to removed.
func TestRecoverCleanupRounds_ActiveRoundOnDirectoryCompletes(t *testing.T) {
	t.Parallel()

	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ws := filepath.Join(root, "PROJ-E")
	if err := os.Mkdir(ws, 0o750); err != nil {
		t.Fatal(err)
	}
	store := newFakeRoundStore()
	insertRecoveryRound(t, store, baseRecoveryRound("ISS-E", root, "PROJ-E"))
	tracker := &sweepTracker{statesByKey: map[string]string{}}

	params := recoverParams(t, root, store, tracker)
	RecoverCleanupRounds(params)

	if _, statErr := os.Stat(ws); !os.IsNotExist(statErr) {
		t.Fatal("present workspace should be removed by the startup engine pass")
	}
}

// a symlink at the pinned path is parked unsafe; the
// link target stays untouched.
func TestRecoverCleanupRounds_ActiveRoundUnsafePath(t *testing.T) {
	t.Parallel()

	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "real-target")
	if err := os.Mkdir(target, 0o750); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "PROJ-U")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	store := newFakeRoundStore()
	insertRecoveryRound(t, store, baseRecoveryRound("ISS-U", root, "PROJ-U"))
	tracker := &sweepTracker{}

	params := recoverParams(t, root, store, tracker)
	RecoverCleanupRounds(params)

	if _, statErr := os.Stat(target); statErr != nil {
		t.Errorf("symlink target must be untouched: %v", statErr)
	}
	active, _ := store.GetActiveCleanupRoundByPath(context.Background(), link)
	if active == nil || active.RemovalState != persistence.CleanupRemovalUnsafe {
		t.Fatalf("round = %+v, want active/unsafe", active)
	}
}

// a terminal directory with no round is adopted and
// cleaned; an active directory and an in-flight directory are not.
func TestRecoverCleanupRounds_AdoptsTerminalUnclaimed(t *testing.T) {
	t.Parallel()

	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	termWS := filepath.Join(root, "PROJ-T")
	activeWS := filepath.Join(root, "PROJ-A")
	claimedWS := filepath.Join(root, "PROJ-R")
	for _, p := range []string{termWS, activeWS, claimedWS} {
		if err := os.Mkdir(p, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	store := newFakeRoundStore()
	tracker := &sweepTracker{statesByKey: map[string]string{
		"PROJ-T": "Done",
		"PROJ-A": "In Progress",
		"PROJ-R": "Done",
	}}
	params := recoverParams(t, root, store, tracker)
	// PROJ-R is in flight (retry queued): must not be adopted.
	params.State.RetryAttempts["ISS-R"] = &RetryEntry{IssueID: "ISS-R", Identifier: "PROJ-R"}

	RecoverCleanupRounds(params)

	if _, statErr := os.Stat(termWS); !os.IsNotExist(statErr) {
		t.Error("terminal unclaimed workspace must be cleaned")
	}
	if _, statErr := os.Stat(activeWS); statErr != nil {
		t.Error("active workspace must be retained")
	}
	if _, statErr := os.Stat(claimedWS); statErr != nil {
		t.Error("in-flight workspace must be retained")
	}
	rounds, _ := store.ListCleanupRounds(context.Background(), false)
	var adoptedTerm bool
	for _, r := range rounds {
		if r.WorkspaceKey == "PROJ-T" && r.TerminalSource == persistence.CleanupSourceStartup {
			adoptedTerm = true
		}
		if r.WorkspaceKey == "PROJ-A" || r.WorkspaceKey == "PROJ-R" {
			t.Errorf("round unexpectedly created for %q", r.WorkspaceKey)
		}
	}
	if !adoptedTerm {
		t.Error("startup adoption round for PROJ-T missing")
	}
}

// a tracker read failure suppresses adoption; the directory
// remains for the sweep, while active rounds still reconcile to disk.
func TestRecoverCleanupRounds_TrackerFetchFailureSuppressesAdoption(t *testing.T) {
	t.Parallel()

	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ws := filepath.Join(root, "PROJ-F")
	if err := os.Mkdir(ws, 0o750); err != nil {
		t.Fatal(err)
	}
	store := newFakeRoundStore()
	tracker := &sweepTracker{fetchErr: errSweepTrackerUnavailable}
	params := recoverParams(t, root, store, tracker)

	RecoverCleanupRounds(params)

	if _, statErr := os.Stat(ws); statErr != nil {
		t.Error("workspace must be retained when the tracker read fails")
	}
	if len(store.rows) != 0 {
		t.Errorf("no rounds expected, got %d", len(store.rows))
	}
}

// a directory that reappears after a completed round is a new
// episode: the completed fact does not suppress adoption, and a second
// terminal cleanup runs on a new row.
func TestRecoverCleanupRounds_ReappearedPathIsNewEpisode(t *testing.T) {
	t.Parallel()

	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	key := "PROJ-NEW"
	ws := filepath.Join(root, key)
	if err := os.Mkdir(ws, 0o750); err != nil {
		t.Fatal(err)
	}
	store := newFakeRoundStore()
	completed := baseRecoveryRound("ISS-NEW", root, key)
	completed.Status = persistence.CleanupRoundCompleted
	completed.CompletedReason = persistence.CleanupCompletedRemoved
	completed.CompletedAt = "2026-10-10T00:00:00Z"
	completed.NextDueAtMS = 0
	first := insertRecoveryRound(t, store, completed)
	if err := store.CompleteCleanupRound(context.Background(), first.ID, persistence.CleanupCompletedRemoved, "2026-10-10T00:00:00Z"); err != nil {
		t.Fatal(err)
	}
	tracker := &sweepTracker{statesByKey: map[string]string{key: "Done"}}
	params := recoverParams(t, root, store, tracker)

	RecoverCleanupRounds(params)

	rows, _ := store.ListCleanupRounds(context.Background(), false)
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want old completed + new episode", len(rows))
	}
	var newEpisode *persistence.CleanupRound
	for i := range rows {
		if rows[i].ID != first.ID && rows[i].WorkspaceKey == key {
			newEpisode = &rows[i]
		}
	}
	if newEpisode == nil {
		t.Fatal("new episode round not created")
	}
	// The startup engine pass completes the new episode immediately; the
	// point is that it is a separate row rather than the old completed
	// fact suppressing the re-terminalized directory.
	if newEpisode.Status != persistence.CleanupRoundCompleted || newEpisode.CompletedReason != persistence.CleanupCompletedRemoved {
		t.Errorf("new episode = %+v, want completed/removed", newEpisode)
	}
	if _, statErr := os.Stat(ws); !os.IsNotExist(statErr) {
		t.Error("new episode workspace must be cleaned")
	}
}
