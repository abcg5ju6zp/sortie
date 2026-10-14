package orchestrator

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/persistence"
)

// active cleanup rounds exclude their keys from both
// sweep gates, terminal keys are adopted as rounds (not removed inline),
// and the summary partitions every candidate exactly once.
func TestSweepWorkspaces_CleanupRoundExclusionAndAdoption(t *testing.T) {
	t.Parallel()

	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	roundKey := "PROJ-ROUND"
	termKey := "PROJ-TERM"
	activeKey := "PROJ-ACT"
	for _, key := range []string{roundKey, termKey, activeKey} {
		mustMkdirSweep(t, filepath.Join(root, key))
	}

	tracker := &sweepTracker{statesByKey: map[string]string{
		termKey:   "Done",
		activeKey: "In Progress",
		// ROUND deliberately absent from the tracker response: it must
		// never be queried at all.
	}}
	state := NewState(5000, 4, 0, nil, AgentTotals{})
	rounds := newFakeRoundStore()

	// A pre-existing active round (e.g. created by reconcile, waiting out
	// a backoff) pins ROUND.
	existing, err := rounds.CreateCleanupRound(context.Background(), persistence.CleanupRound{
		Status:          persistence.CleanupRoundActive,
		IssueID:         "ISS-ROUND",
		Identifier:      roundKey,
		WorkspaceKey:    roundKey,
		WorkspacePath:   filepath.Join(root, roundKey),
		WorkspaceRoot:   root,
		TerminalState:   "Done",
		TerminalSource:  persistence.CleanupSourceReconcile,
		HookState:       persistence.CleanupHookSkipped,
		RemovalState:    persistence.CleanupRemovalPending,
		NextDueAtMS:     time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli(), // not due this pass
		GateConfirmedAt: "2026-10-14T11:00:00Z",
		QuiescedAt:      "2026-10-14T11:00:00Z",
		CreatedAt:       "2026-10-14T11:00:00Z",
	})
	if err != nil {
		t.Fatal(err)
	}
	PopulateCleanupRounds(state, []persistence.CleanupRound{existing})

	handler := &sweepLogHandler{}
	params := defaultSweepParamsWithRounds(t, root, tracker, rounds)
	params.Logger = slog.New(handler)

	// Sweep alone: adoption only, no tree removal (the engine owns that).
	SweepWorkspaces(state, params)

	// ROUND was never offered to the tracker or the terminal gate.
	for _, queried := range tracker.calledWith {
		if queried == roundKey {
			t.Errorf("active-round key %q must not be queried", roundKey)
		}
	}
	assertSweepDirExists(t, filepath.Join(root, roundKey))
	assertSweepDirExists(t, filepath.Join(root, termKey)) // not removed inline

	// TERM got exactly one adopted round; ROUND's row is untouched; no
	// round for the active key.
	activeRows, _ := rounds.ListCleanupRounds(context.Background(), true)
	var termRows int
	for _, r := range activeRows {
		switch r.WorkspaceKey {
		case termKey:
			termRows++
			if r.QuiescedAt == "" || r.TerminalSource != persistence.CleanupSourceSweep {
				t.Errorf("adopted round wrong: %+v", r)
			}
		case roundKey:
			if r.ID != existing.ID {
				t.Errorf("second round created for excluded key")
			}
		case activeKey:
			t.Errorf("active issue must not get a round")
		}
	}
	if termRows != 1 {
		t.Fatalf("terminal rounds = %d, want 1", termRows)
	}

	rec, ok := handler.findByMessage("sweep: pass complete")
	if !ok {
		t.Fatal("sweep summary missing")
	}
	if got := intAttr(t, rec, "candidates"); got != 3 {
		t.Errorf("candidates = %d, want 3", got)
	}
	if got := intAttr(t, rec, "excluded_cleanup_round"); got != 1 {
		t.Errorf("excluded_cleanup_round = %d, want 1", got)
	}
	if got := intAttr(t, rec, "removed_terminal"); got != 1 {
		t.Errorf("removed_terminal = %d, want 1 (adopted)", got)
	}
	if got := intAttr(t, rec, "retained_not_evaluated"); got != 1 {
		t.Errorf("retained_not_evaluated = %d, want 1", got)
	}

	// The engine then removes only the adopted terminal workspace; the
	// not-due ROUND and the active issue's directory survive.
	runSweepCleanupEngine(t, state, rounds, nil)
	assertSweepDirRemoved(t, filepath.Join(root, termKey))
	assertSweepDirExists(t, filepath.Join(root, roundKey))
	assertSweepDirExists(t, filepath.Join(root, activeKey))
}
