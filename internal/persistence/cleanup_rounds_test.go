package persistence

import (
	"context"
	"errors"
	"testing"
)

func newCleanupRoundTestStore(t *testing.T) *Store {
	t.Helper()
	ctx := context.Background()
	s, err := Open(ctx, ":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { closeStore(t, s) })
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return s
}

func sampleCleanupRound() CleanupRound {
	return CleanupRound{
		Status:             CleanupRoundActive,
		IssueID:            "issue-1",
		Identifier:         "PROJ-1",
		DisplayIdentifier:  "org/PROJ-1",
		IssueTitle:         "terminal issue",
		IssueURL:           "https://tracker/PROJ-1",
		WorkspaceKey:       "PROJ-1",
		WorkspacePath:      "/workspace/PROJ-1",
		WorkspaceRoot:      "/workspace",
		SSHHost:            "builder.example",
		LastDispatchID:     "dispatch-1",
		LastRunID:          42,
		LastAttempt:        3,
		LastDispatchedAt:   "2026-10-14T01:02:03Z",
		LastAgentKind:      "claude",
		TerminalState:      "done",
		TerminalSource:     CleanupSourceReconcile,
		TerminalObservedAt: "2026-10-14T04:05:06Z",
		HookScript:         "echo bye",
		HookTimeoutMS:      60000,
		GateConfirmedAt:    "2026-10-14T04:05:06Z",
		HookState:          CleanupHookPending,
		RemovalState:       CleanupRemovalPending,
		CreatedAt:          "2026-10-14T04:05:06Z",
		UpdatedAt:          "2026-10-14T04:05:06Z",
	}
}

// migration 028 creates the table, the status/issue indexes, and
// the partial unique index on active workspace paths.
func TestMigrate_Migration028_CleanupRoundsSchema(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := newCleanupRoundTestStore(t)

	for _, obj := range []struct {
		name string
		sql  string
	}{
		{"table", `SELECT COUNT(*) FROM workspace_cleanup_rounds`},
		{"partial unique index", `SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_cleanup_rounds_active_path'`},
		{"status index", `SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_cleanup_rounds_status'`},
		{"issue index", `SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_cleanup_rounds_issue_id'`},
	} {
		var n int
		if err := s.db.QueryRowContext(ctx, obj.sql).Scan(&n); err != nil {
			t.Fatalf("%s: %v", obj.name, err)
		}
		if obj.name != "table" && n != 1 {
			t.Errorf("%s not created", obj.name)
		}
	}
}

// only one active round per pinned path; a completed round frees
// the path for a new episode's round.
func TestCleanupRound_ActivePathUniqueness(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := newCleanupRoundTestStore(t)

	r1, err := s.CreateCleanupRound(ctx, sampleCleanupRound())
	if err != nil {
		t.Fatalf("CreateCleanupRound first: %v", err)
	}
	if r1.ID == 0 {
		t.Fatal("expected assigned round id")
	}

	if _, err := s.CreateCleanupRound(ctx, sampleCleanupRound()); err == nil {
		t.Fatal("second active round for same path must fail the unique index")
	}

	got, err := s.GetActiveCleanupRoundByPath(ctx, "/workspace/PROJ-1")
	if err != nil {
		t.Fatalf("GetActiveCleanupRoundByPath: %v", err)
	}
	if got == nil || got.ID != r1.ID {
		t.Fatalf("active round lookup = %v, want round %d", got, r1.ID)
	}

	missing, err := s.GetActiveCleanupRoundByPath(ctx, "/workspace/other")
	if err != nil {
		t.Fatalf("GetActiveCleanupRoundByPath missing: %v", err)
	}
	if missing != nil {
		t.Fatalf("expected nil for path with no round, got %+v", missing)
	}

	if err := s.CompleteCleanupRound(ctx, r1.ID, CleanupCompletedRemoved, "2026-10-14T05:00:00Z"); err != nil {
		t.Fatalf("CompleteCleanupRound: %v", err)
	}
	if got, _ := s.GetActiveCleanupRoundByPath(ctx, "/workspace/PROJ-1"); got != nil {
		t.Fatal("completed round must no longer resolve as active")
	}

	// A new episode for the recreated workspace gets its own row.
	next := sampleCleanupRound()
	next.TerminalObservedAt = "2026-10-15T05:00:00Z"
	next.CreatedAt = "2026-10-15T05:00:00Z"
	next.UpdatedAt = "2026-10-15T05:00:00Z"
	r2, err := s.CreateCleanupRound(ctx, next)
	if err != nil {
		t.Fatalf("CreateCleanupRound new episode: %v", err)
	}
	if r2.ID == r1.ID {
		t.Fatal("new episode must get a new row")
	}
}

// SaveCleanupRound advances stage fields but can never rewrite
// the pinned identity/dispatch/observation/hook snapshot columns.
func TestCleanupRound_SaveDoesNotOverwritePinnedFields(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := newCleanupRoundTestStore(t)

	r, err := s.CreateCleanupRound(ctx, sampleCleanupRound())
	if err != nil {
		t.Fatalf("CreateCleanupRound: %v", err)
	}

	// Mutate every pinned field on the in-memory struct, then save: the
	// UPDATE has no pinned columns, so the database row must be immune.
	r.WorkspacePath = "/elsewhere/PROJ-9"
	r.WorkspaceRoot = "/elsewhere"
	r.WorkspaceKey = "PROJ-9"
	r.IssueID = "issue-9"
	r.HookScript = "echo changed"
	r.HookTimeoutMS = 1
	r.TerminalState = "reopened"
	r.LastDispatchID = "dispatch-9"
	r.QuiescedAt = "2026-10-14T04:06:00Z"
	r.HookState = CleanupHookSucceeded
	r.HookFinishedAt = "2026-10-14T04:06:01Z"
	r.HookAttempts = 1
	r.HookExitCode = 0
	r.RemovalState = CleanupRemovalGone
	r.UpdatedAt = "2026-10-14T04:06:02Z"
	if err := s.SaveCleanupRound(ctx, r); err != nil {
		t.Fatalf("SaveCleanupRound: %v", err)
	}

	rows, err := s.ListCleanupRounds(ctx, false)
	if err != nil || len(rows) != 1 {
		t.Fatalf("ListCleanupRounds: rows=%d err=%v", len(rows), err)
	}
	got := rows[0]
	for _, c := range []struct{ name, got, want string }{
		{"workspace_path", got.WorkspacePath, "/workspace/PROJ-1"},
		{"workspace_root", got.WorkspaceRoot, "/workspace"},
		{"workspace_key", got.WorkspaceKey, "PROJ-1"},
		{"issue_id", got.IssueID, "issue-1"},
		{"hook_script", got.HookScript, "echo bye"},
		{"terminal_state", got.TerminalState, "done"},
		{"last_dispatch_id", got.LastDispatchID, "dispatch-1"},
	} {
		if c.got != c.want {
			t.Errorf("%s = %q, want pinned %q", c.name, c.got, c.want)
		}
	}
	if got.HookTimeoutMS != 60000 {
		t.Errorf("hook_timeout_ms = %d, want pinned 60000", got.HookTimeoutMS)
	}
	if got.QuiescedAt != "2026-10-14T04:06:00Z" || got.HookState != CleanupHookSucceeded ||
		got.RemovalState != CleanupRemovalGone {
		t.Errorf("stage fields did not advance: %+v", got)
	}
}

// ownerless rounds (empty issue id) round-trip correctly.
func TestCleanupRound_OwnerlessRound(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := newCleanupRoundTestStore(t)

	r := sampleCleanupRound()
	r.IssueID = ""
	r.IssueTitle = ""
	r.IssueURL = ""
	r.DisplayIdentifier = ""
	created, err := s.CreateCleanupRound(ctx, r)
	if err != nil {
		t.Fatalf("CreateCleanupRound ownerless: %v", err)
	}
	if created.IssueID != "" {
		t.Fatalf("ownerless round issue id = %q, want empty", created.IssueID)
	}

	got, err := s.GetActiveCleanupRoundByPath(ctx, r.WorkspacePath)
	if err != nil {
		t.Fatalf("GetActiveCleanupRoundByPath: %v", err)
	}
	if got == nil || got.IssueID != "" {
		t.Fatalf("ownerless round lookup = %+v", got)
	}

	active, err := s.ListCleanupRounds(ctx, true)
	if err != nil {
		t.Fatalf("ListCleanupRounds active: %v", err)
	}
	if len(active) != 1 || active[0].Identifier != "PROJ-1" {
		t.Fatalf("active rounds = %+v", active)
	}
}

// LatestAgentRunByIdentifier excludes synthetic non-agent rows and
// reports absence explicitly.
func TestCleanupRound_LatestAgentRunByIdentifier(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := newCleanupRoundTestStore(t)

	if _, found, err := s.LatestAgentRunByIdentifier(ctx, "PROJ-1"); err != nil || found {
		t.Fatalf("no rows: found=%v err=%v, want false/nil", found, err)
	}

	agentRun := func(attempt int, adapter string) RunHistory {
		return RunHistory{
			IssueID:      "issue-1",
			Identifier:   "PROJ-1",
			Attempt:      attempt,
			AgentAdapter: adapter,
			Workspace:    "/workspace/PROJ-1",
			StartedAt:    "2026-10-14T01:00:00Z",
			CompletedAt:  "2026-10-14T01:05:00Z",
			Status:       "succeeded",
		}
	}

	first, err := s.AppendRunHistory(ctx, agentRun(1, "claude"))
	if err != nil {
		t.Fatalf("append agent row: %v", err)
	}
	_ = first

	// A synthetic ci_failed row carries no agent adapter and must never be
	// selected, even though it is inserted after the agent row.
	if _, err := s.AppendRunHistory(ctx, func() RunHistory {
		synth := agentRun(2, "")
		synth.Status = "ci_failed"
		return synth
	}()); err != nil {
		t.Fatalf("append synthetic row: %v", err)
	}

	latest, found, err := s.LatestAgentRunByIdentifier(ctx, "PROJ-1")
	if err != nil || !found {
		t.Fatalf("LatestAgentRunByIdentifier: found=%v err=%v", found, err)
	}
	if latest.AgentKind != "claude" || latest.RunID != first.ID {
		t.Fatalf("latest = %+v, want agent row %d", latest, first.ID)
	}
	if latest.IssueID != "issue-1" {
		t.Fatalf("latest issue id = %q", latest.IssueID)
	}

	// No agent rows for a different identifier: still absence.
	_, found, err = s.LatestAgentRunByIdentifier(ctx, "OTHER-2")
	if err != nil || found || !errors.Is(err, nil) {
		t.Fatalf("other identifier: found=%v err=%v, want false/nil", found, err)
	}
}
