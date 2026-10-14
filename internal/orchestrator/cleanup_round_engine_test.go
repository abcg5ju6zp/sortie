package orchestrator

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/workspace"
)

// engineFixture bundles the fakes one engine pass runs against.
type engineFixture struct {
	store *fakeRoundStore
	state *State
	now   time.Time

	hookCalls    []string
	hookOutcomes []string // queued outcomes per launch; default succeeded
	hookOutputs  []string

	removed []string
	// removeFailures is the number of removal calls that fail before
	// succeeding; the filesystem state flips to absent regardless so the
	// verifier can distinguish "failed but gone" from "partial".
	removeFailures int
	keepOnFailure  bool // true: failed removal leaves the directory present

	fs    map[string]workspace.WorkspaceExistence
	fsErr map[string]error

	logBuf bytes.Buffer
}

func newEngineFixture(t *testing.T) *engineFixture {
	t.Helper()
	return &engineFixture{
		store: newFakeRoundStore(),
		state: NewState(5000, 4, 0, nil, AgentTotals{}),
		now:   time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC),
		fs:    make(map[string]workspace.WorkspaceExistence),
		fsErr: make(map[string]error),
	}
}

func (f *engineFixture) clock() time.Time { return f.now }

func (f *engineFixture) seed(t *testing.T, r persistence.CleanupRound) *persistence.CleanupRound {
	t.Helper()
	if r.CreatedAt == "" {
		r.CreatedAt = "2026-10-14T11:00:00Z"
	}
	created, err := f.store.CreateCleanupRound(context.Background(), r)
	if err != nil {
		t.Fatalf("seed round: %v", err)
	}
	rows, _ := f.store.ListCleanupRounds(context.Background(), false)
	PopulateCleanupRounds(f.state, rows)
	return f.store.rows[created.ID]
}

func (f *engineFixture) params() ProcessCleanupRoundsParams {
	return ProcessCleanupRoundsParams{
		Store:          f.store,
		Ctx:            context.Background(),
		Logger:         slog.New(slog.NewTextHandler(&f.logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Metrics:        &spyMetrics{},
		PollIntervalMS: 100,
		MaxBackoffMS:   5000,
		NowFunc:        f.clock,
		ExecuteHook: func(_ context.Context, r *persistence.CleanupRound) RoundHookOutcome {
			f.hookCalls = append(f.hookCalls, r.WorkspacePath)
			kind := persistence.CleanupHookSucceeded
			if len(f.hookOutcomes) >= len(f.hookCalls) {
				kind = f.hookOutcomes[len(f.hookCalls)-1]
			}
			out := RoundHookOutcome{Kind: kind, ExitCode: 0}
			if len(f.hookOutputs) >= len(f.hookCalls) {
				out.Output = f.hookOutputs[len(f.hookCalls)-1]
			}
			if kind == persistence.CleanupHookHostUnreachable {
				out.ExitCode = 255
			}
			if kind == persistence.CleanupHookFailed {
				out.ExitCode = 1
			}
			return out
		},
		RemoveTree: func(path string) error {
			f.removed = append(f.removed, path)
			if f.removeFailures > 0 {
				f.removeFailures--
				if f.keepOnFailure {
					return errors.New("rm: resource busy")
				}
			}
			f.fs[path] = workspace.WorkspaceAbsent
			return nil
		},
		Inspect: func(path string) (workspace.WorkspaceExistence, error) {
			if err := f.fsErr[path]; err != nil {
				return workspace.WorkspaceAbsent, err
			}
			got, ok := f.fs[path]
			if !ok {
				return workspace.WorkspaceAbsent, nil
			}
			return got, nil
		},
	}
}

func (f *engineFixture) round(id int64) *persistence.CleanupRound { return f.store.rows[id] }

func baseEngineRound() persistence.CleanupRound {
	return persistence.CleanupRound{
		Status:          persistence.CleanupRoundActive,
		IssueID:         "ISS-1",
		Identifier:      "PROJ-1",
		WorkspaceKey:    "PROJ-1",
		WorkspacePath:   "/ws/PROJ-1",
		WorkspaceRoot:   "/ws",
		SSHHost:         "builder.example",
		TerminalState:   "done",
		TerminalSource:  persistence.CleanupSourceReconcile,
		HookScript:      "echo bye",
		HookTimeoutMS:   60000,
		HookState:       persistence.CleanupHookPending,
		RemovalState:    persistence.CleanupRemovalPending,
		GateConfirmedAt: "2026-10-14T11:00:00Z",
		QuiescedAt:      "2026-10-14T11:00:00Z",
	}
}

// no hook or removal while an occupying run is live; once the
// running entry is gone the same pass advances through both stages.
func TestProcessCleanupRounds_QuiesceGate(t *testing.T) {
	t.Parallel()

	t.Run("waits for running occupant", func(t *testing.T) {
		t.Parallel()
		f := newEngineFixture(t)
		r := baseEngineRound()
		r.QuiescedAt = ""
		round := f.seed(t, r)
		f.fs[round.WorkspacePath] = workspace.WorkspaceSafeDirectory
		f.state.Running["ISS-1"] = &RunningEntry{Identifier: "PROJ-1"}

		res := ProcessCleanupRounds(context.Background(), f.state, f.params())
		if res.WaitingQuiesce != 1 || len(f.hookCalls) != 0 || len(f.removed) != 0 {
			t.Fatalf("result=%+v hooks=%v removed=%v", res, f.hookCalls, f.removed)
		}
		if f.round(round.ID).QuiescedAt != "" {
			t.Error("quiesce must not be confirmed while the occupant runs")
		}
	})

	t.Run("auto-quiesces without occupant and finishes", func(t *testing.T) {
		t.Parallel()
		f := newEngineFixture(t)
		r := baseEngineRound()
		r.QuiescedAt = ""
		r.HookState = persistence.CleanupHookSkipped
		r.HookScript = ""
		round := f.seed(t, r)
		f.fs[round.WorkspacePath] = workspace.WorkspaceSafeDirectory

		res := ProcessCleanupRounds(context.Background(), f.state, f.params())
		got := f.round(round.ID)
		if res.CompletedRemoved != 1 || got.Status != persistence.CleanupRoundCompleted ||
			got.QuiescedAt == "" || got.RemovalState != persistence.CleanupRemovalGone {
			t.Fatalf("res=%+v round=%+v", res, got)
		}
	})
}

// hook outcome classification drives the correct successor, and
// the hook launches at most once except after host_unreachable.
func TestProcessCleanupRounds_HookOutcomes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		seedState   string
		queued      []string // outcomes per launch
		wantCalls   int
		wantFinal   string // completed reason or "retrying"
		wantHookRow string
	}{
		{name: "succeeded", seedState: persistence.CleanupHookPending, queued: []string{persistence.CleanupHookSucceeded}, wantCalls: 1, wantFinal: persistence.CleanupCompletedRemoved, wantHookRow: persistence.CleanupHookSucceeded},
		{name: "failed proceeds without rerun", seedState: persistence.CleanupHookPending, queued: []string{persistence.CleanupHookFailed}, wantCalls: 1, wantFinal: persistence.CleanupCompletedRemoved, wantHookRow: persistence.CleanupHookFailed},
		{name: "timeout proceeds without rerun", seedState: persistence.CleanupHookPending, queued: []string{persistence.CleanupHookTimeout}, wantCalls: 1, wantFinal: persistence.CleanupCompletedRemoved, wantHookRow: persistence.CleanupHookTimeout},
		{name: "interrupted marker never relaunches", seedState: persistence.CleanupHookRunning, queued: nil, wantCalls: 0, wantFinal: persistence.CleanupCompletedRemoved, wantHookRow: persistence.CleanupHookInterrupted},
		{name: "already confirmed outcome not relaunched", seedState: persistence.CleanupHookSucceeded, queued: nil, wantCalls: 0, wantFinal: persistence.CleanupCompletedRemoved, wantHookRow: persistence.CleanupHookSucceeded},
		{name: "skipped never launches", seedState: persistence.CleanupHookSkipped, queued: nil, wantCalls: 0, wantFinal: persistence.CleanupCompletedRemoved, wantHookRow: persistence.CleanupHookSkipped},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newEngineFixture(t)
			r := baseEngineRound()
			if tc.seedState == persistence.CleanupHookSkipped {
				r.HookScript = ""
			}
			r.HookState = tc.seedState
			round := f.seed(t, r)
			f.fs[round.WorkspacePath] = workspace.WorkspaceSafeDirectory
			f.hookOutcomes = tc.queued
			p := f.params()

			res := ProcessCleanupRounds(context.Background(), f.state, p)
			got := f.round(round.ID)
			if len(f.hookCalls) != tc.wantCalls {
				t.Errorf("hook launches = %d, want %d", len(f.hookCalls), tc.wantCalls)
			}
			if res.CompletedRemoved != 1 {
				t.Errorf("completed removed = %d, res=%+v", res.CompletedRemoved, res)
			}
			if got.Status != persistence.CleanupRoundCompleted || got.HookState != tc.wantHookRow {
				t.Errorf("status=%q hook=%q, want completed/%q", got.Status, got.HookState, tc.wantHookRow)
			}
			if len(f.removed) != 1 {
				t.Errorf("removals = %v, want one", f.removed)
			}
		})
	}
}

// host_unreachable retries only the hook; nothing is removed
// until the host returns, and a later failure/success then finalizes.
func TestProcessCleanupRounds_HostUnreachableRetriesHookOnly(t *testing.T) {
	t.Parallel()
	f := newEngineFixture(t)
	r := baseEngineRound()
	round := f.seed(t, r)
	f.fs[round.WorkspacePath] = workspace.WorkspaceSafeDirectory
	p := f.params()
	f.hookOutcomes = []string{persistence.CleanupHookHostUnreachable}

	res := ProcessCleanupRounds(context.Background(), f.state, p)
	if res.Retrying != 1 || len(f.hookCalls) != 1 || len(f.removed) != 0 {
		t.Fatalf("first pass: res=%+v hooks=%v removed=%v", res, f.hookCalls, f.removed)
	}
	stored := f.round(round.ID)
	if stored.HookState != persistence.CleanupHookHostUnreachable || stored.NextDueAtMS <= f.now.UnixMilli() {
		t.Fatalf("host-unreachable not scheduled for retry: %+v", stored)
	}

	// Before the backoff expires the round is not touched.
	f.now = f.now.Add(50 * time.Millisecond)
	if res := ProcessCleanupRounds(context.Background(), f.state, p); res.Due != 0 || len(f.hookCalls) != 1 {
		t.Fatalf("early pass must skip the round: %+v", res)
	}

	// Host returns: the hook launches again and the round finalizes.
	f.now = f.now.Add(60 * time.Millisecond)
	f.hookOutcomes = []string{persistence.CleanupHookHostUnreachable, persistence.CleanupHookSucceeded}
	res = ProcessCleanupRounds(context.Background(), f.state, p)
	if len(f.hookCalls) != 2 || res.CompletedRemoved != 1 {
		t.Fatalf("recovery pass: hooks=%v res=%+v", f.hookCalls, res)
	}
}

// a half-removed tree retries removal only; the hook never runs
// again, and eventual absence completes the round.
func TestProcessCleanupRounds_PartialRemovalRetriesRemovalOnly(t *testing.T) {
	t.Parallel()
	f := newEngineFixture(t)
	r := baseEngineRound()
	round := f.seed(t, r)
	f.fs[round.WorkspacePath] = workspace.WorkspaceSafeDirectory
	f.hookOutcomes = []string{persistence.CleanupHookSucceeded}
	f.removeFailures = 1
	f.keepOnFailure = true
	p := f.params()

	res := ProcessCleanupRounds(context.Background(), f.state, p)
	stored := f.round(round.ID)
	if res.Retrying != 1 || stored.RemovalState != persistence.CleanupRemovalFailed ||
		stored.RemovalAttempts != 1 || stored.NextDueAtMS <= f.now.UnixMilli() || len(f.removed) != 1 {
		t.Fatalf("first pass res=%+v stored=%+v removed=%v", res, stored, f.removed)
	}
	if stored.HookState != persistence.CleanupHookSucceeded {
		t.Fatalf("hook state = %q", stored.HookState)
	}

	f.now = f.now.Add(200 * time.Millisecond)
	res = ProcessCleanupRounds(context.Background(), f.state, p)
	if res.CompletedRemoved != 1 || len(f.removed) != 2 || len(f.hookCalls) != 1 {
		t.Fatalf("retry pass: res=%+v removed=%v hooks=%v", res, f.removed, f.hookCalls)
	}
}

// an already-absent pinned path is a verifiable
// completion: no hook, no removal, no directory creation.
func TestProcessCleanupRounds_AlreadyGoneCompletes(t *testing.T) {
	t.Parallel()
	f := newEngineFixture(t)
	round := f.seed(t, baseEngineRound())
	// fs has no entry for the path: Inspect reports absent.

	res := ProcessCleanupRounds(context.Background(), f.state, f.params())
	got := f.round(round.ID)
	if res.CompletedAlreadyGone != 1 || got.Status != persistence.CleanupRoundCompleted ||
		got.CompletedReason != persistence.CleanupCompletedAlreadyGone {
		t.Fatalf("res=%+v round=%+v", res, got)
	}
	if len(f.hookCalls) != 0 || len(f.removed) != 0 {
		t.Fatalf("absent path must run no hook and no removal: hooks=%v removed=%v", f.hookCalls, f.removed)
	}
	if cleanupRoundBlocksIssue(f.state, "ISS-1", "PROJ-1") {
		t.Error("gate must clear on completion")
	}
}

// an unsafe entry (symlink/non-directory) blocks both hook and
// removal and schedules a retry for an operator to intervene.
func TestProcessCleanupRounds_UnsafePathBlocked(t *testing.T) {
	t.Parallel()
	f := newEngineFixture(t)
	round := f.seed(t, baseEngineRound())
	f.fs[round.WorkspacePath] = workspace.WorkspaceUnsafeEntry

	res := ProcessCleanupRounds(context.Background(), f.state, f.params())
	got := f.round(round.ID)
	if res.Blocked != 1 || got.RemovalState != persistence.CleanupRemovalUnsafe ||
		got.NextDueAtMS <= f.now.UnixMilli() {
		t.Fatalf("res=%+v round=%+v", res, got)
	}
	if len(f.hookCalls) != 0 || len(f.removed) != 0 {
		t.Fatalf("unsafe path must run no hook/removal: hooks=%v removed=%v", f.hookCalls, f.removed)
	}
}

// A path that escapes its pinned root is refused even when it exists.
func TestProcessCleanupRounds_ContainmentViolationBlocked(t *testing.T) {
	t.Parallel()
	f := newEngineFixture(t)
	r := baseEngineRound()
	r.WorkspacePath = "/elsewhere/PROJ-1" // outside pinned root /ws
	round := f.seed(t, r)
	f.fs[round.WorkspacePath] = workspace.WorkspaceSafeDirectory

	res := ProcessCleanupRounds(context.Background(), f.state, f.params())
	if res.Blocked != 1 || len(f.hookCalls) != 0 || len(f.removed) != 0 {
		t.Fatalf("res=%+v hooks=%v removed=%v", res, f.hookCalls, f.removed)
	}
}

// the engine reads only pinned row fields. Two rounds pinned
// against different roots/scripts/hosts each execute their own snapshot,
// and no parameter exists that could redirect either.
func TestProcessCleanupRounds_PinnedSnapshotsAreIndependent(t *testing.T) {
	t.Parallel()
	f := newEngineFixture(t)

	r1 := baseEngineRound()
	r1.HookState = persistence.CleanupHookSucceeded // pretend hook already ran pre-reload
	r2 := r1
	r2.IssueID = "ISS-2"
	r2.Identifier = "PROJ-2"
	r2.WorkspaceKey = "PROJ-2"
	r2.WorkspacePath = "/old-root/PROJ-2"
	r2.WorkspaceRoot = "/old-root"
	r2.SSHHost = "other.example"
	r2.HookState = persistence.CleanupHookSkipped
	r2.HookScript = ""

	round1 := f.seed(t, r1)
	round2 := f.seed(t, r2)
	f.fs[round1.WorkspacePath] = workspace.WorkspaceSafeDirectory
	f.fs[round2.WorkspacePath] = workspace.WorkspaceSafeDirectory

	res := ProcessCleanupRounds(context.Background(), f.state, f.params())
	if res.CompletedRemoved != 2 {
		t.Fatalf("res=%+v, want both rounds removed", res)
	}
	// Neither round was rewritten toward the other path or host.
	if f.round(round1.ID).WorkspacePath != "/ws/PROJ-1" || f.round(round2.ID).WorkspacePath != "/old-root/PROJ-2" {
		t.Error("pinned paths were rewritten during processing")
	}
	if got := strings.Join(f.removed, ","); got != "/ws/PROJ-1,/old-root/PROJ-2" {
		t.Errorf("removed paths = %q", got)
	}
	// The pre-confirmed r1 hook did not relaunch; skipped r2 never launched.
	if len(f.hookCalls) != 0 {
		t.Errorf("no hook should launch for pre-confirmed rounds, got %v", f.hookCalls)
	}
}

// retry backoff doubles per attempt up to the cap and the round
// stays untouched until due.
func TestProcessCleanupRounds_BackoffSchedule(t *testing.T) {
	t.Parallel()
	f := newEngineFixture(t)
	round := f.seed(t, baseEngineRound())
	f.fs[round.WorkspacePath] = workspace.WorkspaceSafeDirectory
	f.fsErr[round.WorkspacePath] = errors.New("disk offline")

	// Transient inspection failure backs off from the poll interval.
	ProcessCleanupRounds(context.Background(), f.state, f.params())
	stored := f.round(round.ID)
	if stored.RemovalAttempts != 1 || stored.NextDueAtMS != f.now.UnixMilli()+100 {
		t.Fatalf("first backoff = %+v", stored)
	}

	f.fsErr[round.WorkspacePath] = nil
	f.now = f.now.Add(99 * time.Millisecond)
	p := f.params()
	if res := ProcessCleanupRounds(context.Background(), f.state, p); res.Due != 0 {
		t.Fatal("round must not process before its due time")
	}
	// Forced failure again at attempt 2: delay doubles.
	f.fsErr[round.WorkspacePath] = errors.New("disk offline")
	f.now = f.now.Add(2 * time.Millisecond)
	ProcessCleanupRounds(context.Background(), f.state, p)
	if want := f.now.UnixMilli() + 200; f.round(round.ID).NextDueAtMS != want {
		t.Errorf("second backoff = %d, want %d", f.round(round.ID).NextDueAtMS, want)
	}

	// Cap honored.
	capped := cleanupRoundBackoff(20, 100, 5000)
	if capped != 5000 {
		t.Errorf("backoff cap = %d, want 5000", capped)
	}
}

// the three failure classes are distinguishable to an operator
// from log records alone (rubric evidence).
func TestProcessCleanupRounds_FailureClassesAreDistinctInLogs(t *testing.T) {
	t.Parallel()

	// Host unreachable.
	f1 := newEngineFixture(t)
	r1 := f1.seed(t, baseEngineRound())
	f1.fs[r1.WorkspacePath] = workspace.WorkspaceSafeDirectory
	f1.hookOutcomes = []string{persistence.CleanupHookHostUnreachable}
	ProcessCleanupRounds(context.Background(), f1.state, f1.params())
	if !strings.Contains(f1.logBuf.String(), "remote host unreachable") {
		t.Error("host-unreachable log record missing")
	}

	// Hook failure (nonzero exit).
	f2 := newEngineFixture(t)
	r2 := f2.seed(t, baseEngineRound())
	f2.fs[r2.WorkspacePath] = workspace.WorkspaceSafeDirectory
	f2.hookOutcomes = []string{persistence.CleanupHookFailed}
	f2.hookOutputs = []string{"hook exploded"}
	ProcessCleanupRounds(context.Background(), f2.state, f2.params())
	logs2 := f2.logBuf.String()
	if !strings.Contains(logs2, "hook confirmed unsuccessful") || !strings.Contains(logs2, "hook_state=failed") {
		t.Errorf("hook-failure record missing classification: %s", logs2)
	}

	// Partial removal.
	f3 := newEngineFixture(t)
	r3 := f3.seed(t, baseEngineRound())
	f3.fs[r3.WorkspacePath] = workspace.WorkspaceSafeDirectory
	f3.hookOutcomes = []string{persistence.CleanupHookSucceeded}
	f3.removeFailures = 1
	f3.keepOnFailure = true
	ProcessCleanupRounds(context.Background(), f3.state, f3.params())
	if !strings.Contains(f3.logBuf.String(), "partial removal recorded") {
		t.Error("partial-removal log record missing")
	}
}

// A failed outcome save after a successful hook never re-launches the
// hook: the unconfirmed outcome is held in memory with a backoff, and a
// later pass removes using the held confirmation.
func TestProcessCleanupRounds_HookOutcomeSaveFailureHoldsWithoutRelaunch(t *testing.T) {
	t.Parallel()
	f := newEngineFixture(t)
	r := baseEngineRound()
	round := f.seed(t, r)
	f.fs[round.WorkspacePath] = workspace.WorkspaceSafeDirectory
	f.hookOutcomes = []string{persistence.CleanupHookSucceeded}

	p := f.params()
	// Marker save succeeds; the outcome save that follows fails.
	p.ExecuteHook = func(_ context.Context, cr *persistence.CleanupRound) RoundHookOutcome {
		f.hookCalls = append(f.hookCalls, cr.WorkspacePath)
		f.store.saveErr = errors.New("disk full")
		return RoundHookOutcome{Kind: persistence.CleanupHookSucceeded}
	}
	f.now = f.now.Add(time.Second) // advance clear of any backoff

	res := ProcessCleanupRounds(context.Background(), f.state, p)
	if res.Retrying != 1 || res.CompletedRemoved != 0 {
		t.Fatalf("first pass = %+v, want retry/no completion", res)
	}
	if len(f.removed) != 0 {
		t.Fatal("removal must wait for a confirmed durable hook outcome")
	}
	if got := f.round(round.ID).HookState; got != persistence.CleanupHookRunning {
		t.Fatalf("durable row hook state = %q, want running (write failed)", got)
	}

	// Storage recovers; the in-memory held outcome confirms removal on
	// the next due pass without a second launch.
	f.store.saveErr = nil
	f.now = f.now.Add(time.Hour)
	res = ProcessCleanupRounds(context.Background(), f.state, p)
	if res.CompletedRemoved != 1 {
		t.Fatalf("second pass = %+v, want one removed", res)
	}
	if len(f.hookCalls) != 1 {
		t.Fatalf("hook launches = %d, want exactly 1", len(f.hookCalls))
	}
}

// A failed completion write is not counted as completion and the
// in-state row is rolled back to its durable active truth.
func TestProcessCleanupRounds_CompletionSaveFailureRollsBack(t *testing.T) {
	t.Parallel()
	f := newEngineFixture(t)
	round := f.seed(t, baseEngineRound())
	f.fs[round.WorkspacePath] = workspace.WorkspaceSafeDirectory
	f.store.saveErr = errors.New("disk full")

	metrics := &spyMetrics{}
	p := f.params()
	p.Metrics = metrics

	res := ProcessCleanupRounds(context.Background(), f.state, p)
	if res.CompletedRemoved != 0 || res.Retrying != 1 {
		t.Fatalf("pass = %+v, want retry/no completion", res)
	}
	if got := f.round(round.ID).Status; got != persistence.CleanupRoundActive {
		t.Fatalf("in-state status = %q, want active after rollback", got)
	}
	for _, a := range metrics.reconciliationActs {
		if a == actionCleanupRoundRemoved {
			t.Error("completion metric emitted despite failed write")
		}
	}

	f.store.saveErr = nil
	f.now = f.now.Add(time.Hour)
	res = ProcessCleanupRounds(context.Background(), f.state, p)
	if res.CompletedRemoved != 1 {
		t.Fatalf("second pass = %+v, want one removed", res)
	}
}

// MarkRoundQuiesced is idempotent and refuses while the occupant runs.
func TestMarkRoundQuiesced(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := newEngineFixture(t)
	r := baseEngineRound()
	r.QuiescedAt = ""
	round := f.seed(t, r)
	f.state.Running["ISS-1"] = &RunningEntry{Identifier: "PROJ-1"}

	ok, err := MarkRoundQuiesced(ctx, f.store, f.state, round.ID, "2026-10-14T12:01:00Z")
	if ok || err != nil {
		t.Fatalf("busy quiesce: ok=%v err=%v", ok, err)
	}
	delete(f.state.Running, "ISS-1")
	ok, err = MarkRoundQuiesced(ctx, f.store, f.state, round.ID, "2026-10-14T12:01:00Z")
	if !ok || err != nil {
		t.Fatalf("free quiesce: ok=%v err=%v", ok, err)
	}
	if f.round(round.ID).QuiescedAt != "2026-10-14T12:01:00Z" {
		t.Error("quiesced_at not persisted")
	}
	// Idempotent.
	ok, _ = MarkRoundQuiesced(ctx, f.store, f.state, round.ID, "2026-10-14T12:02:00Z")
	if !ok || f.round(round.ID).QuiescedAt != "2026-10-14T12:01:00Z" {
		t.Error("second quiesce must not move the timestamp")
	}
}
