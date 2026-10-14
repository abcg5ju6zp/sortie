package orchestrator

import (
	"context"
	"errors"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/sortie-ai/sortie/internal/agent/procutil"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/workspace"
)

const (
	roundBackoffMaxShift = 16
	roundDefaultPollMS   = 1000
	roundDefaultMaxMS    = 300_000
)

// RoundHookOutcome is the engine-portable result of one before_remove
// launch. Kind carries one of the persistence cleanup hook state
// values; the workspace outcome kind uses the same strings by
// construction.
type RoundHookOutcome struct {
	Kind                string
	ExitCode            int
	Output              string
	TerminatedLeftovers bool
}

// ProcessCleanupRoundsParams are the dependencies of
// [ProcessCleanupRounds]. ExecuteHook, RemoveTree, and Inspect exist for
// deterministic tests; production leaves them nil and gets the workspace
// package implementations.
type ProcessCleanupRoundsParams struct {
	Store CleanupRoundStore

	Ctx     context.Context
	Logger  *slog.Logger
	Metrics domain.Metrics

	// PollIntervalMS seeds retry backoff; MaxBackoffMS caps it.
	PollIntervalMS int
	MaxBackoffMS   int

	NowFunc func() time.Time

	// Injectable side effects; nil selects production behavior.
	ExecuteHook func(ctx context.Context, r *persistence.CleanupRound) RoundHookOutcome
	RemoveTree  func(path string) error
	Inspect     func(path string) (workspace.WorkspaceExistence, error)
}

// CleanupRoundsResult summarizes one processing pass.
type CleanupRoundsResult struct {
	Due                  int
	CompletedRemoved     int
	CompletedAlreadyGone int
	Retrying             int
	Blocked              int
	WaitingQuiesce       int
}

// defaultCleanupHookExecutor runs the pinned before_remove hook with the
// existing restricted environment and point-in-time directory
// verification, then classifies the outcome for the round.
func defaultCleanupHookExecutor(ctx context.Context, r *persistence.CleanupRound) RoundHookOutcome {
	env := workspace.HookEnv(r.IssueID, r.Identifier, r.WorkspacePath, r.LastAttempt, r.SSHHost)
	res, runErr := workspace.RunHook(ctx, workspace.HookParams{
		Script:    r.HookScript,
		Dir:       r.WorkspacePath,
		Env:       env,
		TimeoutMS: r.HookTimeoutMS,
	})
	if runErr == nil {
		return RoundHookOutcome{
			Kind:                persistence.CleanupHookSucceeded,
			ExitCode:            0,
			Output:              res.Output,
			TerminatedLeftovers: res.TerminatedLeftovers,
		}
	}
	var he *workspace.HookError
	errors.As(runErr, &he)
	out := RoundHookOutcome{Kind: persistence.CleanupHookFailed}
	if he != nil {
		out.Kind = string(workspace.ClassifyBeforeRemoveOutcome(he, r.SSHHost))
		out.ExitCode = he.ExitCode
		out.Output = he.Output
		out.TerminatedLeftovers = he.TerminatedLeftovers
	}
	return out
}

// MarkRoundQuiesced confirms that no running entry still occupies the
// round's workspace, recording quiesced_at. It returns false without
// writing when the issue still has a running entry so a caller cannot
// start the hook while the agent process is alive. It is idempotent.
func MarkRoundQuiesced(ctx context.Context, store CleanupRoundStore, state *State, id int64, at string) (bool, error) {
	r := state.CleanupRounds[id]
	if r == nil {
		return false, nil
	}
	if r.QuiescedAt != "" {
		return true, nil
	}
	if r.IssueID != "" {
		if _, busy := state.Running[r.IssueID]; busy {
			return false, nil
		}
	}
	r.QuiescedAt = at
	r.UpdatedAt = at
	if err := store.SaveCleanupRound(ctx, *r); err != nil {
		return false, err
	}
	return true, nil
}

// cleanupRoundEngine carries one pass's dependencies so the stage
// helpers share one clock and backoff configuration.
type cleanupRoundEngine struct {
	ctx            context.Context
	store          CleanupRoundStore
	state          *State
	log            *slog.Logger
	metrics        domain.Metrics
	now            time.Time
	pollIntervalMS int
	maxBackoffMS   int
	execHook       func(context.Context, *persistence.CleanupRound) RoundHookOutcome
	removeTree     func(path string) error
	inspect        func(path string) (workspace.WorkspaceExistence, error)
}

// cleanupRoundBackoff computes the retry delay for the nth attempt of a
// retryable round stage: poll interval doubled per attempt, capped.
func cleanupRoundBackoff(attempt, pollMS, maxMS int) int64 {
	base := pollMS
	if base <= 0 {
		base = roundDefaultPollMS
	}
	capMS := maxMS
	if capMS <= 0 {
		capMS = roundDefaultMaxMS
	}
	shift := min(max(attempt-1, 0), roundBackoffMaxShift)
	d := int64(base) * int64(1<<shift)
	capped := int64(capMS)
	if d <= 0 || d > capped {
		d = capped
	}
	return d
}

// backoff computes the retry delay for this pass's schedule.
func (e *cleanupRoundEngine) backoff(attempt int) int64 {
	return cleanupRoundBackoff(attempt, e.pollIntervalMS, e.maxBackoffMS)
}

func (e *cleanupRoundEngine) dueMS() int64 { return e.now.UnixMilli() }
func (e *cleanupRoundEngine) rfc() string  { return e.now.Format(time.RFC3339) }

// scheduleRetry backs a retryable stage off from now using its own
// attempt counter.
func (e *cleanupRoundEngine) scheduleRetry(r *persistence.CleanupRound, attempts int) {
	r.NextDueAtMS = e.dueMS() + e.backoff(attempts)
}

// save persists the round's mutable stage fields.
func (e *cleanupRoundEngine) save(r *persistence.CleanupRound) error {
	r.UpdatedAt = e.rfc()
	return e.store.SaveCleanupRound(e.ctx, *r)
}

// reloadRound refreshes the in-state row from durable storage after a
// failed write, discarding mutations the write did not confirm. A read
// failure or a concurrently completed row leaves the caller's retry in
// place; the next pass re-evaluates the durable truth.
func (e *cleanupRoundEngine) reloadRound(r *persistence.CleanupRound) {
	reloaded, err := e.store.GetActiveCleanupRoundByPath(e.ctx, r.WorkspacePath)
	if err != nil || reloaded == nil {
		return
	}
	*r = *reloaded
}

// withinPinnedRoot reports whether path is a direct child of the pinned
// root, matching the ComputePath containment contract.
func withinPinnedRoot(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == "." || rel == ".." || filepath.IsAbs(rel) {
		return false
	}
	return !strings.ContainsAny(rel, `/\`)
}

// ProcessCleanupRounds advances every due active round one processing
// pass. Stages already confirmed are never repeated: a confirmed hook
// outcome in particular survives every restart.
func ProcessCleanupRounds(ctx context.Context, state *State, params ProcessCleanupRoundsParams) CleanupRoundsResult {
	log := params.Logger
	if log == nil {
		log = slog.Default()
	}
	metrics := params.Metrics
	if metrics == nil {
		metrics = &domain.NoopMetrics{}
	}
	now := time.Now().UTC()
	if params.NowFunc != nil {
		now = params.NowFunc().UTC()
	}
	inspect := params.Inspect
	if inspect == nil {
		inspect = workspace.InspectWorkspace
	}
	removeTree := params.RemoveTree
	if removeTree == nil {
		removeTree = workspace.RemoveWorkspaceTree
	}
	execHook := params.ExecuteHook
	if execHook == nil {
		execHook = defaultCleanupHookExecutor
	}

	e := &cleanupRoundEngine{
		ctx:            ctx,
		store:          params.Store,
		state:          state,
		log:            log,
		metrics:        metrics,
		now:            now,
		pollIntervalMS: params.PollIntervalMS,
		maxBackoffMS:   params.MaxBackoffMS,
		execHook:       execHook,
		removeTree:     removeTree,
		inspect:        inspect,
	}

	ids := make([]int64, 0, len(state.CleanupRounds))
	for id, r := range state.CleanupRounds {
		if r.Status == persistence.CleanupRoundActive {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)

	var result CleanupRoundsResult
	for _, id := range ids {
		r := state.CleanupRounds[id]
		if r.NextDueAtMS > now.UnixMilli() {
			continue
		}
		result.Due++
		switch e.advance(r) {
		case persistence.CleanupCompletedRemoved:
			result.CompletedRemoved++
		case persistence.CleanupCompletedAlreadyGone:
			result.CompletedAlreadyGone++
		case "retrying":
			result.Retrying++
		case "blocked":
			result.Blocked++
		case "waiting_quiesce":
			result.WaitingQuiesce++
		}
	}
	return result
}

// advance processes one round and reports its pass outcome. Every state
// change is persisted before the side effect it authorizes is
// attempted, so a crash between the save and the effect resumes without
// replaying the effect.
func (e *cleanupRoundEngine) advance(r *persistence.CleanupRound) string {
	entryLog := e.log.With(
		slog.Int64("round_id", r.ID),
		slog.String("issue_id", r.IssueID),
		slog.String("identifier", r.Identifier),
		slog.String("workspace", r.WorkspacePath),
	)

	// complete records the terminal fact in durable storage first; only
	// after the write succeeds do the in-memory gate and metrics follow.
	// On a write failure the in-state row is reloaded from storage and
	// the caller retries, so an unconfirmed completion never parks the
	// round in memory.
	complete := func(reason string) bool {
		r.Status = persistence.CleanupRoundCompleted
		r.CompletedReason = reason
		r.CompletedAt = e.rfc()
		r.NextDueAtMS = 0
		r.RemovalState = persistence.CleanupRemovalGone
		if err := e.save(r); err != nil {
			entryLog.Error("cleanup round: failed to persist completion", slog.Any("error", err))
			e.reloadRound(r)
			return false
		}
		delete(e.state.cleanupRoundIssueIdx, r.IssueID)
		delete(e.state.cleanupRoundKeyIdx, r.WorkspaceKey)
		entryLog.Info("cleanup round completed", slog.String("reason", reason))
		return true
	}
	inspectionRetry := func(reason string) string {
		r.LastRemovalError = reason
		r.RemovalAttempts++
		e.scheduleRetry(r, r.RemovalAttempts)
		if err := e.save(r); err != nil {
			entryLog.Error("cleanup round: failed to persist retry", slog.Any("error", err))
			e.reloadRound(r)
		}
		e.metrics.IncReconciliationActions(actionCleanupRoundRetry)
		return "retrying"
	}

	// Stage 2: quiesce. A still-running occupant parks the round until
	// its exit handler confirms quiescence.
	if r.QuiescedAt == "" {
		if r.IssueID != "" {
			if _, busy := e.state.Running[r.IssueID]; busy {
				return "waiting_quiesce"
			}
		}
		r.QuiescedAt = e.rfc()
		if err := e.save(r); err != nil {
			entryLog.Error("cleanup round: failed to persist quiescence", slog.Any("error", err))
			e.reloadRound(r)
			return "retrying"
		}
	}

	// The pinned path is the only directory the round ever inspects or
	// removes, and its well-formedness/containment are structural checks
	// independent of the current configuration snapshot.
	if err := workspace.VerifyWorkspaceRemovable(r.WorkspacePath); err != nil || !withinPinnedRoot(r.WorkspaceRoot, r.WorkspacePath) {
		return e.blockUnsafe(r, entryLog, "pinned path failed safety verification")
	}

	existence, err := e.inspect(r.WorkspacePath)
	if err != nil {
		entryLog.Warn("cleanup round: workspace inspection failed", slog.Any("error", err))
		return inspectionRetry("inspect: " + err.Error())
	}
	switch existence {
	case workspace.WorkspaceAbsent:
		if complete(persistence.CleanupCompletedAlreadyGone) {
			e.metrics.IncReconciliationActions(actionCleanupRoundGone)
			return persistence.CleanupCompletedAlreadyGone
		}
		return "retrying"
	case workspace.WorkspaceUnsafeEntry:
		return e.blockUnsafe(r, entryLog, "pinned path is a symlink or non-directory entry")
	}

	// Stage 3: the pinned before_remove hook, at most once.
	switch r.HookState {
	case persistence.CleanupHookPending:
		if outcome := e.launchHook(r, entryLog); outcome != "" {
			return outcome
		}
	case persistence.CleanupHookRunning:
		// A "started" marker without an outcome means the previous
		// process died while the hook was live. The script may have
		// reached its own end: never launch it again.
		r.HookState = persistence.CleanupHookInterrupted
		r.HookFinishedAt = e.rfc()
		if err := e.save(r); err != nil {
			entryLog.Error("cleanup round: failed to persist interrupted hook", slog.Any("error", err))
			e.reloadRound(r)
			return "retrying"
		}
		entryLog.Warn("cleanup round: before_remove interrupted by process exit; not re-running")
	case persistence.CleanupHookHostUnreachable:
		// Delivery was never confirmed: the one outcome allowed to
		// re-launch the hook.
		if outcome := e.launchHook(r, entryLog); outcome != "" {
			return outcome
		}
	case persistence.CleanupHookSucceeded,
		persistence.CleanupHookFailed,
		persistence.CleanupHookTimeout,
		persistence.CleanupHookInterrupted,
		persistence.CleanupHookSkipped:
		// Confirmed stage: proceed to removal without another launch.
	}

	// Stage 4: remove the tree, re-inspecting first so a directory the
	// hook (or an external actor) already removed completes cleanly.
	existence, err = e.inspect(r.WorkspacePath)
	if err != nil {
		entryLog.Warn("cleanup round: post-hook inspection failed", slog.Any("error", err))
		return inspectionRetry("inspect: " + err.Error())
	}
	switch existence {
	case workspace.WorkspaceAbsent:
		if complete(persistence.CleanupCompletedAlreadyGone) {
			e.metrics.IncReconciliationActions(actionCleanupRoundGone)
			return persistence.CleanupCompletedAlreadyGone
		}
		return "retrying"
	case workspace.WorkspaceUnsafeEntry:
		return e.blockUnsafe(r, entryLog, "pinned path became a symlink or non-directory entry")
	}

	rmErr := e.removeTree(r.WorkspacePath)
	if rmErr != nil {
		entryLog.Warn("cleanup round: tree removal failed", slog.Any("error", rmErr))
	}
	postExistence, inspectErr := e.inspect(r.WorkspacePath)
	if inspectErr == nil && postExistence == workspace.WorkspaceAbsent {
		if complete(persistence.CleanupCompletedRemoved) {
			e.metrics.IncReconciliationActions(actionCleanupRoundRemoved)
			return persistence.CleanupCompletedRemoved
		}
		return "retrying"
	}

	r.RemovalState = persistence.CleanupRemovalFailed
	r.RemovalAttempts++
	switch {
	case rmErr != nil:
		r.LastRemovalError = rmErr.Error()
	case inspectErr != nil:
		r.LastRemovalError = "post-removal inspect: " + inspectErr.Error()
	default:
		r.LastRemovalError = "path still exists after removal"
	}
	e.scheduleRetry(r, r.RemovalAttempts)
	if err := e.save(r); err != nil {
		entryLog.Error("cleanup round: failed to persist partial removal", slog.Any("error", err))
		e.reloadRound(r)
	}
	entryLog.Warn("cleanup round: partial removal recorded, retrying removal only",
		slog.Int("removal_attempts", r.RemovalAttempts),
		slog.String("last_error", r.LastRemovalError),
		slog.Int64("next_due_at_ms", r.NextDueAtMS),
	)
	e.metrics.IncReconciliationActions(actionCleanupRoundRetry)
	return "retrying"
}

// blockUnsafe records an unsafe pinned path, backs the round off, and
// leaves it for an operator. Neither hook nor removal touches the path.
func (e *cleanupRoundEngine) blockUnsafe(r *persistence.CleanupRound, entryLog *slog.Logger, reason string) string {
	r.RemovalState = persistence.CleanupRemovalUnsafe
	r.RemovalAttempts++
	r.LastRemovalError = reason
	e.scheduleRetry(r, r.RemovalAttempts)
	if err := e.save(r); err != nil {
		entryLog.Error("cleanup round: failed to persist unsafe path", slog.Any("error", err))
		e.reloadRound(r)
	}
	entryLog.Error("cleanup round: refusing unsafe pinned path, leaving for operator",
		slog.String("reason", reason),
		slog.Int("removal_attempts", r.RemovalAttempts),
		slog.Int64("next_due_at_ms", r.NextDueAtMS),
	)
	e.metrics.IncReconciliationActions(actionCleanupRoundBlocked)
	return "blocked"
}

// launchHook persists the running marker before launching the hook,
// records the classified outcome afterward, and reports "retrying" only
// when the pinned host could not be reached. An empty outcome means the
// hook stage is confirmed and the caller proceeds to removal.
func (e *cleanupRoundEngine) launchHook(r *persistence.CleanupRound, entryLog *slog.Logger) string {
	r.HookState = persistence.CleanupHookRunning
	r.HookStartedAt = e.rfc()
	r.HookFinishedAt = ""
	r.HookAttempts++
	r.HookOutput = ""
	r.HookExitCode = 0
	if err := e.save(r); err != nil {
		entryLog.Error("cleanup round: failed to persist hook start marker", slog.Any("error", err))
		e.reloadRound(r)
		return "retrying"
	}

	entryLog.Info("cleanup round: running before_remove hook", slog.Int("hook_attempts", r.HookAttempts))
	// Detached like the legacy cleanup path: shutdown must not kill the
	// hook mid-flight without the timeout and process-group guarantees
	// RunHook already applies.
	outcome := e.execHook(context.WithoutCancel(e.ctx), r)

	r.HookFinishedAt = e.rfc()
	r.HookExitCode = outcome.ExitCode
	r.HookOutput = outcome.Output
	r.HookState = outcome.Kind
	if err := e.save(r); err != nil {
		// The hook already ran: do not reload to the durable pending
		// state (which would risk a second launch) and do not remove
		// this pass. Hold the outcome in memory with a backoff; a crash
		// before a successful save resumes the durable "running" marker
		// as interrupted rather than re-launching.
		entryLog.Error("cleanup round: failed to persist hook outcome; holding in memory", slog.Any("error", err))
		e.scheduleRetry(r, r.HookAttempts)
		e.metrics.IncReconciliationActions(actionCleanupRoundRetry)
		return "retrying"
	}

	if outcome.TerminatedLeftovers {
		entryLog.Info(procutil.LeftoversTerminatedMessage, //nolint:sloglint // procutil.LeftoversTerminatedMessage is a fixed string constant
			slog.String("hook", "before_remove"),
			slog.Bool("terminated_leftovers", true),
		)
	}
	switch outcome.Kind {
	case persistence.CleanupHookSucceeded:
		entryLog.Info("cleanup round: before_remove hook succeeded")
		return ""
	case persistence.CleanupHookHostUnreachable:
		entryLog.Warn("cleanup round: remote host unreachable, hook delivery unconfirmed; will retry the hook",
			slog.String("ssh_host", r.SSHHost),
			slog.Int("hook_attempts", r.HookAttempts),
			slog.Int64("next_due_at_ms", r.NextDueAtMS),
		)
		e.scheduleRetry(r, r.HookAttempts)
		if err := e.save(r); err != nil {
			entryLog.Error("cleanup round: failed to persist host-unreachable retry", slog.Any("error", err))
		}
		e.metrics.IncReconciliationActions(actionCleanupRoundRetry)
		return "retrying"
	default:
		// failed / timeout / interrupted: the best-effort contract keeps
		// removal going, and the hook is never launched again.
		entryLog.Warn("cleanup round: before_remove hook confirmed unsuccessful; proceeding without re-running",
			slog.String("hook_state", outcome.Kind),
			slog.Int("exit_code", outcome.ExitCode),
			slog.Int("hook_attempts", r.HookAttempts),
		)
		return ""
	}
}
