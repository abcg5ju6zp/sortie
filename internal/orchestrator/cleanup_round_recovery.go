package orchestrator

import (
	"context"
	"log/slog"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/workspace"
)

// RecoverCleanupRoundsParams are the startup inputs to
// [RecoverCleanupRounds]: the durable round store, the live state, the
// workspace root to enumerate, the tracker batch reader, and the
// configuration snapshot new rounds pin.
type RecoverCleanupRoundsParams struct {
	Store CleanupRoundStore
	State *State

	WorkspaceRoot  string
	TrackerAdapter domain.TrackerAdapter
	TerminalStates []string

	BeforeRemoveHook string
	HookTimeoutMS    int

	PollIntervalMS int
	MaxBackoffMS   int

	Ctx     context.Context
	Logger  *slog.Logger
	Metrics domain.Metrics
}

// RecoverCleanupRounds performs the startup reconciliation of terminal
// cleanup rounds: it loads every persisted row into the dispatch gate,
// verifies active rounds against disk (an absent path completes
// already_gone without recreating it or re-running a hook, an unsafe
// entry is parked for an operator), adopts terminal directories with no
// round record as new rounds, and runs one engine pass so startup
// cleanup finishes whatever is immediately finishable.
//
// A tracker read failure suppresses only the adoption of new rounds,
// matching the legacy startup behavior: active rounds still reconcile
// against disk, and the next sweep adopts anything missed.
func RecoverCleanupRounds(params RecoverCleanupRoundsParams) {
	log := params.Logger
	if log == nil {
		log = slog.Default()
	}
	metrics := params.Metrics
	if metrics == nil {
		metrics = &domain.NoopMetrics{}
	}
	ctx := params.Ctx
	if ctx == nil {
		ctx = context.Background()
	}
	now := time.Now().UTC()

	rows, err := params.Store.ListCleanupRounds(ctx, false)
	if err != nil {
		log.Warn("startup cleanup: failed to load cleanup rounds; terminal cleanup retries on sweep",
			slog.Any("error", err),
		)
		return
	}
	PopulateCleanupRounds(params.State, rows)
	log.Info("startup cleanup: loaded cleanup rounds",
		slog.Int("active", countRoundsByStatus(rows, persistence.CleanupRoundActive)),
		slog.Int("completed", countRoundsByStatus(rows, persistence.CleanupRoundCompleted)),
	)

	reconcileActiveRoundsToDisk(params.Store, params.State, log, metrics, now)
	adoptTerminalDiskWorkspaces(params, log, metrics, now)

	// Rebuild the gate from everything the two steps changed.
	if refreshed, listErr := params.Store.ListCleanupRounds(ctx, false); listErr == nil {
		PopulateCleanupRounds(params.State, refreshed)
	}

	ProcessCleanupRounds(ctx, params.State, ProcessCleanupRoundsParams{
		Store:          params.Store,
		Ctx:            ctx,
		Logger:         log,
		Metrics:        metrics,
		PollIntervalMS: params.PollIntervalMS,
		MaxBackoffMS:   params.MaxBackoffMS,
	})
}

func countRoundsByStatus(rows []persistence.CleanupRound, status string) int {
	n := 0
	for _, r := range rows {
		if r.Status == status {
			n++
		}
	}
	return n
}

// reconcileActiveRoundsToDisk checks every active round's pinned path.
// Absent paths complete already_gone (verifiable result, never a
// recreated directory); unsafe entries are recorded so the engine
// retries them on schedule and leaves the path for an operator.
func reconcileActiveRoundsToDisk(store CleanupRoundStore, state *State, log *slog.Logger, metrics domain.Metrics, now time.Time) {
	ctx := context.Background()
	nowRFC := now.Format(time.RFC3339)
	for _, r := range state.CleanupRounds {
		if r.Status != persistence.CleanupRoundActive {
			continue
		}
		existence, inspectErr := workspace.InspectWorkspace(r.WorkspacePath)
		entryLog := log.With(
			slog.Int64("round_id", r.ID),
			slog.String("identifier", r.Identifier),
			slog.String("workspace", r.WorkspacePath),
		)
		switch {
		case inspectErr != nil:
			entryLog.Warn("startup cleanup: cannot inspect active round path; leaving for the engine",
				slog.Any("error", inspectErr),
			)
		case existence == workspace.WorkspaceAbsent:
			if err := store.CompleteCleanupRound(ctx, r.ID, persistence.CleanupCompletedAlreadyGone, nowRFC); err != nil {
				entryLog.Error("startup cleanup: failed to record already-gone round",
					slog.Any("error", err),
				)
				continue
			}
			r.Status = persistence.CleanupRoundCompleted
			r.CompletedReason = persistence.CleanupCompletedAlreadyGone
			r.CompletedAt = nowRFC
			r.NextDueAtMS = 0
			delete(state.cleanupRoundIssueIdx, r.IssueID)
			delete(state.cleanupRoundKeyIdx, r.WorkspaceKey)
			entryLog.Info("startup cleanup: active round path already absent, recording completed")
			metrics.IncReconciliationActions(actionCleanupRoundGone)
		case existence == workspace.WorkspaceUnsafeEntry && r.RemovalState != persistence.CleanupRemovalUnsafe:
			r.RemovalState = persistence.CleanupRemovalUnsafe
			r.RemovalAttempts++
			r.LastRemovalError = "startup: pinned path is a symlink or non-directory entry"
			r.UpdatedAt = nowRFC
			if err := store.SaveCleanupRound(ctx, *r); err != nil {
				entryLog.Error("startup cleanup: failed to record unsafe path",
					slog.Any("error", err),
				)
				continue
			}
			entryLog.Error("startup cleanup: unsafe pinned path, leaving for operator")
			metrics.IncReconciliationActions(actionCleanupRoundBlocked)
		}
	}
}

// adoptTerminalDiskWorkspaces enumerates the current workspace root and
// adopts directories that are terminal at the tracker and have neither
// an active round nor in-flight orchestrator state. A directory that
// reappears after a completed round is a new episode and is evaluated
// normally: completed rows never suppress adoption.
func adoptTerminalDiskWorkspaces(params RecoverCleanupRoundsParams, log *slog.Logger, metrics domain.Metrics, now time.Time) {
	if params.WorkspaceRoot == "" || params.TrackerAdapter == nil {
		return
	}
	keys, err := workspace.ListWorkspaceKeys(params.WorkspaceRoot)
	if err != nil {
		log.Warn("startup cleanup: failed to list workspace keys", slog.Any("error", err))
		return
	}
	exclusions := buildSweepExclusions(params.State, log)
	unclaimed := make([]string, 0, len(keys))
	for _, key := range keys {
		if _, excluded := exclusions[key]; excluded {
			continue
		}
		pr, pathErr := workspace.ComputePath(params.WorkspaceRoot, key)
		if pathErr != nil {
			log.Warn("startup cleanup: failed to resolve workspace path",
				slog.String("workspace_key", key), slog.Any("error", pathErr))
			continue
		}
		existing, activeErr := params.Store.GetActiveCleanupRoundByPath(params.Ctx, pr.Path)
		if activeErr != nil {
			log.Warn("startup cleanup: active round lookup failed; skipping key this pass",
				slog.String("workspace_key", key), slog.Any("error", activeErr))
			continue
		}
		if existing != nil {
			continue
		}
		unclaimed = append(unclaimed, key)
	}
	if len(unclaimed) == 0 {
		return
	}

	statesByKey, fetchErr := params.TrackerAdapter.FetchIssueStatesByIdentifiers(params.Ctx, unclaimed)
	if fetchErr != nil {
		log.Warn("startup cleanup: failed to fetch issue states for unclaimed workspaces; sweep will adopt them",
			slog.Any("error", fetchErr),
		)
		return
	}

	adopted := 0
	for _, key := range unclaimed {
		stateName, known := statesByKey[key]
		if !known || !isTerminalState(stateName, params.TerminalStates) {
			continue
		}
		pr, pathErr := workspace.ComputePath(params.WorkspaceRoot, key)
		if pathErr != nil {
			continue
		}
		_, created, ensureErr := EnsureCleanupRoundFromDisk(params.Ctx, params.Store, params.State, DiskObservationParams{
			Path:           pr.Path,
			Identifier:     key,
			TerminalState:  stateName,
			TerminalSource: persistence.CleanupSourceStartup,
			HookScript:     params.BeforeRemoveHook,
			HookTimeoutMS:  params.HookTimeoutMS,
			NowFunc:        func() time.Time { return now },
			Logger:         log,
		})
		if ensureErr != nil {
			log.Warn("startup cleanup: failed to adopt terminal workspace",
				slog.String("workspace_key", key), slog.Any("error", ensureErr))
			continue
		}
		if created {
			adopted++
			metrics.IncReconciliationActions(actionSweepCleanup)
		}
	}
	if adopted > 0 {
		log.Info("startup cleanup: adopted terminal workspaces as cleanup rounds",
			slog.Int("count", adopted),
		)
	}
}
