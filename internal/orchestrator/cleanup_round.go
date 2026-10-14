package orchestrator

import (
	"context"
	"log/slog"
	"path/filepath"
	"time"

	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/workspace"
)

// CleanupRoundStore is the persistence surface the cleanup-round
// factory, gate, and engine depend on. [persistence.Store] satisfies it.
type CleanupRoundStore interface {
	CreateCleanupRound(ctx context.Context, round persistence.CleanupRound) (persistence.CleanupRound, error)
	SaveCleanupRound(ctx context.Context, round persistence.CleanupRound) error
	GetActiveCleanupRoundByPath(ctx context.Context, path string) (*persistence.CleanupRound, error)
	ListCleanupRounds(ctx context.Context, activeOnly bool) ([]persistence.CleanupRound, error)
	CompleteCleanupRound(ctx context.Context, id int64, reason, completedAt string) error
	LatestAgentRunByIdentifier(ctx context.Context, identifier string) (persistence.LatestDispatch, bool, error)
}

var _ CleanupRoundStore = (*persistence.Store)(nil)

// cleanupRoundBlocksIssue reports whether an active cleanup round bars
// new dispatch for an issue identity. A round matches on either the
// issue ID or the sanitized workspace key, so an ownerless round still
// bars a candidate carrying the same identifier. Empty inputs match
// nothing.
func cleanupRoundBlocksIssue(state *State, issueID, identifier string) bool {
	if issueID != "" {
		if _, ok := state.cleanupRoundIssueIdx[issueID]; ok {
			return true
		}
	}
	if identifier != "" {
		if key, err := workspace.SanitizeKey(identifier); err == nil {
			if _, ok := state.cleanupRoundKeyIdx[key]; ok {
				return true
			}
		}
	}
	return false
}

// PopulateCleanupRounds loads persisted rounds into state before the
// first dispatch decision. Active rounds populate the gate indexes;
// completed rows are retained by pinned path so startup reconciliation
// can tell an already-cleaned path from a directory belonging to a new
// lifecycle episode.
func PopulateCleanupRounds(state *State, rows []persistence.CleanupRound) {
	if state.CleanupRounds == nil {
		state.CleanupRounds = make(map[int64]*persistence.CleanupRound, len(rows))
	}
	state.cleanupRoundIssueIdx = make(map[string]struct{})
	state.cleanupRoundKeyIdx = make(map[string]struct{})
	for i := range rows {
		r := rows[i]
		state.CleanupRounds[r.ID] = &r
		if r.Status != persistence.CleanupRoundActive {
			continue
		}
		if r.IssueID != "" {
			state.cleanupRoundIssueIdx[r.IssueID] = struct{}{}
		}
		if r.WorkspaceKey != "" {
			state.cleanupRoundKeyIdx[r.WorkspaceKey] = struct{}{}
		}
	}
}

func indexCleanupRound(state *State, r *persistence.CleanupRound) {
	if state.CleanupRounds == nil {
		state.CleanupRounds = make(map[int64]*persistence.CleanupRound)
	}
	state.CleanupRounds[r.ID] = r
	if r.Status != persistence.CleanupRoundActive {
		return
	}
	if state.cleanupRoundIssueIdx == nil {
		state.cleanupRoundIssueIdx = make(map[string]struct{})
	}
	if state.cleanupRoundKeyIdx == nil {
		state.cleanupRoundKeyIdx = make(map[string]struct{})
	}
	if r.IssueID != "" {
		state.cleanupRoundIssueIdx[r.IssueID] = struct{}{}
	}
	if r.WorkspaceKey != "" {
		state.cleanupRoundKeyIdx[r.WorkspaceKey] = struct{}{}
	}
}

// EnsureCleanupRoundParams carries the facts an active cleanup round is
// created with. Every field except Path, Identifier, TerminalState and
// TerminalSource may be empty: ownerless rounds and workspaces that
// never recorded a dispatch are valid inputs.
type EnsureCleanupRoundParams struct {
	// Path is the pinned absolute canonical workspace path. Required.
	Path string

	// Issue identity; IssueID empty for an ownerless round.
	IssueID        string
	Identifier     string
	DisplayID      string
	IssueTitle     string
	IssueURL       string
	SSHHost        string
	LastDispatchID string
	LastRunID      int64
	LastAttempt    int
	// LastDispatchedAt is RFC3339; fall back to the dispatch start when
	// the pass carries no lineage time.
	LastDispatchedAt string
	LastAgentKind    string

	TerminalState  string
	TerminalSource string

	HookScript    string
	HookTimeoutMS int

	// AlreadyQuiesced is true for observations with no live occupant
	// (sweep, startup, non-running reconcile, worker exit): quiesced_at
	// is written at creation so the engine reaches the hook stage
	// immediately. A round for a still-running worker stays false until
	// the worker exit confirms quiescence.
	AlreadyQuiesced bool

	NowFunc func() time.Time
	Logger  *slog.Logger
}

// EnsureCleanupRound returns the active round pinned to params.Path,
// creating it on the first observation. The second and later
// observation returns the existing row unchanged, so every terminal
// trigger point is idempotent. The boolean reports whether a new row
// was created.
func EnsureCleanupRound(ctx context.Context, store CleanupRoundStore, state *State, params EnsureCleanupRoundParams) (*persistence.CleanupRound, bool, error) {
	existing, err := store.GetActiveCleanupRoundByPath(ctx, params.Path)
	if err != nil {
		return nil, false, err
	}
	if existing != nil {
		if state.CleanupRounds == nil || state.CleanupRounds[existing.ID] == nil {
			indexCleanupRound(state, existing)
		}
		return existing, false, nil
	}

	now := time.Now().UTC()
	if params.NowFunc != nil {
		now = params.NowFunc().UTC()
	}
	nowRFC := now.Format(time.RFC3339)

	key, err := workspace.SanitizeKey(params.Identifier)
	if err != nil {
		return nil, false, err
	}

	hookState := persistence.CleanupHookPending
	if params.HookScript == "" {
		hookState = persistence.CleanupHookSkipped
	}

	round := persistence.CleanupRound{
		Status:            persistence.CleanupRoundActive,
		IssueID:           params.IssueID,
		Identifier:        params.Identifier,
		DisplayIdentifier: params.DisplayID,
		IssueTitle:        params.IssueTitle,
		IssueURL:          params.IssueURL,
		WorkspaceKey:      key,
		WorkspacePath:     params.Path,
		// The pinned path is a direct child of its root by the ComputePath
		// containment contract, so its directory is the actual root the
		// workspace lived under, independent of later config edits.
		WorkspaceRoot:    filepath.Dir(params.Path),
		SSHHost:          params.SSHHost,
		LastDispatchID:   params.LastDispatchID,
		LastRunID:        params.LastRunID,
		LastAttempt:      params.LastAttempt,
		LastDispatchedAt: params.LastDispatchedAt,
		LastAgentKind:    params.LastAgentKind,

		TerminalState:      params.TerminalState,
		TerminalSource:     params.TerminalSource,
		TerminalObservedAt: nowRFC,

		HookScript:    params.HookScript,
		HookTimeoutMS: params.HookTimeoutMS,

		GateConfirmedAt: nowRFC,
		HookState:       hookState,
		RemovalState:    persistence.CleanupRemovalPending,

		NextDueAtMS: 0,
		CreatedAt:   nowRFC,
		UpdatedAt:   nowRFC,
	}
	if params.AlreadyQuiesced {
		round.QuiescedAt = nowRFC
	}

	created, err := store.CreateCleanupRound(ctx, round)
	if err != nil {
		return nil, false, err
	}
	indexCleanupRound(state, &created)

	log := params.Logger
	if log != nil {
		log.InfoContext(ctx, "cleanup round created",
			slog.Int64("round_id", created.ID),
			slog.String("issue_id", created.IssueID),
			slog.String("identifier", created.Identifier),
			slog.String("workspace", created.WorkspacePath),
			slog.String("terminal_state", created.TerminalState),
			slog.String("terminal_source", created.TerminalSource),
			slog.Bool("quiesced", created.QuiescedAt != ""),
		)
	}
	return &created, true, nil
}

// DiskObservationParams carries a terminal observation made from a disk
// enumeration (periodic sweep or startup cleanup), where the only facts
// on hand are the pinned path and the tracker state name. Dispatch
// facts are backfilled from the newest agent run_history row.
type DiskObservationParams struct {
	Path           string
	Identifier     string
	TerminalState  string
	TerminalSource string
	HookScript     string
	HookTimeoutMS  int
	NowFunc        func() time.Time
	Logger         *slog.Logger
}

// EnsureCleanupRoundFromDisk adopts a terminal directory found on disk
// as a quiesced round, backfilling issue identity and last-dispatch
// facts from run_history. Rows created here never wait on a running
// occupant: sweep and startup both exclude in-flight work from their
// candidate set.
func EnsureCleanupRoundFromDisk(ctx context.Context, store CleanupRoundStore, state *State, params DiskObservationParams) (*persistence.CleanupRound, bool, error) {
	ensure := EnsureCleanupRoundParams{
		Path:            params.Path,
		Identifier:      params.Identifier,
		TerminalState:   params.TerminalState,
		TerminalSource:  params.TerminalSource,
		HookScript:      params.HookScript,
		HookTimeoutMS:   params.HookTimeoutMS,
		AlreadyQuiesced: true,
		NowFunc:         params.NowFunc,
		Logger:          params.Logger,
	}

	dispatch, found, err := store.LatestAgentRunByIdentifier(ctx, params.Identifier)
	if err != nil {
		if params.Logger != nil {
			params.Logger.WarnContext(ctx, "cleanup round: dispatch facts lookup failed, adopting without them",
				slog.String("identifier", params.Identifier),
				slog.Any("error", err),
			)
		}
	} else if found {
		ensure.IssueID = dispatch.IssueID
		ensure.DisplayID = dispatch.DisplayID
		ensure.LastRunID = dispatch.RunID
		ensure.LastAttempt = dispatch.Attempt
		ensure.LastAgentKind = dispatch.AgentKind
		ensure.IssueTitle = dispatch.IssueTitle
		ensure.IssueURL = dispatch.IssueURL
		if dispatch.DispatchedAt != "" {
			ensure.LastDispatchedAt = dispatch.DispatchedAt
		} else {
			ensure.LastDispatchedAt = dispatch.StartedAt
		}
	}

	return EnsureCleanupRound(ctx, store, state, ensure)
}

// ReconcileTerminalRoundParams carries a terminal observation made by
// the active-run reconciliation pass. A running entry means the round
// must wait for its worker to exit (AlreadyQuiesced stays false); a
// nil entry (a pending-reaction or stage-hop holder without a worker)
// creates an immediately quiesced round.
type ReconcileTerminalRoundParams struct {
	IssueID       string
	Identifier    string
	TerminalState string
	Running       *RunningEntry

	WorkspaceRoot string
	HookScript    string
	HookTimeoutMS int

	NowFunc func() time.Time
	Logger  *slog.Logger
}

// EnsureReconcileTerminalRound creates (or returns the existing) cleanup
// round for a terminal state observed by reconciliation. The pinned path
// is the running entry's dispatch-time path when known, then the newest
// run_history workspace, and only as a last resort a path derived from
// the configured root, so a workspace-root reload cannot redirect the
// round away from the directory the dispatch actually used.
func EnsureReconcileTerminalRound(ctx context.Context, store CleanupRoundStore, state *State, params ReconcileTerminalRoundParams) (*persistence.CleanupRound, bool, error) {
	ensure := EnsureCleanupRoundParams{
		IssueID:        params.IssueID,
		Identifier:     params.Identifier,
		TerminalState:  params.TerminalState,
		TerminalSource: persistence.CleanupSourceReconcile,
		HookScript:     params.HookScript,
		HookTimeoutMS:  params.HookTimeoutMS,
		NowFunc:        params.NowFunc,
		Logger:         params.Logger,
	}

	if params.Running != nil {
		e := params.Running
		ensure.Path = e.WorkspacePath
		if ensure.IssueID == "" {
			ensure.IssueID = e.Issue.ID
		}
		ensure.DisplayID = e.Issue.DisplayID
		identity := issueIdentityOf(e.Issue)
		ensure.IssueTitle = identity.Title
		ensure.IssueURL = identity.URL
		ensure.SSHHost = e.SSHHost
		ensure.LastDispatchID = e.DispatchID
		ensure.LastAttempt = normalizeAttempt(e.RetryAttempt) + 1
		ensure.LastAgentKind = e.AgentKind
		if !e.StartedAt.IsZero() {
			ensure.LastDispatchedAt = e.StartedAt.Format(time.RFC3339)
		}
		if t := e.StageLineage.DispatchedAt; !t.IsZero() {
			ensure.LastDispatchedAt = t.Format(time.RFC3339)
		}
		// The worker is being cancelled and only its exit confirms
		// quiescence.
		ensure.AlreadyQuiesced = false
	} else {
		if dispatch, found, err := store.LatestAgentRunByIdentifier(ctx, params.Identifier); err != nil {
			if params.Logger != nil {
				params.Logger.WarnContext(ctx, "cleanup round: dispatch facts lookup failed",
					slog.String("identifier", params.Identifier), slog.Any("error", err))
			}
		} else if found {
			ensure.Path = dispatch.Workspace
			if ensure.IssueID == "" {
				ensure.IssueID = dispatch.IssueID
			}
			ensure.DisplayID = dispatch.DisplayID
			ensure.LastRunID = dispatch.RunID
			ensure.LastAttempt = dispatch.Attempt
			ensure.LastAgentKind = dispatch.AgentKind
			ensure.IssueTitle = dispatch.IssueTitle
			ensure.IssueURL = dispatch.IssueURL
			if dispatch.DispatchedAt != "" {
				ensure.LastDispatchedAt = dispatch.DispatchedAt
			} else {
				ensure.LastDispatchedAt = dispatch.StartedAt
			}
		}
		ensure.AlreadyQuiesced = true
	}

	if ensure.Path == "" {
		pr, err := workspace.ComputePath(params.WorkspaceRoot, params.Identifier)
		if err != nil {
			return nil, false, err
		}
		ensure.Path = pr.Path
	}

	return EnsureCleanupRound(ctx, store, state, ensure)
}

// activeCleanupRoundForIssue returns the active round pinned to an issue
// ID, or nil.
func activeCleanupRoundForIssue(state *State, issueID string) *persistence.CleanupRound {
	if issueID == "" {
		return nil
	}
	for _, r := range state.CleanupRounds {
		if r.Status == persistence.CleanupRoundActive && r.IssueID == issueID {
			return r
		}
	}
	return nil
}

// BuildExitRoundParams pins a round's facts from a worker that just
// observed a terminal issue. The actual workspace path comes from the
// worker result, falling back to the running entry's dispatch-time pin;
// callers MUST skip round creation when both are empty rather than
// reconstructing a path from current config.
func BuildExitRoundParams(entry *RunningEntry, result WorkerResult, terminalState, terminalSource string, hookScript string, hookTimeoutMS int, lastRunID int64, now time.Time) (EnsureCleanupRoundParams, bool) {
	path := result.WorkspacePath
	if path == "" && entry != nil {
		path = entry.WorkspacePath
	}
	if path == "" || result.Identifier == "" {
		// Terminal state and source stay on the params for logging even
		// when no round can be pinned; zero value, false skips creation.
		return EnsureCleanupRoundParams{TerminalState: terminalState, TerminalSource: terminalSource}, false
	}

	issueID := result.IssueID
	identifier := result.Identifier
	var displayID, title, url, dispatchID, sshHost, agentKind, dispatchedAt string
	// LastAttempt is stored 1-based for display, matching run_history.
	attempt := normalizeAttempt(result.Attempt) + 1

	if entry != nil {
		if issueID == "" {
			issueID = entry.Issue.ID
		}
		if identifier == "" {
			identifier = entry.Identifier
		}
		displayID = entry.Issue.DisplayID
		identity := issueIdentityOf(entry.Issue)
		title = identity.Title
		url = identity.URL
		dispatchID = entry.DispatchID
		sshHost = entry.SSHHost
		agentKind = entry.AgentKind
		if !entry.StartedAt.IsZero() {
			dispatchedAt = entry.StartedAt.Format(time.RFC3339)
		}
		if t := entry.StageLineage.DispatchedAt; !t.IsZero() {
			dispatchedAt = t.Format(time.RFC3339)
		}
		if result.Attempt == nil {
			attempt = normalizeAttempt(entry.RetryAttempt) + 1
		}
	}

	return EnsureCleanupRoundParams{
		Path:             path,
		IssueID:          issueID,
		Identifier:       identifier,
		DisplayID:        displayID,
		IssueTitle:       title,
		IssueURL:         url,
		SSHHost:          sshHost,
		LastDispatchID:   dispatchID,
		LastRunID:        lastRunID,
		LastAttempt:      attempt,
		LastDispatchedAt: dispatchedAt,
		LastAgentKind:    agentKind,
		TerminalState:    terminalState,
		TerminalSource:   terminalSource,
		HookScript:       hookScript,
		HookTimeoutMS:    hookTimeoutMS,
		// The running entry is removed in the same exit handler that
		// creates this round, so no occupant remains.
		AlreadyQuiesced: true,
		NowFunc:         func() time.Time { return now },
	}, true
}
