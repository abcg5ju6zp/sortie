package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Cleanup round lifecycle statuses.
const (
	// CleanupRoundActive is the only status the engine advances; a round
	// stays active until the pinned path is verified gone.
	CleanupRoundActive = "active"
	// CleanupRoundCompleted is the terminal status; the row is retained as
	// the durable "this path was cleaned" fact.
	CleanupRoundCompleted = "completed"
)

// before_remove hook stage outcomes.
const (
	CleanupHookPending         = "pending"          // hook not reached yet
	CleanupHookRunning         = "running"          // durable "hook started" marker written before launch
	CleanupHookSucceeded       = "succeeded"        // exit code 0
	CleanupHookFailed          = "failed"           // non-zero exit or local launch error
	CleanupHookTimeout         = "timeout"          // timeout or cancellation
	CleanupHookInterrupted     = "interrupted"      // "running" marker survived a restart without an outcome
	CleanupHookHostUnreachable = "host_unreachable" // pinned SSH host could not be reached; delivery unconfirmed
	CleanupHookSkipped         = "skipped"          // the round pinned no hook script
)

// Workspace tree removal stage outcomes.
const (
	CleanupRemovalPending = "pending" // removal not reached yet
	CleanupRemovalGone    = "gone"    // pinned path verified absent
	CleanupRemovalFailed  = "failed"  // RemoveAll failed or the path still stat-ed present
	CleanupRemovalUnsafe  = "unsafe"  // path failed safety verification; left for an operator
)

// Cleanup completion reasons.
const (
	CleanupCompletedRemoved     = "removed"      // the engine removed the tree and verified it gone
	CleanupCompletedAlreadyGone = "already_gone" // the path was already absent when the round ran
)

// Cleanup round terminal observation sources.
const (
	CleanupSourceReconcile  = "reconcile"
	CleanupSourceWorkerExit = "worker_exit"
	CleanupSourceSweep      = "sweep"
	CleanupSourceStartup    = "startup"
)

// CleanupRound is one row of workspace_cleanup_rounds: a resumable
// cleanup episode for one terminal-state workspace. The identity,
// dispatch, terminal-observation, and hook-snapshot fields are pinned at
// creation and never updated; the stage fields advance as the engine
// processes the round.
type CleanupRound struct {
	ID     int64
	Status string

	// Issue identity; IssueID is empty for an ownerless round adopted
	// from a disk listing the tracker read cannot map to an issue ID.
	IssueID           string
	Identifier        string
	DisplayIdentifier string
	IssueTitle        string
	IssueURL          string

	// Pinned filesystem facts. WorkspaceRoot is retained so containment
	// of WorkspacePath can be re-verified on every process.
	WorkspaceKey  string
	WorkspacePath string
	WorkspaceRoot string

	// SSHHost is the host the last dispatch ran on, empty for local.
	// It feeds SORTIE_SSH_HOST and the host-unreachable classification.
	SSHHost string

	// Last dispatch facts, best effort: empty/zero for a workspace that
	// never recorded an agent run.
	LastDispatchID   string
	LastRunID        int64
	LastAttempt      int
	LastDispatchedAt string
	LastAgentKind    string

	// Terminal observation pinned when the round was created.
	TerminalState      string
	TerminalSource     string
	TerminalObservedAt string

	// HookScript is the before_remove script snapshot in force at
	// creation; empty means the round runs no hook.
	HookScript    string
	HookTimeoutMS int

	// Stage timestamps and outcomes.
	GateConfirmedAt string
	QuiescedAt      string

	HookState      string
	HookStartedAt  string
	HookFinishedAt string
	HookAttempts   int
	HookExitCode   int
	HookOutput     string

	RemovalState     string
	RemovalAttempts  int
	LastRemovalError string

	CompletedReason string
	CompletedAt     string

	NextDueAtMS int64

	CreatedAt string
	UpdatedAt string
}

// LatestDispatch holds the dispatch-fact columns of the newest agent
// run_history row for one identifier. Synthetic non-agent rows (for
// example ci_failed) are excluded.
type LatestDispatch struct {
	RunID        int64
	IssueID      string
	DisplayID    string
	Attempt      int
	Workspace    string
	StartedAt    string
	DispatchedAt string
	AgentKind    string
	IssueTitle   string
	IssueURL     string
}

// cleanupRoundColumns is the canonical column order scanned by every
// cleanup round read.
const cleanupRoundColumns = `id, status, issue_id, identifier, display_identifier, issue_title, issue_url,
	workspace_key, workspace_path, workspace_root, ssh_host,
	last_dispatch_id, last_run_id, last_attempt, last_dispatched_at, last_agent_kind,
	terminal_state, terminal_source, terminal_observed_at,
	hook_script, hook_timeout_ms,
	gate_confirmed_at, quiesced_at,
	hook_state, hook_started_at, hook_finished_at, hook_attempts, hook_exit_code, hook_output,
	removal_state, removal_attempts, last_removal_error,
	completed_reason, completed_at, next_due_at_ms, created_at, updated_at`

func scanCleanupRound(scanner interface {
	Scan(dest ...any) error
}) (CleanupRound, error) {
	var r CleanupRound
	var issueID sql.NullString
	err := scanner.Scan(
		&r.ID, &r.Status, &issueID, &r.Identifier, &r.DisplayIdentifier, &r.IssueTitle, &r.IssueURL,
		&r.WorkspaceKey, &r.WorkspacePath, &r.WorkspaceRoot, &r.SSHHost,
		&r.LastDispatchID, &r.LastRunID, &r.LastAttempt, &r.LastDispatchedAt, &r.LastAgentKind,
		&r.TerminalState, &r.TerminalSource, &r.TerminalObservedAt,
		&r.HookScript, &r.HookTimeoutMS,
		&r.GateConfirmedAt, &r.QuiescedAt,
		&r.HookState, &r.HookStartedAt, &r.HookFinishedAt, &r.HookAttempts, &r.HookExitCode, &r.HookOutput,
		&r.RemovalState, &r.RemovalAttempts, &r.LastRemovalError,
		&r.CompletedReason, &r.CompletedAt, &r.NextDueAtMS, &r.CreatedAt, &r.UpdatedAt,
	)
	if err != nil {
		return CleanupRound{}, err
	}
	r.IssueID = issueID.String
	return r, nil
}

// CreateCleanupRound inserts a new round row. The caller is expected to
// have checked GetActiveCleanupRoundByPath for idempotency; the active-per-
// path unique index is the final guard. The inserted row's ID is returned.
func (s *Store) CreateCleanupRound(ctx context.Context, r CleanupRound) (CleanupRound, error) {
	if r.Status == "" {
		r.Status = CleanupRoundActive
	}
	if r.HookState == "" {
		r.HookState = CleanupHookPending
	}
	if r.RemovalState == "" {
		r.RemovalState = CleanupRemovalPending
	}
	var issueID any
	if r.IssueID != "" {
		issueID = r.IssueID
	}
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO workspace_cleanup_rounds
		(status, issue_id, identifier, display_identifier, issue_title, issue_url,
		 workspace_key, workspace_path, workspace_root, ssh_host,
		 last_dispatch_id, last_run_id, last_attempt, last_dispatched_at, last_agent_kind,
		 terminal_state, terminal_source, terminal_observed_at,
		 hook_script, hook_timeout_ms,
		 gate_confirmed_at, quiesced_at,
		 hook_state, hook_started_at, hook_finished_at, hook_attempts, hook_exit_code, hook_output,
		 removal_state, removal_attempts, last_removal_error,
		 completed_reason, completed_at, next_due_at_ms, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		r.Status, issueID, r.Identifier, r.DisplayIdentifier, r.IssueTitle, r.IssueURL,
		r.WorkspaceKey, r.WorkspacePath, r.WorkspaceRoot, r.SSHHost,
		r.LastDispatchID, r.LastRunID, r.LastAttempt, r.LastDispatchedAt, r.LastAgentKind,
		r.TerminalState, r.TerminalSource, r.TerminalObservedAt,
		r.HookScript, r.HookTimeoutMS,
		r.GateConfirmedAt, r.QuiescedAt,
		r.HookState, r.HookStartedAt, r.HookFinishedAt, r.HookAttempts, r.HookExitCode, r.HookOutput,
		r.RemovalState, r.RemovalAttempts, r.LastRemovalError,
		r.CompletedReason, r.CompletedAt, r.NextDueAtMS, r.CreatedAt, r.UpdatedAt,
	)
	if err != nil {
		return CleanupRound{}, fmt.Errorf("create cleanup round for %q: %w", r.WorkspacePath, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return CleanupRound{}, fmt.Errorf("read cleanup round id: %w", err)
	}
	r.ID = id
	return r, nil
}

// SaveCleanupRound persists the mutable stage fields of a round. The
// pinned identity/dispatch/observation/hook-snapshot columns are never
// written here, so a stage update cannot redirect or redefine a round.
func (s *Store) SaveCleanupRound(ctx context.Context, r CleanupRound) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE workspace_cleanup_rounds SET
			status = ?,
			gate_confirmed_at = ?,
			quiesced_at = ?,
			hook_state = ?,
			hook_started_at = ?,
			hook_finished_at = ?,
			hook_attempts = ?,
			hook_exit_code = ?,
			hook_output = ?,
			removal_state = ?,
			removal_attempts = ?,
			last_removal_error = ?,
			completed_reason = ?,
			completed_at = ?,
			next_due_at_ms = ?,
			updated_at = ?
		WHERE id = ?`,
		r.Status, r.GateConfirmedAt, r.QuiescedAt,
		r.HookState, r.HookStartedAt, r.HookFinishedAt, r.HookAttempts, r.HookExitCode, r.HookOutput,
		r.RemovalState, r.RemovalAttempts, r.LastRemovalError,
		r.CompletedReason, r.CompletedAt, r.NextDueAtMS, r.UpdatedAt,
		r.ID,
	)
	if err != nil {
		return fmt.Errorf("save cleanup round %d: %w", r.ID, err)
	}
	return nil
}

// GetActiveCleanupRoundByPath returns the active round pinned to the
// given absolute workspace path. It returns (nil, nil) when no active
// round exists for the path.
func (s *Store) GetActiveCleanupRoundByPath(ctx context.Context, path string) (*CleanupRound, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT `+cleanupRoundColumns+`
		FROM workspace_cleanup_rounds
		WHERE workspace_path = ? AND status = ?`,
		path, CleanupRoundActive)
	r, err := scanCleanupRound(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get active cleanup round for %q: %w", path, err)
	}
	return &r, nil
}

// ListCleanupRounds returns cleanup rounds ordered by ID. When activeOnly
// is true only active rounds are returned (the engine's working set);
// otherwise every row including completed facts is returned.
func (s *Store) ListCleanupRounds(ctx context.Context, activeOnly bool) ([]CleanupRound, error) {
	query := `SELECT ` + cleanupRoundColumns + ` FROM workspace_cleanup_rounds`
	if activeOnly {
		query += ` WHERE status = 'active'`
	}
	query += ` ORDER BY id`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("list cleanup rounds: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-only query; close error is non-actionable

	out := []CleanupRound{}
	for rows.Next() {
		r, scanErr := scanCleanupRound(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scan cleanup round: %w", scanErr)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate cleanup rounds: %w", err)
	}
	return out, nil
}

// CompleteCleanupRound marks a round completed with the given reason
// ("removed" or "already_gone") and clears its retry schedule.
func (s *Store) CompleteCleanupRound(ctx context.Context, id int64, reason, completedAt string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE workspace_cleanup_rounds
		SET status = ?, completed_reason = ?, completed_at = ?, next_due_at_ms = 0, updated_at = ?
		WHERE id = ?`,
		CleanupRoundCompleted, reason, completedAt, completedAt, id,
	)
	if err != nil {
		return fmt.Errorf("complete cleanup round %d: %w", id, err)
	}
	return nil
}

// LatestAgentRunByIdentifier returns the dispatch facts of the newest
// agent run_history row (highest id) for the given identifier. Rows
// without an agent adapter, such as the synthetic ci_failed row, are not
// agent runs and are excluded. It returns false when no such row exists.
func (s *Store) LatestAgentRunByIdentifier(ctx context.Context, identifier string) (LatestDispatch, bool, error) {
	var d LatestDispatch
	var issueID, displayID sql.NullString
	err := s.db.QueryRowContext(ctx,
		`SELECT id, issue_id, display_identifier, attempt, workspace, started_at, dispatched_at,
		        agent_adapter, issue_title, issue_url
		 FROM run_history
		 WHERE identifier = ? AND agent_adapter <> ''
		 ORDER BY id DESC
		 LIMIT 1`,
		identifier,
	).Scan(
		&d.RunID, &issueID, &displayID, &d.Attempt, &d.Workspace, &d.StartedAt, &d.DispatchedAt,
		&d.AgentKind, &d.IssueTitle, &d.IssueURL,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return LatestDispatch{}, false, nil
	}
	if err != nil {
		return LatestDispatch{}, false, fmt.Errorf("latest agent run for %q: %w", identifier, err)
	}
	d.IssueID = issueID.String
	d.DisplayID = displayID.String
	return d, true, nil
}
