package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Reaction triage round stages. A round is the recoverable processing
// turn one fingerprint of one issue and reaction kind travels through.
// Each value names a confirmed stage: a reconcile pass resumes from the
// stored stage and performs only the work the stage has not confirmed.
const (
	// ReactionTriageStageStarted means the external triage command was
	// launched but no result has been recorded yet. A round left at this
	// stage across a restart names a command that was still in flight at
	// shutdown: the command MUST NOT be re-run for this fingerprint.
	ReactionTriageStageStarted = "started"

	// ReactionTriageStageResult means the command result (or the
	// fallback that defaulted it) is recorded. Disposition and Fallback
	// are valid from this stage on.
	ReactionTriageStageResult = "result"

	// ReactionTriageStageContinuation means a dispatch-agent result has
	// created the continuation the reaction schedules. The continuation
	// itself lives in the in-memory retry machinery, so a round resumed
	// at this stage creates it once more rather than re-running triage.
	ReactionTriageStageContinuation = "continuation"

	// ReactionTriageStageEscalated means an escalate result's
	// escalation was applied. A round resumed here never escalates again.
	ReactionTriageStageEscalated = "escalated"

	// ReactionTriageStageDispatched means the fingerprint's dispatched
	// marker landed after the round's terminal action.
	ReactionTriageStageDispatched = "dispatched"

	// ReactionTriageStageHandedOff means the continuation's handed-off
	// comments were saved after its run exited normally. Only the review
	// and bot-review kinds reach it.
	ReactionTriageStageHandedOff = "handed_off"
)

// ReactionTriageRound is one fingerprint's recoverable triage processing
// round, mirrored from the reaction_triage_rounds table.
type ReactionTriageRound struct {
	IssueID      string
	Kind         string
	Fingerprint  string
	Stage        string
	Disposition  string
	Fallback     string
	AttemptsUsed int
	StartedAt    time.Time
	ResultAt     time.Time
	CompletedAt  time.Time
}

// StartReactionTriageRound opens a round for the fingerprint before its
// triage command launches. When no row exists it inserts a started round;
// when the existing row carries a different fingerprint it replaces it
// wholesale with a fresh started round, so the old round stops affecting
// the new subject. When the existing row already carries the same
// fingerprint it is left untouched, because the stage that row records is
// authoritative for it.
func (s *Store) StartReactionTriageRound(
	ctx context.Context,
	issueID, kind, fingerprint string,
	attemptsUsed int,
	startedAt time.Time,
) error {
	startedAtText := startedAt.UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO reaction_triage_rounds
			(issue_id, kind, fingerprint, stage, disposition, fallback, attempts_used, started_at, result_at, completed_at, updated_at)
		VALUES (?, ?, ?, ?, '', '', ?, ?, '', '', ?)
		ON CONFLICT (issue_id, kind) DO UPDATE SET
			fingerprint   = excluded.fingerprint,
			stage         = 'started',
			disposition   = '',
			fallback      = '',
			attempts_used = excluded.attempts_used,
			started_at    = excluded.started_at,
			result_at     = '',
			completed_at  = '',
			updated_at    = excluded.updated_at
		WHERE excluded.fingerprint != reaction_triage_rounds.fingerprint`,
		issueID, kind, fingerprint, ReactionTriageStageStarted, attemptsUsed, startedAtText, startedAtText,
	)
	if err != nil {
		return fmt.Errorf("start reaction triage round for issue %q kind %q: %w", issueID, kind, err)
	}
	return nil
}

// SaveReactionTriageRoundResult records the command result on the round
// the fingerprint names, advancing a started round to result. It is a
// no-op when the row is absent, carries a different fingerprint, or has
// already advanced past started: a late result can never overwrite a
// resumed round.
func (s *Store) SaveReactionTriageRoundResult(
	ctx context.Context,
	issueID, kind, fingerprint, disposition, fallback string,
	resultAt time.Time,
) error {
	resultAtText := resultAt.UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `
		UPDATE reaction_triage_rounds
		SET stage = ?, disposition = ?, fallback = ?, result_at = ?, updated_at = ?
		WHERE issue_id = ? AND kind = ? AND fingerprint = ? AND stage = ?`,
		ReactionTriageStageResult, disposition, fallback, resultAtText, resultAtText,
		issueID, kind, fingerprint, ReactionTriageStageStarted,
	)
	if err != nil {
		return fmt.Errorf("save reaction triage round result for issue %q kind %q: %w", issueID, kind, err)
	}
	return nil
}

// AdvanceReactionTriageRound moves the round the fingerprint names to
// stage while the row still carries that fingerprint, but never moves a
// round backward or out of its stage: a transition lands only from the
// stage it legitimately follows, so repeating the call after the
// transition landed (or after a crash that repeated the call) changes
// nothing. completedAt is set exactly for the terminal stages. The
// transitions are:
//
//   - continuation <- result
//   - escalated    <- result              (terminal)
//   - dispatched   <- result, continuation (terminal)
//   - handed_off   <- continuation, dispatched (terminal)
func (s *Store) AdvanceReactionTriageRound(
	ctx context.Context,
	issueID, kind, fingerprint, stage string,
	at time.Time,
) error {
	atText := at.UTC().Format(time.RFC3339Nano)
	args := []any{stage, atText, issueID, kind, fingerprint}

	var query string
	switch stage {
	case ReactionTriageStageContinuation:
		query = `
			UPDATE reaction_triage_rounds
			SET stage = ?, updated_at = ?
			WHERE issue_id = ? AND kind = ? AND fingerprint = ?
			  AND stage = 'result'`
	case ReactionTriageStageEscalated:
		args = []any{stage, atText, atText, issueID, kind, fingerprint}
		query = `
			UPDATE reaction_triage_rounds
			SET stage = ?, completed_at = ?, updated_at = ?
			WHERE issue_id = ? AND kind = ? AND fingerprint = ?
			  AND stage = 'result'`
	case ReactionTriageStageDispatched:
		args = []any{stage, atText, atText, issueID, kind, fingerprint}
		query = `
			UPDATE reaction_triage_rounds
			SET stage = ?, completed_at = ?, updated_at = ?
			WHERE issue_id = ? AND kind = ? AND fingerprint = ?
			  AND stage IN ('result', 'continuation')`
	case ReactionTriageStageHandedOff:
		args = []any{stage, atText, atText, issueID, kind, fingerprint}
		query = `
			UPDATE reaction_triage_rounds
			SET stage = ?, completed_at = ?, updated_at = ?
			WHERE issue_id = ? AND kind = ? AND fingerprint = ?
			  AND stage IN ('continuation', 'dispatched')`
	default:
		return fmt.Errorf("advance reaction triage round for issue %q kind %q: unknown stage %q", issueID, kind, stage)
	}

	if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("advance reaction triage round for issue %q kind %q to %q: %w", issueID, kind, stage, err)
	}
	return nil
}

// MarkReactionTriageRoundDispatched advances the issue and kind's
// current round to dispatched without naming a fingerprint: the shared
// retry-dispatch path that confirms the continuation launched knows only
// the issue and kind. A result-stage round reaches dispatched here too,
// which covers a kind whose dispatched marker lands on the same tick as
// the continuation; a round at any other stage is left untouched. It
// never moves a terminal escalated or handed-off round backward.
func (s *Store) MarkReactionTriageRoundDispatched(ctx context.Context, issueID, kind string, at time.Time) error {
	atText := at.UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `
		UPDATE reaction_triage_rounds
		SET stage = ?, completed_at = ?, updated_at = ?
		WHERE issue_id = ? AND kind = ?
		  AND stage IN (?, ?)`,
		ReactionTriageStageDispatched, atText, atText,
		issueID, kind,
		ReactionTriageStageResult, ReactionTriageStageContinuation,
	)
	if err != nil {
		return fmt.Errorf("mark reaction triage round dispatched for issue %q kind %q: %w", issueID, kind, err)
	}
	return nil
}

// MarkReactionTriageRoundHandedOff advances the issue and kind's current
// round to handed_off without naming a fingerprint: the worker exit that
// saves handed-off comments continues the round the dispatched run was
// created from, and only a round at continuation or dispatched can be
// that round. A no-op everywhere else, it never changes a round bound to
// an older or newer subject incorrectly because escalation and
// superseding delete such rows.
func (s *Store) MarkReactionTriageRoundHandedOff(ctx context.Context, issueID, kind string, at time.Time) error {
	atText := at.UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(ctx, `
		UPDATE reaction_triage_rounds
		SET stage = ?, completed_at = ?, updated_at = ?
		WHERE issue_id = ? AND kind = ?
		  AND stage IN (?, ?)`,
		ReactionTriageStageHandedOff, atText, atText,
		issueID, kind,
		ReactionTriageStageContinuation, ReactionTriageStageDispatched,
	)
	if err != nil {
		return fmt.Errorf("mark reaction triage round handed off for issue %q kind %q: %w", issueID, kind, err)
	}
	return nil
}

// GetReactionTriageRound returns the current round for the issue and
// kind, or nil when none exists.
func (s *Store) GetReactionTriageRound(ctx context.Context, issueID, kind string) (*ReactionTriageRound, error) {
	var (
		r                                            ReactionTriageRound
		startedAtText, resultAtText, completedAtText string
	)
	err := s.db.QueryRowContext(ctx, `
		SELECT issue_id, kind, fingerprint, stage, disposition, fallback,
		       attempts_used, started_at, result_at, completed_at
		FROM reaction_triage_rounds
		WHERE issue_id = ? AND kind = ?`,
		issueID, kind,
	).Scan(
		&r.IssueID, &r.Kind, &r.Fingerprint, &r.Stage, &r.Disposition, &r.Fallback,
		&r.AttemptsUsed, &startedAtText, &resultAtText, &completedAtText,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get reaction triage round for issue %q kind %q: %w", issueID, kind, err)
	}
	r.StartedAt = parseRoundTime(startedAtText)
	r.ResultAt = parseRoundTime(resultAtText)
	r.CompletedAt = parseRoundTime(completedAtText)
	return &r, nil
}

// DeleteReactionTriageRound removes the current round for the issue and
// kind. It is called when the episode the round belonged to closes
// (subject resolved, escalation applied, or the pending entry dropped),
// so the next subject opens a fresh round. A missing row is not an error.
func (s *Store) DeleteReactionTriageRound(ctx context.Context, issueID, kind string) error {
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM reaction_triage_rounds WHERE issue_id = ? AND kind = ?`,
		issueID, kind,
	)
	if err != nil {
		return fmt.Errorf("delete reaction triage round for issue %q kind %q: %w", issueID, kind, err)
	}
	return nil
}

// parseRoundTime parses an RFC 3339Nano timestamp the round columns
// carry, returning the zero time for an empty value.
func parseRoundTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	at, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}
	return at.UTC()
}
