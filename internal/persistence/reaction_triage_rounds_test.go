package persistence

import (
	"context"
	"testing"
	"time"
)

func TestReactionTriageRoundLifecycle(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openTestStore(t)
	migrateOrFatal(t, s)

	const (
		issueID = "ISS-1"
		kind    = "ci"
		fp      = "head-1"
	)
	start := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)

	// A missing round reads as nil.
	round, err := s.GetReactionTriageRound(ctx, issueID, kind)
	if err != nil {
		t.Fatalf("Get before start: %v", err)
	}
	if round != nil {
		t.Fatalf("round before start = %+v, want nil", round)
	}

	if err := s.StartReactionTriageRound(ctx, issueID, kind, fp, 2, start); err != nil {
		t.Fatalf("Start: %v", err)
	}
	round, err = s.GetReactionTriageRound(ctx, issueID, kind)
	if err != nil {
		t.Fatalf("Get after start: %v", err)
	}
	if round == nil || round.Stage != ReactionTriageStageStarted || round.Fingerprint != fp || round.AttemptsUsed != 2 {
		t.Fatalf("round after start = %+v, want started/%s with attempts 2", round, fp)
	}
	if !round.StartedAt.Equal(start) || !round.ResultAt.IsZero() || !round.CompletedAt.IsZero() {
		t.Errorf("timestamps after start = started %v result %v completed %v", round.StartedAt, round.ResultAt, round.CompletedAt)
	}

	// Starting again for the same fingerprint changes nothing, including
	// the attempts snapshot and the start time.
	if err := s.StartReactionTriageRound(ctx, issueID, kind, fp, 9, start.Add(time.Hour)); err != nil {
		t.Fatalf("Start same fp: %v", err)
	}
	round, _ = s.GetReactionTriageRound(ctx, issueID, kind)
	if round.AttemptsUsed != 2 || !round.StartedAt.Equal(start) {
		t.Errorf("same-fp start replaced the round: %+v", round)
	}

	// Record the command result.
	resultAt := start.Add(2 * time.Second)
	if err := s.SaveReactionTriageRoundResult(ctx, issueID, kind, fp, "dispatch-agent", "", resultAt); err != nil {
		t.Fatalf("SaveResult: %v", err)
	}
	round, _ = s.GetReactionTriageRound(ctx, issueID, kind)
	if round.Stage != ReactionTriageStageResult || round.Disposition != "dispatch-agent" || !round.ResultAt.Equal(resultAt) {
		t.Fatalf("round after result = %+v", round)
	}

	// A late result for the same fingerprint cannot move the round
	// backward.
	if err := s.SaveReactionTriageRoundResult(ctx, issueID, kind, fp, "escalate", "", resultAt.Add(time.Second)); err != nil {
		t.Fatalf("SaveResult late: %v", err)
	}
	round, _ = s.GetReactionTriageRound(ctx, issueID, kind)
	if round.Disposition != "dispatch-agent" {
		t.Errorf("late result overwrote the recorded answer: %+v", round)
	}

	// result -> continuation -> dispatched is the dispatch-agent path.
	if err := s.AdvanceReactionTriageRound(ctx, issueID, kind, fp, ReactionTriageStageContinuation, resultAt.Add(time.Second)); err != nil {
		t.Fatalf("Advance continuation: %v", err)
	}
	round, _ = s.GetReactionTriageRound(ctx, issueID, kind)
	if round.Stage != ReactionTriageStageContinuation || !round.CompletedAt.IsZero() {
		t.Fatalf("round after continuation = %+v, want non-terminal", round)
	}
	// Escalated cannot follow continuation.
	if err := s.AdvanceReactionTriageRound(ctx, issueID, kind, fp, ReactionTriageStageEscalated, time.Now()); err != nil {
		t.Fatalf("Advance escalated: %v", err)
	}
	round, _ = s.GetReactionTriageRound(ctx, issueID, kind)
	if round.Stage != ReactionTriageStageContinuation {
		t.Errorf("illegal escalated transition moved the round: %+v", round)
	}

	completedAt := resultAt.Add(3 * time.Second)
	if err := s.MarkReactionTriageRoundDispatched(ctx, issueID, kind, completedAt); err != nil {
		t.Fatalf("MarkDispatched: %v", err)
	}
	round, _ = s.GetReactionTriageRound(ctx, issueID, kind)
	if round.Stage != ReactionTriageStageDispatched || !round.CompletedAt.Equal(completedAt) {
		t.Fatalf("round after dispatched = %+v", round)
	}
	// dispatched is terminal: handed_off may follow, escalation may not.
	if err := s.MarkReactionTriageRoundHandedOff(ctx, issueID, kind, completedAt.Add(time.Second)); err != nil {
		t.Fatalf("MarkHandedOff: %v", err)
	}
	round, _ = s.GetReactionTriageRound(ctx, issueID, kind)
	if round.Stage != ReactionTriageStageHandedOff {
		t.Errorf("round after handed off = %+v", round)
	}
}

func TestReactionTriageRoundHandledAndEscalated(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	cases := []struct {
		name        string
		disposition string
		stage       string
	}{
		{name: "handled", disposition: "handled", stage: ReactionTriageStageDispatched},
		{name: "escalated", disposition: "escalate", stage: ReactionTriageStageEscalated},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := openTestStore(t)
			migrateOrFatal(t, s)
			const kind = "review"
			issueID := "ISS-" + tc.name
			fp := "fp-" + tc.name
			now := time.Now().UTC()

			if err := s.StartReactionTriageRound(ctx, issueID, kind, fp, 0, now); err != nil {
				t.Fatalf("Start: %v", err)
			}
			if err := s.SaveReactionTriageRoundResult(ctx, issueID, kind, fp, tc.disposition, "", now); err != nil {
				t.Fatalf("SaveResult: %v", err)
			}
			if err := s.AdvanceReactionTriageRound(ctx, issueID, kind, fp, tc.stage, now); err != nil {
				t.Fatalf("Advance: %v", err)
			}
			round, _ := s.GetReactionTriageRound(ctx, issueID, kind)
			if round.Stage != tc.stage || round.CompletedAt.IsZero() {
				t.Fatalf("round = %+v, want terminal %s", round, tc.stage)
			}
			// A terminal round cannot move to another terminal stage.
			if err := s.MarkReactionTriageRoundDispatched(ctx, issueID, kind, now.Add(time.Second)); err != nil {
				t.Fatalf("MarkDispatched: %v", err)
			}
			round, _ = s.GetReactionTriageRound(ctx, issueID, kind)
			if round.Stage != tc.stage {
				t.Errorf("terminal round moved: %+v", round)
			}
		})
	}
}

func TestReactionTriageRoundSupersedesAcrossFingerprints(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openTestStore(t)
	migrateOrFatal(t, s)
	const issueID, kind = "ISS-2", "merge-conflict"

	if err := s.StartReactionTriageRound(ctx, issueID, kind, "fp-a", 1, time.Now()); err != nil {
		t.Fatalf("Start a: %v", err)
	}
	if err := s.SaveReactionTriageRoundResult(ctx, issueID, kind, "fp-a", "escalate", "", time.Now()); err != nil {
		t.Fatalf("SaveResult a: %v", err)
	}
	if err := s.AdvanceReactionTriageRound(ctx, issueID, kind, "fp-a", ReactionTriageStageEscalated, time.Now()); err != nil {
		t.Fatalf("Advance a: %v", err)
	}

	// A new fingerprint replaces the completed round wholesale.
	newStart := time.Now().Add(time.Minute)
	if err := s.StartReactionTriageRound(ctx, issueID, kind, "fp-b", 0, newStart); err != nil {
		t.Fatalf("Start b: %v", err)
	}
	round, _ := s.GetReactionTriageRound(ctx, issueID, kind)
	if round.Fingerprint != "fp-b" || round.Stage != ReactionTriageStageStarted || round.Disposition != "" || round.CompletedAt.IsZero() == false {
		t.Fatalf("superseded round = %+v, want a fresh started round for fp-b", round)
	}
	if !round.StartedAt.Equal(newStart) {
		t.Errorf("started_at = %v, want %v", round.StartedAt, newStart)
	}

	// Delete removes the row.
	if err := s.DeleteReactionTriageRound(ctx, issueID, kind); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	round, err := s.GetReactionTriageRound(ctx, issueID, kind)
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if round != nil {
		t.Fatalf("round after delete = %+v, want nil", round)
	}
	// Deleting a missing row is not an error.
	if err := s.DeleteReactionTriageRound(ctx, issueID, kind); err != nil {
		t.Errorf("Delete missing: %v", err)
	}
}

func TestReactionTriageRoundTransitionsAreFingerprintScoped(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openTestStore(t)
	migrateOrFatal(t, s)
	const issueID, kind = "ISS-3", "bot-review"

	if err := s.StartReactionTriageRound(ctx, issueID, kind, "fp-current", 0, time.Now()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// A result carrying a different fingerprint lands nothing.
	if err := s.SaveReactionTriageRoundResult(ctx, issueID, kind, "fp-other", "handled", "", time.Now()); err != nil {
		t.Fatalf("SaveResult other: %v", err)
	}
	round, _ := s.GetReactionTriageRound(ctx, issueID, kind)
	if round.Stage != ReactionTriageStageStarted {
		t.Errorf("mismatched fingerprint advanced the round: %+v", round)
	}
}
