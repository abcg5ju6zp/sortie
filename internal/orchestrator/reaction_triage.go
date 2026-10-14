package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sortie-ai/sortie/internal/agent/procutil"
	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/workspace"
)

// ReactionTriageDisposition is a triage command's answer.
type ReactionTriageDisposition string

const (
	// TriageHandled means the command resolved the subject itself, so
	// no agent continuation is warranted for it.
	TriageHandled ReactionTriageDisposition = "handled"

	// TriageDispatchAgent means the command wants the agent
	// continuation the reaction would have scheduled anyway. It is
	// also the fallback applied to every failure mode.
	TriageDispatchAgent ReactionTriageDisposition = "dispatch-agent"

	// TriageEscalate means the command wants a person, so the kind's
	// configured escalation is applied instead of a continuation.
	TriageEscalate ReactionTriageDisposition = "escalate"
)

// triageInputSchemaVersion is the version of the input document the
// runner writes. A script reads it to tell one document shape from a
// later one.
const triageInputSchemaVersion = 1

// triageMaxResultBytes is the largest result file the runner reads. A
// larger file is rejected rather than parsed, because a triage answer
// is a single small object and an oversized file is a script writing
// something else to that path.
const triageMaxResultBytes = 64 * 1024

// Fallback reasons carried on a [ReactionTriageOutcome] whose
// disposition was defaulted rather than read from the command.
const (
	triageFallbackWorkspaceMissing     = "workspace_missing"
	triageFallbackWorkspacePathInvalid = "workspace_path_invalid"
	triageFallbackStartFailed          = "start_failed"
	triageFallbackTimeout              = "timeout"
	triageFallbackExitStatus           = "exit_status"
	triageFallbackHookRejected         = "hook_rejected"
	triageFallbackNoResult             = "no_result"
	triageFallbackResultTooLarge       = "result_too_large"
	triageFallbackMalformedResult      = "malformed_result"
	triageFallbackUnknownDisposition   = "unknown_disposition"

	// triageFallbackShutdownInterrupted is not a reason the runner ever
	// reports: it is recorded by the gate when it resumes a round whose
	// triage command was still in flight when the previous process ended.
	// Such a command is never re-run for the fingerprint, because it may
	// already have acted; the round proceeds as dispatch-agent instead.
	triageFallbackShutdownInterrupted = "shutdown_interrupted"
)

// ReactionTriageOutcome is one completed triage run. It holds no
// captured output: the tail belongs to the completion log record that
// [RunReactionTriage] writes, and an outcome is retained on a pending
// entry for as long as its fingerprint stands, which can be the whole
// watch window.
type ReactionTriageOutcome struct {
	// Disposition is the applied answer. It is [TriageDispatchAgent]
	// whenever Fallback is non-empty.
	Disposition ReactionTriageDisposition

	// Fallback names why the default was applied. Empty when the
	// command produced a recognized disposition.
	Fallback string

	// Elapsed is the command's wall-clock duration.
	Elapsed time.Duration
}

// ReactionTriageRun is an in-flight or finished triage run. It is
// runtime-only: the recoverable half of the run's state lives in the
// reaction_triage_rounds row the durable gate keeps ([ReactionTriageRound]).
//
// Outcome is written by the runner goroutine before Done is closed and
// MUST NOT be read until a receive on Done has succeeded. That receive
// is the only synchronization between the goroutine and the reconcile
// pass. ResultRecorded is written by the event loop alone after a
// successful receive, Applied and Marked by the in-memory gate alone, so
// none of them needs synchronization. No field is written by both the
// goroutine and the event loop.
type ReactionTriageRun struct {
	// Fingerprint binds the run to the subject it was started for.
	Fingerprint string

	// StartedAt is the UTC time the run began.
	StartedAt time.Time

	// Cancel terminates the subprocess and its process group.
	Cancel context.CancelFunc

	// Done is closed once Outcome is set.
	Done <-chan struct{}

	// Outcome is valid only after a successful receive on Done.
	Outcome *ReactionTriageOutcome

	// ResultRecorded reports whether Outcome has been written through to
	// the round's durable row. The gate waits until this lands before it
	// applies the outcome, so a restart never has to guess what a
	// finished run answered.
	ResultRecorded bool

	// Applied reports whether the in-memory gate has already acted on
	// Outcome. Used only by stores without a durable round table.
	Applied bool

	// Marked reports whether the fingerprint write that a handled or
	// escalate outcome requires has succeeded. Used only by the in-memory
	// gate; false after a failed write is the one case a later pass
	// retries.
	Marked bool
}

// ReactionTriageRequest carries everything the input document needs.
type ReactionTriageRequest struct {
	// Kind is the runtime reaction discriminator, for example
	// [ReactionKindCI].
	Kind string

	// WorkspaceRoot is the configured workspace root the per-issue
	// directory is derived from.
	WorkspaceRoot string

	// IssueID is the tracker-assigned issue ID.
	IssueID string

	// Identifier is the human-readable ticket key the workspace key is
	// derived from.
	Identifier string

	// DisplayID is the display identifier carried into the document.
	DisplayID string

	// Attempt is the overall run attempt number of the entry.
	Attempt int

	// SSHHost is the host preference carried through to the command's
	// environment, exactly as a workspace hook receives it.
	SSHHost string

	// Fingerprint is the value the kind stores for the current subject.
	Fingerprint string

	// AttemptsUsed is the kind's continuation counter for this
	// reaction key at the moment the run starts.
	AttemptsUsed int

	// MaxAttempts is the kind's own budget field: the continuation-turn
	// cap for review and bot-review, the retry cap for the other two.
	MaxAttempts int

	// Subject is the same map the continuation template receives for
	// this kind, so the command sees what the agent would have seen.
	Subject any
}

// triageInputIssue is the issue identity block of the input document.
type triageInputIssue struct {
	ID         string `json:"id"`
	Identifier string `json:"identifier"`
	DisplayID  string `json:"display_id"`
}

// triageInputDocument is the JSON document written to the path named by
// SORTIE_REACTION_INPUT. Externally authored text reaches the command
// only through this document, never through an environment variable or
// a shell word.
type triageInputDocument struct {
	SchemaVersion int              `json:"schema_version"`
	ReactionKind  string           `json:"reaction_kind"`
	Issue         triageInputIssue `json:"issue"`
	Attempt       int              `json:"attempt"`
	Workspace     string           `json:"workspace"`
	Fingerprint   string           `json:"fingerprint"`
	AttemptsUsed  int              `json:"attempts_used"`
	MaxAttempts   int              `json:"max_attempts"`
	Subject       any              `json:"subject"`
}

// triageResultDocument is the JSON document the command writes to the
// path named by SORTIE_REACTION_RESULT. Unknown keys are ignored so a
// later extension can add fields without breaking existing scripts.
type triageResultDocument struct {
	Disposition string `json:"disposition"`
}

// RunReactionTriage executes one triage command and returns its
// outcome. It never returns an error: every failure mode maps to a
// [ReactionTriageOutcome] whose Disposition is [TriageDispatchAgent]
// and whose Fallback names the reason.
//
// It writes exactly one completion log record before it returns, on
// every path including the ones that never start a subprocess. That
// record is the only place the command's captured output tail is
// reported, which is why it is written here rather than by the caller:
// the tail lives on the [workspace.HookResult] or [workspace.HookError]
// and is not carried on the returned outcome.
//
// It blocks for at most cfg.TimeoutMS milliseconds plus the process
// group teardown grace, and MUST be called from a goroutine, never from
// the orchestrator event loop.
func RunReactionTriage(
	ctx context.Context,
	cfg config.ReactionTriageConfig,
	req ReactionTriageRequest,
	log *slog.Logger,
) ReactionTriageOutcome {
	if log == nil {
		log = slog.Default()
	}

	// The record is written through a context stripped of cancellation
	// so a run cancelled at shutdown still reports its outcome.
	logCtx := context.WithoutCancel(ctx)
	finish := func(outcome ReactionTriageOutcome, hookOutput string) ReactionTriageOutcome {
		logTriageCompletion(logCtx, log, req, outcome, hookOutput)
		return outcome
	}

	pathResult, err := workspace.ComputePath(req.WorkspaceRoot, req.Identifier)
	if err != nil {
		return finish(triageFallbackOutcome(triageFallbackWorkspacePathInvalid, 0), "")
	}

	// The directory is never created here: creating it would make the
	// next dispatch see an already-existing workspace, skip the
	// after-create hook, and run the before-run hook in an empty tree.
	info, statErr := os.Stat(pathResult.Path)
	if statErr != nil || !info.IsDir() {
		return finish(triageFallbackOutcome(triageFallbackWorkspaceMissing, 0), "")
	}

	// The temporary directory sits outside the workspace so a stale
	// result file cannot be read as this run's answer and so externally
	// authored text stays out of the tree the agent reads.
	tmpDir, tmpErr := os.MkdirTemp("", "sortie-triage-")
	if tmpErr != nil {
		return finish(triageFallbackOutcome(triageFallbackStartFailed, 0), "")
	}
	defer func() {
		if rmErr := os.RemoveAll(tmpDir); rmErr != nil {
			log.Warn("failed to remove reaction triage temporary directory",
				slog.String("reaction_kind", req.Kind),
				slog.Any("error", rmErr),
			)
		}
	}()

	inputPath := filepath.Join(tmpDir, "input.json")
	resultPath := filepath.Join(tmpDir, "result.json")

	if writeErr := writeTriageInput(inputPath, req, pathResult.Path); writeErr != nil {
		return finish(triageFallbackOutcome(triageFallbackStartFailed, 0), "")
	}

	env := workspace.HookEnv(req.IssueID, req.Identifier, pathResult.Path, req.Attempt, req.SSHHost)
	env["SORTIE_REACTION_KIND"] = req.Kind
	env["SORTIE_REACTION_INPUT"] = inputPath
	env["SORTIE_REACTION_RESULT"] = resultPath

	started := time.Now()
	hookResult, hookErr := workspace.RunHook(ctx, workspace.HookParams{
		Script:    cfg.Script,
		Dir:       pathResult.Path,
		Env:       env,
		TimeoutMS: cfg.TimeoutMS,
	})
	elapsed := time.Since(started)

	if hookErr != nil {
		output := ""
		op := ""
		terminatedLeftovers := false
		if he, ok := errors.AsType[*workspace.HookError](hookErr); ok {
			output = he.Output
			op = he.Op
			terminatedLeftovers = he.TerminatedLeftovers
		}
		if terminatedLeftovers {
			log.Info(procutil.LeftoversTerminatedMessage, slog.String("reaction_kind", req.Kind)) //nolint:sloglint // procutil.LeftoversTerminatedMessage is a fixed string constant
		}
		return finish(triageFallbackOutcome(fallbackForHookOp(op), elapsed), output)
	}

	if hookResult.TerminatedLeftovers {
		log.Info(procutil.LeftoversTerminatedMessage, slog.String("reaction_kind", req.Kind)) //nolint:sloglint // procutil.LeftoversTerminatedMessage is a fixed string constant
	}
	return finish(decodeResult(resultPath, elapsed), "")
}

// triageFallbackOutcome builds the defaulted outcome for one fallback
// reason.
func triageFallbackOutcome(reason string, elapsed time.Duration) ReactionTriageOutcome {
	return ReactionTriageOutcome{
		Disposition: TriageDispatchAgent,
		Fallback:    reason,
		Elapsed:     elapsed,
	}
}

// fallbackForHookOp maps a [workspace.HookError] Op to the fallback
// reason reported for it. The mapping is total: an Op this function
// does not name still yields a named reason rather than an unlabelled
// fallback.
func fallbackForHookOp(op string) string {
	switch op {
	case "start":
		return triageFallbackStartFailed
	case "timeout":
		return triageFallbackTimeout
	case "run":
		return triageFallbackExitStatus
	default:
		return triageFallbackHookRejected
	}
}

// writeTriageInput marshals the input document and writes it at mode
// 0600.
func writeTriageInput(path string, req ReactionTriageRequest, workspacePath string) error {
	doc := triageInputDocument{
		SchemaVersion: triageInputSchemaVersion,
		ReactionKind:  req.Kind,
		Issue: triageInputIssue{
			ID:         req.IssueID,
			Identifier: req.Identifier,
			DisplayID:  req.DisplayID,
		},
		Attempt:      req.Attempt,
		Workspace:    workspacePath,
		Fingerprint:  req.Fingerprint,
		AttemptsUsed: req.AttemptsUsed,
		MaxAttempts:  req.MaxAttempts,
		Subject:      req.Subject,
	}

	encoded, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("marshal triage input: %w", err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		return fmt.Errorf("write triage input: %w", err)
	}
	return nil
}

// decodeResult reads the result file a successful command left behind
// and returns the outcome it implies. Every rejection yields
// [TriageDispatchAgent] with a named fallback reason.
func decodeResult(path string, elapsed time.Duration) ReactionTriageOutcome {
	info, err := os.Stat(path)
	if err != nil {
		return triageFallbackOutcome(triageFallbackNoResult, elapsed)
	}
	if info.Size() > triageMaxResultBytes {
		return triageFallbackOutcome(triageFallbackResultTooLarge, elapsed)
	}

	raw, err := os.ReadFile(path) //nolint:gosec // the path is orchestrator-generated inside a per-run temporary directory
	if err != nil {
		return triageFallbackOutcome(triageFallbackMalformedResult, elapsed)
	}

	var doc triageResultDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return triageFallbackOutcome(triageFallbackMalformedResult, elapsed)
	}

	switch ReactionTriageDisposition(strings.TrimSpace(doc.Disposition)) {
	case TriageHandled:
		return ReactionTriageOutcome{Disposition: TriageHandled, Elapsed: elapsed}
	case TriageDispatchAgent:
		return ReactionTriageOutcome{Disposition: TriageDispatchAgent, Elapsed: elapsed}
	case TriageEscalate:
		return ReactionTriageOutcome{Disposition: TriageEscalate, Elapsed: elapsed}
	default:
		return triageFallbackOutcome(triageFallbackUnknownDisposition, elapsed)
	}
}

// logTriageCompletion writes the single completion record every run
// emits, whether or not a reconcile pass ever consumes the outcome. It
// is info when the command produced a recognized disposition and warn
// when a fallback applied.
func logTriageCompletion(ctx context.Context, log *slog.Logger, req ReactionTriageRequest, outcome ReactionTriageOutcome, hookOutput string) {
	attrs := []slog.Attr{
		slog.String("reaction_kind", req.Kind),
		slog.String("fingerprint", req.Fingerprint),
		slog.String("disposition", string(outcome.Disposition)),
		slog.Int64("elapsed_ms", outcome.Elapsed.Milliseconds()),
	}
	if outcome.Fallback != "" {
		attrs = append(attrs, slog.String("fallback", outcome.Fallback))
	}
	if hookOutput != "" {
		attrs = append(attrs, slog.String("hook_output", hookOutput))
	}

	level := slog.LevelInfo
	if outcome.Fallback != "" {
		level = slog.LevelWarn
	}
	log.LogAttrs(ctx, level, "reaction triage completed", attrs...)
}

// reactionTriageVerdict tells the calling reconcile pass what to do with
// the entry the gate just advanced.
type reactionTriageVerdict int

const (
	// triageProceed means no triage is configured, or the run returned
	// dispatch-agent. The pass continues to its dispatch block.
	triageProceed reactionTriageVerdict = iota

	// triageWait means a run is in flight or was just started. The
	// caller re-enqueues the entry ready for the next tick and continues
	// to the next entry.
	triageWait

	// triageHandled means the run returned handled. The gate has already
	// marked the fingerprint dispatched. The caller re-enqueues the entry
	// with its poll interval and continues to the next entry.
	triageHandled

	// triageEscalate means the run returned escalate. The gate has
	// already marked the fingerprint dispatched. The caller invokes its
	// own escalation function with [EscalationTriggerTriage] and
	// continues to the next entry.
	triageEscalate

	// triageResumeDispatch means the durable round already holds a
	// dispatch-agent answer from a previous tick or process and its
	// continuation creation was never confirmed. The caller creates the
	// continuation exactly once and skips the once-only bookkeeping a
	// first drive performs, rather than re-running the triage command.
	triageResumeDispatch
)

// triageRoundStore is the optional persistence contract the durable
// reaction gate drives. [persistence.Store] satisfies it in production.
// A store that does not (every hand-written test double) makes the gate
// fall back to the in-memory behavior, so the gate's durability is
// additive rather than forced on callers.
type triageRoundStore interface {
	StartReactionTriageRound(ctx context.Context, issueID, kind, fingerprint string, attemptsUsed int, startedAt time.Time) error
	SaveReactionTriageRoundResult(ctx context.Context, issueID, kind, fingerprint, disposition, fallback string, resultAt time.Time) error
	AdvanceReactionTriageRound(ctx context.Context, issueID, kind, fingerprint, stage string, at time.Time) error
	MarkReactionTriageRoundDispatched(ctx context.Context, issueID, kind string, at time.Time) error
	MarkReactionTriageRoundHandedOff(ctx context.Context, issueID, kind string, at time.Time) error
	GetReactionTriageRound(ctx context.Context, issueID, kind string) (*persistence.ReactionTriageRound, error)
	DeleteReactionTriageRound(ctx context.Context, issueID, kind string) error
}

// triageRounds returns the durable round store behind params.Store, or
// nil when the store carries no round table.
func triageRounds(store any) triageRoundStore {
	rounds, _ := store.(triageRoundStore)
	return rounds
}

// triageCapacityAvailable reports whether a new triage run fits under
// the in-flight cap. The gate calls this BEFORE it writes the round's
// started row, so a cap deferral leaves no round behind that a later
// pass could mistake for an interrupted run.
func triageCapacityAvailable(state *State, log *slog.Logger, req ReactionTriageRequest) bool {
	// The cap is sized from the concurrency the operator already chose
	// for this host. It adds to the agent processes that number bounds
	// rather than sharing slots with them.
	limit := int64(max(state.MaxConcurrentAgents, 1))
	if state.TriageInFlight.Load() >= limit {
		log.Debug("reaction triage deferred: in-flight cap reached",
			slog.String("reaction_kind", req.Kind),
			slog.String("fingerprint", req.Fingerprint),
			slog.Int64("cap", limit),
		)
		return false
	}
	return true
}

// startReactionTriage launches a triage run on its own goroutine,
// registers it with state.TriageWg, counts it in state.TriageInFlight,
// and returns the handle to store on the pending entry. The caller MUST
// have confirmed the in-flight cap with triageCapacityAvailable and, for
// the durable gate, written the round's started row first. When rounds
// is non-nil the runner writes the result to the round before it closes
// Done; a failed write stays recorded on the handle for the gate to
// retry.
func startReactionTriage(
	state *State,
	ctx context.Context,
	cfg config.ReactionTriageConfig,
	req ReactionTriageRequest,
	rounds triageRoundStore,
	log *slog.Logger,
) *ReactionTriageRun {
	if log == nil {
		log = slog.Default()
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	run := &ReactionTriageRun{
		Fingerprint: req.Fingerprint,
		StartedAt:   time.Now().UTC(),
		Cancel:      cancel,
		Done:        done,
	}

	log.Info("reaction triage started",
		slog.String("reaction_kind", req.Kind),
		slog.String("fingerprint", req.Fingerprint),
		slog.Int("timeout_ms", cfg.TimeoutMS),
	)

	state.TriageInFlight.Add(1)
	state.TriageWg.Go(func() {
		defer state.TriageInFlight.Add(-1)
		outcome := RunReactionTriage(runCtx, cfg, req, log)
		run.Outcome = &outcome
		// The result lands in the round row before Done closes: a pass
		// that can read Outcome can also read it back after a restart.
		if rounds != nil {
			if err := rounds.SaveReactionTriageRoundResult(
				context.WithoutCancel(ctx),
				req.IssueID, req.Kind, req.Fingerprint,
				string(outcome.Disposition), outcome.Fallback, time.Now().UTC(),
			); err != nil {
				log.Warn("failed to persist reaction triage result, the pass will retry the write",
					slog.String("reaction_kind", req.Kind),
					slog.String("fingerprint", req.Fingerprint),
					slog.Any("error", err),
				)
			} else {
				run.ResultRecorded = true
			}
		}
		close(done)
		cancel()
	})

	return run
}

// triageRunFinished reports whether a run's outcome is readable, using a
// non-blocking receive so no reconcile pass ever waits on a subprocess.
func triageRunFinished(run *ReactionTriageRun) bool {
	if run == nil || run.Done == nil {
		return false
	}
	select {
	case <-run.Done:
		return true
	default:
		return false
	}
}

// reactionTriageGate advances a pending entry's triage state for one
// reconcile pass and reports what the pass should do next. With a store
// that keeps a durable round table it drives the recoverable round:
// every stage is persisted before the pass acts on it, a restart resumes
// from the stored stage, and an in-flight command left by a dead process
// is never re-run. With a store without the table it keeps the former
// in-memory behavior. The gate never escalates, dispatches, or
// re-enqueues: those remain the pass's own responsibility.
func reactionTriageGate(
	state *State,
	params ReconcileParams,
	pending *PendingReaction,
	cfg config.ReactionTriageConfig,
	req ReactionTriageRequest,
	log *slog.Logger,
	ctx context.Context,
) reactionTriageVerdict {
	if log == nil {
		log = slog.Default()
	}
	if params.WorkspaceRoot == "" || !cfg.Enabled() {
		return triageProceed
	}
	if rounds := triageRounds(params.Store); rounds != nil {
		return reactionTriageGateDurable(state, rounds, params, pending, cfg, req, log, ctx)
	}
	return reactionTriageGateInMemory(state, params, pending, cfg, req, log, ctx)
}

// reactionTriageGateDurable is the restart-safe gate. Its ordering on
// every pass is: settle the live in-memory run, read the durable round,
// then act only on the stage that is still unconfirmed.
func reactionTriageGateDurable(
	state *State,
	rounds triageRoundStore,
	params ReconcileParams,
	pending *PendingReaction,
	cfg config.ReactionTriageConfig,
	req ReactionTriageRequest,
	log *slog.Logger,
	ctx context.Context,
) reactionTriageVerdict {
	// 1. A live run bound to the current subject is the one producer of
	//    this round's result.
	if pending.Triage != nil {
		run := pending.Triage
		finished := triageRunFinished(run)

		if run.Fingerprint != req.Fingerprint {
			// The subject moved: kill the old run if it is still alive
			// and fall through, where the old round is superseded before
			// a fresh run starts.
			logTriageDiscarded(ctx, log, run, req, finished)
			if !finished && run.Cancel != nil {
				run.Cancel()
			}
			pending.Triage = nil
		} else if !finished {
			return triageWait
		} else {
			// A finished run only makes its verdict available once the
			// result row landed; until then the pass waits and retries
			// the write from the in-memory outcome.
			if run.Outcome == nil {
				log.Warn("reaction triage finished without an outcome, dispatching the agent",
					slog.String("reaction_kind", req.Kind),
					slog.String("fingerprint", req.Fingerprint),
				)
				// Nothing was recorded; record the dispatch-agent
				// default the runner would have and drive on.
				if !recordTriageResult(rounds, req, ReactionTriageOutcome{Disposition: TriageDispatchAgent, Fallback: triageFallbackMalformedResult}, log, ctx) {
					return triageWait
				}
			} else if !run.ResultRecorded {
				if !recordTriageResult(rounds, req, *run.Outcome, log, ctx) {
					return triageWait
				}
				run.ResultRecorded = true
			}
		}
	}

	// 2. Read the durable round, the authority for what is confirmed.
	round, err := rounds.GetReactionTriageRound(ctx, req.IssueID, req.Kind)
	if err != nil {
		log.Warn("failed to read reaction triage round, deferring",
			slog.String("reaction_kind", req.Kind),
			slog.String("fingerprint", req.Fingerprint),
			slog.Any("error", err),
		)
		return triageWait
	}

	// 3. No round, or a round bound to an older subject: open this
	//    subject's round before launching, so the command never starts
	//    without a recoverable row.
	if round == nil || round.Fingerprint != req.Fingerprint {
		if round != nil {
			logTriageRoundSuperseded(ctx, log, req, round)
		}
		if !triageCapacityAvailable(state, log, req) {
			return triageWait
		}
		now := time.Now().UTC()
		if err := rounds.StartReactionTriageRound(ctx, req.IssueID, req.Kind, req.Fingerprint, req.AttemptsUsed, now); err != nil {
			log.Warn("failed to open reaction triage round, deferring",
				slog.String("reaction_kind", req.Kind),
				slog.String("fingerprint", req.Fingerprint),
				slog.Any("error", err),
			)
			return triageWait
		}
		run := startReactionTriage(state, ctx, cfg, req, rounds, log)
		if run == nil {
			// Unreachable after the capacity check on this event loop;
			// void the just-opened row so it cannot read as interrupted.
			if delErr := rounds.DeleteReactionTriageRound(ctx, req.IssueID, req.Kind); delErr != nil {
				log.Warn("failed to void deferred reaction triage round",
					slog.String("reaction_kind", req.Kind),
					slog.Any("error", delErr),
				)
			}
			return triageWait
		}
		pending.Triage = run
		return triageWait
	}

	// 4. Same subject: resume from the confirmed stage.
	switch round.Stage {
	case persistence.ReactionTriageStageStarted:
		// A started row with no live run in this process means the
		// previous process ended with the command in flight. The command
		// may already have acted, so it is never re-run: record the
		// explicit interrupted result and take the universal
		// dispatch-agent fallback for the first drive.
		outcome := ReactionTriageOutcome{
			Disposition: TriageDispatchAgent,
			Fallback:    triageFallbackShutdownInterrupted,
		}
		if !recordTriageResult(rounds, req, outcome, log, ctx) {
			return triageWait
		}
		logTriageInterrupted(ctx, log, req)
		pending.Triage = nil
		return triageProceed

	case persistence.ReactionTriageStageResult:
		verdict, ok := applyTriageResult(rounds, params, req, round, log, ctx)
		if !ok {
			return triageWait
		}
		pending.Triage = nil
		return verdict

	case persistence.ReactionTriageStageContinuation:
		// The answer was dispatch-agent and the continuation creation is
		// unconfirmed. Re-create that continuation only; the command is
		// not re-run and no escalation is reposted.
		pending.Triage = nil
		return triageResumeDispatch

	case persistence.ReactionTriageStageEscalated,
		persistence.ReactionTriageStageDispatched,
		persistence.ReactionTriageStageHandedOff:
		// A terminal round is the durable equivalent of the memoized
		// answer: re-enqueue, do nothing twice.
		pending.Triage = nil
		return triageHandled

	default:
		log.Warn("unknown reaction triage round stage, deferring",
			slog.String("reaction_kind", req.Kind),
			slog.String("fingerprint", req.Fingerprint),
			slog.String("stage", round.Stage),
		)
		return triageWait
	}
}

// recordTriageResult writes outcome to the round's result stage. It
// returns false when the write fails, in which case the caller waits and
// retries; the in-memory outcome is retained on the run handle.
func recordTriageResult(
	rounds triageRoundStore,
	req ReactionTriageRequest,
	outcome ReactionTriageOutcome,
	log *slog.Logger,
	ctx context.Context,
) bool {
	if err := rounds.SaveReactionTriageRoundResult(
		ctx, req.IssueID, req.Kind, req.Fingerprint,
		string(outcome.Disposition), outcome.Fallback, time.Now().UTC(),
	); err != nil {
		log.Warn("failed to persist reaction triage result, deferring",
			slog.String("reaction_kind", req.Kind),
			slog.String("fingerprint", req.Fingerprint),
			slog.Any("error", err),
		)
		return false
	}
	return true
}

// applyTriageResult acts on a round at the result stage once: handled and
// escalate mark the fingerprint dispatched and advance the round before
// reporting their verdict, dispatch-agent reports proceed and leaves the
// stage for the pass to advance once the continuation is created. The
// boolean is false when a required write failed and the pass must wait.
func applyTriageResult(
	rounds triageRoundStore,
	params ReconcileParams,
	req ReactionTriageRequest,
	round *persistence.ReactionTriageRound,
	log *slog.Logger,
	ctx context.Context,
) (reactionTriageVerdict, bool) {
	outcome := ReactionTriageOutcome{
		Disposition: ReactionTriageDisposition(round.Disposition),
		Fallback:    round.Fallback,
	}
	logTriageApplied(ctx, log, req, outcome)

	switch ReactionTriageDisposition(round.Disposition) {
	case TriageHandled, TriageEscalate:
		if !markTriageDispatched(params, req, log, ctx) {
			return triageWait, false
		}
		stage := persistence.ReactionTriageStageDispatched
		verdict := triageHandled
		if ReactionTriageDisposition(round.Disposition) == TriageEscalate {
			stage = persistence.ReactionTriageStageEscalated
			verdict = triageEscalate
		}
		if err := rounds.AdvanceReactionTriageRound(ctx, req.IssueID, req.Kind, req.Fingerprint, stage, time.Now().UTC()); err != nil {
			log.Warn("failed to advance reaction triage round after the dispatched mark",
				slog.String("reaction_kind", req.Kind),
				slog.String("fingerprint", req.Fingerprint),
				slog.String("stage", stage),
				slog.Any("error", err),
			)
			return triageWait, false
		}
		return verdict, true

	default:
		return triageProceed, true
	}
}

// markTriageContinuationScheduled records that the dispatch-agent result
// created its continuation, the confirmation a restart resumes from. It
// is called by each triage-enabled pass immediately after it schedules
// the continuation. A failed write is logged: the unconfirmed round
// would re-create the continuation once after a restart rather than lose
// it.
func markTriageContinuationScheduled(
	store any,
	issueID, kind, fingerprint string,
	log *slog.Logger,
	ctx context.Context,
) {
	rounds := triageRounds(store)
	if rounds == nil {
		return
	}
	if err := rounds.AdvanceReactionTriageRound(ctx, issueID, kind, fingerprint, persistence.ReactionTriageStageContinuation, time.Now().UTC()); err != nil {
		log.Warn("failed to record the scheduled continuation on the reaction triage round",
			slog.String("reaction_kind", kind),
			slog.String("fingerprint", fingerprint),
			slog.Any("error", err),
		)
	}
}

// markTriageRoundDispatched records the dispatched marker confirmation
// on the round after the reaction fingerprint itself was marked, from
// call sites that do not hold the fingerprint (the shared retry-dispatch
// path and the merge-conflict dispatch site). Best effort: a failed
// advance only loses the cross-restart confirmation, never the marker.
func markTriageRoundDispatched(store any, issueID, kind string, log *slog.Logger, ctx context.Context) {
	rounds := triageRounds(store)
	if rounds == nil {
		return
	}
	if err := rounds.MarkReactionTriageRoundDispatched(ctx, issueID, kind, time.Now().UTC()); err != nil {
		log.Warn("failed to advance the reaction triage round to dispatched",
			slog.String("reaction_kind", kind),
			slog.String("issue_id", issueID),
			slog.Any("error", err),
		)
	}
}

// markTriageRoundHandedOff records that the continuation run's
// handed-off comments were saved, the last stage the review-family
// rounds reach. Called only after that save succeeded.
func markTriageRoundHandedOff(store any, issueID, kind string, log *slog.Logger, ctx context.Context) {
	rounds := triageRounds(store)
	if rounds == nil {
		return
	}
	if err := rounds.MarkReactionTriageRoundHandedOff(ctx, issueID, kind, time.Now().UTC()); err != nil {
		log.Warn("failed to advance the reaction triage round to handed off",
			slog.String("reaction_kind", kind),
			slog.String("issue_id", issueID),
			slog.Any("error", err),
		)
	}
}

// deleteTriageRound voids the current round for an episode that ends. It
// is best effort: a failed void only makes a same-fingerprint next
// episode take the conservative resume path once.
func deleteTriageRound(store any, issueID, kind string, log *slog.Logger, ctx context.Context) {
	rounds := triageRounds(store)
	if rounds == nil {
		return
	}
	if err := rounds.DeleteReactionTriageRound(ctx, issueID, kind); err != nil {
		log.Warn("failed to delete reaction triage round",
			slog.String("reaction_kind", kind),
			slog.String("issue_id", issueID),
			slog.Any("error", err),
		)
	}
}

// reactionTriageGateInMemory is the former gate, kept for stores without
// a durable round table. A consumed outcome is re-applied from memory
// rather than re-running the command; a memoized escalate re-applies as
// handled, and the fingerprint write is the one action a later pass
// repeats, and only while it has failed.
func reactionTriageGateInMemory(
	state *State,
	params ReconcileParams,
	pending *PendingReaction,
	cfg config.ReactionTriageConfig,
	req ReactionTriageRequest,
	log *slog.Logger,
	ctx context.Context,
) reactionTriageVerdict {
	if pending.Triage == nil {
		if !triageCapacityAvailable(state, log, req) {
			return triageWait
		}
		if run := startReactionTriage(state, ctx, cfg, req, nil, log); run != nil {
			pending.Triage = run
		}
		return triageWait
	}

	run := pending.Triage
	finished := triageRunFinished(run)

	// A moved fingerprint voids the run bound to the previous subject,
	// whether it has finished or not.
	if run.Fingerprint != req.Fingerprint {
		logTriageDiscarded(ctx, log, run, req, finished)
		if !finished && run.Cancel != nil {
			run.Cancel()
		}
		pending.Triage = nil
		if !triageCapacityAvailable(state, log, req) {
			return triageWait
		}
		if fresh := startReactionTriage(state, ctx, cfg, req, nil, log); fresh != nil {
			pending.Triage = fresh
		}
		return triageWait
	}

	if !finished {
		return triageWait
	}

	outcome := run.Outcome
	if outcome == nil {
		log.Warn("reaction triage finished without an outcome, dispatching the agent",
			slog.String("reaction_kind", req.Kind),
			slog.String("fingerprint", req.Fingerprint),
		)
		return triageProceed
	}

	if !run.Applied {
		run.Applied = true
		logTriageApplied(ctx, log, req, *outcome)

		switch outcome.Disposition {
		case TriageHandled:
			if markTriageDispatched(params, req, log, ctx) {
				run.Marked = true
			}
			return triageHandled
		case TriageEscalate:
			if markTriageDispatched(params, req, log, ctx) {
				run.Marked = true
			}
			return triageEscalate
		default:
			return triageProceed
		}
	}

	if outcome.Disposition == TriageDispatchAgent {
		return triageProceed
	}
	if !run.Marked && markTriageDispatched(params, req, log, ctx) {
		run.Marked = true
	}
	return triageHandled
}

// markTriageDispatched marks the reaction fingerprint dispatched. It
// reports whether the write landed, so a failed mark is retried by a
// later pass and a successful one is never repeated.
func markTriageDispatched(
	params ReconcileParams,
	req ReactionTriageRequest,
	log *slog.Logger,
	ctx context.Context,
) bool {
	if params.Store == nil {
		return false
	}
	if err := params.Store.MarkReactionDispatched(ctx, req.IssueID, req.Kind); err != nil {
		log.Warn("failed to mark reaction dispatched after triage",
			slog.String("reaction_kind", req.Kind),
			slog.String("fingerprint", req.Fingerprint),
			slog.Any("error", err),
		)
		return false
	}
	return true
}

// logTriageApplied writes the record reporting that a pass consumed an
// outcome. It is warn when the outcome carried a fallback reason.
func logTriageApplied(ctx context.Context, log *slog.Logger, req ReactionTriageRequest, outcome ReactionTriageOutcome) {
	attrs := []slog.Attr{
		slog.String("reaction_kind", req.Kind),
		slog.String("fingerprint", req.Fingerprint),
		slog.String("disposition", string(outcome.Disposition)),
	}
	level := slog.LevelInfo
	if outcome.Fallback != "" {
		attrs = append(attrs, slog.String("fallback", outcome.Fallback))
		level = slog.LevelWarn
	}
	log.LogAttrs(ctx, level, "reaction triage applied", attrs...)
}

// logTriageDiscarded writes the record reporting that a handle bound to
// a fingerprint the pass no longer computes is being thrown away.
func logTriageDiscarded(ctx context.Context, log *slog.Logger, run *ReactionTriageRun, req ReactionTriageRequest, finished bool) {
	attrs := []slog.Attr{
		slog.String("reaction_kind", req.Kind),
		slog.String("fingerprint", req.Fingerprint),
		slog.String("previous_fingerprint", run.Fingerprint),
	}

	level := slog.LevelInfo
	if !finished {
		attrs = append(attrs, slog.String("discarded", "cancelled_in_flight"))
	} else {
		attrs = append(attrs, slog.String("discarded", "outcome_superseded"))
		if run.Outcome != nil {
			attrs = append(attrs, slog.String("disposition", string(run.Outcome.Disposition)))
			if run.Outcome.Fallback != "" {
				attrs = append(attrs, slog.String("fallback", run.Outcome.Fallback))
				level = slog.LevelWarn
			}
		}
	}

	log.LogAttrs(ctx, level, "reaction triage discarded: subject changed", attrs...)
}

// logTriageRoundSuperseded writes the record reporting that a durable
// round bound to a fingerprint the pass no longer computes is being
// replaced by a fresh round for the new subject.
func logTriageRoundSuperseded(ctx context.Context, log *slog.Logger, req ReactionTriageRequest, round *persistence.ReactionTriageRound) {
	log.LogAttrs(ctx, slog.LevelInfo, "reaction triage round superseded: subject changed",
		slog.String("reaction_kind", req.Kind),
		slog.String("fingerprint", req.Fingerprint),
		slog.String("previous_fingerprint", round.Fingerprint),
		slog.String("previous_stage", round.Stage),
	)
}

// logTriageInterrupted writes the record reporting that a round resumed
// with its triage command still in flight from a process that has ended,
// so the command is not re-run and the round falls back to
// dispatch-agent.
func logTriageInterrupted(ctx context.Context, log *slog.Logger, req ReactionTriageRequest) {
	log.LogAttrs(ctx, slog.LevelWarn, "reaction triage interrupted by shutdown, dispatching the agent without re-running the command",
		slog.String("reaction_kind", req.Kind),
		slog.String("fingerprint", req.Fingerprint),
		slog.String("fallback", triageFallbackShutdownInterrupted),
	)
}

// cancelReactionTriage detaches an entry's triage run, terminating it
// first when it is still in flight, and voids the run's durable round.
// It is called by every path that ends the episode the run was started
// for, whether that path drops the pending entry or re-enqueues it, so no
// subprocess outlives its episode and no verdict or round is carried into
// the next one. Detaching a finished run and voiding a round with no live
// run matter as much as cancelling a live one: without the void the gate
// would resume the old round whenever the next episode recomputes an
// identical fingerprint. The store is typed as any so callers can pass
// the ReconcileStore they already hold; a store without round storage
// voids the run alone. A nil entry is a no-op.
func cancelReactionTriage(ctx context.Context, store any, log *slog.Logger, pending *PendingReaction) {
	if pending == nil {
		return
	}
	if pending.Triage != nil {
		if !triageRunFinished(pending.Triage) && pending.Triage.Cancel != nil {
			pending.Triage.Cancel()
		}
		pending.Triage = nil
	}
	deleteTriageRound(store, pending.IssueID, pending.Kind, log, ctx)
}

// ReactionEscalationTrigger names why an escalation fired. It selects
// the escalation's log message and, for the comment action, its text; it
// changes no action, no metric, and no post-condition.
type ReactionEscalationTrigger string

const (
	// EscalationTriggerBudget is the escalation a kind fires when its
	// own retry or continuation budget is spent.
	EscalationTriggerBudget ReactionEscalationTrigger = "budget_exhausted"

	// EscalationTriggerTriage is the escalation a triage command asked
	// for, with no budget spent on the subject.
	EscalationTriggerTriage ReactionEscalationTrigger = "triage"
)

// buildTriageEscalationComment returns the operator-facing comment for
// an escalation a triage command requested. One builder serves all four
// kinds. subject is the short phrase naming what was triaged: "ref <head
// sha>" for CI, and "PR #<number>" for the three pull-request kinds, the
// same subjects the budget escalations name.
func buildTriageEscalationComment(kind string, subject string) string {
	return fmt.Sprintf(
		"The %s triage command requested human attention for %s.\n\n"+
			"Sortie ran the configured triage command before dispatching a continuation "+
			"and the command answered \"escalate\", so no continuation was dispatched and "+
			"no retry or continuation budget was spent. Manual intervention required.",
		kind, subject)
}
