package orchestrator

import (
	"sync"
	"time"

	"github.com/sortie-ai/sortie/internal/redact"
	"github.com/sortie-ai/sortie/internal/workflow"
)

// Poll outcomes reported by [SnapshotPollHealth.Outcome].
const (
	pollOutcomeSucceeded       = "succeeded"
	pollOutcomePreflightFailed = "preflight_failed"
	pollOutcomeTrackerFailed   = "tracker_failed"
)

// SnapshotHealthCheck is one check's latest runs; Error is masked.
type SnapshotHealthCheck struct {
	// CheckedAt is when the latest run ended; zero before the first run.
	CheckedAt time.Time
	// LastSuccessAt is when a run last passed; zero before the first pass.
	LastSuccessAt time.Time
	// ConsecutiveFailures counts the runs failed since LastSuccessAt; the
	// check fails exactly when it is above 0.
	ConsecutiveFailures int
	// FailingSince is when the first of those runs ended; zero while the
	// check passes.
	FailingSince time.Time
	// Error is the latest failure's text, masked.
	Error string
}

// SnapshotWorkflowHealth is the workflow file check.
type SnapshotWorkflowHealth struct {
	SnapshotHealthCheck
	// File is the base filename of the workflow file in effect.
	File string
}

// SnapshotPollHealth is the poll loop check.
type SnapshotPollHealth struct {
	SnapshotHealthCheck
	// Outcome is "succeeded", "preflight_failed", or "tracker_failed"; empty
	// before the first poll.
	Outcome string
	// NextPollAt is when the poll timer fires next; zero while no poll is
	// scheduled.
	NextPollAt time.Time
}

// SnapshotSCMHealth is the source-control provider check.
type SnapshotSCMHealth struct {
	SnapshotHealthCheck
	// Provider is the provider kind every reaction shares.
	Provider string
}

// SnapshotHealth is the health of Sortie's own checks.
type SnapshotHealth struct {
	Database  SnapshotHealthCheck
	Preflight SnapshotHealthCheck
	Workflow  SnapshotWorkflowHealth
	Poll      SnapshotPollHealth
	// SCM is nil when no reaction uses a source-control provider.
	SCM *SnapshotSCMHealth
}

// Health returns the latest runs of Sortie's own checks with every error
// text masked. Safe for concurrent use; it performs no I/O.
func (o *Orchestrator) Health() SnapshotHealth {
	health := SnapshotHealth{
		Database:  o.databaseHealth.snapshot().masked(),
		Preflight: o.preflightHealth.snapshot().masked(),
		Workflow:  SnapshotWorkflowHealth{File: o.workflowFile()},
		Poll:      o.pollHealth.snapshot(),
	}
	health.Poll.SnapshotHealthCheck = health.Poll.masked()

	if o.workflowManager != nil {
		health.Workflow.SnapshotHealthCheck = workflowCheck(o.workflowManager.LoadStatus())
	}
	if o.scmHealth != nil {
		health.SCM = &SnapshotSCMHealth{
			SnapshotHealthCheck: o.scmHealth.snapshot().masked(),
			Provider:            o.scmProvider,
		}
	}
	return health
}

func workflowCheck(status workflow.LoadStatus) SnapshotHealthCheck {
	check := SnapshotHealthCheck{
		CheckedAt:           status.LastLoadAt,
		LastSuccessAt:       status.LastSuccessAt,
		ConsecutiveFailures: status.ConsecutiveFailures,
		FailingSince:        status.FailingSince,
	}
	if status.LastError != nil {
		check.Error = status.LastError.Error()
	}
	return check.masked()
}

// recordPreflight records a tick's dispatch preflight; warnings neither
// fail the check nor appear in it.
func (o *Orchestrator) recordPreflight(validation PreflightResult) {
	var failure error
	if !validation.OK() {
		failure = validation
	}
	o.preflightHealth.record(time.Now(), failure)
}

// advance leaves Error to the caller after a failure, because the poll
// check sets it only for some failures.
func (c *SnapshotHealthCheck) advance(at time.Time, failed bool) {
	c.CheckedAt = at
	if !failed {
		c.LastSuccessAt = at
		c.ConsecutiveFailures = 0
		c.FailingSince = time.Time{}
		c.Error = ""
		return
	}
	if c.ConsecutiveFailures == 0 {
		c.FailingSince = at
	}
	c.ConsecutiveFailures++
}

func (c SnapshotHealthCheck) masked() SnapshotHealthCheck {
	c.Error = redact.Mask(c.Error)
	return c
}

// healthRecord holds one check's runs. Its error text is unmasked and
// leaves the record only through [Orchestrator.Health].
type healthRecord struct {
	mu    sync.Mutex
	check SnapshotHealthCheck
}

func (r *healthRecord) record(at time.Time, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.check.advance(at, err != nil)
	if err != nil {
		r.check.Error = err.Error()
	}
}

func (r *healthRecord) snapshot() SnapshotHealthCheck {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.check
}

// pollRecord holds the poll check. Its error is the latest failed
// candidate read's text since the last succeeded poll.
type pollRecord struct {
	mu      sync.Mutex
	check   SnapshotHealthCheck
	outcome string
	next    time.Time
}

// record sets the error only for a tracker_failed poll, so a
// preflight_failed poll leaves the last tracker error in place.
func (p *pollRecord) record(at time.Time, outcome string, fetchErr error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.check.advance(at, outcome != pollOutcomeSucceeded)
	p.outcome = outcome
	if outcome == pollOutcomeTrackerFailed && fetchErr != nil {
		p.check.Error = fetchErr.Error()
	}
}

// schedule stores when the poll timer fires next; a zero time clears it.
func (p *pollRecord) schedule(next time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.next = next
}

func (p *pollRecord) snapshot() SnapshotPollHealth {
	p.mu.Lock()
	defer p.mu.Unlock()
	return SnapshotPollHealth{SnapshotHealthCheck: p.check, Outcome: p.outcome, NextPollAt: p.next}
}
