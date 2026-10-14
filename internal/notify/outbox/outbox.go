// Package outbox delivers orchestrator events durably. An event is
// persisted with its notification identity, rendered body, and frozen
// routing snapshot before any target is contacted, and each target's
// result is persisted as pending, succeeded, retryable, or permanently
// failed. Targets send independently under their own deadline, retries
// reach only targets no row confirms and reuse the original
// notification, and a restart resumes the targets still open.
//
// The package registers no notifier kind and imports no notifier
// package: the caller supplies a [route.NotifierLookup], exactly as the
// router does. Construct one [Dispatcher] with [New], recover earlier
// events with [Dispatcher.Recover], and hand routed deliveries to
// [Dispatcher.Enqueue].
package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/notify/route"
	"github.com/sortie-ai/sortie/internal/persistence"
)

// Delivery policy constants. Retries are a fixed budget with no
// configuration surface: a transient failure gets a short first and a
// longer second retry, and a target still unconfirmed after the third
// attempt is permanently failed. Every send carries its own deadline;
// the result write that follows gets its own short budget, so one
// attempt finishes inside the shutdown drain wait.
const (
	maxAttempts    = 3
	sendTimeout    = 30 * time.Second
	recordTimeout  = 5 * time.Second
	enqueueTimeout = 10 * time.Second
	retention      = 7 * 24 * time.Hour

	// persistRetryWait spaces retries of an attempt whose result did not
	// survive, so a broken database cannot spin a chain at full speed.
	persistRetryWait = 250 * time.Millisecond
)

// Default retry waits between attempts: a short first retry and a
// longer second one.
const (
	firstBackoff  = 1 * time.Second
	secondBackoff = 2 * time.Second
)

// maxAttempts is the total send budget per target: the initial attempt
// plus one per configured backoff.
func (d *Dispatcher) maxAttempts() int { return len(d.backoffs) + 1 }

// backoffAfter returns the wait before the attempt following an attempt
// numbered attemptsDone (1-based). Zero means the budget is spent.
func (d *Dispatcher) backoffAfter(attemptsDone int) time.Duration {
	if attemptsDone < 1 || attemptsDone > len(d.backoffs) {
		return 0
	}
	return d.backoffs[attemptsDone-1]
}

// Store is the persistence contract the dispatcher needs.
// [*persistence.Store] satisfies it.
type Store interface {
	CreateNotificationOutbox(ctx context.Context, event persistence.NotificationOutboxEvent, targets []persistence.NotificationOutboxTarget) error
	RecordNotificationTargetResult(ctx context.Context, notificationID, destination, status string, attempts int, lastError string, nextAttemptAtMS *int64, updatedAt, completedAt string) (bool, error)
	MarkNotificationOutboxCompleted(ctx context.Context, notificationID, completedAt string) error
	ListPendingNotificationOutbox(ctx context.Context) ([]persistence.NotificationOutbox, error)
	PruneCompletedNotificationOutbox(ctx context.Context, cutoff string) error
}

// TrackerResultError is the terminal failure a tracker_comment target
// recorded. Producers that wait on the tracker target use [errors.As]
// to distinguish a recorded failure from their own wait context ending.
type TrackerResultError struct {
	Category string
}

func (e *TrackerResultError) Error() string { return e.Category }

var (
	errUnstamped       = errors.New("notification carries no identity")
	errNoTrackerTarget = errors.New("delivery has no tracker_comment target")
)

// Params constructs a [Dispatcher]. Store is required; Tracker is nil
// in a deployment with no tracker, in which case a recovered
// tracker_comment target is permanently failed; Lookup is nil only
// where notification backends are never constructed; WaitGroup counts
// each target chain for its whole delivery life so the process's
// existing shutdown drain bounds them, and defaults to an internal
// group when nil. The count is added synchronously from the
// Enqueue/Recover caller, never from a chain goroutine, so the group
// keeps its Add-before-Wait contract.
type Params struct {
	Store     Store
	Tracker   domain.TrackerAdapter
	Lookup    route.NotifierLookup
	Logger    *slog.Logger
	WaitGroup *sync.WaitGroup
	NowFunc   func() time.Time

	// RetryBackoffs overrides the waits between attempts, one entry per
	// retry. Production leaves it nil for the fixed default; tests shrink
	// it.
	RetryBackoffs []time.Duration

	// AttemptTimeout overrides the per-send deadline. Production leaves
	// it zero for the fixed default; tests shrink it.
	AttemptTimeout time.Duration
}

// Dispatcher owns the adopted durable deliveries and their in-flight
// target chains. Enqueue is safe for concurrent use.
type Dispatcher struct {
	store          Store
	tracker        domain.TrackerAdapter
	lookup         route.NotifierLookup
	log            *slog.Logger
	wg             *sync.WaitGroup
	now            func() time.Time
	backoffs       []time.Duration
	attemptTimeout time.Duration

	mu            sync.Mutex
	events        map[string]*eventRuntime
	inFlightCount int
	stopped       bool
	stopOnce      sync.Once
	stopCh        chan struct{}
}

// eventRuntime is one adopted outbox event. Runtimes stay in the map
// for the process's life; the persisted rows are pruned past retention.
type eventRuntime struct {
	notification domain.Notification
	targets      map[string]*targetRuntime

	// trackerHooks run once when the tracker_comment target reaches a
	// terminal result. Empty for an event without a tracker target and
	// for every event recovered after a restart.
	trackerHooks []func(received bool, err error)
	trackerFired bool

	// waiters are signaled on every tracker target status change.
	waiters []chan struct{}
}

// targetRuntime is one adopted target and its mutable delivery state.
type targetRuntime struct {
	spec          route.TargetSpec
	notifier      domain.Notifier
	status        string
	attempts      int
	nextAttemptAt time.Time
	inFlight      bool
	lastError     string
}

// New returns a Dispatcher. Earlier events are adopted through
// [Dispatcher.Recover]; each adopted target runs as its own chain
// goroutine that performs the attempt budget and the retry waits
// itself, so no scheduler goroutine is needed.
func New(params Params) *Dispatcher {
	log := params.Logger
	if log == nil {
		log = slog.Default()
	}
	wg := params.WaitGroup
	if wg == nil {
		wg = new(sync.WaitGroup)
	}
	now := params.NowFunc
	if now == nil {
		now = time.Now
	}

	backoffs := params.RetryBackoffs
	if backoffs == nil {
		backoffs = []time.Duration{firstBackoff, secondBackoff}
	}
	attemptTimeout := params.AttemptTimeout
	if attemptTimeout <= 0 {
		attemptTimeout = sendTimeout
	}

	d := &Dispatcher{
		store:          params.Store,
		tracker:        params.Tracker,
		lookup:         params.Lookup,
		log:            log,
		wg:             wg,
		now:            now,
		backoffs:       backoffs,
		attemptTimeout: attemptTimeout,
		events:         make(map[string]*eventRuntime),
		stopCh:         make(chan struct{}),
	}
	return d
}

// Recover adopts events persisted before this process started: it prunes
// completed events past retention, then adopts every open event with
// its targets so due targets resume and confirmed targets are never
// sent again. It runs before the event loop starts. A recovered event
// has no producer hooks: delivery resumes, but counters and logs of the
// run that died are not replayed.
func (d *Dispatcher) Recover(ctx context.Context) error {
	cutoff := d.now().UTC().Add(-retention).Format(time.RFC3339Nano)
	if err := d.store.PruneCompletedNotificationOutbox(ctx, cutoff); err != nil {
		return err
	}

	pending, err := d.store.ListPendingNotificationOutbox(ctx)
	if err != nil {
		return err
	}
	var recovered []*eventRuntime
	for _, record := range pending {
		if err := ctx.Err(); err != nil {
			return err
		}
		if runtime := d.adoptRecovered(ctx, record); runtime != nil {
			recovered = append(recovered, runtime)
		}
	}

	var chains []chainTarget
	d.mu.Lock()
	for _, runtime := range recovered {
		chains = append(chains, d.claimChainsLocked(runtime)...)
	}
	d.mu.Unlock()
	d.launchChains(chains)
	return nil
}

// StopScheduling ends retry waits: every chain sleeping between
// attempts returns without another send, leaving its row open for the
// next process's Recover. A send already in flight keeps running under
// its own deadline and still records its result, finishing in the wait
// group the process drains at shutdown. Safe to call more than once.
// Enqueue after stop still persists and launches a chain whose first
// attempt sends, but a retryable result is not retried in this process.
func (d *Dispatcher) StopScheduling() {
	d.stopOnce.Do(func() {
		d.mu.Lock()
		d.stopped = true
		d.mu.Unlock()
		close(d.stopCh)
	})
}

// WaitIdle blocks until every target of every adopted event has
// reached a terminal result and its chain goroutine has finished all
// persisted side effects, or ctx ends. It is the orderly wait callers
// use when they need a producer's counters settled; the process
// shutdown drains in-flight chains through the wait group instead.
func (d *Dispatcher) WaitIdle(ctx context.Context) error {
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		d.mu.Lock()
		settled := d.inFlightCount == 0
		for _, runtime := range d.events {
			for _, target := range runtime.targets {
				if target.inFlight || !isTerminal(target.status) {
					settled = false
				}
			}
		}
		d.mu.Unlock()
		if settled {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Enqueue persists the routed event and every frozen target before any
// send and starts delivery. It returns the notification id and whether
// tracker_comment is one of the targets. An empty delivery persists
// nothing, starts nothing, and returns "", false, nil. onTracker, when
// non-nil, runs exactly once after the tracker_comment target reaches a
// terminal result; it is never invoked when tracker_comment is not a
// target.
func (d *Dispatcher) Enqueue(delivery route.Delivery, onTracker func(received bool, err error)) (string, bool, error) {
	specs := delivery.Specs()
	if len(specs) == 0 {
		return "", false, nil
	}
	notification := delivery.Notification()
	if notification.Envelope.NotificationID == "" {
		return "", false, errUnstamped
	}
	id := notification.Envelope.NotificationID

	payload, err := json.Marshal(notification)
	if err != nil {
		return "", false, &domain.PermanentNotifierError{Err: fmt.Errorf("encode notification payload: %w", err)}
	}

	now := d.now().UTC()
	nowText := now.Format(time.RFC3339Nano)

	eventRow := persistence.NotificationOutboxEvent{
		NotificationID: id,
		EventType:      string(notification.Envelope.EventType),
		Payload:        string(payload),
		Status:         persistence.NotificationOutboxPending,
		CreatedAt:      nowText,
	}
	targetRows := make([]persistence.NotificationOutboxTarget, 0, len(specs))
	for _, spec := range specs {
		row := persistence.NotificationOutboxTarget{
			NotificationID: id,
			Destination:    spec.Destination,
			Kind:           spec.Kind,
			CreatedAt:      nowText,
			UpdatedAt:      nowText,
		}
		if spec.Kind != domain.TrackerCommentKind {
			raw, err := json.Marshal(spec.Config)
			if err != nil {
				return "", false, &domain.PermanentNotifierError{Err: fmt.Errorf("encode backend configuration: %w", err)}
			}
			row.BackendConfig = string(raw)
		}
		targetRows = append(targetRows, row)
	}

	enqueueCtx, cancel := context.WithTimeout(context.Background(), enqueueTimeout)
	defer cancel()
	if err := d.store.CreateNotificationOutbox(enqueueCtx, eventRow, targetRows); err != nil {
		return "", false, err
	}

	var immediateHooks []func(bool, error)
	var immediateErr error
	d.mu.Lock()
	runtime := d.events[id]
	if runtime == nil {
		runtime = d.adoptLocked(notification, specs, nil)
		d.events[id] = runtime
	}
	trackerTarget := false
	if t := runtime.targets[domain.TrackerCommentKind]; t != nil {
		trackerTarget = true
		if onTracker != nil && isTerminal(t.status) {
			immediateHooks = d.fireTrackerHookLocked(runtime)
			if t.status == persistence.NotificationTargetFailed {
				immediateErr = &TrackerResultError{Category: t.lastError}
			}
		} else if onTracker != nil {
			runtime.trackerHooks = append(runtime.trackerHooks, onTracker)
		}
	}
	chains := d.claimChainsLocked(runtime)
	d.mu.Unlock()

	// Add before launch, from this caller's goroutine: a producer that
	// waits the group after Enqueue gets the Add-before-Wait ordering
	// WaitGroup requires, including when that caller is itself a group
	// goroutine, because its own add already happened earlier.
	d.launchChains(chains)
	fireHooks(immediateHooks, immediateErr)
	return id, trackerTarget, nil
}

// AwaitTrackerSettled waits until the tracker_comment target leaves
// pending. It reports terminal=true with nil on success and
// terminal=true with a [*TrackerResultError] on a recorded permanent
// failure; a retryable first attempt returns terminal=false with nil so
// a producer can proceed while the retry chain finishes, with the
// terminal hook recording the eventual result. It returns false with
// the context error when ctx ends first.
func (d *Dispatcher) AwaitTrackerSettled(ctx context.Context, notificationID string) (bool, error) {
	return d.awaitTracker(ctx, notificationID, true)
}

// AwaitTrackerTerminal waits until the tracker_comment target reaches a
// terminal result. A recorded permanent failure returns true with a
// [*TrackerResultError]; success returns true with nil; an ended ctx
// returns false with its error.
func (d *Dispatcher) AwaitTrackerTerminal(ctx context.Context, notificationID string) (bool, error) {
	return d.awaitTracker(ctx, notificationID, false)
}

func (d *Dispatcher) awaitTracker(ctx context.Context, notificationID string, settleOK bool) (bool, error) {
	d.mu.Lock()
	runtime := d.events[notificationID]
	if runtime == nil || runtime.targets[domain.TrackerCommentKind] == nil {
		d.mu.Unlock()
		return false, errNoTrackerTarget
	}
	target := runtime.targets[domain.TrackerCommentKind]
	switch {
	case isTerminal(target.status):
		lastErr := target.lastError
		d.mu.Unlock()
		if target.status == persistence.NotificationTargetFailed {
			return true, &TrackerResultError{Category: lastErr}
		}
		return true, nil
	case settleOK && target.status == persistence.NotificationTargetRetryable:
		d.mu.Unlock()
		return false, nil
	}
	ch := make(chan struct{}, 1)
	runtime.waiters = append(runtime.waiters, ch)
	d.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			d.mu.Lock()
			runtime.waiters = removeWaiter(runtime.waiters, ch)
			d.mu.Unlock()
			return false, ctx.Err()
		case <-ch:
		}

		d.mu.Lock()
		target = runtime.targets[domain.TrackerCommentKind]
		status := target.status
		lastErr := target.lastError
		d.mu.Unlock()

		switch {
		case isTerminal(status):
			if status == persistence.NotificationTargetFailed {
				return true, &TrackerResultError{Category: lastErr}
			}
			return true, nil
		case settleOK && status == persistence.NotificationTargetRetryable:
			return false, nil
		}
	}
}

// chainTarget pairs an adopted target with its event for a chain launch.
type chainTarget struct {
	runtime *eventRuntime
	dest    string
	target  *targetRuntime
}

// claimChainsLocked returns and marks in flight every open target of
// runtime that no chain owns yet. Caller holds d.mu.
func (d *Dispatcher) claimChainsLocked(runtime *eventRuntime) []chainTarget {
	var chains []chainTarget
	for dest, target := range runtime.targets {
		if target.inFlight || isTerminal(target.status) {
			continue
		}
		target.inFlight = true
		d.inFlightCount++
		chains = append(chains, chainTarget{runtime: runtime, dest: dest, target: target})
	}
	return chains
}

// launchChains counts every chain in the wait group and starts it. The
// add happens in the caller's goroutine before the goroutine starts,
// which is what keeps a later Wait on the group race-free.
func (d *Dispatcher) launchChains(chains []chainTarget) {
	if len(chains) == 0 {
		return
	}
	d.wg.Add(len(chains))
	for _, chain := range chains {
		go d.runChain(chain.runtime, chain.dest, chain.target)
	}
}

// chainFinished releases the target's in-flight slot after all of its
// persisted result and hook side effects are complete.
func (d *Dispatcher) chainFinished(target *targetRuntime) {
	d.mu.Lock()
	target.inFlight = false
	d.inFlightCount--
	d.mu.Unlock()
}

// sleepStoppable waits for wait or until StopScheduling fires. It
// reports whether the wait elapsed; false means this process must leave
// the row open for the next one.
func (d *Dispatcher) sleepStoppable(wait time.Duration) bool {
	if wait <= 0 {
		return true
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-d.stopCh:
		return false
	case <-timer.C:
		return true
	}
}

// runChain owns one target for its whole delivery life: it waits out a
// retry time a previous process calculated, performs the attempt budget
// with the retry waits between, and returns on a terminal result or when
// stopping leaves the row open. The send context is independent of any
// producer context and bounded by its own deadline, so a slow target
// neither blocks its sibling targets nor is cut off by a producer that
// returned; the result write gets a separate short budget so the chain
// finishes inside the shutdown drain wait.
func (d *Dispatcher) runChain(runtime *eventRuntime, destination string, target *targetRuntime) {
	defer d.wg.Done()
	defer d.chainFinished(target)
	notification := runtime.notification

	// Honor a retry due time a previous process already persisted. A
	// stop during that wait hands the attempt to the next process.
	d.mu.Lock()
	nextAttemptAt := target.nextAttemptAt
	d.mu.Unlock()
	if wait := time.Until(nextAttemptAt); !nextAttemptAt.IsZero() && wait > 0 {
		if !d.sleepStoppable(wait) {
			return
		}
	}

	for {
		sendCtx, sendCancel := context.WithTimeout(context.Background(), d.attemptTimeout)
		err := target.notifier.Send(sendCtx, notification)
		sendCancel()

		status, category := classifySend(err)

		d.mu.Lock()
		target.attempts++
		attempts := target.attempts
		target.lastError = category

		var nextMS *int64
		var backoff time.Duration
		if status == persistence.NotificationTargetRetryable {
			backoff = d.backoffAfter(attempts)
			if attempts >= d.maxAttempts() || backoff == 0 {
				status = persistence.NotificationTargetFailed
				category = exhausted(category)
				target.lastError = category
			} else {
				nextAt := d.now().Add(backoff)
				nextMS = new(int64)
				*nextMS = nextAt.UnixMilli()
				target.nextAttemptAt = nextAt
			}
		}
		target.status = status
		d.mu.Unlock()

		nowText := d.now().UTC().Format(time.RFC3339Nano)
		recordCtx, recordCancel := context.WithTimeout(context.Background(), recordTimeout)
		_, persistErr := d.store.RecordNotificationTargetResult(
			recordCtx, notification.Envelope.NotificationID, destination,
			status, attempts, category, nextMS, nowText, nowText,
		)
		recordCancel()
		if persistErr != nil {
			// The send may have reached the destination but the result did
			// not survive; reopen the target so it sends again, the only
			// recovery the durable row allows. The category never names the
			// endpoint or payload.
			d.log.Error("failed to persist notification target result; target stays open",
				slog.String("event_type", string(notification.Envelope.EventType)),
				slog.String("destination", destination),
				slog.String("notifier_kind", target.spec.Kind),
				slog.Any("error", persistErr),
			)
			d.mu.Lock()
			target.status = persistence.NotificationTargetPending
			target.attempts = attempts - 1
			target.nextAttemptAt = time.Time{}
			d.mu.Unlock()
			if !d.sleepStoppable(persistRetryWait) {
				return
			}
			continue
		}

		var hooks []func(bool, error)
		var waiters []chan struct{}
		trackerTerminalResult := target.spec.Kind == domain.TrackerCommentKind && isTerminal(status)
		d.mu.Lock()
		if trackerTerminalResult {
			hooks = d.fireTrackerHookLocked(runtime)
		}
		waiters = append(waiters, runtime.waiters...)
		d.mu.Unlock()

		d.logOutcome(notification, target, destination, status, attempts, len(hooks) > 0)

		if trackerTerminalResult {
			var hookErr error
			if status == persistence.NotificationTargetFailed {
				hookErr = &TrackerResultError{Category: category}
			}
			fireHooks(hooks, hookErr)
		}
		for _, ch := range waiters {
			select {
			case ch <- struct{}{}:
			default:
			}
		}

		if isTerminal(status) {
			return
		}
		// A recorded retryable result after scheduling stops is left for
		// the next process; its next attempt time is already persisted.
		if !d.sleepStoppable(backoff) {
			return
		}
	}
}

// adoptRecovered rebuilds one persisted event's runtime. Terminal
// targets are kept but never sent; an open target that cannot rebuild a
// backend in this process is permanently failed through the same
// persistence path a normal failure uses. It returns the adopted
// runtime, or nil when nothing was adopted.
func (d *Dispatcher) adoptRecovered(ctx context.Context, record persistence.NotificationOutbox) *eventRuntime {
	var notification domain.Notification
	if err := json.Unmarshal([]byte(record.Event.Payload), &notification); err != nil {
		d.log.Error("skipping malformed persisted notification event",
			slog.String("notification_id", record.Event.NotificationID),
			slog.Any("error", err),
		)
		return nil
	}

	specs := make([]route.TargetSpec, 0, len(record.Targets))
	for _, row := range record.Targets {
		var raw map[string]any
		if row.BackendConfig != "" {
			if err := json.Unmarshal([]byte(row.BackendConfig), &raw); err != nil {
				d.log.Error("skipping notification target with malformed frozen configuration",
					slog.String("notification_id", record.Event.NotificationID),
					slog.String("destination", row.Destination),
					slog.Any("error", err),
				)
				continue
			}
		}
		specs = append(specs, route.TargetSpec{
			Destination: row.Destination,
			Kind:        row.Kind,
			Config:      raw,
		})
	}
	if len(specs) == 0 {
		return nil
	}

	d.mu.Lock()
	if d.events[record.Event.NotificationID] != nil {
		d.mu.Unlock()
		return nil
	}
	runtime := d.adoptLocked(notification, specs, record.Targets)
	d.events[record.Event.NotificationID] = runtime
	d.mu.Unlock()

	// Correct open targets that cannot make progress in this process:
	// a tracker target without a tracker adapter, a backend kind the
	// binary no longer provides, or a retryable row whose attempt
	// budget was spent before the previous process recorded the final
	// failure.
	nowText := d.now().UTC().Format(time.RFC3339Nano)
	for _, row := range record.Targets {
		target := runtime.targets[row.Destination]
		if target == nil || isTerminal(row.Status) {
			continue
		}

		category := ""
		switch {
		case target.notifier == nil:
			category = "backend unavailable: " + target.spec.Kind
		case target.status == persistence.NotificationTargetFailed:
			category = target.lastError
		default:
			continue
		}

		if _, err := d.store.RecordNotificationTargetResult(
			ctx, record.Event.NotificationID, target.spec.Destination,
			persistence.NotificationTargetFailed, d.maxAttempts(), category, nil, nowText, nowText,
		); err != nil {
			d.log.Error("failed to permanently fail an unrecoverable notification target",
				slog.String("destination", target.spec.Destination),
				slog.Any("error", err),
			)
			continue
		}
		target.status = persistence.NotificationTargetFailed
		target.attempts = d.maxAttempts()
		target.lastError = category
		d.log.Warn("notification target permanently failed",
			slog.String("event_type", string(notification.Envelope.EventType)),
			slog.String("destination", target.spec.Destination),
			slog.String("notifier_kind", target.spec.Kind),
			slog.String("error", category),
		)
	}

	if allTerminal(runtime) {
		if err := d.store.MarkNotificationOutboxCompleted(ctx, record.Event.NotificationID, nowText); err != nil {
			d.log.Error("failed to mark recovered notification event completed",
				slog.String("notification_id", record.Event.NotificationID),
				slog.Any("error", err),
			)
		}
	}
	return runtime
}

// adoptLocked builds the runtime for notification and specs, restoring
// state from rows when non-nil. Caller holds d.mu.
func (d *Dispatcher) adoptLocked(notification domain.Notification, specs []route.TargetSpec, rows []persistence.NotificationOutboxTarget) *eventRuntime {
	rowsByDest := make(map[string]persistence.NotificationOutboxTarget, len(rows))
	for _, row := range rows {
		rowsByDest[row.Destination] = row
	}

	runtime := &eventRuntime{
		notification: notification,
		targets:      make(map[string]*targetRuntime, len(specs)),
	}
	for _, spec := range specs {
		target := &targetRuntime{spec: spec, notifier: spec.Notifier, status: persistence.NotificationTargetPending}
		if target.notifier == nil {
			target.notifier = d.rebuildNotifier(spec)
		}
		if row, ok := rowsByDest[spec.Destination]; ok {
			target.status = row.Status
			target.attempts = row.Attempts
			target.lastError = row.LastError
			if row.NextAttemptAtMS != nil {
				target.nextAttemptAt = time.UnixMilli(*row.NextAttemptAtMS)
			}
			if target.status == persistence.NotificationTargetRetryable && target.attempts >= d.maxAttempts() {
				target.status = persistence.NotificationTargetFailed
				target.lastError = exhausted(target.lastError)
			}
		}
		runtime.targets[spec.Destination] = target
	}
	return runtime
}

// rebuildNotifier reconstructs a frozen target's backend outside the
// main-process router: tracker_comment from the tracker adapter, every
// other kind through the lookup with env references re-expanded. It
// returns nil when the backend cannot be built, which recovery turns
// into a permanent target failure.
func (d *Dispatcher) rebuildNotifier(spec route.TargetSpec) domain.Notifier {
	if spec.Kind == domain.TrackerCommentKind {
		if d.tracker == nil {
			return nil
		}
		return route.NewTrackerComment(d.tracker)
	}
	if d.lookup == nil {
		return nil
	}
	constructor, err := d.lookup(spec.Kind)
	if err != nil {
		return nil
	}
	resolved := cloneMap(spec.Config)
	notifier, err := constructor(resolved)
	if err != nil {
		return nil
	}
	return notifier
}

// fireTrackerHookLocked returns and clears the tracker terminal hooks,
// ensuring they fire at most once. Caller holds d.mu.
func (d *Dispatcher) fireTrackerHookLocked(runtime *eventRuntime) []func(bool, error) {
	if runtime == nil || runtime.trackerFired {
		return nil
	}
	runtime.trackerFired = true
	hooks := runtime.trackerHooks
	runtime.trackerHooks = nil
	return hooks
}

func (d *Dispatcher) logOutcome(notification domain.Notification, target *targetRuntime, destination, status string, attempts int, trackerHooked bool) {
	attrs := []any{
		slog.String("event_type", string(notification.Envelope.EventType)),
		slog.String("destination", destination),
		slog.String("notifier_kind", target.spec.Kind),
		slog.Int("attempts", attempts),
	}
	switch status {
	case persistence.NotificationTargetSucceeded:
		if target.spec.Kind != domain.TrackerCommentKind {
			d.log.Debug("notification delivered", attrs...)
		}
	case persistence.NotificationTargetRetryable:
		attrs = append(attrs, slog.String("error", target.lastError))
		if target.spec.Kind != domain.TrackerCommentKind || !trackerHooked {
			d.log.Warn("notification delivery failed, retry scheduled", attrs...)
		}
	case persistence.NotificationTargetFailed:
		attrs = append(attrs, slog.String("error", target.lastError))
		if target.spec.Kind != domain.TrackerCommentKind || !trackerHooked {
			d.log.Warn("notification delivery permanently failed", attrs...)
		}
	}
}

func fireHooks(hooks []func(bool, error), err error) {
	for _, hook := range hooks {
		hook(true, err)
	}
}

func allTerminal(runtime *eventRuntime) bool {
	for _, target := range runtime.targets {
		if !isTerminal(target.status) {
			return false
		}
	}
	return true
}

func isTerminal(status string) bool {
	return status == persistence.NotificationTargetSucceeded || status == persistence.NotificationTargetFailed
}

func removeWaiter(waiters []chan struct{}, ch chan struct{}) []chan struct{} {
	for i, waiter := range waiters {
		if waiter == ch {
			return append(waiters[:i], waiters[i+1:]...)
		}
	}
	return waiters
}

func cloneMap(in map[string]any) map[string]any {
	if in == nil {
		return map[string]any{}
	}
	return config.ResolveEnvReferences(cloneValue(in)).(map[string]any)
}

func cloneValue(v any) any {
	switch typed := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for k, child := range typed {
			out[k] = cloneValue(child)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, child := range typed {
			out[i] = cloneValue(child)
		}
		return out
	default:
		return v
	}
}
