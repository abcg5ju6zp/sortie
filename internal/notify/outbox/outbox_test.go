package outbox

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/notify/route"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/registry"
)

// fakeNotifier is a controllable domain.Notifier.
type fakeNotifier struct {
	mu      sync.Mutex
	sends   []domain.Notification
	err     error
	block   chan struct{}
	entered chan struct{}
	attempt int
}

func (n *fakeNotifier) Send(ctx context.Context, notification domain.Notification) error {
	n.mu.Lock()
	n.attempt++
	n.sends = append(n.sends, notification)
	entered, block := n.entered, n.block
	n.mu.Unlock()
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return n.err
}

func (n *fakeNotifier) snapshot() ([]domain.Notification, int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]domain.Notification, len(n.sends))
	copy(out, n.sends)
	return out, n.attempt
}

// flakyNotifier fails its first failuresLeft sends, then succeeds.
type flakyNotifier struct {
	mu           sync.Mutex
	attempts     int
	failuresLeft int
	sends        []domain.Notification
}

func (n *flakyNotifier) Send(_ context.Context, notification domain.Notification) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.attempts++
	n.sends = append(n.sends, notification)
	if n.failuresLeft > 0 {
		n.failuresLeft--
		return &classifiedError{text: "timeout"}
	}
	return nil
}

// fakeTracker records comments and can block or fail them.
type fakeTracker struct {
	domain.TrackerAdapter
	mu       sync.Mutex
	comments []string
	err      error
	block    chan struct{}
	entered  chan struct{}
}

func (t *fakeTracker) CommentIssue(_ context.Context, _ string, body string) error {
	t.mu.Lock()
	t.comments = append(t.comments, body)
	entered, block, err := t.entered, t.block, t.err
	t.mu.Unlock()
	if entered != nil {
		select {
		case entered <- struct{}{}:
		default:
		}
	}
	if block != nil {
		<-block
	}
	return err
}

func (t *fakeTracker) count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.comments)
}

// retryOnceTracker fails once with a retryable error, then succeeds.
type retryOnceTracker struct {
	domain.TrackerAdapter
	mu       sync.Mutex
	failLeft int
	calls    int
}

func (t *retryOnceTracker) CommentIssue(_ context.Context, _ string, _ string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.calls++
	if t.failLeft > 0 {
		t.failLeft--
		return &domain.TrackerError{Kind: domain.ErrTrackerAPI, Message: "rate limited"}
	}
	return nil
}

func (t *retryOnceTracker) callCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.calls
}

// classifiedError implements the same capabilities as the notifier
// backends' classified failure.
type classifiedError struct {
	text      string
	permanent bool
}

func (e *classifiedError) Error() string           { return e.text }
func (e *classifiedError) FailureCategory() string { return e.text }
func (e *classifiedError) PermanentFailure() bool  { return e.permanent }

func permanentError(text string) error { return &classifiedError{text: text, permanent: true} }

// fakeLookup resolves registered notifiers through the same constructor
// shape the registry exposes.
type fakeLookup struct {
	mu        sync.Mutex
	notifiers map[string]domain.Notifier
	calls     int
	failKind  string
}

func (l *fakeLookup) lookup(kind string) (registry.NotifierConstructor, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls++
	if kind == l.failKind {
		return nil, fmt.Errorf("backend unavailable: %s", kind)
	}
	n, ok := l.notifiers[kind]
	if !ok {
		return nil, fmt.Errorf("unknown kind: %s", kind)
	}
	return func(map[string]any) (domain.Notifier, error) { return n, nil }, nil
}

func (l *fakeLookup) callCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls
}

func newTestStore(t *testing.T) *persistence.Store {
	t.Helper()
	s, err := persistence.Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { s.Close() }) //nolint:errcheck
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s
}

type harness struct {
	t         *testing.T
	store     *persistence.Store
	tracker   *fakeTracker
	slack     *fakeNotifier
	lookup    *fakeLookup
	router    *route.Router
	wg        sync.WaitGroup
	fastRetry bool
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	t.Setenv("OUTBOX_TEST_URL", "https://restart.example")

	h := &harness{
		t:       t,
		store:   newTestStore(t),
		tracker: &fakeTracker{},
		slack:   &fakeNotifier{},
	}
	h.lookup = &fakeLookup{notifiers: map[string]domain.Notifier{
		"slack": h.slack,
	}}
	h.rebuildRouter(h.tracker)
	return h
}

func (h *harness) rebuildRouter(tracker domain.TrackerAdapter) {
	h.router = route.NewRouter(tracker, h.lookup.lookup)
	if err := h.router.Update(route.Inputs{
		Entries: []config.NotificationBackend{{
			Kind:      "slack",
			RawConfig: map[string]any{"webhook_url": "$OUTBOX_TEST_URL"},
			Config:    map[string]any{"webhook_url": "https://resolved.example"},
			Events:    []domain.EventType{domain.EventSessionCompleted},
		}},
		Comments: config.TrackerCommentsConfig{OnCompletion: true},
	}); err != nil {
		panic(err)
	}
}

func (h *harness) newDispatcher(tracker domain.TrackerAdapter) *Dispatcher {
	if tracker == nil {
		tracker = h.tracker
	}
	params := Params{
		Store:     h.store,
		Tracker:   tracker,
		Lookup:    h.lookup.lookup,
		Logger:    slog.New(slog.DiscardHandler),
		WaitGroup: &h.wg,
	}
	if h.fastRetry {
		params.RetryBackoffs = []time.Duration{5 * time.Millisecond, 5 * time.Millisecond}
	}
	d := New(params)
	h.t.Cleanup(d.StopScheduling)
	return d
}

func (h *harness) routeEvent() route.Delivery {
	n := domain.Notification{
		Envelope: domain.NotificationEnvelope{
			IssueID:    "ISSUE-1",
			Identifier: "ISSUE-1",
			EventType:  domain.EventSessionCompleted,
		},
		Message: domain.NotificationMessage{
			Severity: "info",
			Title:    "ISSUE-1: session.completed",
			Body:     "done",
		},
	}
	return h.router.Route(n)
}

// waitIdle waits for every target d adopted to reach a terminal result
// and for its chain goroutines to return. It polls dispatcher state
// before waiting on the group so a failure reports the open target
// instead of hanging.
func (h *harness) waitIdle(t *testing.T, d *Dispatcher) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		d.mu.Lock()
		settled := true
		for _, runtime := range d.events {
			for _, target := range runtime.targets {
				if target.inFlight || !isTerminal(target.status) {
					settled = false
				}
			}
		}
		d.mu.Unlock()
		if settled {
			h.wg.Wait()
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("not all targets reached a terminal result before timeout")
}

func targetRow(t *testing.T, store *persistence.Store, id, destination string) persistence.NotificationOutboxTarget {
	t.Helper()
	row, ok, err := store.GetNotificationOutboxTarget(context.Background(), id, destination)
	if err != nil {
		t.Fatalf("get target %s/%s: %v", id, destination, err)
	}
	if !ok {
		t.Fatalf("target %s/%s not persisted", id, destination)
	}
	return row
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

func TestEnqueue_PersistsAndDeliversFrozenIdentity(t *testing.T) {
	h := newHarness(t)
	d := h.newDispatcher(nil)

	delivery := h.routeEvent()
	id, trackerTarget, err := d.Enqueue(delivery, nil)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if !trackerTarget || id == "" {
		t.Fatalf("Enqueue = (%q, %v), want a tracker target and an id", id, trackerTarget)
	}
	h.waitIdle(t, d)

	if got := h.tracker.count(); got != 1 {
		t.Fatalf("tracker comments = %d, want 1", got)
	}
	sends, _ := h.slack.snapshot()
	if len(sends) != 1 {
		t.Fatalf("slack sends = %d, want 1", len(sends))
	}
	if sends[0].Envelope.NotificationID != id {
		t.Fatalf("slack id = %q, want %q", sends[0].Envelope.NotificationID, id)
	}

	pending, err := h.store.ListPendingNotificationOutbox(context.Background())
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending events = %d, want 0 after delivery", len(pending))
	}
}

func TestEnqueue_EmptyDeliveryPersistsNothing(t *testing.T) {
	h := newHarness(t)
	d := h.newDispatcher(nil)

	id, trackerTarget, err := d.Enqueue(route.Delivery{}, nil)
	if err != nil || id != "" || trackerTarget {
		t.Fatalf("empty delivery = (%q, %v, %v), want zero values", id, trackerTarget, err)
	}
	pending, err := h.store.ListPendingNotificationOutbox(context.Background())
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending events for empty delivery = %d, want 0", len(pending))
	}
}

func TestDelivery_SlowTargetDoesNotBlockOthers(t *testing.T) {
	h := newHarness(t)
	trackerEntered := make(chan struct{}, 1)
	h.tracker.entered = trackerEntered
	h.tracker.block = make(chan struct{})

	d := h.newDispatcher(nil)
	if _, _, err := d.Enqueue(h.routeEvent(), nil); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	<-trackerEntered

	waitFor(t, 2*time.Second, func() bool {
		_, n := h.slack.snapshot()
		return n == 1
	})
	close(h.tracker.block)
	h.waitIdle(t, d)
}

func TestDelivery_PermanentFailureRecordsFailedWithoutRetry(t *testing.T) {
	h := newHarness(t)
	h.slack.err = permanentError("unauthorized (HTTP 401)")
	d := h.newDispatcher(nil)

	id, _, err := d.Enqueue(h.routeEvent(), nil)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	h.waitIdle(t, d)

	_, attempts := h.slack.snapshot()
	if attempts != 1 {
		t.Fatalf("slack attempts on permanent rejection = %d, want 1", attempts)
	}
	row := targetRow(t, h.store, id, "notifications[0]")
	if row.Status != persistence.NotificationTargetFailed || row.LastError != "unauthorized (HTTP 401)" {
		t.Fatalf("target = %q/%q, want failed with the rejection category", row.Status, row.LastError)
	}
}

func TestDelivery_RetryOnlyUnconfirmedTargetKeepsIdentity(t *testing.T) {
	h := newHarness(t)
	h.fastRetry = true
	flaky := &flakyNotifier{failuresLeft: 2}
	h.lookup.notifiers["slack"] = flaky
	h.rebuildRouter(h.tracker)
	d := h.newDispatcher(nil)

	id, _, err := d.Enqueue(h.routeEvent(), nil)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	h.waitIdle(t, d)

	if flaky.attempts != 3 {
		t.Fatalf("slack attempts = %d, want 3 (fail, fail, success)", flaky.attempts)
	}
	for _, n := range flaky.sends {
		if n.Envelope.NotificationID != id {
			t.Fatalf("retry sent id %q, want original %q", n.Envelope.NotificationID, id)
		}
	}
	if got := h.tracker.count(); got != 1 {
		t.Fatalf("tracker sends = %d, want 1: a confirmed target is never retried", got)
	}
	row := targetRow(t, h.store, id, "notifications[0]")
	if row.Status != persistence.NotificationTargetSucceeded {
		t.Fatalf("slack status = %q, want succeeded", row.Status)
	}
}

func TestTrackerHook_FiresFromPersistedTerminalResult(t *testing.T) {
	h := newHarness(t)
	d := h.newDispatcher(nil)

	hookCh := make(chan error, 1)
	_, trackerTarget, err := d.Enqueue(h.routeEvent(), func(_ bool, err error) { hookCh <- err })
	if err != nil || !trackerTarget {
		t.Fatalf("Enqueue = tracker %v, err %v", trackerTarget, err)
	}
	select {
	case err := <-hookCh:
		if err != nil {
			t.Fatalf("hook error on success: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("tracker terminal hook never fired")
	}
}

func TestTrackerHook_PermanentFailureReportsResultError(t *testing.T) {
	h := newHarness(t)
	h.tracker.err = &domain.TrackerError{Kind: domain.ErrTrackerAuth, Message: "forbidden"}
	d := h.newDispatcher(nil)

	hookCh := make(chan error, 1)
	if _, _, err := d.Enqueue(h.routeEvent(), func(_ bool, err error) { hookCh <- err }); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	select {
	case err := <-hookCh:
		if _, ok := errors.AsType[*TrackerResultError](err); !ok {
			t.Fatalf("hook error = %T, want *TrackerResultError", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("tracker terminal hook never fired")
	}
}

func TestAwaitTrackerSettled_RetryableFirstAttempt(t *testing.T) {
	h := newHarness(t)
	h.fastRetry = true
	tracker := &retryOnceTracker{failLeft: 1}
	h.rebuildRouter(tracker)
	d := h.newDispatcher(tracker)

	id, _, err := d.Enqueue(h.routeEvent(), nil)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	terminal, err := d.AwaitTrackerSettled(context.Background(), id)
	if err != nil || terminal {
		t.Fatalf("settle = terminal %v err %v, want a retryable deferral", terminal, err)
	}

	waitFor(t, 2*time.Second, func() bool { return tracker.callCount() == 2 })
	h.waitIdle(t, d)

	row := targetRow(t, h.store, id, domain.TrackerCommentKind)
	if row.Status != persistence.NotificationTargetSucceeded || row.Attempts != 2 {
		t.Fatalf("tracker target after retries = %q/%d attempts, want succeeded in 2", row.Status, row.Attempts)
	}
}

func TestRecovery_ResumesOnlyUnconfirmedTargets(t *testing.T) {
	h := newHarness(t)
	seedEventWithSucceededTracker(t, h.store, "evt-recover", "slack", `{"webhook_url":"$OUTBOX_TEST_URL"}`)

	d := h.newDispatcher(nil)
	if err := d.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	h.waitIdle(t, d)

	if got := h.tracker.count(); got != 0 {
		t.Fatalf("tracker sends after recovery = %d, want 0 for a confirmed target", got)
	}
	sends, _ := h.slack.snapshot()
	if len(sends) != 1 {
		t.Fatalf("slack sends after recovery = %d, want 1", len(sends))
	}
	if sends[0].Envelope.NotificationID != "evt-recover" {
		t.Fatalf("recovered send id = %q, want evt-recover", sends[0].Envelope.NotificationID)
	}

	pending, err := h.store.ListPendingNotificationOutbox(context.Background())
	if err != nil {
		t.Fatalf("list pending: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending events = %d, want 0 after resumed delivery completed", len(pending))
	}
}

func TestRecovery_ReconstructsBackendFromFrozenReference(t *testing.T) {
	h := newHarness(t)
	seedEventWithSucceededTracker(t, h.store, "evt-config", "slack", `{"webhook_url":"$OUTBOX_TEST_URL"}`)

	d := h.newDispatcher(nil)
	if err := d.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	h.waitIdle(t, d)

	if h.lookup.callCount() < 1 {
		t.Fatal("backend never reconstructed through the lookup after recovery")
	}
	sends, _ := h.slack.snapshot()
	if len(sends) != 1 || sends[0].Envelope.NotificationID != "evt-config" {
		t.Fatalf("reconstructed backend did not send the recovered event: %+v", sends)
	}
}

func TestRecovery_MissingBackendKindPermanentlyFails(t *testing.T) {
	h := newHarness(t)
	seedEventWithSucceededTracker(t, h.store, "evt-kind-gone", "vanished", `{"url":"$OUTBOX_TEST_URL"}`)

	d := h.newDispatcher(nil)
	h.lookup.failKind = "vanished"
	if err := d.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}

	row := targetRow(t, h.store, "evt-kind-gone", "notifications[0]")
	if row.Status != persistence.NotificationTargetFailed || row.LastError == "" {
		t.Fatalf("target = %q/%q, want a permanent failure with a category", row.Status, row.LastError)
	}
}

func TestStopScheduling_InFlightAttemptPersistsResult(t *testing.T) {
	h := newHarness(t)
	entered := make(chan struct{}, 1)
	h.slack.entered = entered
	h.slack.block = make(chan struct{})

	d := h.newDispatcher(nil)
	id, _, err := d.Enqueue(h.routeEvent(), nil)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	<-entered

	stopDone := make(chan struct{})
	go func() {
		d.StopScheduling()
		close(stopDone)
	}()
	select {
	case <-stopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("StopScheduling blocked while an attempt was in flight")
	}

	close(h.slack.block)
	h.waitIdle(t, d)

	row := targetRow(t, h.store, id, "notifications[0]")
	if row.Status != persistence.NotificationTargetSucceeded {
		t.Fatalf("status after drain = %q, want succeeded persisted", row.Status)
	}
}

func TestEnqueueAfterStop_LaunchesFirstAttemptIntoWaitGroup(t *testing.T) {
	h := newHarness(t)
	d := h.newDispatcher(nil)
	d.StopScheduling()

	if _, _, err := d.Enqueue(h.routeEvent(), nil); err != nil {
		t.Fatalf("Enqueue after stop: %v", err)
	}
	h.waitIdle(t, d)

	if got := h.tracker.count(); got != 1 {
		t.Fatalf("tracker sends = %d, want 1 for an event enqueued during shutdown", got)
	}
}

// seedEventWithSucceededTracker inserts one open event with two
// targets: a confirmed tracker comment (what recovery must not re-send)
// and one still-open notification backend (what recovery resumes), as a
// process that died mid-delivery leaves them.
func seedEventWithSucceededTracker(t *testing.T, store *persistence.Store, id, openKind, openConfig string) {
	t.Helper()
	const now = "2026-10-14T12:00:00Z"
	payload := `{"Envelope":{"NotificationID":"` + id + `","IssueID":"ISSUE-1","Identifier":"ISSUE-1","EventType":"session.completed"},"Message":{"Severity":"info","Title":"t","Body":"done"}}`
	event := persistence.NotificationOutboxEvent{
		NotificationID: id,
		EventType:      string(domain.EventSessionCompleted),
		Payload:        payload,
		Status:         persistence.NotificationOutboxPending,
		CreatedAt:      now,
	}
	targets := []persistence.NotificationOutboxTarget{
		{
			NotificationID: id,
			Destination:    domain.TrackerCommentKind,
			Kind:           domain.TrackerCommentKind,
			Status:         persistence.NotificationTargetPending,
			CreatedAt:      now,
			UpdatedAt:      now,
		},
		{
			NotificationID: id,
			Destination:    "notifications[0]",
			Kind:           openKind,
			BackendConfig:  openConfig,
			Status:         persistence.NotificationTargetPending,
			CreatedAt:      now,
			UpdatedAt:      now,
		},
	}
	if err := store.CreateNotificationOutbox(context.Background(), event, targets); err != nil {
		t.Fatalf("seed event: %v", err)
	}
	if _, err := store.RecordNotificationTargetResult(
		context.Background(), id, domain.TrackerCommentKind,
		persistence.NotificationTargetSucceeded, 1, "", nil, now, now,
	); err != nil {
		t.Fatalf("seed tracker success: %v", err)
	}
}
