package orchestrator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/notify/outbox"
	"github.com/sortie-ai/sortie/internal/notify/route"
	"github.com/sortie-ai/sortie/internal/persistence"
)

// testPermanentError is a deterministic send failure for tests: the
// dispatcher records it as a permanent target failure without retry.
type testPermanentError struct{ text string }

func (e *testPermanentError) Error() string           { return e.text }
func (e *testPermanentError) FailureCategory() string { return e.text }
func (e *testPermanentError) PermanentFailure() bool  { return true }

func permanentTestErr(text string) error { return &testPermanentError{text: text} }

// permanentTrackerErr returns a tracker failure the durable dispatcher
// treats as a permanent rejection (auth/payload/not-found class).
func permanentTrackerErr(text string) error {
	return &domain.TrackerError{Kind: domain.ErrTrackerAuth, Message: text}
}

// newTestDispatcher builds a durable notification dispatcher backed by
// an in-memory migrated store, counting attempts on wg, with retry
// waits shrunk to milliseconds so a failed target settles fast.
func newTestDispatcher(t *testing.T, tracker domain.TrackerAdapter, lookup route.NotifierLookup, wg *sync.WaitGroup) *outbox.Dispatcher {
	t.Helper()

	store, err := persistence.Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatalf("open outbox store: %v", err)
	}
	t.Cleanup(func() { store.Close() }) //nolint:errcheck
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate outbox store: %v", err)
	}

	box := outbox.New(outbox.Params{
		Store:         store,
		Tracker:       tracker,
		Lookup:        lookup,
		Logger:        discardLogger(),
		WaitGroup:     wg,
		RetryBackoffs: []time.Duration{time.Millisecond, 2 * time.Millisecond},
	})
	t.Cleanup(box.StopScheduling)
	return box
}

// newWorkerTestOutbox builds a dispatcher and the attempt wait group a
// WorkerDeps that is constructed directly needs.
func newWorkerTestOutbox(t *testing.T, tracker domain.TrackerAdapter) (*outbox.Dispatcher, *sync.WaitGroup) {
	t.Helper()
	wg := &sync.WaitGroup{}
	return newTestDispatcher(t, tracker, nil, wg), wg
}

// wireTestOutbox builds a durable notification dispatcher and installs
// it on state. Its tracker and lookup are the test's fakes, and its
// wait group is state.TrackerOpsWg.
func wireTestOutbox(t *testing.T, state *State, tracker domain.TrackerAdapter, lookup route.NotifierLookup) *outbox.Dispatcher {
	t.Helper()
	box := newTestDispatcher(t, tracker, lookup, &state.TrackerOpsWg)
	state.Outbox = box
	return box
}

// waitNotifications waits until every durable notification the test
// enqueued has a terminal result for every target and every producer or
// chain goroutine has returned. A state without an outbox only drains
// the wait group.
func waitNotifications(t *testing.T, state *State) {
	t.Helper()
	if state.Outbox == nil {
		state.TrackerOpsWg.Wait()
		return
	}
	waitForOutboxSettle(t, state.Outbox, &state.TrackerOpsWg)
}

// waitForOutboxSettle waits for the dispatcher's idle condition, then
// for the wait group once. The dispatcher counts every chain in the
// group synchronously from the Enqueue/Recover caller, so every event a
// test triggered before this call is already counted when it waits;
// the final Wait only bridges the gap between the terminal result the
// idle poll observed and the chain's Done.
func waitForOutboxSettle(t *testing.T, box *outbox.Dispatcher, wg *sync.WaitGroup) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := box.WaitIdle(ctx); err != nil {
		t.Fatalf("notification targets did not settle: %v", err)
	}
	wg.Wait()
}
