package orchestrator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/notify/outbox"
	"github.com/sortie-ai/sortie/internal/notify/route"
	"github.com/sortie-ai/sortie/internal/persistence"
	"github.com/sortie-ai/sortie/internal/registry"
)

// failingOnceNotifier fails its first send with a retryable, transport-
// class failure, then succeeds.
type failingOnceNotifier struct {
	mu       sync.Mutex
	left     int
	received []domain.Notification
}

func (n *failingOnceNotifier) Send(_ context.Context, notification domain.Notification) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.received = append(n.received, notification)
	if n.left > 0 {
		n.left--
		return &domain.TrackerError{Kind: domain.ErrTrackerTransport, Message: "connection reset"}
	}
	return nil
}

func (n *failingOnceNotifier) sends() []domain.Notification {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]domain.Notification, len(n.received))
	copy(out, n.received)
	return out
}

// TestNotificationOutbox_RestartResumesUnconfirmedWithSameIdentity drives
// the producer path through a real persistence store: a process dying
// after the tracker comment succeeded but before the subscriber
// confirmed is simulated by stopping the first dispatcher with the
// subscriber target still open. The next dispatcher resumes the
// subscriber only, under the original notification identity, and never
// re-sends the confirmed comment.
func TestNotificationOutbox_RestartResumesUnconfirmedWithSameIdentity(t *testing.T) {
	t.Parallel()

	store, err := persistence.Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { store.Close() }) //nolint:errcheck
	if err := store.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	tracker := &opsTracker{mockTrackerAdapter: &mockTrackerAdapter{}}
	slack1 := &failingOnceNotifier{left: 1}
	lookup1 := func(string) (registry.NotifierConstructor, error) {
		return func(map[string]any) (domain.Notifier, error) { return slack1, nil }, nil
	}
	router1 := route.NewRouter(tracker, lookup1)
	if err := router1.Update(route.Inputs{
		Entries: []config.NotificationBackend{{
			Kind:      "slack",
			RawConfig: map[string]any{"webhook_url": "$SLACK_URL"},
			Config:    map[string]any{"webhook_url": "https://resolved.example"},
			Events:    []domain.EventType{domain.EventSessionCompleted},
		}},
		Comments: config.TrackerCommentsConfig{OnCompletion: true},
	}); err != nil {
		t.Fatalf("router update: %v", err)
	}

	var wg1 sync.WaitGroup
	box1 := outbox.New(outbox.Params{
		Store:         store,
		Tracker:       tracker,
		Lookup:        lookup1,
		Logger:        discardLogger(),
		WaitGroup:     &wg1,
		RetryBackoffs: []time.Duration{50 * time.Millisecond},
	})

	delivery := router1.Route(func() domain.Notification {
		return domain.Notification{
			Envelope: domain.NotificationEnvelope{
				IssueID:    "DURABLE-1",
				Identifier: "DURABLE-1",
				EventType:  domain.EventSessionCompleted,
			},
			Message: domain.NotificationMessage{
				Severity: "info",
				Title:    "DURABLE-1: session.completed",
				Body:     "completed durably",
			},
		}
	}())
	id, trackerTarget, err := box1.Enqueue(delivery, nil)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if !trackerTarget {
		t.Fatal("tracker target missing")
	}

	// Wait for the first attempts to land: tracker confirmed, slack
	// recorded as retryable.
	waitTargetRow := func(expectStatus string) persistence.NotificationOutboxTarget {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			row, ok, err := store.GetNotificationOutboxTarget(context.Background(), id, "notifications[0]")
			if err != nil {
				t.Fatalf("get target: %v", err)
			}
			if ok && row.Status == expectStatus {
				return row
			}
			time.Sleep(2 * time.Millisecond)
		}
		t.Fatalf("slack target never reached %q", expectStatus)
		return persistence.NotificationOutboxTarget{}
	}
	slackRow := waitTargetRow("retryable")
	if slackRow.Attempts != 1 || slackRow.NextAttemptAtMS == nil {
		t.Fatalf("retryable row = %+v, want one attempt scheduled", slackRow)
	}
	box1.StopScheduling()
	wg1.Wait()

	if got := len(tracker.comments); got != 1 || tracker.comments[0] != "completed durably" {
		t.Fatalf("tracker comments = %+v, want the single frozen body", tracker.comments)
	}
	if sends := slack1.sends(); len(sends) != 1 || sends[0].Envelope.NotificationID != id {
		t.Fatalf("first subscriber sends = %+v, want one attempt carrying %q", sends, id)
	}

	// Simulate the restart: a fresh dispatcher over the same database
	// with a succeeding subscriber.
	slack2 := &failingOnceNotifier{}
	lookup2 := func(string) (registry.NotifierConstructor, error) {
		return func(map[string]any) (domain.Notifier, error) { return slack2, nil }, nil
	}
	var wg2 sync.WaitGroup
	box2 := outbox.New(outbox.Params{
		Store:         store,
		Tracker:       tracker,
		Lookup:        lookup2,
		Logger:        discardLogger(),
		WaitGroup:     &wg2,
		RetryBackoffs: []time.Duration{time.Millisecond},
	})
	t.Cleanup(box2.StopScheduling)
	if err := box2.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	settleCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := box2.WaitIdle(settleCtx); err != nil {
		t.Fatalf("resumed delivery did not settle: %v", err)
	}
	wg2.Wait()

	if got := len(tracker.comments); got != 1 {
		t.Fatalf("tracker comments after restart = %d, want the confirmed comment not re-sent", got)
	}
	sends := slack2.sends()
	if len(sends) != 1 {
		t.Fatalf("resumed subscriber sends = %d, want 1", len(sends))
	}
	if sends[0].Envelope.NotificationID != id {
		t.Fatalf("resumed send id = %q, want the frozen identity %q", sends[0].Envelope.NotificationID, id)
	}
	if sends[0].Message.Body != "completed durably" {
		t.Fatalf("resumed send body = %q, want the frozen body", sends[0].Message.Body)
	}
}
