package persistence

import (
	"context"
	"testing"
)

func TestNotificationOutbox_CreateAndListPending(t *testing.T) {
	t.Parallel()

	s := openTestStore(t)
	migrateOrFatal(t, s)
	ctx := context.Background()

	event := NotificationOutboxEvent{
		NotificationID: "evt-1",
		EventType:      "session.started",
		Payload:        `{"envelope":{}}`,
		Status:         NotificationOutboxPending,
		CreatedAt:      "2026-10-14T00:00:00Z",
	}
	targets := []NotificationOutboxTarget{
		{
			Destination: "tracker_comment",
			Kind:        "tracker_comment",
			CreatedAt:   "2026-10-14T00:00:00Z",
			UpdatedAt:   "2026-10-14T00:00:00Z",
		},
		{
			Destination:   "notifications[0]",
			Kind:          "slack",
			BackendConfig: `{"webhook_url":"$SLACK_URL"}`,
			CreatedAt:     "2026-10-14T00:00:00Z",
			UpdatedAt:     "2026-10-14T00:00:00Z",
		},
	}

	if err := s.CreateNotificationOutbox(ctx, event, targets); err != nil {
		t.Fatalf("CreateNotificationOutbox: %v", err)
	}

	pending, err := s.ListPendingNotificationOutbox(ctx)
	if err != nil {
		t.Fatalf("ListPendingNotificationOutbox: %v", err)
	}
	if len(pending) != 1 {
		t.Fatalf("pending events = %d, want 1", len(pending))
	}
	got := pending[0]
	if got.Event.NotificationID != "evt-1" || got.Event.Status != NotificationOutboxPending {
		t.Fatalf("event = %+v", got.Event)
	}
	if len(got.Targets) != 2 {
		t.Fatalf("targets = %d, want 2", len(got.Targets))
	}
	byDest := map[string]NotificationOutboxTarget{}
	for _, target := range got.Targets {
		byDest[target.Destination] = target
	}
	if t0 := byDest["tracker_comment"]; t0.Status != NotificationTargetPending || t0.Attempts != 0 || t0.BackendConfig != "" {
		t.Fatalf("tracker target = %+v, want pending, zero attempts, no config", t0)
	}
	if t1 := byDest["notifications[0]"]; t1.Status != NotificationTargetPending || t1.Attempts != 0 || t1.BackendConfig == "" {
		t.Fatalf("subscriber target = %+v, want pending with frozen backend config", t1)
	}

	// Duplicate create fails: identity is fixed and never overwritten.
	err = s.CreateNotificationOutbox(ctx, event, targets[:1])
	if err == nil {
		t.Fatal("second CreateNotificationOutbox with same id succeeded, want error")
	}
}

func TestNotificationOutbox_ResultCompletesEventWhenAllTerminal(t *testing.T) {
	t.Parallel()

	s := openTestStore(t)
	migrateOrFatal(t, s)
	ctx := context.Background()

	event := NotificationOutboxEvent{
		NotificationID: "evt-2",
		EventType:      "session.completed",
		Payload:        `{}`,
		Status:         NotificationOutboxPending,
		CreatedAt:      "2026-10-14T00:00:00Z",
	}
	targets := []NotificationOutboxTarget{
		{Destination: "tracker_comment", Kind: "tracker_comment", CreatedAt: "2026-10-14T00:00:00Z", UpdatedAt: "2026-10-14T00:00:00Z"},
		{Destination: "notifications[0]", Kind: "slack", CreatedAt: "2026-10-14T00:00:00Z", UpdatedAt: "2026-10-14T00:00:00Z"},
	}
	if err := s.CreateNotificationOutbox(ctx, event, targets); err != nil {
		t.Fatalf("CreateNotificationOutbox: %v", err)
	}

	next := int64(1234)
	completed, err := s.RecordNotificationTargetResult(
		ctx, "evt-2", "tracker_comment", NotificationTargetRetryable, 1, "timeout", &next,
		"2026-10-14T00:00:01Z", "2026-10-14T00:00:01Z")
	if err != nil {
		t.Fatalf("RecordNotificationTargetResult: %v", err)
	}
	if completed {
		t.Fatal("event completed with an open target, want false")
	}

	completed, err = s.RecordNotificationTargetResult(
		ctx, "evt-2", "notifications[0]", NotificationTargetSucceeded, 1, "", nil,
		"2026-10-14T00:00:01Z", "2026-10-14T00:00:01Z")
	if err != nil {
		t.Fatalf("RecordNotificationTargetResult: %v", err)
	}
	if completed {
		t.Fatal("event completed while tracker target stays retryable, want false")
	}

	completed, err = s.RecordNotificationTargetResult(
		ctx, "evt-2", "tracker_comment", NotificationTargetFailed, 3, "retries exhausted: timeout", nil,
		"2026-10-14T00:00:05Z", "2026-10-14T00:00:05Z")
	if err != nil {
		t.Fatalf("RecordNotificationTargetResult: %v", err)
	}
	if !completed {
		t.Fatal("event not completed once every target is terminal")
	}

	pending, err := s.ListPendingNotificationOutbox(ctx)
	if err != nil {
		t.Fatalf("ListPendingNotificationOutbox: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("pending events = %d, want 0 after completion", len(pending))
	}
}

func TestNotificationOutbox_PruneCompleted(t *testing.T) {
	t.Parallel()

	s := openTestStore(t)
	migrateOrFatal(t, s)
	ctx := context.Background()

	mk := func(id, completedAt string) {
		t.Helper()
		event := NotificationOutboxEvent{
			NotificationID: id,
			EventType:      "session.completed",
			Payload:        `{}`,
			Status:         NotificationOutboxCompleted,
			CreatedAt:      "2026-10-01T00:00:00Z",
			CompletedAt:    completedAt,
		}
		target := NotificationOutboxTarget{
			Destination: "tracker_comment",
			Kind:        "tracker_comment",
			Status:      NotificationTargetSucceeded,
			CreatedAt:   "2026-10-01T00:00:00Z",
			UpdatedAt:   completedAt,
		}
		// Completed rows are inserted directly to test pruning.
		if _, err := s.db.ExecContext(ctx,
			`INSERT INTO notification_outbox (notification_id, event_type, payload, status, created_at, completed_at)
			VALUES (?, ?, ?, ?, ?, ?)`,
			id, event.EventType, event.Payload, event.Status, event.CreatedAt, event.CompletedAt,
		); err != nil {
			t.Fatalf("insert event %s: %v", id, err)
		}
		if _, err := s.db.ExecContext(ctx,
			`INSERT INTO notification_targets (notification_id, destination, kind, status, attempts, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)`,
			id, target.Destination, target.Kind, target.Status, 1, target.CreatedAt, target.UpdatedAt,
		); err != nil {
			t.Fatalf("insert target %s: %v", id, err)
		}
	}

	mk("old", "2026-10-02T00:00:00Z")
	mk("new", "2026-10-13T00:00:00Z")

	if err := s.PruneCompletedNotificationOutbox(ctx, "2026-10-10T00:00:00Z"); err != nil {
		t.Fatalf("PruneCompletedNotificationOutbox: %v", err)
	}

	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM notification_outbox`).Scan(&count); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if count != 1 {
		t.Fatalf("events after prune = %d, want 1", count)
	}
	var targetCount int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM notification_targets`).Scan(&targetCount); err != nil {
		t.Fatalf("count targets: %v", err)
	}
	if targetCount != 1 {
		t.Fatalf("targets after prune = %d, want 1 (cascade delete)", targetCount)
	}
}
