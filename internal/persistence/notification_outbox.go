package persistence

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Notification outbox target statuses. A target is pending before its
// first attempt, retryable while a transient failure leaves attempts
// due, succeeded once its backend confirmed delivery, and failed when a
// rejection is permanent or the retry budget is exhausted.
const (
	NotificationTargetPending   = "pending"
	NotificationTargetSucceeded = "succeeded"
	NotificationTargetRetryable = "retryable"
	NotificationTargetFailed    = "failed"
)

// Notification outbox event statuses: pending while any target is not
// terminal, completed once every target is succeeded or failed.
const (
	NotificationOutboxPending   = "pending"
	NotificationOutboxCompleted = "completed"
)

// NotificationOutboxEvent is one row of the notification_outbox table:
// a routed orchestrator event whose identity and rendered body are frozen
// for every present and future delivery attempt.
type NotificationOutboxEvent struct {
	NotificationID string
	EventType      string
	Payload        string // JSON-encoded domain.Notification
	Status         string
	CreatedAt      string
	CompletedAt    string // empty while the event is pending
}

// NotificationOutboxTarget is one row of the notification_targets table:
// one destination of an outbox event and its persisted delivery result.
// BackendConfig holds the destination's configuration as written in
// WORKFLOW.md (secret references unexpanded), and is empty for the
// built-in tracker_comment target.
type NotificationOutboxTarget struct {
	NotificationID  string
	Destination     string
	Kind            string
	BackendConfig   string
	Status          string
	Attempts        int
	LastError       string
	NextAttemptAtMS *int64
	CreatedAt       string
	UpdatedAt       string
}

// NotificationOutbox is one pending event with its targets, the shape
// [Store.ListPendingNotificationOutbox] returns for restart recovery.
type NotificationOutbox struct {
	Event   NotificationOutboxEvent
	Targets []NotificationOutboxTarget
}

// CreateNotificationOutbox persists an event and every target in one
// transaction, so an event no target has been contacted for yet still
// carries its full frozen target set across a crash. All targets are
// inserted pending with zero attempts.
func (s *Store) CreateNotificationOutbox(ctx context.Context, event NotificationOutboxEvent, targets []NotificationOutboxTarget) error {
	if event.NotificationID == "" {
		return fmt.Errorf("create notification outbox event: notification id is required")
	}
	if len(targets) == 0 {
		return fmt.Errorf("create notification outbox event %q: at least one target is required", event.NotificationID)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin create notification outbox event %q: %w", event.NotificationID, err)
	}
	defer tx.Rollback() //nolint:errcheck // rollback on error path; no-op after commit

	if _, err := tx.ExecContext(ctx,
		`INSERT INTO notification_outbox
			(notification_id, event_type, payload, status, created_at, completed_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		event.NotificationID, event.EventType, event.Payload, event.Status, event.CreatedAt, nullableString(event.CompletedAt),
	); err != nil {
		return fmt.Errorf("insert notification outbox event %q: %w", event.NotificationID, err)
	}

	for _, target := range targets {
		if target.NotificationID == "" {
			target.NotificationID = event.NotificationID
		}
		if target.NotificationID != event.NotificationID {
			return fmt.Errorf("insert notification target %q: notification id %q does not match event %q",
				target.Destination, target.NotificationID, event.NotificationID)
		}
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO notification_targets
				(notification_id, destination, kind, backend_config, status, attempts,
				 last_error, next_attempt_at_ms, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			target.NotificationID, target.Destination, target.Kind, nullableString(target.BackendConfig),
			NotificationTargetPending, 0, nil, nil, target.CreatedAt, target.UpdatedAt,
		); err != nil {
			return fmt.Errorf("insert notification target %q for event %q: %w",
				target.Destination, event.NotificationID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit create notification outbox event %q: %w", event.NotificationID, err)
	}
	return nil
}

// RecordNotificationTargetResult persists one target's attempt result.
// status is one of the NotificationTarget* constants; attempts is the
// total attempts made including the one being recorded; lastError holds
// the redaction-safe failure category and is empty on success;
// nextAttemptAtMS is set for a retryable result and nil otherwise. When
// the write leaves every target of the event terminal, the event row is
// marked completed in the same transaction and completed is true.
func (s *Store) RecordNotificationTargetResult(
	ctx context.Context,
	notificationID, destination, status string,
	attempts int,
	lastError string,
	nextAttemptAtMS *int64,
	updatedAt string,
	completedAt string,
) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin record notification target %q for event %q: %w", destination, notificationID, err)
	}
	defer tx.Rollback() //nolint:errcheck // rollback on error path; no-op after commit

	if _, err := tx.ExecContext(ctx,
		`UPDATE notification_targets
		SET status = ?, attempts = ?, last_error = ?, next_attempt_at_ms = ?, updated_at = ?
		WHERE notification_id = ? AND destination = ?`,
		status, attempts, nullableString(lastError), nextAttemptAtMS, updatedAt, notificationID, destination,
	); err != nil {
		return false, fmt.Errorf("update notification target %q for event %q: %w", destination, notificationID, err)
	}

	var openTargets int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM notification_targets
		WHERE notification_id = ? AND status IN (?, ?)`,
		notificationID, NotificationTargetPending, NotificationTargetRetryable,
	).Scan(&openTargets); err != nil {
		return false, fmt.Errorf("count open notification targets for event %q: %w", notificationID, err)
	}

	completed := openTargets == 0
	if completed {
		if _, err := tx.ExecContext(ctx,
			`UPDATE notification_outbox
			SET status = ?, completed_at = ?
			WHERE notification_id = ? AND status = ?`,
			NotificationOutboxCompleted, completedAt, notificationID, NotificationOutboxPending,
		); err != nil {
			return false, fmt.Errorf("complete notification outbox event %q: %w", notificationID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit record notification target %q for event %q: %w", destination, notificationID, err)
	}
	return completed, nil
}

// MarkNotificationOutboxCompleted marks a pending event completed when
// every target is already terminal without recording a new result, the
// state restart recovery reconstructs. It is a no-op when no target is
// still open or the event is already completed.
func (s *Store) MarkNotificationOutboxCompleted(ctx context.Context, notificationID, completedAt string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin complete notification outbox event %q: %w", notificationID, err)
	}
	defer tx.Rollback() //nolint:errcheck // rollback on error path; no-op after commit

	var openTargets int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM notification_targets
		WHERE notification_id = ? AND status IN (?, ?)`,
		notificationID, NotificationTargetPending, NotificationTargetRetryable,
	).Scan(&openTargets); err != nil {
		return fmt.Errorf("count open notification targets for event %q: %w", notificationID, err)
	}
	if openTargets > 0 {
		return nil
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE notification_outbox
		SET status = ?, completed_at = ?
		WHERE notification_id = ? AND status = ?`,
		NotificationOutboxCompleted, completedAt, notificationID, NotificationOutboxPending,
	); err != nil {
		return fmt.Errorf("complete notification outbox event %q: %w", notificationID, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit complete notification outbox event %q: %w", notificationID, err)
	}
	return nil
}

// ListPendingNotificationOutbox returns every outbox event that is not
// completed with its targets, for restart recovery. Ordering is
// unspecified. The targets of each event retain their persisted order
// only by destination name.
func (s *Store) ListPendingNotificationOutbox(ctx context.Context) ([]NotificationOutbox, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT notification_id, event_type, payload, status, created_at, COALESCE(completed_at, '')
		FROM notification_outbox
		WHERE status = ?`,
		NotificationOutboxPending,
	)
	if err != nil {
		return nil, fmt.Errorf("list pending notification outbox events: %w", err)
	}
	defer rows.Close() //nolint:errcheck // read-only query; close error is non-actionable

	eventsByID := make(map[string]*NotificationOutbox)
	var ids []string
	for rows.Next() {
		var event NotificationOutboxEvent
		if err := rows.Scan(
			&event.NotificationID, &event.EventType, &event.Payload,
			&event.Status, &event.CreatedAt, &event.CompletedAt,
		); err != nil {
			return nil, fmt.Errorf("scan pending notification outbox event: %w", err)
		}
		eventsByID[event.NotificationID] = &NotificationOutbox{Event: event, Targets: []NotificationOutboxTarget{}}
		ids = append(ids, event.NotificationID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending notification outbox events: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close pending notification outbox events: %w", err)
	}

	if len(ids) == 0 {
		return []NotificationOutbox{}, nil
	}

	targetRows, err := s.db.QueryContext(ctx,
		`SELECT notification_id, destination, kind, COALESCE(backend_config, ''),
			status, attempts, COALESCE(last_error, ''), next_attempt_at_ms,
			created_at, updated_at
		FROM notification_targets
		WHERE notification_id IN (SELECT notification_id FROM notification_outbox WHERE status = ?)
		ORDER BY notification_id, destination`,
		NotificationOutboxPending,
	)
	if err != nil {
		return nil, fmt.Errorf("list pending notification outbox targets: %w", err)
	}
	defer targetRows.Close() //nolint:errcheck // read-only query; close error is non-actionable

	for targetRows.Next() {
		var target NotificationOutboxTarget
		if err := targetRows.Scan(
			&target.NotificationID, &target.Destination, &target.Kind, &target.BackendConfig,
			&target.Status, &target.Attempts, &target.LastError, &target.NextAttemptAtMS,
			&target.CreatedAt, &target.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan pending notification outbox target: %w", err)
		}
		event, ok := eventsByID[target.NotificationID]
		if !ok {
			return nil, fmt.Errorf("pending notification target %q references unknown event %q", target.Destination, target.NotificationID)
		}
		event.Targets = append(event.Targets, target)
	}
	if err := targetRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending notification outbox targets: %w", err)
	}

	out := make([]NotificationOutbox, 0, len(ids))
	for _, id := range ids {
		out = append(out, *eventsByID[id])
	}
	return out, nil
}

// GetNotificationOutboxTarget returns one persisted target. The bool is
// false when no target row exists for the pair.
func (s *Store) GetNotificationOutboxTarget(ctx context.Context, notificationID, destination string) (NotificationOutboxTarget, bool, error) {
	var target NotificationOutboxTarget
	err := s.db.QueryRowContext(ctx,
		`SELECT notification_id, destination, kind, COALESCE(backend_config, ''),
			status, attempts, COALESCE(last_error, ''), next_attempt_at_ms,
			created_at, updated_at
		FROM notification_targets
		WHERE notification_id = ? AND destination = ?`,
		notificationID, destination,
	).Scan(
		&target.NotificationID, &target.Destination, &target.Kind, &target.BackendConfig,
		&target.Status, &target.Attempts, &target.LastError, &target.NextAttemptAtMS,
		&target.CreatedAt, &target.UpdatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return NotificationOutboxTarget{}, false, nil
	}
	if err != nil {
		return NotificationOutboxTarget{}, false, fmt.Errorf("get notification target %q for event %q: %w", destination, notificationID, err)
	}
	return target, true, nil
}

// PruneCompletedNotificationOutbox deletes completed events completed
// before the given RFC3339 cutoff, together with their target rows, in
// one transaction. It is a no-op when no row qualifies.
func (s *Store) PruneCompletedNotificationOutbox(ctx context.Context, cutoff string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin prune notification outbox: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // rollback on error path; no-op after commit

	if _, err := tx.ExecContext(ctx,
		`DELETE FROM notification_targets
		WHERE notification_id IN (
			SELECT notification_id FROM notification_outbox
			WHERE status = ? AND completed_at != '' AND completed_at < ?
		)`,
		NotificationOutboxCompleted, cutoff,
	); err != nil {
		return fmt.Errorf("delete pruned notification targets: %w", err)
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM notification_outbox
		WHERE status = ? AND completed_at != '' AND completed_at < ?`,
		NotificationOutboxCompleted, cutoff,
	); err != nil {
		return fmt.Errorf("delete pruned notification events: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit prune notification outbox: %w", err)
	}
	return nil
}

func nullableString(v string) any {
	if v == "" {
		return nil
	}
	return v
}
