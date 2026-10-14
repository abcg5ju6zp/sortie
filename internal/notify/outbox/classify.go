package outbox

import (
	"context"
	"errors"
	"fmt"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/persistence"
)

// permanentFailure is the capability a classified send error implements
// when a later attempt cannot succeed. Both notifier backends mark a
// 4xx rejection (other than 429) this way, and a deterministic local
// failure wraps [domain.PermanentNotifierError].
type permanentFailure interface {
	error
	PermanentFailure() bool
}

// classifySend maps one send error to a persisted target status and a
// redaction-safe category. nil is success. A tracker error follows the
// tracker error kind's own retry classification, and the stored
// category is the normalized kind rather than the error text, so a
// tracker message that embeds a host or URL never reaches the database.
// Anything unrecognized is treated as retryable.
func classifySend(err error) (status string, category string) {
	if err == nil {
		return persistence.NotificationTargetSucceeded, ""
	}

	if trackerErr, ok := errors.AsType[*domain.TrackerError](err); ok {
		status = persistence.NotificationTargetRetryable
		if !trackerErr.Kind.RetryClassification().Retryable {
			status = persistence.NotificationTargetFailed
		}
		return status, "tracker " + string(trackerErr.Kind)
	}

	if permanentErr, ok := errors.AsType[permanentFailure](err); ok && permanentErr.PermanentFailure() {
		return persistence.NotificationTargetFailed, safeCategory(err)
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return persistence.NotificationTargetRetryable, "timeout"
	}
	return persistence.NotificationTargetRetryable, safeCategory(err)
}

// exhausted marks a retryable failure whose budget is spent as a
// permanent failure while keeping the last category.
func exhausted(category string) string {
	return fmt.Sprintf("retries exhausted: %s", category)
}

// categorizedError is implemented by classified notifier failures whose
// text is deliberately a secret-free category.
type categorizedError interface {
	error
	FailureCategory() string
}

// safeCategory returns the error's category for storage. Notifier
// backends contractually render only a secret-free category here; an
// error that carries no such guarantee is stored under a generic label
// rather than its raw text.
func safeCategory(err error) string {
	if err == nil {
		return ""
	}
	if trackerErr, ok := errors.AsType[*domain.TrackerError](err); ok {
		return "tracker " + string(trackerErr.Kind)
	}
	if categorized, ok := errors.AsType[categorizedError](err); ok {
		return categorized.FailureCategory()
	}
	if permanentErr, ok := errors.AsType[permanentFailure](err); ok && permanentErr.PermanentFailure() {
		return err.Error()
	}
	return "delivery failure"
}
