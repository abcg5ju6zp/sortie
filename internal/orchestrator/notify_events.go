package orchestrator

import (
	"cmp"
	"context"
	"errors"
	"log/slog"

	"github.com/sortie-ai/sortie/internal/config"
	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/notify/outbox"
	"github.com/sortie-ai/sortie/internal/notify/route"
)

// sessionEvent holds the identity of the run an orchestrator session
// event describes.
type sessionEvent struct {
	IssueID    string
	Identifier string
	DisplayID  string
	DispatchID string
	SessionID  string
	Attempt    *int
	Agent      string

	// AgentText is the agent's stop statement after publicStatement, empty
	// when there is none.
	AgentText string
}

// notification builds the event for t with body as its text. severity
// overrides the catalog severity when non-empty.
func (s sessionEvent) notification(t domain.EventType, severity, body string) domain.Notification {
	n := orchestratorNotification(t, severity, domain.NotificationEnvelope{
		IssueID:    s.IssueID,
		Identifier: s.Identifier,
		DispatchID: s.DispatchID,
		SessionID:  s.SessionID,
		Attempt:    s.Attempt,
		Agent:      s.Agent,
	}, cmp.Or(s.DisplayID, s.Identifier, s.IssueID), body)
	n.Message.AgentText = s.AgentText
	return n
}

func reactionNotification(t domain.EventType, pending *PendingReaction, body string) domain.Notification {
	return orchestratorNotification(t, "", domain.NotificationEnvelope{
		IssueID:    pending.IssueID,
		Identifier: pending.Identifier,
	}, cmp.Or(pending.DisplayID, pending.Identifier, pending.IssueID), body)
}

func budgetHeldNotification(issueID string, entry *BudgetExhaustedEntry) domain.Notification {
	return orchestratorNotification(domain.EventBudgetHeld, "", domain.NotificationEnvelope{
		IssueID:    issueID,
		Identifier: entry.Identifier,
	}, cmp.Or(entry.DisplayID, entry.Identifier, issueID), buildBudgetHoldComment(entry))
}

func orchestratorNotification(t domain.EventType, severity string, envelope domain.NotificationEnvelope, display, body string) domain.Notification {
	envelope.EventType = t
	return domain.Notification{
		Envelope: envelope,
		Message: domain.NotificationMessage{
			Severity: cmp.Or(severity, t.Severity()),
			Title:    display + ": " + string(t),
			Body:     body,
		},
	}
}

// deliverDetached durably persists d and delivers it through the
// outbox. onTrackerComment, when non-nil, runs exactly once after the
// tracker_comment target reaches a terminal result; it never runs when
// tracker_comment is not a target. An empty delivery starts nothing. A
// nil outbox (a caller with no durable sender) drops the event: this
// only happens in callers that route nowhere.
func deliverDetached(box *outbox.Dispatcher, d route.Delivery, log *slog.Logger, onTrackerComment func(received bool, err error)) {
	if d.Empty() {
		return
	}
	if box == nil {
		log.Debug("notification outbox unavailable; notification not persisted or sent",
			slog.String("event_type", string(d.EventType())),
		)
		return
	}
	if _, _, err := box.Enqueue(d, onTrackerComment); err != nil {
		log.Error("failed to persist notification for delivery",
			slog.String("event_type", string(d.EventType())),
			slog.Any("error", err),
		)
	}
}

// publishEscalation durably delivers an escalation event. logFailure
// receives a failed tracker comment, and record receives the escalation
// counter action: comment on a confirmed comment, error on a
// permanently failed one, and none when tracker_comment was no target
// (including an empty delivery). A producer that applied a label passes
// nil because the label owns the counter.
func publishEscalation(box *outbox.Dispatcher, d route.Delivery, log *slog.Logger, record func(action string), logFailure func(err error)) {
	if d.Empty() || box == nil {
		if record != nil {
			record(escalationActionNone)
		}
		return
	}

	var onTracker func(received bool, err error)
	if record != nil || logFailure != nil {
		onTracker = func(received bool, err error) {
			if err != nil && logFailure != nil {
				logFailure(err)
			}
			if record == nil {
				return
			}
			if err != nil {
				record(outcomeError)
				return
			}
			record(escalationActionComment)
		}
	}

	_, trackerTarget, err := box.Enqueue(d, onTracker)
	if err != nil {
		log.Error("failed to persist escalation notification",
			slog.String("event_type", string(d.EventType())),
			slog.Any("error", err),
		)
		if record != nil {
			record(escalationActionNone)
		}
		return
	}
	if !trackerTarget && record != nil {
		record(escalationActionNone)
	}
}

const (
	escalationActionNone    = "none"
	escalationActionComment = "comment"
)

// routeInputs derives the routing inputs from cfg and from the
// escalation modes the producers hold. Five reaction blocks are frozen at
// construction, so their modes come from the producer-held configuration
// rather than from the reloaded file; otherwise a reload could leave an
// escalation with neither a label nor a comment.
func (o *Orchestrator) routeInputs(cfg config.ServiceConfig) route.Inputs {
	var escalations []domain.EventType
	for _, producer := range []struct {
		event domain.EventType
		mode  string
	}{
		{domain.EventEscalationCIFailure, cfg.CIFeedback.Escalation},
		{domain.EventEscalationReviewComments, o.reviewConfig.Escalation},
		{domain.EventEscalationBotReview, o.botReviewConfig.Escalation},
		{domain.EventEscalationMergeConflicts, o.mergeConflictConfig.Escalation},
		{domain.EventEscalationAutoMerge, o.autoMergeConfig.Escalation},
		{domain.EventEscalationMergeCompletion, o.mergeCompletionConfig.Escalation},
	} {
		if producer.mode == "comment" {
			escalations = append(escalations, producer.event)
		}
	}
	return route.Inputs{
		Entries:            cfg.Notifications.Backends,
		Comments:           cfg.Tracker.Comments,
		CommentEscalations: escalations,
	}
}

// updateRouter installs the routing table for cfg. A failure keeps the
// installed table.
func (o *Orchestrator) updateRouter(cfg config.ServiceConfig) {
	if o.router == nil {
		return
	}
	if err := o.router.Update(o.routeInputs(cfg)); err != nil {
		o.logger.Error("notification routing update failed", slog.Any("error", err))
	}
}

// publishSessionStarted routes the session.started event, persists it
// with its frozen identity, body, and routing snapshot, and delivers it.
// Every target sends through the outbox independently. The dispatch
// comment is confirmed before the caller goes on: the tracker_comment
// target's first result is awaited on ctx, and its success or failure is
// logged and counted from the persisted target result, so a callback lost
// to a crash never misjudges it; a retryable first attempt lets the
// agent start while the retry chain finishes the count.
func publishSessionStarted(ctx context.Context, issue domain.Issue, attempt *int, agentKind string, deps WorkerDeps, log *slog.Logger) {
	event := sessionEvent{
		IssueID:    issue.ID,
		Identifier: issue.Identifier,
		DisplayID:  issue.DisplayID,
		DispatchID: deps.DispatchID,
		Attempt:    attempt,
		Agent:      agentKind,
	}.notification(domain.EventTypeSessionStarted, "", dispatchComment)

	delivery := deps.Router.Route(event)
	if delivery.Empty() || deps.Outbox == nil {
		return
	}

	box := deps.Outbox
	id, trackerTarget, err := box.Enqueue(delivery, func(received bool, err error) {
		if err != nil {
			log.Warn("dispatch comment failed", slog.Any("error", err))
			deps.Metrics.IncTrackerComments("dispatch", "error")
			return
		}
		log.Info("dispatch comment posted")
		deps.Metrics.IncTrackerComments("dispatch", "success")
	})
	if err != nil {
		log.Error("failed to persist session.started notification", slog.Any("error", err))
		return
	}

	if trackerTarget {
		// Order the dispatch comment ahead of the agent's start for the
		// normal one-shot case. A wait ended by the caller's context
		// (shutdown) returns while the attempt still drains and persists.
		if _, awaitErr := box.AwaitTrackerSettled(ctx, id); awaitErr != nil && !errors.Is(awaitErr, context.Canceled) && !errors.Is(awaitErr, context.DeadlineExceeded) {
			if _, ok := errors.AsType[*outbox.TrackerResultError](awaitErr); !ok {
				log.Warn("dispatch comment confirmation interrupted", slog.Any("error", awaitErr))
			}
		}
	}
}
