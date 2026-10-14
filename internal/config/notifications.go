package config

import (
	"fmt"
	"slices"
	"strings"

	"github.com/sortie-ai/sortie/internal/domain"
	"github.com/sortie-ai/sortie/internal/typeutil"
)

// NotificationsConfig holds the parsed notifications list. The zero
// value (nil Backends) means no destination is configured and the
// notify_operator tool is not registered.
type NotificationsConfig struct {
	Backends []NotificationBackend
}

// NotificationBackend is one entry in the notifications list. Kind is
// the registry discriminator; Config carries the entry's remaining keys
// verbatim, with $VAR references resolved, for the backend constructor.
type NotificationBackend struct {
	// Kind is the required registry discriminator, resolved against the
	// notifier registry, or [domain.TrackerCommentKind] for the built-in
	// tracker comment destination.
	Kind string

	// MaxPerSession is the notification cap. 0 selects the default at
	// cap selection time; it never means unlimited.
	MaxPerSession int

	// Events lists the event types the entry receives, in written order.
	// A registered kind written without events receives
	// [domain.EventAgentMessage] only.
	Events []domain.EventType

	// EventsDeclared reports whether the entry wrote an events key.
	EventsDeclared bool

	// Config holds the entry's per-backend fields with $VAR references
	// resolved. It is passed through to the backend constructor untyped.
	// It excludes kind, max_per_session, and events.
	Config map[string]any

	// RawConfig holds the same per-backend fields exactly as written,
	// before $VAR expansion. A frozen routing snapshot carries it so the
	// durable notification outbox can reconstruct the backend after a
	// restart without persisting a resolved secret value; callers
	// constructing a backend run it through [ResolveEnvReferences]
	// first.
	RawConfig map[string]any
}

// buildNotificationsConfig parses the top-level notifications sequence
// into a [NotificationsConfig]. An absent section yields the zero value
// and a nil error. The value must be a YAML sequence; each entry must
// be a map carrying a non-empty string kind, and an optional
// max_per_session must be a non-negative integer. An optional events key
// must be a sequence of distinct event type names. Every other key is
// copied into the entry's Config for pass-through, with $VAR and ${VAR}
// references resolved on every string leaf. Returns the first
// [*ConfigError] on a malformed section.
func buildNotificationsConfig(raw map[string]any) (NotificationsConfig, error) {
	value, ok := raw["notifications"]
	if !ok || value == nil {
		return NotificationsConfig{}, nil
	}

	seq, ok := value.([]any)
	if !ok {
		return NotificationsConfig{}, &ConfigError{
			Field:   "notifications",
			Message: fmt.Sprintf("expected sequence, got %T", value),
		}
	}

	backends := make([]NotificationBackend, 0, len(seq))
	for i, elem := range seq {
		backend, err := parseNotificationBackend(i, elem)
		if err != nil {
			return NotificationsConfig{}, err
		}
		backends = append(backends, backend)
	}

	if len(backends) == 0 {
		return NotificationsConfig{}, nil
	}
	return NotificationsConfig{Backends: backends}, nil
}

// parseNotificationBackend converts a single notifications entry into a
// [NotificationBackend], resolving $VAR references on its pass-through
// leaves.
func parseNotificationBackend(index int, elem any) (NotificationBackend, error) {
	field := fmt.Sprintf("notifications[%d]", index)

	entry, ok := elem.(map[string]any)
	if !ok {
		return NotificationBackend{}, &ConfigError{
			Field:   field,
			Message: fmt.Sprintf("expected map, got %T", elem),
		}
	}

	kind, fault := typeutil.StringField(entry, "kind")
	if fault != nil {
		return NotificationBackend{}, &ConfigError{
			Field:   field + ".kind",
			Message: fault.Reason(),
		}
	}
	if kind == "" {
		return NotificationBackend{}, &ConfigError{
			Field:   field + ".kind",
			Message: "kind is required and must be a non-empty string",
		}
	}

	maxPerSession := 0
	if rawMax, exists := entry["max_per_session"]; exists && rawMax != nil {
		n, err := coerceInt(rawMax)
		if err != nil {
			return NotificationBackend{}, &ConfigError{
				Field:   field + ".max_per_session",
				Message: integerFaultMessage(err, fmt.Sprintf("invalid integer value: %v", rawMax)),
			}
		}
		if n < 0 {
			return NotificationBackend{}, &ConfigError{
				Field:   field + ".max_per_session",
				Message: "must not be negative",
			}
		}
		maxPerSession = n
	}

	events, eventsDeclared, err := parseNotificationEvents(field, kind, entry)
	if err != nil {
		return NotificationBackend{}, err
	}

	if kind == domain.TrackerCommentKind {
		if key, extra := firstExtraTrackerCommentKey(entry); extra {
			return NotificationBackend{}, &ConfigError{
				Field:   field + "." + key,
				Message: "kind tracker_comment takes only kind and events",
			}
		}
	}

	passthrough := make(map[string]any, len(entry))
	for key, val := range entry {
		if key == "kind" || key == "max_per_session" || key == "events" {
			continue
		}
		passthrough[key] = val
	}

	// Keep the fields as written before resolving them in place, so a
	// frozen route can persist a secret-free backend specification.
	rawConfig := cloneConfigValue(passthrough).(map[string]any)

	// A typed knownTopLevelKeys section is excluded from extensions, and
	// the only existing $VAR walker runs over extensions, so resolve the
	// entry leaves here. An unset reference resolves to the empty string,
	// which the backend constructor then rejects as a missing secret.
	var snapshot map[string]string
	for key, val := range passthrough {
		passthrough[key] = resolveExtensionEnvValue(key, val, &snapshot)
	}

	return NotificationBackend{
		Kind:           kind,
		MaxPerSession:  maxPerSession,
		Events:         events,
		EventsDeclared: eventsDeclared,
		Config:         passthrough,
		RawConfig:      rawConfig,
	}, nil
}

// ResolveEnvReferences expands $VAR and ${VAR} references on every
// string leaf under v, mirroring the resolution a notifications entry
// receives when WORKFLOW.md loads. It mutates and returns v in place;
// non-string leaves round-trip unchanged. The durable notification
// outbox reconstructs a frozen backend after a restart with it, so the
// stored specification keeps the reference rather than the resolved
// secret value.
func ResolveEnvReferences(v any) any {
	var snapshot map[string]string
	return resolveExtensionEnvValue("", v, &snapshot)
}

// cloneConfigValue deep-copies the yaml-decoded value types a
// notifications pass-through config can hold (maps, slices, strings,
// and other scalars), so resolving one copy leaves the raw copy
// untouched.
func cloneConfigValue(v any) any {
	switch typed := v.(type) {
	case map[string]any:
		clone := make(map[string]any, len(typed))
		for k, child := range typed {
			clone[k] = cloneConfigValue(child)
		}
		return clone
	case []any:
		clone := make([]any, len(typed))
		for i, child := range typed {
			clone[i] = cloneConfigValue(child)
		}
		return clone
	default:
		return v
	}
}

// parseNotificationEvents reads the events key of one entry. A missing
// or null key yields [domain.EventAgentMessage] for a registered kind and
// a [*ConfigError] for the tracker_comment kind, which must list its
// events.
func parseNotificationEvents(field, kind string, entry map[string]any) ([]domain.EventType, bool, error) {
	eventsField := field + ".events"

	raw, exists := entry["events"]
	if !exists || raw == nil {
		if kind == domain.TrackerCommentKind {
			return nil, false, &ConfigError{
				Field:   eventsField,
				Message: "a tracker_comment entry must list its events; write [] to post only what the deprecated settings enable",
			}
		}
		return []domain.EventType{domain.EventAgentMessage}, false, nil
	}

	seq, ok := raw.([]any)
	if !ok {
		return nil, false, &ConfigError{
			Field:   eventsField,
			Message: "expected sequence, got " + typeutil.DescribeYAMLType(raw),
		}
	}

	events := make([]domain.EventType, 0, len(seq))
	for j, elem := range seq {
		elemField := fmt.Sprintf("%s[%d]", eventsField, j)

		name, ok := elem.(string)
		if !ok {
			return nil, false, &ConfigError{
				Field:   elemField,
				Message: "expected string, got " + typeutil.DescribeYAMLType(elem),
			}
		}
		event := domain.EventType(name)
		if !event.Valid() {
			return nil, false, &ConfigError{
				Field:   elemField,
				Message: fmt.Sprintf("unknown event type %q; valid: %s", name, joinEventTypes(domain.EventTypes())),
			}
		}
		if event == domain.EventAgentMessage && kind == domain.TrackerCommentKind {
			return nil, false, &ConfigError{
				Field:   elemField,
				Message: "agent.message cannot be posted as a tracker comment",
			}
		}
		if slices.Contains(events, event) {
			return nil, false, &ConfigError{
				Field:   elemField,
				Message: fmt.Sprintf("event type %q is listed more than once", name),
			}
		}
		events = append(events, event)
	}
	return events, true, nil
}

func joinEventTypes(events []domain.EventType) string {
	names := make([]string, len(events))
	for i, event := range events {
		names[i] = string(event)
	}
	return strings.Join(names, ", ")
}

// firstExtraTrackerCommentKey returns the alphabetically first key of a
// tracker_comment entry other than kind and events.
func firstExtraTrackerCommentKey(entry map[string]any) (string, bool) {
	var first string
	found := false
	for key := range entry {
		if key == "kind" || key == "events" {
			continue
		}
		if !found || key < first {
			first, found = key, true
		}
	}
	return first, found
}

// validateTrackerCommentEntries enforces the rules that span entries or
// reach into the tracker section: at most one tracker_comment entry, and
// only with a configured tracker.
func validateTrackerCommentEntries(notifications NotificationsConfig, tracker TrackerConfig) error {
	seen := false
	for i, backend := range notifications.Backends {
		if backend.Kind != domain.TrackerCommentKind {
			continue
		}
		field := fmt.Sprintf("notifications[%d].kind", i)
		if seen {
			return &ConfigError{
				Field:   field,
				Message: "only one notifications entry may have kind tracker_comment",
			}
		}
		seen = true
		if tracker.Kind == "" {
			return &ConfigError{
				Field:   field,
				Message: "kind tracker_comment requires a configured tracker",
			}
		}
	}
	return nil
}
