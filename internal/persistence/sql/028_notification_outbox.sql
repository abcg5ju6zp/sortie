-- Migration 28: durable notification outbox
--
-- One row per orchestrator event whose delivery is in flight, and one
-- row per target the event's routing snapshot selected. The event row
-- freezes the notification identity and the rendered body. The target
-- row freezes the destination and the backend configuration as written
-- in WORKFLOW.md (never the secret value a $VAR reference resolved to),
-- plus the per-target delivery result. A target is sent at most once
-- after it reaches 'succeeded', so a restart resumes only the targets
-- no row confirms. Rows of completed events are pruned past the fixed
-- retention on startup.

CREATE TABLE notification_outbox (
    notification_id TEXT PRIMARY KEY,
    event_type      TEXT NOT NULL,
    payload         TEXT NOT NULL,
    status          TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    completed_at    TEXT
);

CREATE TABLE notification_targets (
    notification_id   TEXT NOT NULL,
    destination       TEXT NOT NULL,
    kind              TEXT NOT NULL,
    backend_config    TEXT,
    status            TEXT NOT NULL,
    attempts          INTEGER NOT NULL,
    last_error        TEXT,
    next_attempt_at_ms INTEGER,
    created_at        TEXT NOT NULL,
    updated_at        TEXT NOT NULL,
    PRIMARY KEY (notification_id, destination),
    FOREIGN KEY (notification_id) REFERENCES notification_outbox(notification_id)
);

CREATE INDEX idx_notification_targets_status ON notification_targets(status, next_attempt_at_ms);
