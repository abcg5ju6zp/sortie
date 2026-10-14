-- Migration 28: resumable cleanup rounds for terminal-state workspaces
--
-- One row per terminal workspace cleanup episode. The row pins the facts
-- observed when the terminal state was decided (actual path, issue identity,
-- last dispatch, terminal observation, before_remove hook snapshot) and
-- records every pipeline stage separately so a restart retries only
-- unconfirmed stages. Completed rows are retained as the durable "this path
-- was cleaned" fact. A later dispatch/terminal episode for a recreated
-- workspace inserts a new row.

CREATE TABLE workspace_cleanup_rounds (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    status                 TEXT    NOT NULL DEFAULT 'active',
    issue_id               TEXT,
    identifier             TEXT    NOT NULL,
    display_identifier     TEXT    NOT NULL DEFAULT '',
    issue_title            TEXT    NOT NULL DEFAULT '',
    issue_url              TEXT    NOT NULL DEFAULT '',
    workspace_key          TEXT    NOT NULL,
    workspace_path         TEXT    NOT NULL,
    workspace_root         TEXT    NOT NULL,
    ssh_host               TEXT    NOT NULL DEFAULT '',
    last_dispatch_id       TEXT    NOT NULL DEFAULT '',
    last_run_id            INTEGER NOT NULL DEFAULT 0,
    last_attempt           INTEGER NOT NULL DEFAULT 0,
    last_dispatched_at     TEXT    NOT NULL DEFAULT '',
    last_agent_kind        TEXT    NOT NULL DEFAULT '',
    terminal_state         TEXT    NOT NULL,
    terminal_source        TEXT    NOT NULL,
    terminal_observed_at   TEXT    NOT NULL,
    hook_script            TEXT    NOT NULL DEFAULT '',
    hook_timeout_ms        INTEGER NOT NULL DEFAULT 0,
    gate_confirmed_at      TEXT    NOT NULL DEFAULT '',
    quiesced_at            TEXT    NOT NULL DEFAULT '',
    hook_state             TEXT    NOT NULL DEFAULT 'pending',
    hook_started_at        TEXT    NOT NULL DEFAULT '',
    hook_finished_at       TEXT    NOT NULL DEFAULT '',
    hook_attempts          INTEGER NOT NULL DEFAULT 0,
    hook_exit_code         INTEGER NOT NULL DEFAULT 0,
    hook_output            TEXT    NOT NULL DEFAULT '',
    removal_state          TEXT    NOT NULL DEFAULT 'pending',
    removal_attempts       INTEGER NOT NULL DEFAULT 0,
    last_removal_error     TEXT    NOT NULL DEFAULT '',
    completed_reason       TEXT    NOT NULL DEFAULT '',
    completed_at           TEXT    NOT NULL DEFAULT '',
    next_due_at_ms         INTEGER NOT NULL DEFAULT 0,
    created_at             TEXT    NOT NULL,
    updated_at             TEXT    NOT NULL
);

-- At most one active round per pinned workspace path. A path cleaned by a
-- completed round may host one new active round (a fresh lifecycle episode).
CREATE UNIQUE INDEX idx_cleanup_rounds_active_path
    ON workspace_cleanup_rounds (workspace_path)
    WHERE status = 'active';

CREATE INDEX idx_cleanup_rounds_status ON workspace_cleanup_rounds (status);

CREATE INDEX idx_cleanup_rounds_issue_id ON workspace_cleanup_rounds (issue_id);
