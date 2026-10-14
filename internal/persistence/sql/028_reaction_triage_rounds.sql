-- Migration 28: recoverable reaction triage processing rounds
--
-- One row per (issue, reaction-kind) holding the current triage round
-- bound to one fingerprint. The round records, in order: that the
-- external triage command was started, its result, and which terminal
-- side effect the round reached (escalation, continuation, dispatched
-- marker, handed-off comment save). A reconcile pass resumes from the
-- persisted stage after a restart instead of re-running the command,
-- re-posting an escalation, or scheduling a second continuation.
--
-- stage is one of:
--   started       - the command was launched and no result is recorded yet
--   result        - a result is recorded (disposition/fallback columns)
--   continuation  - a dispatch-agent result created the continuation
--   escalated     - an escalate result's escalation was applied
--   dispatched    - the fingerprint's dispatched marker landed
--   handed_off    - the continuation's handed-off comments were saved
-- A row whose fingerprint changes is replaced wholesale by a fresh
-- started round, so an old round never affects a new subject.

CREATE TABLE reaction_triage_rounds (
    issue_id      TEXT    NOT NULL,
    kind          TEXT    NOT NULL,
    fingerprint   TEXT    NOT NULL,
    stage         TEXT    NOT NULL,
    disposition   TEXT    NOT NULL DEFAULT '',
    fallback      TEXT    NOT NULL DEFAULT '',
    attempts_used INTEGER NOT NULL DEFAULT 0,
    started_at    TEXT    NOT NULL,
    result_at     TEXT    NOT NULL DEFAULT '',
    completed_at  TEXT    NOT NULL DEFAULT '',
    updated_at    TEXT    NOT NULL,
    PRIMARY KEY (issue_id, kind)
);
