-- Migration 25: issue identity on retry entries and run history
--
-- issue_title and issue_url hold the tracker's title and web link as the
-- orchestrator read them when the row was written, so a restart can name
-- the issue of a queued retry or of a rebuilt pending reaction.
-- retry_entries also gains the display identifier that run_history has
-- carried since migration 6. Rows written earlier keep empty values:
-- nothing is backfilled.

ALTER TABLE retry_entries ADD COLUMN display_identifier TEXT NOT NULL DEFAULT '';
ALTER TABLE retry_entries ADD COLUMN issue_title TEXT NOT NULL DEFAULT '';
ALTER TABLE retry_entries ADD COLUMN issue_url TEXT NOT NULL DEFAULT '';
ALTER TABLE run_history ADD COLUMN issue_title TEXT NOT NULL DEFAULT '';
ALTER TABLE run_history ADD COLUMN issue_url TEXT NOT NULL DEFAULT '';
