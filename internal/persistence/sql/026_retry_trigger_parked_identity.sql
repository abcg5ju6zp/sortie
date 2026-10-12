-- Migration 26: retry trigger and parked issue identity
--
-- retry_entries.trigger holds the sortie_retries_total trigger label of
-- the schedule that created the row, so a restart serves the same answer;
-- it stays empty for a schedule that records no label. parked_issues
-- gains issue_title and issue_url, the tracker's title and web link as
-- the orchestrator read them for the park. Rows written earlier keep
-- empty values: nothing is backfilled.

ALTER TABLE retry_entries ADD COLUMN trigger TEXT NOT NULL DEFAULT '';
ALTER TABLE parked_issues ADD COLUMN issue_title TEXT NOT NULL DEFAULT '';
ALTER TABLE parked_issues ADD COLUMN issue_url TEXT NOT NULL DEFAULT '';
