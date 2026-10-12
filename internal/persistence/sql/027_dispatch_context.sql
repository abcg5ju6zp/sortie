-- Migration 27: dispatch context on retry entries, run history, and stage hops
--
-- dispatched_at is when the first agent run of a pass started, as UTC
-- RFC 3339, so a restart serves the same pick-up time and the same order.
-- retry_entries also gains the configured model and the tracker's issue
-- type, labels (a JSON array of strings, empty when never read), and state.
-- stage_hops gains the issue identity of its latest hop and run_dispatched,
-- set once an agent run of the issue started after the hop.
--
-- Rows written earlier are dated from run_history. A pass is the agent runs
-- of one issue that share a non-empty chain_id, and a row with no chain_id
-- or no such run keeps an empty dispatched_at. Timestamps are RFC 3339 text
-- whose precision and offset can differ, so every comparison goes through
-- unixepoch rather than string order.

ALTER TABLE retry_entries ADD COLUMN configured_model TEXT NOT NULL DEFAULT '';
ALTER TABLE retry_entries ADD COLUMN issue_type TEXT NOT NULL DEFAULT '';
ALTER TABLE retry_entries ADD COLUMN issue_labels TEXT NOT NULL DEFAULT '';
ALTER TABLE retry_entries ADD COLUMN issue_state TEXT NOT NULL DEFAULT '';
ALTER TABLE retry_entries ADD COLUMN dispatched_at TEXT NOT NULL DEFAULT '';
ALTER TABLE run_history ADD COLUMN dispatched_at TEXT NOT NULL DEFAULT '';
ALTER TABLE stage_hops ADD COLUMN display_identifier TEXT NOT NULL DEFAULT '';
ALTER TABLE stage_hops ADD COLUMN issue_title TEXT NOT NULL DEFAULT '';
ALTER TABLE stage_hops ADD COLUMN issue_url TEXT NOT NULL DEFAULT '';
ALTER TABLE stage_hops ADD COLUMN dispatched_at TEXT NOT NULL DEFAULT '';
ALTER TABLE stage_hops ADD COLUMN run_dispatched INTEGER NOT NULL DEFAULT 0;

UPDATE run_history SET dispatched_at = COALESCE((
    SELECT p.started_at FROM run_history p
    WHERE p.issue_id = run_history.issue_id
      AND p.chain_id = run_history.chain_id
      AND p.agent_adapter <> ''
      AND unixepoch(p.started_at) IS NOT NULL
    ORDER BY unixepoch(p.started_at, 'subsec'), p.id
    LIMIT 1), '')
WHERE chain_id <> '';

UPDATE retry_entries SET dispatched_at = COALESCE((
    SELECT p.started_at FROM run_history p
    WHERE p.issue_id = retry_entries.issue_id
      AND p.chain_id = retry_entries.chain_id
      AND p.agent_adapter <> ''
      AND unixepoch(p.started_at) IS NOT NULL
    ORDER BY unixepoch(p.started_at, 'subsec'), p.id
    LIMIT 1), '')
WHERE chain_id <> '';

UPDATE retry_entries SET configured_model = COALESCE((
    SELECT p.configured_model FROM run_history p
    WHERE p.issue_id = retry_entries.issue_id
      AND p.chain_id = retry_entries.chain_id
      AND p.agent_adapter <> ''
    ORDER BY p.id DESC
    LIMIT 1), '')
WHERE chain_id <> '';

UPDATE stage_hops SET dispatched_at = COALESCE((
    SELECT p.started_at FROM run_history p
    WHERE p.issue_id = stage_hops.issue_id
      AND p.chain_id = stage_hops.chain_id
      AND p.agent_adapter <> ''
      AND unixepoch(p.started_at) IS NOT NULL
    ORDER BY unixepoch(p.started_at, 'subsec'), p.id
    LIMIT 1), '')
WHERE chain_id <> '';

UPDATE stage_hops SET display_identifier = COALESCE((
    SELECT p.display_identifier FROM run_history p
    WHERE p.issue_id = stage_hops.issue_id
      AND (p.issue_title <> '' OR p.issue_url <> '')
    ORDER BY p.id DESC
    LIMIT 1), '');

UPDATE stage_hops SET issue_title = COALESCE((
    SELECT p.issue_title FROM run_history p
    WHERE p.issue_id = stage_hops.issue_id
      AND (p.issue_title <> '' OR p.issue_url <> '')
    ORDER BY p.id DESC
    LIMIT 1), '');

UPDATE stage_hops SET issue_url = COALESCE((
    SELECT p.issue_url FROM run_history p
    WHERE p.issue_id = stage_hops.issue_id
      AND (p.issue_title <> '' OR p.issue_url <> '')
    ORDER BY p.id DESC
    LIMIT 1), '');

UPDATE stage_hops SET run_dispatched = EXISTS (
    SELECT 1 FROM run_history p
    WHERE p.issue_id = stage_hops.issue_id
      AND p.agent_adapter <> ''
      AND unixepoch(p.started_at, 'subsec') >= unixepoch(stage_hops.hopped_at, 'subsec'))
