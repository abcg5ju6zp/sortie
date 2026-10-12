## 13. Logging, Status, and Observability

### 13.1 Logging Conventions

Required context fields for issue-related logs:

- `issue_id`
- `issue_identifier`

Required context for coding-agent session lifecycle logs:

- `session_id`

Message formatting requirements:

- Use stable `key=value` phrasing.
- Include action outcome (`completed`, `failed`, `retrying`, etc.).
- Include concise failure reason when present.
- Avoid logging large raw payloads unless necessary.

A configuration advisory is recorded once, at construction or at workflow load, and logged from nowhere else: not at the point it is recorded, and not from configuration construction run for any other purpose. The running orchestrator reports a recorded advisory through one `Warn` record per entry, at the tick that first draws it and again only after a tick whose effective configuration stopped drawing it; a dry run logs the same records once, before it fetches candidate issues. A reachable deprecated agent kind draws one such record, message `"agent kind is deprecated and will be removed in a later release"`, carrying `agent_kind` and `replacement_kind`. A configuration that names a retired agent kind draws one such record per converted kind, message `"agent kind was removed and its configuration was converted to the replacement kind; the conversion will be removed in a later release"`, carrying `agent_kind` (the retired kind) and `replacement_kind`; the same advisory is a `sortie validate` warning under the check `agent.kind.retired`, and its text names the converted references, the launch the replacement kind now performs, the settings not carried, and the remedy, without printing `agent.command` as written. An invalid or incomplete `token_rates` entry draws one such record per warning, message `"skipped invalid token rate entry"`, carrying `detail` (the warning text). Each deprecated notification form the configuration relies on draws one such record, message `"deprecated configuration in use"`, carrying `setting` (the key path of the deprecated form, or the implicit event `auto_merge.merged` or `budget.held`) and `replacement` (the `events` list to write, preceded by `escalation: none` for a reaction's `escalation: comment`); the same advisory is a `sortie validate` warning whose check names the form: `tracker.comments.on_dispatch.deprecated`, `tracker.comments.on_completion.deprecated`, `tracker.comments.on_failure.deprecated`, `reactions.<key>.escalation.deprecated`, `notifications.tracker_comment.implicit_auto_merge`, `notifications.tracker_comment.implicit_budget_hold`, and `notifications[<i>].events.missing`. Nothing is logged per event.

A retry whose selection the configuration in force changes emits one `Info` record, message `"retry dispatching on the selection the configuration in force gives it"`, carrying `agent_kind`, `template_id`, and `rule_name` of the frozen selection and `dispatch_agent_kind`, `dispatch_template_id`, and `dispatch_rule_name` of the selection it dispatches on. A retry that finds the adapter of its selected kind unavailable emits one `Error` record, message `"retry agent kind unavailable"`, carrying `rule_name`, `agent_kind`, `attempt`, `delay_ms`, and `error`, and is rescheduled (Section 8.4).

An issue that carries the stage labels of two or more dispatch rules emits one `Warn` record, message `"several stage labels found"`, through the issue-scoped logger, carrying `stage_labels` (every stage label the issue carries, as written in the workflow, in rule list order) and `rule_name` (the rule the evaluation selected). The record is emitted once per evaluation of the rules, on the poll tick and on a retry that is routed afresh, and never for a retry whose frozen selection stands.

A stage hop (§7.3) records its decision result on the run's `run_history` row and emits `stage.advanced` or `stage.not_advanced` (§10.4.7), and it emits its log records through the issue-scoped logger. The attributes `source_rule`, `target_rule`, `target_label`, `hop_count`, `hop_ceiling`, and `chain_path` recur: `hop_ceiling` is the per-issue ceiling in force, and `chain_path` is the rule chain through the source rule, rule names joined with ` -> `.

- A made hop emits one `Info` record, message `"stage hop made"`, carrying `source_rule`, `target_rule`, `target_label`, `hop_count` (after the hop), `hop_ceiling`, and `chain_path`.
- A made hop whose removal of a source stage label failed emits one `Warn` record instead, message `"stage hop made, stage labels left on the issue"`, carrying the attributes above plus `stage_labels_left` and `error` (the first removal error). The hop stands.
- A hop that is not made emits one `Warn` record, message `"stage hop not made"`, carrying `reason` (`ceiling` or `add_failed`), `source_rule`, `target_rule`, `target_label`, `hop_count` (before the hop), `hop_ceiling`, and `chain_path`. For `add_failed` it also carries `error` and `missing_stage_labels` (every configured stage label the dispatch read showed that a fresh read no longer shows), or `stage_label_read_error` when that read failed. The exit then takes the handoff write.
- A reset of the hop count emits one `Info` record, message `"stage hop count reset"`, carrying `trigger` (`handoff`, `terminal`, `not_active`, `parked`, or `unparked`) and `hop_count`.
- A failed direct read in the hold emits one `Warn` record, message `"stage hop target read failed, holding issue"`, carrying `target_label` and `error`; the issue is held for that tick.
- A hold released by a read that does not show the target label emits one `Info` record, message `"stage hop target label absent, hold released"`, carrying `target_rule` and `target_label`; the issue is then routed by the labels that read returned.

A failed write of a hop record, of its observation flag, or of its deletion emits one `Error` record, message `"failed to persist stage hop"`, `"failed to persist stage hop observation"`, `"failed to delete stage hop record"`, or `"failed to persist stage hop result"` (the write of the decision result onto the run's row), carrying `error`; the runtime value stands in each case, and a failed result write leaves the row's decision fields empty while the hop and its event stand. A failed load of the records at startup emits one `Warn` record, message `"failed to load stage hop records, starting with none"`, carrying `error`. A failed `tracker_comment` send of a stage event emits one `Warn` record, message `"stage event comment failed"`, carrying `event_type` and `error`; every other destination's failure keeps its existing record. A hop adds no metric; the session events keep their bodies.

Four records carry the settings an attempt dispatches with (Section 8.4). The first dispatch of a claim emits one `Info` record, message `"issue dispatched"`, through the issue-scoped logger, carrying `agent_kind`, `rule_name`, `model`, and `effort`, the last two being the configured values and empty when unset. A retry or continuation dispatch emits the same four attributes on its `"retried issue dispatched"` record, beside `attempt`. An attempt whose resolved settings block fails an error-severity check emits one `Error` record and starts no session: `"agent settings refused"` on the first dispatch, and `"retry agent settings refused"` on the retry lane, carrying `attempt` and `delay_ms` as well. Both carry `error` (the preflight result naming every failed check), `rule_name`, `agent_kind`, and the first failed check's `check` and `diagnostic`. A retry whose claim held a rule's settings block on its previous attempt, whose selection still names that rule, and whose rule no longer carries a block for the kind, emits one `Info` record, message `"rule settings no longer present, attempt runs on the kind's top-level settings"`, carrying `rule_name` and `agent_kind`, once, on the attempt that runs without the block.

Handoff-evidence records are part of the required operator surface:

- A withheld verdict whose verification read (§11.5, §14.2) does not find the issue terminal emits a `Warn` record naming the verdict and carrying `turns_completed` plus the resulting `consecutive_absences` count. The standard issue context fields identify the affected issue.
- A withheld verdict whose verification read does find the issue terminal emits an `Info` record at the read site naming the discarded verdict, its reason, `state_source="verified"`, and `turns_completed`, instead of the `Warn` record above. The terminal disposition's own `Info` record then follows, carrying the verified state and the same `state_source="verified"` value.
- An `evidence not determinable` verdict emits an `Info` record under both `observed` and `strict`, carrying the policy and `turns_completed`. Under `strict`, the separate withheld warning is also emitted because that policy converts the verdict into the absence disposition, unless the verification read above finds the issue terminal.
- A run frozen to `tracker.handoff_evidence: off` emits none of these evidence records.

Parking an issue, whichever trigger produced it, emits exactly one `Warn` record, message `"issue parked"`, carrying `reason` (`agent_blocked` or `handoff_absence`), `parked_state` (the tracker state recorded, empty when unobserved), and `label` (the parking label applied). The consecutive-absence ceiling's park additionally carries `consecutive_absences`, `absence_ceiling`, and `ceiling_setting` (the dotted configuration path that produced the ceiling); a `blocked` park carries none of the three. A failed label write is recorded separately, message `"park label write failed"`, at `Warn`, and does not suppress the parking record. Lifting a park emits one `Info` record, message `"issue unparked"`, carrying `trigger` (`state_changed`, `label_removed`, or `evidence_observed`) and `reason`.

An issue entering the per-issue budget-exhausted set, on either the poll tick's rebuild or the retry lane, emits exactly one `Warn` record, message `"candidate held by budget ceiling"`, carrying the standard issue context fields plus `reason` (`token_budget` or `session_budget`), `used_sessions`, `budget_sessions`, and, when the token ceiling was evaluated for that issue, `used_tokens` and `budget_tokens`. It also carries `ceiling_setting`, the dotted configuration path of the setting that governs the fired ceiling, when `reason` maps to a known setting; a reason with no known governing setting emits no `ceiling_setting` attribute rather than an empty or invented one. The record fires once per hold: a tick that re-observes an already-announced hold under the same reason emits nothing further, and whichever lane discovers a hold is the only one that announces it.

The in-flight token ceiling adds four records of its own, beyond the hold record above, so budget observability covers a run already in flight and not only a re-dispatch that never starts. A dispatch emits at most one of the two freeze records, never both. A dispatch whose resolved usage arrival reports no figure at all emits one `Warn` record, message `"token ceiling cannot bound this run"`, carrying `agent_kind`, `usage_arrival`, and `budget_tokens`, and also `error` when the baseline read failed on the same dispatch. A dispatch whose arrival does report figures but whose baseline read fails emits one `Warn` record instead, message `"prior token spend unknown, token ceiling bounds this session only"`, carrying `error` and `budget_tokens`. A run the arrival cannot bound never gets the second record, because the ceiling does not bound that session either and saying so would contradict the first. A confirming read that fails while a running session is over the pre-filter, and whose session has not reached the ceiling on its own spend, emits one `Warn` record, message `"in-flight token ceiling check failed, run continues"`, carrying `error` and `budget_tokens`, at most once per run regardless of how many failing reads follow. A run recorded `budget_stopped` emits one `Warn` record, message `"run stopped by token ceiling"`, at that exit, carrying `reason`, `used_tokens`, `budget_tokens`, `issue_tokens_completed`, `session_tokens`, `sum_source`, and `ceiling_setting` from the moment the ceiling decided the stop. `sum_source` is `confirmed_read` when a read established the completed sum and `session_spend_alone` when the session's own spend reached the ceiling and no read was needed; `unmeasured_sessions` and `unaccounted_turns` join the record only in the first case, because only a read supplies those counts.

The token warning threshold adds one record of its own, at most once per run: the live per-issue figure reaching the configured threshold emits one `Warn` record, message `"token warning threshold reached"`, carrying `used_tokens`, `warning_tokens`, `budget_tokens`, `issue_tokens_completed`, `session_tokens`, `ceiling_setting`, and `warning_setting` (the dotted configuration path `agent.token_warning_percent`) from the moment the threshold was reached. A run that never reaches the threshold emits no such record, and a run whose figure reaches or passes it more than once still emits exactly one.

A run that ends under a non-zero ceiling, whose resolved usage arrival reports figures and which recorded none, emits one `Warn` record at its exit, message `"run reported no token usage, token ceiling could not bound it"`, carrying `agent_kind`, `usage_arrival`, and `budget_tokens`, once per run. An arrival is declared by the kind while the figures come from whatever that kind runs, so a kind that carries another runtime keeps its declaration whatever that runtime reports, and this record is the only one that reaches that pair: the dispatch records above all key on the declaration and stay silent. A dispatch whose arrival reports no figure at all is already announced by the first of them and never reaches this record.

A running session whose resolved usage arrival is `none` and whose runtime reports a usage figure, on an event or a turn result, emits one `Warn` record per run, message `"token usage discarded: agent kind declares this session reports none"`, carrying `agent_kind`, and the figure is discarded as Section 13.5 states.

The notice this same hold delivers is recorded separately, at the delivery site rather than alongside the log record above. When the `tracker_comment` destination received the event, a successful write emits one `Info` record, message `"budget hold notice posted"`, carrying the standard issue context fields. A failed write emits one `Warn` record, message `"budget hold notice failed"`, carrying the standard issue context fields and `error`, and does not suppress the log record above. A hold whose notice has no destination emits neither record.

Every orchestrator event is routed to its destinations before it is delivered, and each outcome other than the `tracker_comment` outcome is logged at the delivery site. A destination whose send failed emits one `Warn` record, message `"notification delivery failed"`, carrying `event_type`, `destination` (`notifications[<i>]`, the position of the entry in the list), `notifier_kind`, and `error`. A destination whose send succeeded emits one `Debug` record, message `"notification delivered"`, carrying the same attributes except `error`. The `tracker_comment` outcome is logged by its producer under that producer's own message, so a tracker write failure is as visible as before the destination existed. A routing table that cannot be installed from a loaded configuration emits one `Error` record, message `"notification routing update failed"`, carrying `error`, and the table in force stays installed. Workflow validation normally rejects such a configuration first, so this record is a defensive path.

The periodic workspace sweep emits exactly one summary record per pass, at `Info` level, message `"sweep: pass complete"`, on every pass that produced a candidate set, including a pass over zero keys, a pass whose tracker read failed, and a pass that removed nothing. This is deliberate: a sweep that finds nothing to remove and says nothing is indistinguishable from a sweep that is not running at all, which is the failure mode this record exists to close. The record carries thirteen attributes:

- `candidates`, `excluded_running`, `excluded_retry`, `excluded_reaction`, `removed_terminal`, `removed_age`, `retained_in_window`, `retained_no_activity`, `retained_not_evaluated`, and `failed` are integer counts. The nine counters after `candidates` partition the candidate set, so `candidates` equals their sum on every pass; `retained_not_evaluated` is what makes that identity hold on a pass where the age bound did not evaluate anything.
- `retention_days` is the configured `workspace.retention_days` value for that pass; `0` means the bound is off.
- `age_pass` is `"on"` when the age bound evaluated its candidates that pass, `"off"` when `retention_days` is `0` or below the floor (`WorkspaceRetentionMinDays`), and `"unavailable"` when the persistence read is not configured or its query failed.
- `tracker_read` is `"ok"` or `"failed"`.

Each workspace removed by the age bound also emits its own `Info` record, message `"sweep: removed expired workspace"`, carrying `workspace_key`, `last_activity` (RFC3339), and `age_days`.

The `"tick completed"` `Info` record carries four integer attributes for the blocker gate in addition to its existing ones: `held_by_blockers`, `blockers_unresolved`, `blockers_not_read`, and `blockers_incomplete`, each counting the candidates the tick held for that reason. It also carries `budget_exhausted`, the number of this tick's candidates in the per-issue budget-exhausted set after the rebuild.

A candidate the dispatch gate holds for a blocker reason produces exactly one of five per-issue records, and a pass whose reads were refused by the forge produces exactly one pass-level record in addition:

- Held by a non-terminal blocker: one `Debug` record, message `"candidate held by blocker"`, carrying the issue context fields plus `blocker_identifier` and `blocker_state` for the first non-terminal blocker.
- Held because its own blocker read was attempted and failed: one `Warn` record, message `"candidate blockers unresolved, holding issue"`, carrying the issue context fields and `error`.
- Held because the pass had already halted on another candidate's failure: one `Debug` record, message `"candidate blockers not read this tick, pass halted"`, carrying `error_kind` and no `error`, because nothing failed on this candidate.
- Held because its producer declared the list incomplete, with no read attempted: one `Debug` record, message `"candidate blocker list incomplete, holding issue"`, carrying the issue context fields and no `error`.
- Held because the pass had already spent its read budget: one `Debug` record, message `"candidate blockers not read this tick, holding issue"`, carrying `reads_spent` and no `error`.

A pass whose reads halted emits exactly one `Error` record, message `"blocker reads halted for this tick"`, carrying `error_kind`, `http_status` (`0` when the failure carried none), `operation` (`"fetch_blockers"`), and `held_unread`. A tick that dispatches nothing because every blocker read it attempted failed transiently, and that did not already emit the pass-level `Error` above, emits one additional `Warn` record, message `"tick dispatched nothing: every attempted candidate blocker read failed"`, carrying `reads_failed`, the count of reads the pass attempted and lost.

### 13.2 Logging Outputs and Sinks

Sortie does not prescribe where logs must go (stderr, file, remote sink, etc.).

Requirements:

- Operators must be able to see startup/validation/dispatch failures without attaching a debugger.
- Sortie may write to one or more sinks.
- If a configured log sink fails, Sortie continues running when possible and emits an operator-visible warning through any remaining sink.
- Every sink receives records already masked, as §15.3 states.

### 13.3 Runtime Snapshot / Monitoring Interface

If the implementation exposes a synchronous runtime snapshot (for dashboards or monitoring), it should return:

- `running` (list of running session rows)
- each running row should include `turn_count`
- each running row should include `tokens_measured`, meaning at least one usage measurement the orchestrator records has been reported so far in that session; it stays false throughout for a session whose resolved `usage_arrival` is `none`, whatever its runtime reports
- each running row should include `usage_arrival` and `usage_attribution`, the usage-reporting disposition resolved for that session's kind, passthrough, and launch mode, frozen at dispatch
- each running row should include `tokens_pending`, true only when the frozen arrival settles at most one figure per turn, the session is measured, and the turn that figure would settle for is still in flight
- each running row should include `tokens_awaited`, true only when the session has recorded no figure and the moment its frozen arrival names for one has not passed: no turn has begun for an arrival that reports during a turn, and no turn has ended for one that settles at most one figure per turn. A row whose arrival reports figures and whose `tokens_awaited` is false recorded nothing and will record nothing, so a consumer must not present it as a figure still to come
- each running row should include `api_requests_measured`, true when the row's `api_request_count` is a count of model API requests the session measured
- each running row should include the issue's spend and the limits it runs against: the usage of the issue's completed sessions, read from run history at every dispatch whether or not a token ceiling is set and refreshed by each confirming read of the in-flight token ceiling; whether the issue's token sum is its whole spend, which counts the running session's unaccounted turns as its worker reports them while the session runs; the token ceiling and the session budget in force as of the latest poll tick; and the estimated cost of the running session and of the whole issue, each with a state that names why a figure is partial or absent. Section 13.7.2 gives the keys and how to read them
- each running row should include `rule_name`, `configured_model`, and `configured_effort`, the dispatch rule the session runs under and the model and reasoning level its resolved settings block carries, empty (and omitted from the JSON form) when unset; `model_name` keeps the model the runtime reported, which can differ from the configured one under routing aliases and runtime fallbacks
- each running row should include `issue_title`, `issue_url`, and `issue_labels`, the issue's title, web link, and labels as the session was dispatched with them, unchanged for the whole run and the same text the agent's prompt received. `issue_title` and `issue_url` are empty (and omitted from the JSON form) when the tracker returned none; `issue_labels` is always present, an empty list when the issue carries none, lowercase in the tracker's order as Section 11 normalizes them
- every such string is masked as Section 15.3 states, in the snapshot rather than in the state it is copied from, because stage advancement reads the labels as the tracker returned them
- `retrying` (list of retry queue rows). Each row carries `issue_title` and `issue_url` beside the display identifier, the read of the issue the retry was scheduled with. The read is persisted with the retry and restored at startup, so a restart serves what was served before it, and a reaction retry scheduled from a pending reaction rebuilt at startup carries the read its run recorded. A retry that holds no read, because it was restored from a row persisted before the identity was recorded, takes the first read the orchestrator takes of its issue: the next poll tick whose candidates list it, or its next fire. A read already held is never replaced. Each row also says why the retry waits. `trigger` is the `sortie_retries_total` label of the schedule that created the retry (`error`, `continuation`, `timer`, `stall`, or `ci_fix`); it is omitted when that schedule records none, as for a review, bot review, merge conflict, or label command continuation, which `reaction_kind` names instead. The trigger is persisted with the retry and restored at startup. `reaction_kind` names the reaction behind the retry, and `reaction_config_key` the `reactions` key of WORKFLOW.md that kind is configured under. `rule_name` is the dispatch rule frozen with the retry. `stage_lineage` names the stage chain and the stage hop that reached the run the retry follows. `paused` is true while the issue's state does not permit dispatching the retry. When the retry timer puts a retry back instead of dispatching it, the row's error is the reason it was put back, never the error of the run before it
- `agent_totals`
  - `input_tokens`
  - `output_tokens`
  - `total_tokens`
  - `cache_read_tokens`
  - `cache_write_tokens`
  - `seconds_running` (aggregate runtime seconds as of snapshot time, including active sessions)
  - `unmeasured_sessions` (cumulative count of ended sessions whose usage was never recorded; persisted alongside the counters above and restored on startup, so it names the sessions they excluded across every restart, not just the current process)
  - `running_unreported` (count of the current running set whose kind reports usage, whose figure has not arrived, and whose `tokens_awaited` is true, so it may still arrive)
  - `running_non_reporting` (count of the current running set reporting no usage at all: either the resolved `usage_arrival` is `none`, in which case any figure the session's runtime sends is discarded, or the arrival reports figures and the moment it names for one passed with nothing recorded)
- `rate_limits` (latest coding-agent rate limit payload, if available)
- `budget_exhausted_count` (number of issues currently blocked by a re-dispatch budget; always present)
- `budget_exhausted` (list of blocked-issue records, sorted by identifier; always present, empty when the set is empty). Each record carries the issue's ID and identifier, the reason (`token_budget` or `session_budget`; `token_budget` takes precedence over `session_budget` when one issue reaches both gates), the used and budgeted session and token counts, the count of the issue's sessions the token ceiling stopped in flight (present only when the token ceiling was evaluated for the issue), the time the hold began, and the issue's title and tracker link. The title and link come from the read of the poll tick that last rebuilt the set, or from the retry the hold was found on until the next tick. The set is rebuilt per tick from those two gates (Section 8.4) and also updated by the retry lane when it discovers a hold between ticks.
- `parked_count` (number of issues currently held out of primary dispatch; always present)
- `parked` (list of park records, sorted by identifier and then issue ID; always present, empty when no issue is parked). Each record carries the issue's ID and identifier, the display identifier when the tracker needs a qualified form, the issue's title and tracker link, the reason (`agent_blocked` or `handoff_absence`), the tracker state observed when the park was recorded, the parking label the orchestrator applied, whether a read has shown that label on the issue, and the time the park was recorded. The title, link, and state are masked as Section 15.3 states, in the snapshot rather than in the state they are copied from. The park persists the title and link and restores them at startup. A park that holds no read of its issue, because it was recorded or restored without one, takes the first read the orchestrator takes of the issue: the next poll tick's candidate, or one single-issue read when the candidate fetch omits the issue and the batched state read of the release step finds it (Section 8.1). A read already held is never replaced or read again
- `config` (the effective workflow shape: the tracker states, the dispatch rules in order with how each is selected, the stage chains their `next` links form, the hop ceiling, the reaction kinds the process applies with their limits, escalation, and timing, the token rates, the session and token budgets, the poll interval, the concurrency caps, the SSH hosts, and whether each agent selection reports token usage). It reports the configuration in force after the last successful load, except that a reaction appears exactly when the process applies it, with the settings it applies, and the token rates are the ones the process started with. It carries no credential. Section 13.7.2 gives the keys and how to read them

Recommended snapshot error modes:

- `timeout`
- `unavailable`

### 13.4 Optional Human-Readable Status Surface

A human-readable status surface is optional and implementation-defined. When the HTTP server is enabled (Section 13.7), the HTML dashboard served at `/` (Section 13.7.1) is the concrete realization of this surface.

If present, it should draw from orchestrator state/metrics only and must not be required for correctness.

Live status answers what is happening now. A second, offline reporting surface answers what happened over a past window: it reads persisted run history without contacting the tracker, the coding agent, or a running orchestrator, and summarizes outcomes, durations, turn counts, and token spend, optionally grouped by the recorded run attributes and bounded by a time range. It groups runs by stage chain, listing only the chains in which a hop was due or made and leaving out runs without a chain; its text form bounds that listing to the chains with the most runs and states how many chains it left out, while its machine-readable form lists them all. It prices token spend from the operator-supplied rates in workflow configuration; with no rates configured it reports the countable figures and omits cost rather than guessing at one. Because it reads only what earlier runs already recorded, a database written by an older schema yields a reduced set of figures, and the surface reports which figures it could not derive instead of presenting a zero. This surface performs no state mutation and is never required for orchestrator correctness.

A figure can be missing for two distinct reasons: the database predates the schema that records it, or nothing measured it because the coding agent behind the run reported no token usage. The schema tier alone no longer separates the two: a full-tier report can still carry a null `tokens` summary or breakdown row when every run in range is unmeasured. `tokens_unmeasured_runs` is the field that distinguishes a figure missing for want of a schema from one missing for want of a measurement. This supersedes the clause in ADR-0019 (`docs/decisions/0019-keep-usage-data-on-the-host.md`) stating that `schema_tier` alone tells a consumer whether token and cost figures were available at all.

### 13.5 Session Metrics and Token Accounting

A run is measured when the runtime reported at least one usage figure for the session and the adapter carried it into the recorded counters, or when the worker never entered an agent turn, because a run that launched no agent spent exactly zero. A run is unmeasured when an agent turn began and no usage figure ever arrived; its recorded token figures are zero and that zero carries no information. A measurement of zero is a measured run whose reported figures are zero, which is a legitimate statement recorded as measured. A figure reported for a session whose resolved usage arrival is `none` is not a measurement: it enters no token counter, persisted row, or reporting surface, and the run stays unmeasured unless it never entered an agent turn.

The run record carries this distinction alongside the five token counters. An unmeasured run contributes nothing to any token counter and is excluded from cost pricing. It advances no Prometheus token counter and creates no series, the same as a run that never emitted a usage event.

The run's usage includes the credential-verification step (§10.9): the step's own request is a model request like any other, and the working session's figures continue its series rather than starting over from zero. The worker captures the step's componentwise watermark once the step ends and adds it to every later working-session figure, event and turn result alike, before that figure is folded into the run's totals and before it is relayed; an all-zero working figure, or a step that reported nothing, changes nothing. A verification `SpendUnaccounted` result raises the run's unaccounted count exactly as a working turn's does. The relayed verification events carry the message `verifying the agent credential` and are `token_usage` or `notification` only, with no `session_id`, `agent_pid`, tool field, or rate-limit payload.

Token accounting rules:

- Agent adapters normalize token counts before emitting events. The orchestrator receives `{input_tokens, output_tokens, total_tokens, cache_read_tokens, cache_write_tokens}` directly.
- For absolute totals, track deltas relative to last reported totals to avoid double-counting. The `cache_read_tokens` and `cache_write_tokens` fields follow the same cumulative-delta accounting as `input_tokens` / `output_tokens`. Deltas are accumulated from any event carrying a non-zero usage payload, not only `token_usage` events, so an adapter can attach the authoritative run-cumulative snapshot to a turn-finalization event without losing it.
- Every cost figure Sortie shows prices `input_tokens`, `cache_read_tokens`, `cache_write_tokens`, and `output_tokens` each once, at the rate configured for its own token class: fresh input (`input_tokens` minus the two cache counters, floored at zero) at the input rate, cache reads at the cache-read rate, cache writes at the cache-write rate, and output at the output rate. An unset cache-read or cache-write rate prices that class at the input rate. An agent kind's rate entry prices nothing, and that kind's sessions and runs count as unpriced, unless the entry carries both an input and an output rate.
- `api_request_count` is incremented monotonically, and only, per `token_usage` event; a usage-bearing terminal event does not count as an additional request. The count is a measurement of API requests only when the session's resolved `usage_arrival` is `incremental` and either a figure has arrived or no turn has begun. A kind resolving `turn_end` settles the count at most once per turn and never measures requests, and a session whose runtime stopped delivering per-request figures reports the count as unmeasured rather than as zero, so a consumer never reads a fabricated zero where the declaration alone promised a request count.
- `tokens_pending` distinguishes a settled figure from one still in flight: it is true only when the resolved `usage_arrival` is `turn_end`, the session is measured, and the turn that figure would settle for has not yet reached a terminal event. A consumer presenting the current token total alongside this flag can tell an operator the figure excludes the turn in progress, rather than presenting a stale total as final.
- Accumulate aggregate totals in orchestrator state (`agent_totals`).
- At session exit, the session's token totals are written to the `run_history` row alongside the aggregate update. The worker derives the run's final usage, its model name, and its request count from every agent event it relays during the run, self-review turns included. The final usage is reconciled from the worker result before that row is written, and the model name and the request count before the `session_metadata` row is written, so an event dropped on its way to the orchestrator cannot lower the recorded total, leave the `session_metadata` row without the model its figure came from, or leave a measured request count below the number of `token_usage` events the worker relayed. The per-issue token budget (`agent.max_tokens`) sums `run_history` `total_tokens` per issue; the in-flight lane adds the running session's live in-memory total, which is not the same figure the `cost_budget` tool reads. The tool adds the running session's recorded `session_metadata` total instead, written at most once per throttled write interval, so its advisory reading trails the in-flight lane's enforced figure by up to that interval; the one write per run that carries the figure for which "token warning threshold reached" (§13.1) was logged is the exception, made immediately rather than waiting for that interval.

Timing accounting rules:

- `api_time_ms` is the cumulative LLM API wait time in milliseconds for the session. Accumulated from `api_duration_ms` fields on any agent event that carries timing data.
- `tool_time_ms` is the cumulative tool execution time in milliseconds. Accumulated from `tool_result` events that carry `duration_ms`.
- `tool_time_percent` and `api_time_percent` are computed at render time as `(cumulative_ms / session_elapsed_ms) * 100`. Displayed as null/"N/A" when no timing data has been received.

Runtime accounting:

- Runtime should be reported as a live aggregate at snapshot/render time.
- Sortie maintains a cumulative counter for ended sessions and adds active-session elapsed time derived from `running` entries (for example `started_at`) when producing a snapshot/status view.
- Add run duration seconds to the cumulative ended-session runtime when a session ends (normal exit or cancellation/termination).
- Continuous background ticking of runtime totals is not required.

Rate-limit tracking:

- Track the latest rate-limit payload seen in any agent update.
- Any human-readable presentation of rate-limit data is implementation-defined.

### 13.6 Humanized Agent Event Summaries (Optional)

Humanized summaries of raw agent protocol events are optional.

If implemented:

- Treat them as observability-only output.
- Do not make orchestrator logic depend on humanized strings.

### 13.7 HTTP Server

Sortie includes an embedded HTTP server for observability and operational control. The server starts unconditionally on port **7678** unless explicitly disabled. It is not required for orchestrator correctness, but its absence silently removes health probes, Prometheus metrics, and the dashboard.

Enablement and configuration:

- The HTTP server starts by default on `127.0.0.1:7678` with no flags required.
- `--port N` overrides the listening port. Port `0` disables the server entirely.
- `--host ADDR` overrides the bind address. `ADDR` must be a parseable IP address. Default: `127.0.0.1`. Container deployments use `0.0.0.0`.
- `server.port` and `server.host` extension keys provide the same overrides via workflow front matter. CLI flags take precedence over extension keys.
- `server.port` must be an integer in the range 1–65535, or `0` to disable.
- `server.host` must be a parseable IP address string. DNS hostnames are not accepted.
- When the default port (7678) is occupied and the operator did not explicitly request a port (`--port` absent, `server.port` extension absent), Sortie logs a warning and starts without the HTTP server; the orchestrator continues normally. When the operator explicitly requested a port (via `--port` or `server.port`) and it is already in use, Sortie exits with code 1 and a descriptive error. No automatic port selection occurs.
- The `--dry-run` flag suppresses server startup regardless of port or host settings.
- Changes to HTTP listener settings require restart (hot-rebind is not supported).

#### 13.7.1 Human-Readable Dashboard (`/`)

- Host a human-readable dashboard at `/`.
- The returned document should depict the current state of the system (for example active sessions, retry delays, token consumption, runtime totals, recent events, health/error indicators, and run history from SQLite).
- It is up to the implementation whether this is server-generated HTML or a client-side app that consumes the JSON API below.
- A run-history row whose selection came through a hop, or whose exit reached a hop decision, shows its rule, its stage chain, and its stage path: the previous rule, the row's own rule, and, when the exit reached a decision, the target rule with its result. Every other row, including a row written before the chain was recorded, renders as before.

#### 13.7.2 JSON REST API (`/api/v1/*`)

Provide a JSON REST API under `/api/v1/*` for current runtime state and operational debugging.

Minimum endpoints:

- `GET /api/v1/state`
  - Returns a summary view of the current system state (running sessions, retry queue/delays, aggregate token/runtime totals, latest rate limits, the effective workflow shape, and any additional tracked summary fields).
  - Suggested response shape:

    ```json
    {
      "generated_at": "2026-02-24T20:15:30Z",
      "counts": {
        "running": 2,
        "retrying": 1,
        "budget_exhausted": 1,
        "parked": 1
      },
      "running": [
        {
          "issue_id": "abc123",
          "issue_identifier": "MT-649",
          "issue_title": "Fix the flaky login test",
          "issue_url": "https://tracker.example.com/browse/MT-649",
          "issue_labels": ["bug", "backend"],
          "state": "In Progress",
          "session_id": "thread-1-turn-1",
          "turn_count": 7,
          "self_review_active": false,
          "self_review_iteration": 0,
          "last_event": "turn_completed",
          "last_message": "",
          "started_at": "2026-02-24T20:10:12Z",
          "last_event_at": "2026-02-24T20:14:59Z",
          "tokens": {
            "input_tokens": 1200,
            "output_tokens": 800,
            "total_tokens": 2000,
            "cache_read_tokens": 400,
            "cache_write_tokens": 100
          },
          "model_name": "claude-sonnet-4-20250514",
          "rule_name": "hard",
          "configured_model": "claude-sonnet-4-20250514",
          "configured_effort": "high",
          "workflow_file": "WORKFLOW.md",
          "agent_kind": "claude-code",
          "api_request_count": 3,
          "requests_by_model": {"claude-sonnet-4-20250514": 3},
          "tool_time_percent": 12.3,
          "api_time_percent": 45.6,
          "tokens_measured": true,
          "usage_arrival": "incremental",
          "usage_attribution": "per_model",
          "tokens_pending": false,
          "tokens_awaited": false,
          "api_requests_measured": true,
          "used_sessions": 2,
          "budget_sessions": 5,
          "issue_tokens_completed": 105000,
          "unmeasured_sessions": 0,
          "used_tokens": 107000,
          "used_tokens_complete": true,
          "budget_tokens": 500000,
          "estimated_cost_usd": 0.014595,
          "cost_status": "estimated",
          "issue_estimated_cost_usd": 0.235095,
          "issue_cost_status": "estimated"
        }
      ],
      "retrying": [
        {
          "issue_id": "def456",
          "issue_identifier": "MT-650",
          "issue_title": "Retry the export job",
          "issue_url": "https://tracker.example.com/browse/MT-650",
          "attempt": 3,
          "due_at": "2026-02-24T20:16:00Z",
          "error": "no available orchestrator slots",
          "trigger": "timer",
          "rule_name": "default",
          "stage_lineage": {
            "chain_id": "7q2m4x9b1k5d8f3h6j0n2p4rst",
            "previous": "review",
            "previous_outcome": "succeeded"
          },
          "paused": false
        }
      ],
      "budget_exhausted": [
        {
          "issue_id": "ghi789",
          "issue_identifier": "MT-651",
          "issue_title": "Migrate the billing tables",
          "issue_url": "https://tracker.example.com/browse/MT-651",
          "reason": "session_budget",
          "used_sessions": 3,
          "budget_sessions": 3,
          "used_tokens": null,
          "budget_tokens": 0,
          "unmeasured_sessions": null,
          "stopped_in_flight": null,
          "exhausted_at": "2026-02-24T20:12:00Z"
        }
      ],
      "parked": [
        {
          "issue_id": "jkl012",
          "issue_identifier": "MT-652",
          "issue_title": "Rotate the signing keys",
          "issue_url": "https://tracker.example.com/browse/MT-652",
          "reason": "agent_blocked",
          "parked_state": "In Progress",
          "label": "needs-human",
          "label_applied": true,
          "parked_at": "2026-02-24T20:05:00Z"
        }
      ],
      "agent_totals": {
        "input_tokens": 5000,
        "output_tokens": 2400,
        "total_tokens": 7400,
        "cache_read_tokens": 1500,
        "cache_write_tokens": 300,
        "seconds_running": 1834.2,
        "unmeasured_sessions": 3,
        "running_unreported": 1,
        "running_non_reporting": 1
      },
      "rate_limits": null,
      "config": {
        "tracker": {"kind": "jira", "active_states": ["To Do", "In Progress"], "in_progress_state": "In Progress", "handoff_state": "In Review", "terminal_states": ["Done"]},
        "polling": {"interval_ms": 30000},
        "agent": {
          "kind": "claude-code", "usage_arrival": "incremental", "max_turns": 20, "stall_timeout_ms": 300000,
          "max_retry_backoff_ms": 300000, "max_sessions": 6, "max_tokens": null,
          "max_concurrent_agents": 6, "max_concurrent_agents_by_state": {}
        },
        "worker": {"ssh_hosts": [], "max_concurrent_agents_per_host": null},
        "dispatch": {
          "rules": [
            {"name": "specify", "selected_by": "match", "match": {"issue_type": ["Story"]}, "next": "implement", "agent_kind": "claude-code", "usage_arrival": "incremental"},
            {"name": "implement", "selected_by": "stage", "stage": "stage-implement", "agent_kind": "claude-code", "usage_arrival": "incremental"},
            {"name": "deps", "selected_by": "match", "match": {"labels": ["dependencies"], "priority": {"op": "lte", "value": 2}}, "agent_kind": "codex", "usage_arrival": "incremental"},
            {"name": "everything", "selected_by": "catch_all", "agent_kind": "claude-code", "usage_arrival": "incremental"}
          ],
          "default": null,
          "chains": [["specify", "implement"]],
          "max_consecutive_hops": null,
          "hop_ceiling": 10
        },
        "reactions": {
          "ci_failure": {"max_retries": 2, "escalation": "label", "escalation_label": "needs-human"},
          "review_comments": {"max_continuation_turns": 3, "debounce_ms": 60000, "poll_interval_ms": 120000, "escalation": "label", "escalation_label": "needs-human"},
          "bot_review": {"max_continuation_turns": 5, "poll_interval_ms": 60000, "bot_usernames": ["review-bot"], "escalation": "none"},
          "auto_merge": {"strategy": "squash", "require_ci": true, "delete_branch": true, "poll_interval_ms": 60000, "max_retries": null, "escalation": "label", "escalation_label": "needs-human"}
        },
        "token_rates": {
          "claude-code": {"input_per_mtok": 3.0, "output_per_mtok": 15.0, "cache_read_per_mtok": 0.3, "cache_write_per_mtok": null, "complete": true},
          "codex": {"input_per_mtok": 2.5, "output_per_mtok": null, "cache_read_per_mtok": null, "cache_write_per_mtok": null, "complete": false}
        }
      }
    }
    ```

- `GET /api/v1/<issue_identifier>`
  - Returns issue-specific runtime/debug details for the identified issue, including any information tracked that is useful for debugging.
  - Suggested response shape:

    ```json
    {
      "issue_identifier": "MT-649",
      "issue_id": "abc123",
      "status": "running",
      "workspace": {
        "path": "/tmp/sortie_workspaces/MT-649"
      },
      "attempts": {
        "restart_count": 1,
        "current_retry_attempt": 2
      },
      "running": {
        "issue_title": "Fix the flaky login test",
        "issue_url": "https://tracker.example.com/browse/MT-649",
        "issue_labels": ["bug", "backend"],
        "session_id": "thread-1-turn-1",
        "turn_count": 7,
        "state": "In Progress",
        "started_at": "2026-02-24T20:10:12Z",
        "last_event": "notification",
        "last_message": "Working on tests",
        "last_event_at": "2026-02-24T20:14:59Z",
        "tokens": {
          "input_tokens": 1200,
          "output_tokens": 800,
          "total_tokens": 2000,
          "cache_read_tokens": 400,
          "cache_write_tokens": 100
        },
        "api_request_count": 3,
        "tokens_measured": true,
        "usage_arrival": "incremental",
        "usage_attribution": "per_model",
        "tokens_pending": false,
        "api_requests_measured": true,
        "used_sessions": 2,
        "budget_sessions": 5,
        "issue_tokens_completed": 105000,
        "unmeasured_sessions": 0,
        "used_tokens": 107000,
        "used_tokens_complete": true,
        "budget_tokens": 500000,
        "estimated_cost_usd": 0.014595,
        "cost_status": "estimated",
        "issue_estimated_cost_usd": 0.235095,
        "issue_cost_status": "estimated"
      },
      "retry": null,
      "budget_exhausted": null,
      "parked": null,
      "logs": {
        "agent_session_logs": [
          {
            "label": "latest",
            "path": "/var/log/sortie/agent/MT-649/latest.log",
            "url": null
          }
        ]
      },
      "recent_events": [
        {
          "at": "2026-02-24T20:14:59Z",
          "event": "notification",
          "message": "Working on tests"
        }
      ],
      "last_error": null,
      "tracked": {}
    }
    ```

  - `status` is `"running"` when the issue has a running session, otherwise `"retrying"` when it has a pending retry, otherwise `"budget_exhausted"` when it is held out of dispatch by a per-issue budget ceiling, otherwise `"parked"` when it is parked; `budget_exhausted` in the response body carries the same record shape as the `budget_exhausted` array on `GET /api/v1/state` and is `null` when the issue is not in that set, and `parked` carries the record shape of the `parked` array and is `null` when the issue is not parked. An issue that is only parked answers with a `null` workspace, zero attempts, and a `null` `last_error`, as a budget-held one does. `last_error` is the retry row's error, so after the retry timer reschedules a retry it is the reschedule reason.
  - If the issue is unknown to the current in-memory state, return `404` with an error response (for example `{"error":{"code":"issue_not_found","message":"..."}}`); an issue held by a budget ceiling or parked is not unknown to it, so this case is distinct from those.

- `POST /api/v1/refresh`
  - Queues an immediate tracker poll + reconciliation cycle (best-effort trigger; implementations may coalesce repeated requests).
  - Suggested request body: empty body or `{}`.
  - Suggested response (`202 Accepted`) shape:

    ```json
    {
      "queued": true,
      "coalesced": false,
      "requested_at": "2026-02-24T20:15:30Z",
      "operations": ["poll", "reconcile"]
    }
    ```

API design notes:

- The JSON shapes above are the recommended baseline for interoperability and debugging ergonomics.
- Implementations may add fields, but should avoid breaking existing fields within a version.
- Running, retry, budget-exhausted, and parked rows name their issue in flat keys beside `issue_id`, `issue_identifier`, and `display_identifier`, never in a nested object, and the same rows inside `GET /api/v1/<issue_identifier>` carry them. `issue_title` and `issue_url` are the tracker's title and web link, each omitted when empty; a running row always carries a title, and a retry row lacks it only while the retry holds no read of its issue, and a parked row only while the park holds none. `issue_labels` is on running rows only and always present. Every such string is masked as Section 15.3 states.
- On a running row, `api_request_count` is `null` exactly when `api_requests_measured` is false, the five members of `tokens` are `null` exactly when `tokens_measured` is false, and `requests_by_model` is absent on the first condition and when the attribution names no model.
- Sortie deviates from the field-stability note above for those figures, narrowing each from an integer to a nullable one, because a consumer reading a number cannot tell a measured zero from an unmeasured one. A typed consumer is forced to handle the null; an untyped one, in a language where `null` coerces to `0` in arithmetic, is no worse off than it was.
- A running row, and the `running` object of the per-issue response, which serves the same keys, carries the issue's spend and limits in flat keys. The keys are additive; no existing key changes name, type, or meaning.
  - `used_sessions` counts the issue's completed sessions: the run-history rows that `agent.max_sessions` is compared with. The running session is not counted, and a recorded CI result counts as a session. `unmeasured_sessions` counts the completed sessions whose usage was never recorded, which the token figures leave out.
  - `issue_tokens_completed` is the summed total tokens of the completed sessions, the baseline the in-flight ceiling adds the running session's spend to. `used_tokens` is `issue_tokens_completed` plus the row's `tokens.total_tokens`, each counted as `0` when `null`: the figure the ceiling compares with `agent.max_tokens`.
  - `used_tokens_complete` is true exactly when `used_tokens` is the issue's whole spend: the completed sessions were read, none is unmeasured, none recorded an unaccounted turn, the running session is measured, and it has recorded no unaccounted turn. When it is false, `used_tokens` is a lower bound. It ignores `tokens_pending`, which keeps telling a consumer that the figure excludes a turn in flight.
  - `budget_tokens` and `budget_sessions` are `agent.max_tokens` and `agent.max_sessions` as of the latest poll tick. A limit key whose limit is not configured serves `null`, present and never `0` or omitted, because `0` computes as a ceiling of zero. The budget-exhausted record keeps its existing encoding, `0` for a ceiling that is not set.
  - `null` on a limit key means no ceiling; `null` on a usage key (`used_sessions`, `unmeasured_sessions`, `issue_tokens_completed`, `used_tokens`) means unknown, and it is `null` for the first three when no read of the issue's run history has succeeded for the run. Headroom is a limit key minus its usage key (`budget_tokens` minus `used_tokens`, `budget_sessions` minus `used_sessions`) and has no value when either is `null`. Token headroom turns negative when a run overshoots, because the ceiling stops a run at the first usage figure at or past it; the API serves no clamped headroom key.
  - A key never serves a sum of `0` that leaves spend out. `issue_tokens_completed` and `used_tokens` serve `null` instead, so a completed session that never reported usage, or a measured zero beside unknown spend, never reads as a spend of zero. A completed session is measured when it reported a usage figure or never entered an agent turn; a measured zero is exact and `issue_tokens_completed` serves it.
  - `estimated_cost_usd` is the running session's estimate under `token_rates` for its agent kind, and `cost_status` says why it is absent: `estimated`, `unpriced` when the session is measured and its kind has no complete rate entry, or `unmeasured` when the session has reported no usage. `issue_estimated_cost_usd` sums the estimates of the issue's priced parts, completed sessions included, and `issue_cost_status` is `estimated` when the sum covers the whole spend, `partial` when it is a lower bound because a part that spent tokens is left out, `unpriced` when no priced part spent tokens and a part that did has no rate, and `unmeasured` when `used_tokens` is `null` or no part is priced or unpriced. A running session that has reported zero tokens leaves nothing out, so a missing rate for its kind does not make a complete, otherwise priced issue `partial`.
  - Estimates are unrounded USD, priced at the rates the process started with. `used_tokens` is `null` only when `issue_cost_status` is `unmeasured`, and `issue_cost_status` is `estimated` only when `used_tokens_complete` is true.
  - A budget-exhausted record also carries `stopped_in_flight`, the count of the issue's sessions the token ceiling stopped mid-run, `null` exactly when the record's `used_tokens` is `null`.
- A retry row, on `GET /api/v1/state` and in the `retry` object of the per-issue response, says why the retry waits in these keys. They are additive; no existing key changes name or type.
  - `trigger` is the `sortie_retries_total` label of the schedule that created the row: `error`, `continuation`, `timer`, `stall`, or `ci_fix`. It is omitted when that schedule records none: a review, bot review, merge conflict, or label command continuation, which `reaction_kind` names.
  - `reaction_kind` is the retry's reaction kind, omitted when it carries none. An `error` or `stall` retry of a run a reaction dispatched carries that run's kind, and a `timer` row keeps the kind of the retry it replaced. `reaction_config_key` is present exactly when `reaction_kind` is, and names the `reactions` key of WORKFLOW.md the kind is configured under, so a client holds no mapping of its own: `ci` is `ci_failure`, `review` is `review_comments`, `bot-review` is `bot_review`, `merge` is `auto_merge`, `merge-conflict` is `merge_conflicts`, `label-review` and `label-fix` are `label_commands`, and `merge-completion` is `merge_completion`.
  - `rule_name` is the dispatch rule frozen with the retry, `default` for `dispatch.default`, omitted when the workflow-wide fallback routed the run. When the configuration in force no longer reaches that selection, the retry dispatches on another rule (Section 8.4, "Retry selection").
  - `stage_lineage` is always present and `null` unless a stage hop reached the run the retry follows. The object holds `chain_id`, `previous`, and `previous_outcome`, each omitted when empty: the run's stage chain, the rule whose hop reached the run, and that hop's outcome, `succeeded` or `no_change`. A chain's first stage, which no hop reached, reports `null`.
  - `paused` is always present, and true exactly when the retry timer last rescheduled the retry because its issue's state does not permit dispatching it, which is when `error` is `reaction retry paused by an open blocking issue` or `reaction retry paused by issue state`.
  - When the retry timer reschedules a retry, `error` carries the reason, one of `worker still running`, `retry issue fetch failed`, `issue without a state label`, `reaction retry paused by an open blocking issue`, `reaction retry paused by issue state`, `no available orchestrator slots`, `no available SSH hosts`, `retry agent kind unavailable`, or `retry agent settings refused`, and the trigger is `timer`. The error of the run before it stays in the logs and in that run's run history. The per-issue `last_error` copies `error`, so it carries the reason too.
- A parked record, on the `parked` array and in the `parked` object of the per-issue response, carries these keys.
  - `issue_id` and `issue_identifier` are always present. `display_identifier` is omitted when the tracker needs no qualified form.
  - `issue_title` and `issue_url` are the tracker's title and web link as read when the park was recorded or, for a park recorded or restored without a read, at the first poll tick on which the tracker answers for the issue. Each is omitted when empty and masked.
  - `reason` is `agent_blocked` or `handoff_absence`. `parked_state` is the tracker state observed when the park was recorded, or at the first observation after a park recorded before any read, omitted while unobserved and masked. `label` is the parking label the orchestrator applied, served unmasked because it is configuration, and `label_applied` is true once a read showed the label on the issue. `parked_at` is the time the park was recorded, in UTC.
  - The list is sorted by identifier and then issue ID, and `counts.parked` equals its length. An issue appears in every list whose set holds it: a parked issue with a review continuation queued is in `parked` and `retrying` both.
- `active_estimated_cost_usd`, an implementation extension beyond the baseline shape above, sums the estimated cost of running, measured sessions whose agent kind has a configured token rate; it is omitted when no running, measured session prices. `cost_unpriced_running` sits beside it and counts the running, measured sessions the sum leaves out for want of a rate. It is present, zero included, whenever `token_rates` holds an entry for any agent kind, complete or not, and omitted otherwise, so its presence alone tells a consumer whether cost pricing is configured at all. While `token_rates` holds an entry for any agent kind, `active_estimated_cost_usd` equals the sum of `estimated_cost_usd` over the running rows whose `cost_status` is `estimated` (and is absent when no row is), and `cost_unpriced_running` equals the count of rows whose `cost_status` is `unpriced`.
- `config` on `GET /api/v1/state` is the workflow shape in effect, so a client draws the rules, stages, and reactions the workflow has and assumes none it lacks. The key is additive; no existing key changes name, type, or meaning.
  - `config` reports the configuration promoted at the last successful load, kept when a reload fails. A setting whose change needs a restart reports what the process applies: every reaction except `ci_failure` is the one built at startup, and `token_rates` are the rates the process started with. So `config` names no reaction a reload added and keeps naming one a reload removed, until the restart that applies the edit.
  - The poll loop copies budgets, concurrency caps, and SSH hosts into its own state at its next tick, so for at most one poll interval after a reload `agent.max_tokens` and `agent.max_sessions` in `config` can lead a running row's `budget_tokens` and `budget_sessions`. A client shows a row's own budget keys on that row, and the `config` values where it describes the workflow.
  - A member whose setting imposes no ceiling serves `null`, present and never `0`: `agent.stall_timeout_ms`, `agent.max_sessions`, `agent.max_tokens`, `worker.max_concurrent_agents_per_host`, and the auto-merge `max_retries`, where `0` turns count-based escalation off. The other `max_retries` members serve the value held, `0` included: a merge-conflict `0` escalates on the first detection. `max_turns` is the limit a session applies, so a configured value below 1 serves as 1.
  - `tracker.in_progress_state`, `tracker.handoff_state`, and `tracker.no_change_state` are omitted when unset. A list member is `[]` when empty, and `max_concurrent_agents_by_state` is `{}`. Its keys are lowercase states and its caps are positive.
  - `dispatch.rules` lists the rules in configuration order, which is the order match rules are tried; a rule selected by its stage label is chosen ahead of them. `selected_by` is `stage` for a rule that carries a stage label, `catch_all` for a rule with no `match` block that takes every remaining issue, and `match` otherwise. `stage` appears only on a `stage` rule. `match` appears only on a `match` rule and holds each non-empty predicate as written: `labels`, `issue_type`, `title`, `identifier`, and `assignee` as lists, and `priority` as `op` with `value`, or `op` `in` with `values`. `name` and `next` are omitted when the rule has none. Each rule carries the `agent_kind` dispatch resolves for it.
  - `dispatch.default` holds `agent_kind` and `usage_arrival` when the `dispatch.default` block names an agent or a template, and is `null` otherwise. Running, retry, and run-history rows name this selection `default`; a row with no `rule_name` ran on the workflow-wide fallback, which `agent` describes. A catch-all rule takes every issue that no stage rule and no earlier match rule takes, so while one is configured neither `dispatch.default` nor the workflow-wide fallback selects an issue. `config` still serves both, and a client draws no dispatch path through them.
  - `dispatch.chains` lists each stage chain the `next` links form, as rule names in `next` order. A chain starts at a rule that carries `next` and that no `next` names, the chains follow the order of the rules that start them, and a walk stops at a rule without `next`, at a `next` that names no rule, or before a repeated rule. Chains that share a tail list it in each chain. A rule that carries no `next` and that no `next` names belongs to no chain. The list is `[]` when no rule carries `next`.
  - `dispatch.max_consecutive_hops` is the value WORKFLOW.md writes and `null` when the key is absent. `dispatch.hop_ceiling` is the limit a worker exit compares: the written value, or the larger of 10 and the hop count of the longest chain. With no chain no hop occurs, and `hop_ceiling` reports the ceiling a reload that adds `next` would apply.
  - `usage_arrival` appears on `agent` for the workflow-wide fallback, on `dispatch.default`, and on each rule. It is resolved the way a session resolves it at its start: the kind's declaration, the selection's settings block, and the launch mode, so one kind can report under one rule and report nothing under another. Its values are those of a running row: `incremental`, `turn_end`, `none`, or `""` for a kind the registry does not hold or that declares nothing. A selection reports token usage exactly when the value is `incremental` or `turn_end`. The launch mode is remote exactly when `worker.ssh_hosts` is non-empty, because with hosts configured every dispatch takes one, so a kind that reports nothing over SSH serves `none` while hosts are configured. `worker.ssh_hosts` lists each host once, in the order written, and is `[]` in local mode.
  - `reactions` holds a member per reaction kind the process applies, keyed as in WORKFLOW.md, and is `{}` when it applies none; its key order carries no meaning. A retry row's `reaction_config_key` names a member of it. `ci_failure` is present exactly while the process runs the CI check, which needs both a CI provider and a source-control adapter. Every other kind is present exactly when it was configured at startup, and `label_commands` when the review command or the fix command was. Each member carries the limits and timing its kind applies: `max_retries` for `ci_failure`, `merge_conflicts`, `auto_merge`, and `merge_completion`; `max_continuation_turns` for `review_comments` and `bot_review`; `debounce_ms` for `review_comments`; `bot_usernames` for `bot_review`; `strategy`, `require_ci`, and `delete_branch` for `auto_merge`; `target_state` for `merge_completion`; and `poll_interval_ms` for every kind except `ci_failure`.
  - `escalation` is the mode a reaction applies: `label`, `comment`, or `none`. `escalation_label` is present exactly when `escalation` is `label`, and holds the label the reaction applies: the configured value, or `needs-human` where the block writes none. `ci_failure` reads its block on every tick, so when the configuration in force carries no `ci_failure` block while the CI check still runs from startup, it serves `escalation` `none` and `max_retries` `0`, which is what the check applies. `label_commands` has no escalation members; it carries `review_label` while the review command is configured and `fix_label` while the fix command is, and both commands share its one `poll_interval_ms`.
  - `config.token_rates` is `null` when the process started with no entry, which is how the shape says `token_rates` is not configured. Otherwise it holds a member per entry keyed by the agent kind as written, with each `*_per_mtok` rate (`null` when not set) and `complete`, true exactly when `input_per_mtok` and `output_per_mtok` are both set, the condition under which a session is priced. An unset cache rate prices that class at the input rate, and `config` serves the configured `null`, never the substituted rate.
  - `config` is an allowlist of typed settings and holds no credential: it carries no tracker endpoint, key, or query filter, no agent command, hook, workspace or database path, or notification destination, no reaction extra key or triage script, and no settings block. Its configuration strings are served unmasked, as the parked `label` is, because they are configuration.
  - A row's `rule_name`, `stage_lineage.previous`, and `reaction_config_key` are those in force when its run was dispatched or the row was stored, while `config` follows the rule above, so each can name a member `config` does not hold. A reload that renames or removes a rule leaves running and retry rows naming it, as does a restart on the edited file for the retry rows it restores, and a row whose `rule_name` is `default` keeps it while `dispatch.default` serves `null`. A client then draws the row from the row's own keys.
- Endpoints should be read-only except for operational triggers like `/refresh`.
- Unsupported methods on defined routes should return `405 Method Not Allowed`.
- API errors should use a JSON envelope such as `{"error":{"code":"...","message":"..."}}`.
- If the dashboard is a client-side app, it should consume this API rather than duplicating state logic.

#### 13.7.3 Prometheus Metrics Endpoint (`/metrics`)

When the HTTP server is enabled, Sortie exposes a Prometheus exposition-format scrape endpoint at `/metrics`, backed by a dedicated registry so the process exports only its own series and none of the runtime's defaults. See ADR-0008. The endpoint is co-located with the JSON API and HTML dashboard on the same address and port; no separate configuration is required.

Implementation requirements:

- Use a dedicated `prometheus.Registry` (not the global default) to prevent pollution from unrelated collectors and to enable isolated test assertions.
- Register the handler via `promhttp.InstrumentMetricHandler(registry, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))`. `InstrumentMetricHandler` wraps `HandlerFor` and registers `promhttp_metric_handler_*` counters on the same dedicated registry automatically, ensuring scrape self-instrumentation appears in scrape output rather than landing silently on the global default.
- Register standard Go runtime and process collectors (`collectors.NewGoCollector`, `collectors.NewProcessCollector`) on the dedicated registry so that `go_*` and `process_*` metrics appear in scrape output alongside Sortie's own metrics.

Defined metrics (label sets and buckets are specified here; see ADR-0008 for historical rationale):

| Name | Type | Description |
|------|------|-------------|
| `sortie_sessions_running` | Gauge | Number of agent sessions currently executing. |
| `sortie_sessions_retrying` | Gauge | Number of issues in the retry queue. |
| `sortie_slots_available` | Gauge | Remaining dispatch capacity under current concurrency limits. |
| `sortie_active_sessions_elapsed_seconds` | Gauge | Cumulative wall-clock elapsed time across all currently running sessions. |
| `sortie_ssh_host_usage{host}` | Gauge | Current session count per remote SSH host, partitioned by host. Recomputed from the host-usage map after each tick, worker exit, and retry-timer event; hosts removed by config reload decrement to zero rather than freezing at their last value. |
| `sortie_tokens_total{type}` | Counter | Tokens consumed, partitioned by type (`input`, `output`, `cache_read`, `cache_write`). `cache_read` and `cache_write` are subsets of `input`, so summing across every `type` double-counts them. |
| `sortie_agent_runtime_seconds_total` | Counter | Cumulative agent-session wall-clock time for completed sessions. |
| `sortie_dispatches_total{outcome}` | Counter | Dispatch attempts, partitioned by outcome (`success`, `error`). |
| `sortie_worker_exits_total{exit_type}` | Counter | Worker exits, partitioned by exit type (`normal`, `error`, `cancelled`, `soft_stop`). A soft-stop exit reports `soft_stop` rather than the exit kind it would otherwise map to. |
| `sortie_retries_total{trigger}` | Counter | Retry schedule events, partitioned by trigger (`error`, `continuation`, `timer`, `stall`, `ci_fix`). A review, bot review, merge conflict, or label command continuation records no trigger. |
| `sortie_reconciliation_actions_total{action}` | Counter | Reconciliation outcomes per issue, partitioned by action (`stop`, `cleanup`, `keep`, `sweep_cleanup`, `sweep_expired`). |
| `sortie_poll_cycles_total{result}` | Counter | Poll tick completions, partitioned by result (`success`, `error`, `skipped`). |
| `sortie_tracker_requests_total{operation,result}` | Counter | Tracker adapter API calls, partitioned by operation (`fetch_candidates`, `fetch_issue`, `fetch_by_states`, `fetch_states_by_ids`, `fetch_states_by_identifiers`, `fetch_comments`, `fetch_blockers`, `transition`, `comment`, `add_label`, `remove_label`) and result (`success`, `error`). |
| `sortie_tracker_comments_total{lifecycle,result}` | Counter | Tracker comment attempts, partitioned by lifecycle point (`dispatch`, `completion`, `failure`, `budget_hold`, among others the project may add) and result (`success`, `error`). Recorded from the `tracker_comment` destination's outcome, and only for an event that destination received; an event routed elsewhere records nothing. |
| `sortie_handoff_transitions_total{result}` | Counter | Handoff-state dispositions, partitioned by result (`success`, `error`, `skipped`, `withheld`). `withheld` means the handoff-evidence policy selected the absence failure path before any transition attempt, distinguishing it from an ordinary worker failure; a withholding verdict whose own verification read then finds the issue terminal does not increment this label, it increments `skipped` instead. `skipped` names three causes and does not distinguish them: the issue was no longer in an active state at worker exit, the issue was already reported terminal and the handoff write was suppressed (Section 11.5), or a withheld verdict's own verification read reported the issue terminal and suppressed the absence-failure recording (Section 14.2). All four values are counted only when `tracker.handoff_state` is configured. |
| `sortie_dispatch_transitions_total{result}` | Counter | Dispatch-time in-progress transition attempts, partitioned by result (`success`, `error`, `skipped`). `skipped` indicates the issue was already in the target state. |
| `sortie_issue_parks_total{reason}` | Counter | Issue park events, partitioned by reason (`agent_blocked`, `handoff_absence`). Incremented once per park, whichever trigger produced it. |
| `sortie_dispatch_rule_match_total{layer,rule}` | Counter | Dispatch rule match outcomes, partitioned by resolution layer (`rule`, `default`, `fallback`) and matched rule name. Empty rule names report as `<none>` to bound label cardinality. |
| `sortie_candidate_holds_total{reason}` | Counter | `IncCandidateHolds`. Candidates the dispatch loop held, partitioned by reason (`blocked_by`, `blockers_unresolved`, `blockers_not_read`, `blockers_incomplete`). Incremented once per held candidate; never incremented for a candidate rejected by an eligibility or capacity gate, and never incremented a second time for the pass-level `blocker reads halted for this tick` ERROR that accompanies a run of `blockers_unresolved` holds. |
| `sortie_budget_exhaustions_total{reason}` | Counter | `IncBudgetExhaustions`. Issue entries into the per-issue budget-exhausted set, partitioned by reason (`token_budget`, `session_budget`, open to a later value). Incremented once per hold, from whichever lane discovered it; never incremented on a tick that merely re-observes an already-announced hold. |
| `sortie_budget_exhausted_issues{reason}` | Gauge | `SetBudgetExhaustedIssues`. Issues currently held out of dispatch, partitioned by reason. Recomputed from the full budget-exhausted set on every gauge update, including every declared reason at zero when nothing is held, so a reason that clears reports zero rather than freezing at its last value. |
| `sortie_runs_stopped_by_budget_total{reason}` | Counter | `IncRunsStoppedByBudget`. Runs recorded `budget_stopped`, partitioned by reason. Only `token_budget` is reachable: a session is a discrete unit, so the session ceiling cannot be crossed part-way through one. Incremented once per run whose exit is recorded `budget_stopped`, at that exit. |
| `sortie_tool_calls_total{tool,result}` | Counter | Agent tool call completions, partitioned by tool name and result (`success`, `error`). |
| `sortie_ci_status_checks_total{result}` | Counter | CI status check outcomes, partitioned by result (`passing`, `pending`, `failing`, `error`). |
| `sortie_ci_escalations_total{action}` | Counter | CI escalation actions when fix retries are exhausted, partitioned by action (`label`, `comment`, `none`, `error`). Under `escalation: comment` or `none`, `comment` records an accepted `tracker_comment` write, `error` a failed one, and `none` an escalation event that `tracker_comment` did not receive. |
| `sortie_reactions_auto_merge_total{result}` | Counter | Auto-merge reaction outcomes, partitioned by result (`merged`, `error`, `escalated`). |
| `sortie_review_checks_total{result}` | Counter | Review comment check outcomes, partitioned by result (`dispatched`, `error`, `skipped`). |
| `sortie_review_escalations_total{action}` | Counter | Review escalation actions when continuation turns are exhausted, partitioned by action (`label`, `comment`, `none`, `error`), with the same meaning of `comment`, `none`, and `error` as `sortie_ci_escalations_total`. |
| `sortie_self_review_iterations_total{verdict}` | Counter | Self-review iterations, partitioned by per-iteration verdict (`pass`, `iterate`, `none`). `none` indicates a missing or unparseable verdict for that iteration. |
| `sortie_self_review_sessions_total{final_verdict}` | Counter | Self-review sessions, partitioned by final verdict (`pass`, `iterate`, `none`). |
| `sortie_self_review_cap_reached_total` | Counter | Self-review sessions that reached the iteration cap without a passing verdict. |
| `sortie_poll_duration_seconds` | Histogram | Wall-clock time per poll cycle; buckets via `ExponentialBuckets(0.1, 2, 10)` (0.1 s–51.2 s). |
| `sortie_worker_duration_seconds{exit_type}` | Histogram | Worker session wall-clock time; buckets via `ExponentialBuckets(10, 2, 12)` (10 s–5.7 h). `exit_type` carries the same values as `sortie_worker_exits_total`. |
| `sortie_self_review_verification_duration_seconds{command}` | Histogram | Per-command verification duration during self-review; buckets via `ExponentialBuckets(10, 2, 12)` (10 s–5.7 h). The `command` label is truncated to the first 64 characters. |
| `sortie_build_info{version,go_version}` | Gauge | Always `1`; carries build metadata as labels. |

### 13.8 Self-Review Observability on an Admitting Status Signal

A run that ends on the `needs-human-review` status signal, or on a `no-change-needed` declaration, and passes through the self-review phase records review metadata on `run_history` exactly as a run that reached the phase by exhausting the turn budget does: the JSON document under `review_metadata`, described in Section 19, carries a value rather than null for this population.

The `after_run` hook on that path receives the run's real `SORTIE_SELF_REVIEW_STATUS` value (`passed`, `cap_reached`, or `error`) instead of the `disabled` value it receives when self-review is not configured or the run did not reach the phase. `SORTIE_SELF_REVIEW_SUMMARY_PATH` is populated on the same terms as on any other path that ran the phase. A declared run supplies both values on these same terms whether the phase confirms the declaration or retracts it: the retraction is a separate outcome recorded in the run's own log, not a change to what this section populates.
