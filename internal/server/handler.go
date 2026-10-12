package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/sortie-ai/sortie/internal/orchestrator"
)

// stateResponse is the JSON wire format for GET /api/v1/state.
type stateResponse struct {
	GeneratedAt            time.Time                        `json:"generated_at"`
	Counts                 stateCounts                      `json:"counts"`
	Running                []runningEntryResponse           `json:"running"`
	Retrying               []retryEntryResponse             `json:"retrying"`
	BudgetExhausted        []budgetExhaustedResponse        `json:"budget_exhausted"`
	Parked                 []parkedEntryResponse            `json:"parked"`
	AgentTotals            orchestrator.SnapshotAgentTotals `json:"agent_totals"`
	RateLimits             map[string]any                   `json:"rate_limits"`
	ActiveEstimatedCostUSD *float64                         `json:"active_estimated_cost_usd,omitempty"`

	// CostUnpricedRunning counts running, measured sessions
	// ActiveEstimatedCostUSD excludes for lacking a configured rate.
	// Present, zero included, when any agent kind has a rate
	// configured; omitted otherwise.
	CostUnpricedRunning *int `json:"cost_unpriced_running,omitempty"`
}

type stateCounts struct {
	Running         int `json:"running"`
	Retrying        int `json:"retrying"`
	BudgetExhausted int `json:"budget_exhausted"`
	Parked          int `json:"parked"`
}

type runningEntryResponse struct {
	IssueID             string         `json:"issue_id"`
	IssueIdentifier     string         `json:"issue_identifier"`
	DisplayIdentifier   string         `json:"display_identifier,omitempty"`
	IssueTitle          string         `json:"issue_title,omitempty"`
	IssueURL            string         `json:"issue_url,omitempty"`
	IssueLabels         []string       `json:"issue_labels"`
	State               string         `json:"state"`
	SessionID           string         `json:"session_id"`
	TurnCount           int            `json:"turn_count"`
	SelfReviewActive    bool           `json:"self_review_active"`
	SelfReviewIteration int            `json:"self_review_iteration"`
	LastEvent           string         `json:"last_event"`
	LastMessage         string         `json:"last_message"`
	StartedAt           time.Time      `json:"started_at"`
	LastEventAt         time.Time      `json:"last_event_at"`
	WorkspacePath       string         `json:"workspace_path"`
	SSHHost             string         `json:"ssh_host,omitempty"`
	Tokens              tokenInfo      `json:"tokens"`
	ModelName           string         `json:"model_name,omitempty"`
	RuleName            string         `json:"rule_name,omitempty"`
	ConfiguredModel     string         `json:"configured_model,omitempty"`
	ConfiguredEffort    string         `json:"configured_effort,omitempty"`
	WorkflowFile        string         `json:"workflow_file,omitempty"`
	AgentKind           string         `json:"agent_kind,omitempty"`
	APIRequestCount     *int           `json:"api_request_count"`
	RequestsByModel     map[string]int `json:"requests_by_model,omitempty"`
	ToolTimePercent     *float64       `json:"tool_time_percent"`
	APITimePercent      *float64       `json:"api_time_percent"`
	TokensMeasured      bool           `json:"tokens_measured"`

	// UsageArrival and UsageAttribution mirror
	// [orchestrator.SnapshotRunningEntry]'s frozen pair verbatim, as
	// their string values. TokensPending derives from that frozen
	// pair; APIRequestsMeasured is the session-resolved verdict the
	// snapshot carries, and it is true exactly when APIRequestCount
	// is non-nil.
	UsageArrival        string `json:"usage_arrival"`
	UsageAttribution    string `json:"usage_attribution"`
	TokensPending       bool   `json:"tokens_pending"`
	TokensAwaited       bool   `json:"tokens_awaited"`
	APIRequestsMeasured bool   `json:"api_requests_measured"`

	// The issue-level figures below are nil when unknown. The two budget
	// keys are nil when no limit is configured and are never omitted.
	UsedSessions          *int     `json:"used_sessions"`
	BudgetSessions        *int     `json:"budget_sessions"`
	IssueTokensCompleted  *int64   `json:"issue_tokens_completed"`
	UnmeasuredSessions    *int     `json:"unmeasured_sessions"`
	UsedTokens            *int64   `json:"used_tokens"`
	UsedTokensComplete    bool     `json:"used_tokens_complete"`
	BudgetTokens          *int64   `json:"budget_tokens"`
	EstimatedCostUSD      *float64 `json:"estimated_cost_usd"`
	CostStatus            string   `json:"cost_status"`
	IssueEstimatedCostUSD *float64 `json:"issue_estimated_cost_usd"`
	IssueCostStatus       string   `json:"issue_cost_status"`
}

// tokenInfo carries a running row's five token figures. The members
// are nil together, exactly when the row's TokensMeasured is false,
// and a nil member serializes as JSON null rather than being omitted,
// so a consumer can tell a measured zero from an unmeasured figure.
type tokenInfo struct {
	InputTokens      *int64 `json:"input_tokens"`
	OutputTokens     *int64 `json:"output_tokens"`
	TotalTokens      *int64 `json:"total_tokens"`
	CacheReadTokens  *int64 `json:"cache_read_tokens"`
	CacheWriteTokens *int64 `json:"cache_write_tokens"`
}

type retryEntryResponse struct {
	IssueID           string    `json:"issue_id"`
	IssueIdentifier   string    `json:"issue_identifier"`
	DisplayIdentifier string    `json:"display_identifier,omitempty"`
	IssueTitle        string    `json:"issue_title,omitempty"`
	IssueURL          string    `json:"issue_url,omitempty"`
	Attempt           int       `json:"attempt"`
	DueAt             time.Time `json:"due_at"`
	Error             string    `json:"error"`

	Trigger           string                `json:"trigger,omitempty"`
	ReactionKind      string                `json:"reaction_kind,omitempty"`
	ReactionConfigKey string                `json:"reaction_config_key,omitempty"`
	RuleName          string                `json:"rule_name,omitempty"`
	StageLineage      *stageLineageResponse `json:"stage_lineage"`
	Paused            bool                  `json:"paused"`
}

// stageLineageResponse is the stage hop that reached the run a retry
// follows. It is nil in the wire format unless a hop reached that run.
type stageLineageResponse struct {
	ChainID         string `json:"chain_id,omitempty"`
	Previous        string `json:"previous,omitempty"`
	PreviousOutcome string `json:"previous_outcome,omitempty"`
}

// parkedEntryResponse is the JSON wire format for one issue held out of
// primary dispatch until a person acts on it. Used by GET /api/v1/state and
// GET /api/v1/{identifier}.
type parkedEntryResponse struct {
	IssueID           string    `json:"issue_id"`
	IssueIdentifier   string    `json:"issue_identifier"`
	DisplayIdentifier string    `json:"display_identifier,omitempty"`
	IssueTitle        string    `json:"issue_title,omitempty"`
	IssueURL          string    `json:"issue_url,omitempty"`
	Reason            string    `json:"reason"`
	ParkedState       string    `json:"parked_state,omitempty"`
	Label             string    `json:"label"`
	LabelApplied      bool      `json:"label_applied"`
	ParkedAt          time.Time `json:"parked_at"`
}

// budgetExhaustedResponse is the JSON wire format for one issue held out
// of dispatch by a budget ceiling. Used by GET /api/v1/state and
// GET /api/v1/{identifier}.
type budgetExhaustedResponse struct {
	IssueID            string    `json:"issue_id"`
	IssueIdentifier    string    `json:"issue_identifier"`
	DisplayIdentifier  string    `json:"display_identifier,omitempty"`
	IssueTitle         string    `json:"issue_title,omitempty"`
	IssueURL           string    `json:"issue_url,omitempty"`
	Reason             string    `json:"reason"`
	UsedSessions       int       `json:"used_sessions"`
	BudgetSessions     int       `json:"budget_sessions"`
	UsedTokens         *int64    `json:"used_tokens"`
	BudgetTokens       int64     `json:"budget_tokens"`
	UnmeasuredSessions *int      `json:"unmeasured_sessions"`
	StoppedInFlight    *int      `json:"stopped_in_flight"`
	ExhaustedAt        time.Time `json:"exhausted_at"`
}

type issueDetailResponse struct {
	IssueIdentifier string                   `json:"issue_identifier"`
	IssueID         string                   `json:"issue_id"`
	Status          string                   `json:"status"`
	Workspace       *workspaceInfo           `json:"workspace"`
	Attempts        *attemptsInfo            `json:"attempts"`
	Running         *runningEntryResponse    `json:"running"`
	Retry           *retryEntryResponse      `json:"retry"`
	BudgetExhausted *budgetExhaustedResponse `json:"budget_exhausted"`
	Parked          *parkedEntryResponse     `json:"parked"`
	RecentEvents    []any                    `json:"recent_events"`
	LastError       *string                  `json:"last_error"`
	Tracked         map[string]any           `json:"tracked"`
}

type workspaceInfo struct {
	Path string `json:"path"`
}

type attemptsInfo struct {
	RestartCount        int `json:"restart_count"`
	CurrentRetryAttempt int `json:"current_retry_attempt"`
}

type refreshResponse struct {
	Queued      bool      `json:"queued"`
	Coalesced   bool      `json:"coalesced"`
	RequestedAt time.Time `json:"requested_at"`
	Operations  []string  `json:"operations"`
}

type errorResponse struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func toRunningEntryResponse(e orchestrator.SnapshotRunningEntry, nowArgs ...time.Time) runningEntryResponse {
	resp := runningEntryResponse{
		IssueID:             e.IssueID,
		IssueIdentifier:     e.Identifier,
		DisplayIdentifier:   e.DisplayID,
		IssueTitle:          e.IssueTitle,
		IssueURL:            e.IssueURL,
		IssueLabels:         e.IssueLabels,
		State:               e.State,
		SessionID:           e.SessionID,
		TurnCount:           e.TurnCount,
		SelfReviewActive:    e.SelfReviewActive,
		SelfReviewIteration: e.SelfReviewIteration,
		LastEvent:           string(e.LastAgentEvent),
		LastMessage:         e.LastAgentMessage,
		StartedAt:           e.StartedAt.UTC(),
		LastEventAt:         e.LastAgentTimestamp.UTC(),
		WorkspacePath:       e.WorkspacePath,
		SSHHost:             e.SSHHost,
		ModelName:           e.ModelName,
		RuleName:            e.RuleName,
		ConfiguredModel:     e.ConfiguredModel,
		ConfiguredEffort:    e.ConfiguredEffort,
		WorkflowFile:        e.WorkflowFile,
		AgentKind:           e.AgentKind,
		RequestsByModel:     e.RequestsByModel,
		TokensMeasured:      e.UsageMeasured,
		UsageArrival:        string(e.UsageArrival),
		UsageAttribution:    string(e.UsageAttribution),
		TokensPending:       e.TokensPending,
		TokensAwaited:       e.TokensAwaited,
		APIRequestsMeasured: e.APIRequestsMeasured,
	}

	// A figure no measurement produced is null in the place it already
	// occupies, beside the flag that explains it.
	if e.UsageMeasured {
		resp.Tokens = tokenInfo{
			InputTokens:      &e.AgentInputTokens,
			OutputTokens:     &e.AgentOutputTokens,
			TotalTokens:      &e.AgentTotalTokens,
			CacheReadTokens:  &e.CacheReadTokens,
			CacheWriteTokens: &e.CacheWriteTokens,
		}
	}
	if e.APIRequestsMeasured {
		resp.APIRequestCount = &e.APIRequestCount
	}

	if len(nowArgs) > 0 && !e.StartedAt.IsZero() {
		now := nowArgs[0]
		elapsedMs := now.Sub(e.StartedAt).Milliseconds()
		if elapsedMs > 0 && e.ToolTimeMs > 0 {
			resp.ToolTimePercent = new(float64(e.ToolTimeMs) / float64(elapsedMs) * 100.0)
		}
		if elapsedMs > 0 && e.APITimeMs > 0 {
			resp.APITimePercent = new(float64(e.APITimeMs) / float64(elapsedMs) * 100.0)
		}
	}

	return resp
}

// toServedRunningEntry converts a running row for both endpoints, adding
// the issue-level figures that depend on the snapshot's limits and on rates.
func toServedRunningEntry(e orchestrator.SnapshotRunningEntry, snap orchestrator.RuntimeSnapshotResult, rates TokenRates) runningEntryResponse {
	resp := toRunningEntryResponse(e, snap.GeneratedAt)

	var completed int64
	var completedIncomplete bool
	if usage := e.IssueUsage; usage != nil {
		completed = usage.TotalTokens
		completedIncomplete = issueSpendIncomplete(usage)
		resp.UsedSessions = new(usage.Sessions)
		resp.UnmeasuredSessions = new(usage.UnmeasuredSessions)
	}

	known := completed
	if e.UsageMeasured {
		known += e.AgentTotalTokens
	}
	resp.UsedTokensComplete = e.IssueUsage != nil && !completedIncomplete && e.UsageMeasured && e.UnaccountedTurns == 0

	// A zero that leaves spend out would stand in for spend Sortie does not
	// know, so it is served as null.
	if e.IssueUsage != nil && (completed != 0 || !completedIncomplete) {
		resp.IssueTokensCompleted = new(completed)
	}
	if known != 0 || resp.UsedTokensComplete {
		resp.UsedTokens = new(known)
	}

	if snap.MaxTokens > 0 {
		resp.BudgetTokens = new(int64(snap.MaxTokens))
	}
	if snap.MaxSessions > 0 {
		resp.BudgetSessions = new(snap.MaxSessions)
	}

	resp.EstimatedCostUSD, resp.CostStatus = sessionCostOf(e, rates)
	resp.IssueEstimatedCostUSD, resp.IssueCostStatus = issueCostOf(e, resp.UsedTokens, resp.UsedTokensComplete, resp.EstimatedCostUSD, resp.CostStatus, rates)
	return resp
}

// issueSpendIncomplete reports whether usage.TotalTokens leaves spend out,
// by the same condition the persistence layer applies to its own figure.
func issueSpendIncomplete(usage *orchestrator.SnapshotIssueUsage) bool {
	return usage.UnmeasuredSessions > 0 || usage.UnaccountedTurns > 0
}

func toRetryEntryResponse(e orchestrator.SnapshotRetryEntry) retryEntryResponse {
	var lineage *stageLineageResponse
	if e.StageLineage.PreviousRule != "" {
		lineage = &stageLineageResponse{
			ChainID:         e.StageLineage.ChainID,
			Previous:        e.StageLineage.PreviousRule,
			PreviousOutcome: e.StageLineage.PreviousOutcome,
		}
	}
	return retryEntryResponse{
		IssueID:           e.IssueID,
		IssueIdentifier:   e.Identifier,
		DisplayIdentifier: e.DisplayID,
		IssueTitle:        e.IssueTitle,
		IssueURL:          e.IssueURL,
		Attempt:           e.Attempt,
		DueAt:             time.UnixMilli(e.DueAtMS).UTC(),
		Error:             e.Error,

		Trigger:           e.Trigger,
		ReactionKind:      e.ReactionKind,
		ReactionConfigKey: e.ReactionConfigKey,
		RuleName:          e.RuleName,
		StageLineage:      lineage,
		Paused:            e.Paused,
	}
}

func toParkedEntryResponse(e orchestrator.SnapshotParkedEntry) parkedEntryResponse {
	return parkedEntryResponse{
		IssueID:           e.IssueID,
		IssueIdentifier:   e.Identifier,
		DisplayIdentifier: e.DisplayID,
		IssueTitle:        e.IssueTitle,
		IssueURL:          e.IssueURL,
		Reason:            e.Reason,
		ParkedState:       e.ParkedState,
		Label:             e.Label,
		LabelApplied:      e.LabelApplied,
		ParkedAt:          e.ParkedAt.UTC(),
	}
}

func toBudgetExhaustedResponse(e orchestrator.SnapshotBudgetEntry) budgetExhaustedResponse {
	return budgetExhaustedResponse{
		IssueID:            e.IssueID,
		IssueIdentifier:    e.Identifier,
		DisplayIdentifier:  e.DisplayID,
		IssueTitle:         e.IssueTitle,
		IssueURL:           e.IssueURL,
		Reason:             e.Reason,
		UsedSessions:       e.UsedSessions,
		BudgetSessions:     e.BudgetSessions,
		UsedTokens:         e.UsedTokens,
		BudgetTokens:       e.BudgetTokens,
		UnmeasuredSessions: e.UnmeasuredSessions,
		StoppedInFlight:    e.StoppedInFlight,
		ExhaustedAt:        e.ExhaustedAt,
	}
}

func toStateResponse(snap orchestrator.RuntimeSnapshotResult, tokenRates TokenRates) stateResponse {
	running := make([]runningEntryResponse, 0, len(snap.Running))
	for _, e := range snap.Running {
		running = append(running, toServedRunningEntry(e, snap, tokenRates))
	}

	retrying := make([]retryEntryResponse, 0, len(snap.Retrying))
	for _, e := range snap.Retrying {
		retrying = append(retrying, toRetryEntryResponse(e))
	}

	budgetExhausted := make([]budgetExhaustedResponse, 0, len(snap.BudgetExhausted))
	for _, e := range snap.BudgetExhausted {
		budgetExhausted = append(budgetExhausted, toBudgetExhaustedResponse(e))
	}

	parked := make([]parkedEntryResponse, 0, len(snap.Parked))
	for _, e := range snap.Parked {
		parked = append(parked, toParkedEntryResponse(e))
	}

	rateLimits := snap.RateLimits
	if rateLimits == nil {
		rateLimits = map[string]any{}
	}

	resp := stateResponse{
		GeneratedAt: snap.GeneratedAt.UTC(),
		Counts: stateCounts{
			Running:         len(running),
			Retrying:        len(retrying),
			BudgetExhausted: len(budgetExhausted),
			Parked:          len(parked),
		},
		Running:         running,
		Retrying:        retrying,
		BudgetExhausted: budgetExhausted,
		Parked:          parked,
		AgentTotals:     snap.AgentTotals,
		RateLimits:      rateLimits,
	}

	if len(tokenRates) > 0 {
		total, anySet, unpriced := activeCostTotal(snap.Running, tokenRates)
		if anySet {
			resp.ActiveEstimatedCostUSD = &total
		}
		resp.CostUnpricedRunning = &unpriced
	}

	return resp
}

func writeJSON(w http.ResponseWriter, logger *slog.Logger, status int, v any) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(v); err != nil {
		logger.Error("failed to marshal JSON response", slog.Any("error", err))
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"code":"internal_error","message":"failed to encode response"}}` + "\n"))
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if _, err := w.Write(buf.Bytes()); err != nil {
		logger.Warn("failed to write JSON response", slog.Any("error", err))
	}
}

func writeErrorJSON(w http.ResponseWriter, logger *slog.Logger, status int, code string, message string) {
	writeJSON(w, logger, status, errorResponse{
		Error: errorDetail{Code: code, Message: message},
	})
}

func (s *Server) routeState(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.methodNotAllowed(w, r, http.MethodGet)
		return
	}
	s.handleState(w, r)
}

func (s *Server) routeRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		s.methodNotAllowed(w, r, http.MethodPost)
		return
	}
	s.handleRefresh(w, r)
}

func (s *Server) routeIssueDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s.methodNotAllowed(w, r, http.MethodGet)
		return
	}
	s.handleIssueDetail(w, r)
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	snap, err := s.snapshotFn()
	if err != nil {
		s.logger.Warn("snapshot request failed",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Any("error", err),
		)
		writeErrorJSON(w, s.logger, http.StatusServiceUnavailable,
			"snapshot_unavailable",
			"orchestrator state snapshot unavailable")
		return
	}

	resp := toStateResponse(snap, s.tokenRates)
	writeJSON(w, s.logger, http.StatusOK, resp)

	s.logger.Debug("request served",
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.Int("status", http.StatusOK),
		slog.Duration("duration", time.Since(start)),
	)
}

func (s *Server) handleIssueDetail(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	identifier := r.PathValue("identifier")
	if identifier == "" {
		writeErrorJSON(w, s.logger, http.StatusNotFound,
			"issue_not_found",
			"issue identifier is empty")
		return
	}

	snap, err := s.snapshotFn()
	if err != nil {
		s.logger.Warn("snapshot request failed",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Any("error", err),
		)
		writeErrorJSON(w, s.logger, http.StatusServiceUnavailable,
			"snapshot_unavailable",
			"orchestrator state snapshot unavailable")
		return
	}

	resp := buildIssueDetail(identifier, snap, s.tokenRates)
	if resp == nil {
		writeErrorJSON(w, s.logger, http.StatusNotFound,
			"issue_not_found",
			fmt.Sprintf("issue identifier %q not found in current state", identifier))
		return
	}

	writeJSON(w, s.logger, http.StatusOK, resp)

	s.logger.Debug("request served",
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.Int("status", http.StatusOK),
		slog.Duration("duration", time.Since(start)),
	)
}

func (s *Server) handleRefresh(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	if s.drainingFlag.Load() {
		resp := refreshResponse{
			Queued:      false,
			Coalesced:   false,
			RequestedAt: time.Now().UTC(),
			Operations:  []string{},
		}
		writeJSON(w, s.logger, http.StatusConflict, resp)

		s.logger.Debug("request served",
			slog.String("method", r.Method),
			slog.String("path", r.URL.Path),
			slog.Int("status", http.StatusConflict),
			slog.Duration("duration", time.Since(start)),
		)
		return
	}

	accepted := s.refreshFn()

	resp := refreshResponse{
		Queued:      true,
		Coalesced:   !accepted,
		RequestedAt: time.Now().UTC(),
		Operations:  []string{"poll", "reconcile"},
	}

	writeJSON(w, s.logger, http.StatusAccepted, resp)

	s.logger.Debug("request served",
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
		slog.Int("status", http.StatusAccepted),
		slog.Duration("duration", time.Since(start)),
	)
}

func (s *Server) methodNotAllowed(w http.ResponseWriter, r *http.Request, allowed ...string) {
	s.logger.Warn("method not allowed",
		slog.String("method", r.Method),
		slog.String("path", r.URL.Path),
	)
	if len(allowed) > 0 {
		w.Header().Set("Allow", strings.Join(allowed, ", "))
	}
	writeErrorJSON(w, s.logger, http.StatusMethodNotAllowed,
		"method_not_allowed",
		fmt.Sprintf("method %s is not allowed on this endpoint", r.Method))
}

func buildIssueDetail(identifier string, snap orchestrator.RuntimeSnapshotResult, rates TokenRates) *issueDetailResponse {
	var runEntry *runningEntryResponse
	var retryEntry *retryEntryResponse
	var budgetEntry *budgetExhaustedResponse
	var parkedEntry *parkedEntryResponse

	for _, e := range snap.Running {
		if e.Identifier == identifier {
			runEntry = new(toServedRunningEntry(e, snap, rates))
			break
		}
	}

	for _, e := range snap.Retrying {
		if e.Identifier == identifier {
			retryEntry = new(toRetryEntryResponse(e))
			break
		}
	}

	for _, e := range snap.BudgetExhausted {
		if e.Identifier == identifier {
			budgetEntry = new(toBudgetExhaustedResponse(e))
			break
		}
	}

	for _, e := range snap.Parked {
		if e.Identifier == identifier {
			parkedEntry = new(toParkedEntryResponse(e))
			break
		}
	}

	if runEntry == nil && retryEntry == nil && budgetEntry == nil && parkedEntry == nil {
		return nil
	}

	var status string
	var issueID string

	switch {
	case runEntry != nil:
		status = "running"
		issueID = runEntry.IssueID
	case retryEntry != nil:
		status = "retrying"
		issueID = retryEntry.IssueID
	case budgetEntry != nil:
		status = "budget_exhausted"
		issueID = budgetEntry.IssueID
	case parkedEntry != nil:
		status = "parked"
		issueID = parkedEntry.IssueID
	default:
		status = "unknown"
	}

	var ws *workspaceInfo
	if runEntry != nil && runEntry.WorkspacePath != "" {
		ws = &workspaceInfo{Path: runEntry.WorkspacePath}
	}

	var attempts *attemptsInfo
	if retryEntry != nil {
		restartCount := retryEntry.Attempt
		if restartCount > 0 {
			restartCount--
		}
		attempts = &attemptsInfo{
			RestartCount:        restartCount,
			CurrentRetryAttempt: retryEntry.Attempt,
		}
	} else {
		attempts = &attemptsInfo{}
	}

	var lastError *string
	if retryEntry != nil && retryEntry.Error != "" {
		lastError = &retryEntry.Error
	}

	return &issueDetailResponse{
		IssueIdentifier: identifier,
		IssueID:         issueID,
		Status:          status,
		Workspace:       ws,
		Attempts:        attempts,
		Running:         runEntry,
		Retry:           retryEntry,
		BudgetExhausted: budgetEntry,
		Parked:          parkedEntry,
		RecentEvents:    []any{},
		LastError:       lastError,
		Tracked:         map[string]any{},
	}
}

// healthResponse is the JSON wire format for GET /livez.
type healthResponse struct {
	Status string `json:"status"`
}

// readyResponse is the JSON wire format for GET /readyz.
type readyResponse struct {
	Status        string            `json:"status"`
	Version       string            `json:"version"`
	UptimeSeconds float64           `json:"uptime_seconds"`
	Checks        map[string]string `json:"checks"`
}

func (s *Server) handleLivez(w http.ResponseWriter, _ *http.Request) {
	if s.drainingFlag.Load() {
		writeJSON(w, s.logger, http.StatusServiceUnavailable, healthResponse{Status: "fail"})
		return
	}
	writeJSON(w, s.logger, http.StatusOK, healthResponse{Status: "pass"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	var uptime float64
	if !s.startedAt.IsZero() {
		d := max(time.Since(s.startedAt), 0)
		uptime = d.Seconds()
	}

	if s.drainingFlag.Load() {
		writeJSON(w, s.logger, http.StatusServiceUnavailable, readyResponse{
			Status:        "fail",
			Version:       s.version,
			UptimeSeconds: uptime,
			Checks: map[string]string{
				"database":  "fail",
				"preflight": "fail",
				"workflow":  "fail",
			},
		})
		return
	}

	status := "pass"
	checks := map[string]string{}

	if s.dbPingFn != nil {
		if err := s.dbPingFn(r.Context()); err != nil {
			checks["database"] = "fail"
			status = "fail"
		} else {
			checks["database"] = "pass"
		}
	} else {
		checks["database"] = "pass"
	}

	if s.preflightFn != nil && !s.preflightFn() {
		checks["preflight"] = "fail"
		status = "fail"
	} else {
		checks["preflight"] = "pass"
	}

	if s.workflowLoadedFn != nil && !s.workflowLoadedFn() {
		checks["workflow"] = "fail"
		status = "fail"
	} else {
		checks["workflow"] = "pass"
	}

	httpStatus := http.StatusOK
	if status != "pass" {
		httpStatus = http.StatusServiceUnavailable
	}

	writeJSON(w, s.logger, httpStatus, readyResponse{
		Status:        status,
		Version:       s.version,
		UptimeSeconds: uptime,
		Checks:        checks,
	})
}
