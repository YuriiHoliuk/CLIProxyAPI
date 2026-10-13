package auth

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

type claudePoolRecoveryError struct {
	error
	headers http.Header
}

func (e *claudePoolRecoveryError) Unwrap() error { return e.error }
func (e *claudePoolRecoveryError) ClaudeQuotaRecoveryHeaders() http.Header {
	return e.headers.Clone()
}

// The last upstream refusal may belong to the account that resets last.
// Once the pool is exhausted, advertise the first eligible account's recovery.
func (m *Manager) withClaudePoolRecovery(ctx context.Context, model string, opts cliproxyexecutor.Options, err error) error {
	var recovery interface{ ClaudeQuotaRecoveryHeaders() http.Header }
	var cooldown *modelCooldownError
	fromUpstream := errors.As(err, &recovery) && len(recovery.ClaudeQuotaRecoveryHeaders()) > 0
	fromSelection := errors.As(err, &cooldown) && (cooldown.provider == "claude" || strings.HasPrefix(model, "claude-"))
	if m.HomeEnabled() || (!fromUpstream && !fromSelection) {
		return err
	}
	now := time.Now()
	eligibility := authSelectionEligibilityForRequest(ctx, opts)
	pinned := pinnedAuthIDFromMetadata(opts.Metadata)
	reg := registry.GetGlobalRegistry()
	var earliest time.Time
	claim := "five_hour"
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, account := range m.auths {
		if account == nil || !strings.EqualFold(account.Provider, "claude") || account.Disabled || account.Status == StatusDisabled || authWeight(account) <= 0 || !eligibility.allows(account) || (pinned != "" && account.ID != pinned) || !m.authSupportsRouteModel(reg, account, model) {
			continue
		}
		blocked, reason, next := isAuthBlockedForModel(account, m.selectionModelForAuth(account, model), now)
		if !blocked {
			return err
		}
		if reason == blockReasonCooldown && next.After(now) && (earliest.IsZero() || next.Before(earliest)) {
			earliest = next
			claim = "five_hour"
			if candidate := account.Quota.Signals["Anthropic-Ratelimit-Unified-Representative-Claim"]; candidate == "five_hour" || candidate == "seven_day" || candidate == "seven_day_opus" {
				claim = candidate
			} else if account.Quota.Signals["Anthropic-Ratelimit-Unified-7d-Status"] == "rejected" {
				claim = "seven_day"
			}
		}
	}
	if earliest.IsZero() {
		return err
	}
	headers := safeRetryAfterHeader(earliest.Sub(now))
	headers.Set("Anthropic-Ratelimit-Unified-Status", "rejected")
	headers.Set("Anthropic-Ratelimit-Unified-Reset", strconv.FormatInt(earliest.Unix(), 10))
	headers.Set("Anthropic-Ratelimit-Unified-Representative-Claim", claim)
	return &claudePoolRecoveryError{error: err, headers: headers}
}
