package claude

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// Claude Code emits rate_limit_event from unified headers, not Retry-After or
// the error body. Selection-only cooldowns need the same protocol as upstream 429s.
func claudeQuotaRecoveryHeaders(msg *interfaces.ErrorMessage, now time.Time) http.Header {
	if msg == nil || msg.StatusCode != http.StatusTooManyRequests {
		return nil
	}
	var scoped interface{ IsRequestScoped() bool }
	if errors.As(msg.Error, &scoped) && scoped.IsRequestScoped() {
		return nil
	}
	var recovery interface{ ClaudeQuotaRecoveryHeaders() http.Header }
	if errors.As(msg.Error, &recovery) {
		if headers := recovery.ClaudeQuotaRecoveryHeaders(); len(headers) > 0 {
			return headers
		}
	}
	retry := int64(3600)
	if seconds, err := strconv.ParseInt(coreauth.SafeResponseHeaders(msg.Error).Get("Retry-After"), 10, 64); err == nil && seconds > 0 && seconds <= int64((time.Duration(1<<63-1))/time.Second) {
		retry = seconds
	}
	return http.Header{
		"Retry-After":                                      {strconv.FormatInt(retry, 10)},
		"Anthropic-Ratelimit-Unified-Status":               {"rejected"},
		"Anthropic-Ratelimit-Unified-Reset":                {strconv.FormatInt(now.Add(time.Duration(retry)*time.Second).Unix(), 10)},
		"Anthropic-Ratelimit-Unified-Representative-Claim": {"five_hour"},
	}
}
