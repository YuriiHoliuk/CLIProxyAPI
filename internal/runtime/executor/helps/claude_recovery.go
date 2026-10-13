package helps

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// ClaudeQuotaRecovery retains reset observations when a rejected response omits
// them. Never replay a relative Retry-After from an earlier response.
func ClaudeQuotaRecovery(headers http.Header, auth *cliproxyauth.Auth, now time.Time) (time.Duration, http.Header) {
	parsed := ParseClaudeRateLimitReset(headers, now)
	current := headers.Clone()
	if current == nil {
		current = make(http.Header)
	}
	claim := strings.ToLower(getHeaderCaseInsensitive(current, "Anthropic-Ratelimit-Unified-Representative-Claim"))
	anyRejected := false
	for _, name := range []string{"5h", "7d", "7d_oi"} {
		anyRejected = anyRejected || strings.EqualFold(getHeaderCaseInsensitive(current, "Anthropic-Ratelimit-Unified-"+name+"-Status"), "rejected")
	}
	changed := false
	for _, window := range cliproxyauth.AccountBalanceWindows(auth) {
		if parsed != nil && !anyRejected {
			break
		}
		prefix := "Anthropic-Ratelimit-Unified-" + window.Name + "-"
		rejected := strings.EqualFold(getHeaderCaseInsensitive(current, prefix+"Status"), "rejected")
		if !anyRejected {
			rejected = (window.Name == "5h" && (claim == "" || claim == "five_hour")) ||
				(window.Name == "7d" && (claim == "seven_day" || window.UsedPercent >= 100))
		}
		if !rejected || !window.ResetsAt.After(now) {
			continue
		}
		if reset, ok := parseUnixOrTimestamp(getHeaderCaseInsensitive(current, prefix+"Reset")); ok && reset.After(now) {
			continue
		}
		current.Set(prefix+"Status", "rejected")
		current.Set(prefix+"Reset", strconv.FormatInt(window.ResetsAt.Unix(), 10))
		changed = true
	}
	// The same parser governs scheduler cooldown and the client-visible deadline.
	retry := time.Hour
	if changed {
		parsed = ParseClaudeRateLimitReset(current, now)
	}
	if parsed != nil {
		retry = *parsed
	}
	if claim != "five_hour" && claim != "seven_day" && claim != "seven_day_opus" {
		claim = "five_hour"
		if strings.EqualFold(getHeaderCaseInsensitive(current, "Anthropic-Ratelimit-Unified-7d-Status"), "rejected") {
			claim = "seven_day"
		}
	}
	// Only expose scheduling signals. Do not forward account identity or unrelated
	// upstream headers, and do not advertise overage from the last rejected account.
	return retry, http.Header{
		"Retry-After":                                      {strconv.FormatInt(int64((retry+time.Second-1)/time.Second), 10)},
		"Anthropic-Ratelimit-Unified-Status":               {"rejected"},
		"Anthropic-Ratelimit-Unified-Reset":                {strconv.FormatInt(now.Add(retry).Unix(), 10)},
		"Anthropic-Ratelimit-Unified-Representative-Claim": {claim},
	}
}
