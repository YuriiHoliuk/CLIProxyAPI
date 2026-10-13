package helps

import (
	"net/http"
	"strconv"
	"testing"
	"time"

	auth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestClaudeQuotaRecovery(t *testing.T) {
	now := time.Date(2026, 10, 13, 14, 0, 0, 0, time.UTC)
	cached := &auth.Auth{Quota: auth.QuotaState{BalanceWindows: []auth.BalanceWindow{
		{Name: "5h", WindowSeconds: 18000, UsedPercent: 97, ResetsAt: now.Add(3 * time.Hour)},
		{Name: "7d", WindowSeconds: 604800, UsedPercent: 69, ResetsAt: now.Add(4 * 24 * time.Hour)},
	}}}
	for _, tc := range []struct {
		name    string
		headers http.Header
		account *auth.Auth
		want    time.Duration
		claim   string
	}{
		{"current reset wins", http.Header{"Anthropic-Ratelimit-Unified-Status": {"rejected"}, "Anthropic-Ratelimit-Unified-Reset": {strconv.FormatInt(now.Add(2*time.Hour).Unix(), 10)}}, cached, 2 * time.Hour, "five_hour"},
		{"rejected window missing reset", http.Header{"Anthropic-Ratelimit-Unified-5h-Status": {"rejected"}}, cached, 3 * time.Hour, "five_hour"},
		{"no headers retains measured 97 percent reset", nil, cached, 3 * time.Hour, "five_hour"},
		{"weekly rejected", http.Header{"Anthropic-Ratelimit-Unified-7d-Status": {"rejected"}}, cached, 4 * 24 * time.Hour, "seven_day"},
		{"both rejected with partial resets", http.Header{"Anthropic-Ratelimit-Unified-5h-Status": {"rejected"}, "Anthropic-Ratelimit-Unified-5h-Reset": {strconv.FormatInt(now.Add(2*time.Hour).Unix(), 10)}, "Anthropic-Ratelimit-Unified-7d-Status": {"rejected"}}, cached, 4 * 24 * time.Hour, "seven_day"},
		{"unknown", nil, nil, time.Hour, "five_hour"},
		{"malformed", http.Header{"Retry-After": {"invalid"}, "Anthropic-Ratelimit-Unified-Reset": {"NaN"}}, nil, time.Hour, "five_hour"},
		{"expired", nil, &auth.Auth{Quota: auth.QuotaState{BalanceWindows: []auth.BalanceWindow{{Name: "5h", WindowSeconds: 18000, UsedPercent: 100, ResetsAt: now.Add(-time.Second)}}}}, time.Hour, "five_hour"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			retry, headers := ClaudeQuotaRecovery(tc.headers, tc.account, now)
			if retry < tc.want || retry > tc.want+30*time.Second {
				t.Fatalf("retry = %v, want %v plus bounded grace", retry, tc.want)
			}
			reset, _ := strconv.ParseInt(headers.Get("Anthropic-Ratelimit-Unified-Reset"), 10, 64)
			if reset != now.Add(retry).Unix() {
				t.Fatalf("reset = %d, retry = %v", reset, retry)
			}
			if got := headers.Get("Anthropic-Ratelimit-Unified-Representative-Claim"); got != tc.claim {
				t.Fatalf("claim = %q", got)
			}
			if len(headers) != 4 {
				t.Fatalf("unexpected headers: %v", headers)
			}
		})
	}
}
