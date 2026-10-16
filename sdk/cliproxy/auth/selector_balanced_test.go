package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func balanceAuth(id string, weight int, used float64, reset time.Time, now time.Time) *Auth {
	return &Auth{ID: id, Provider: "claude", Attributes: map[string]string{AttributeWeight: fmt.Sprint(weight)}, Quota: QuotaState{ObservedAt: now, Signals: map[string]string{"Anthropic-Ratelimit-Unified-7d-Utilization": fmt.Sprint(used / 100), "Anthropic-Ratelimit-Unified-7d-Reset": fmt.Sprint(reset.Unix())}}}
}
func TestBalancedConveyorUrgencyAffinity(t *testing.T) {
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	a := balanceAuth("20x", 4, 50, now.Add(100*time.Hour), now)
	b := balanceAuth("5x", 1, 10, now.Add(12*time.Hour), now)
	selector := &BalancedSelector{Now: func() time.Time { return now }}
	picked, err := selector.Pick(context.Background(), "claude", "", cliproxyexecutor.Options{}, []*Auth{a, b})
	if err != nil || picked.ID != "5x" {
		t.Fatalf("urgency: %v %v", picked, err)
	}
	affinity := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: selector, TTL: time.Hour})
	opts := cliproxyexecutor.Options{Headers: http.Header{"X-Claude-Code-Session-Id": []string{"root"}}, Metadata: map[string]any{}}
	picked, err = affinity.Pick(context.Background(), "claude", "model", opts, []*Auth{a, b})
	if err != nil {
		t.Fatal(err)
	}
	id := picked.ID
	b.Quota.Signals["Anthropic-Ratelimit-Unified-7d-Utilization"] = "0.99"
	picked, err = affinity.Pick(context.Background(), "claude", "model", opts, []*Auth{a, b})
	if err != nil || picked.ID != id {
		t.Fatalf("affinity changed: %v %v", picked, err)
	}
	b.Disabled = true
	picked, err = affinity.Pick(context.Background(), "claude", "model", opts, []*Auth{a, b})
	if err != nil || picked.ID != "20x" {
		t.Fatalf("failover: %v %v", picked, err)
	}
}
func TestBalancedWindowsCodexAndLocal(t *testing.T) {
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	a := &Auth{ID: "codex", Provider: "codex", Quota: QuotaState{ObservedAt: now, Signals: map[string]string{"X-Codex-Primary-Used-Percent": "42", "X-Codex-Primary-Window-Minutes": "10080", "X-Codex-Primary-Reset-After-Seconds": "3600", "X-Codex-Secondary-Used-Percent": "12", "X-Codex-Secondary-Window-Minutes": "300", "X-Codex-Secondary-Reset-At": fmt.Sprint(now.Add(2 * time.Hour).Unix())}}}
	windows := AccountBalanceWindows(a)
	if len(windows) != 2 || windows[0].UsedPercent != 12 || windows[1].WindowSeconds != 604800 || !windows[1].ResetsAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("codex windows: %+v", windows)
	}
	if later := AccountBalance(a, now.Add(2*time.Hour), 1, 0); later.LongScore != 1 {
		t.Fatalf("elapsed reset must become neutral: %+v", later)
	}
	path := filepath.Join(t.TempDir(), "windows.json")
	raw, _ := json.Marshal(map[string]any{"updated_at": now.Add(time.Minute).Unix(), "five_hour": map[string]any{"used_percentage": 70, "resets_at": now.Add(3 * time.Hour).Unix()}})
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	a.Metadata = map[string]any{"windows_file": path}
	windows = AccountBalanceWindows(a)
	if windows[0].UsedPercent != 70 || windows[0].Source != "local_file" {
		t.Fatalf("local freshness: %+v", windows)
	}
	a.Quota.Signals["X-Codex-Primary-Used-Percent"] = "NaN"
	if len(AccountBalanceWindows(a)) != 1 {
		t.Fatal("nonfinite quota accepted")
	}
	d := AccountBalance(&Auth{ID: "generic"}, now, 1, 1)
	if d.Score != .5 || len(d.Windows) != 0 {
		t.Fatalf("unknown generic: %+v", d)
	}
	a = balanceAuth("stale", 4, 50, now.Add(100*time.Hour), now.Add(-2*time.Hour))
	if AccountBalance(a, now, 4, 0).Score >= AccountBalance(balanceAuth("fresh", 4, 50, now.Add(100*time.Hour), now), now, 4, 0).Score {
		t.Fatal("stale usage not projected")
	}
}

type balancedStreamExecutor struct {
	aliasRoutingExecutor
	channels chan chan cliproxyexecutor.StreamChunk
}

func (e *balancedStreamExecutor) ExecuteStream(_ context.Context, a *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	ch := make(chan cliproxyexecutor.StreamChunk, 1)
	ch <- cliproxyexecutor.StreamChunk{Payload: []byte("data: {}\n\n")}
	e.channels <- ch
	return &cliproxyexecutor.StreamResult{Headers: http.Header{"Selected": []string{a.ID}}, Chunks: ch}, nil
}

func TestBalancedManagerReservationsAndStreams(t *testing.T) {
	for _, provider := range []string{"claude", "codex", "gemini"} {
		t.Run(provider, func(t *testing.T) {
			m := NewManager(nil, &BalancedSelector{}, nil)
			e := &balancedStreamExecutor{aliasRoutingExecutor: aliasRoutingExecutor{id: provider}, channels: make(chan chan cliproxyexecutor.StreamChunk, 4)}
			m.RegisterExecutor(e)
			model := "balanced-test-" + provider
			for _, id := range []string{"balanced-a-" + provider, "balanced-b-" + provider} {
				registry.GetGlobalRegistry().RegisterClient(id, provider, []*registry.ModelInfo{{ID: model}})
				t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
				if _, err := m.Register(context.Background(), &Auth{ID: id, Provider: provider}); err != nil {
					t.Fatal(err)
				}
			}
			req := cliproxyexecutor.Request{Model: model, Payload: []byte(`{}`)}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			first, err := m.ExecuteStream(ctx, []string{provider}, req, cliproxyexecutor.Options{})
			if err != nil {
				t.Fatal(err)
			}
			up1 := <-e.channels
			second, err := m.ExecuteStream(ctx, []string{provider}, req, cliproxyexecutor.Options{})
			if err != nil {
				t.Fatal(err)
			}
			up2 := <-e.channels
			if first.Headers.Get("Selected") == second.Headers.Get("Selected") {
				t.Fatal("concurrent cold sessions ignored active load")
			}
			close(up1)
			for range first.Chunks {
			}
			close(up2)
			for range second.Chunks {
			}
			if len(m.BalanceLoads()) != 0 {
				t.Fatal("stream load leaked")
			}
			third, err := m.ExecuteStream(ctx, []string{provider}, req, cliproxyexecutor.Options{})
			if err != nil {
				t.Fatal(err)
			}
			up3 := <-e.channels
			cancel()
			close(up3)
			for range third.Chunks {
			}
			if len(m.BalanceLoads()) != 0 {
				t.Fatal("cancel load leaked")
			}
			if _, err = m.Execute(context.Background(), []string{provider}, req, cliproxyexecutor.Options{}); err != nil {
				t.Fatal(err)
			}
			if len(m.BalanceLoads()) != 0 {
				t.Fatal("nonstream load leaked")
			}
		})
	}
}
func TestBalancedAtomicParallelReservations(t *testing.T) {
	m := NewManager(nil, &BalancedSelector{}, nil)
	m.RegisterExecutor(&aliasRoutingExecutor{id: "gemini"})
	for _, id := range []string{"load-a", "load-b"} {
		_, _ = m.Register(context.Background(), &Auth{ID: id, Provider: "gemini"})
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	releases := []func(){}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _, release, err := m.pickExecutionAuth(context.Background(), []string{"gemini"}, "", cliproxyexecutor.Options{}, nil)
			if err != nil {
				t.Error(err)
				return
			}
			mu.Lock()
			releases = append(releases, release)
			mu.Unlock()
		}()
	}
	wg.Wait()
	loads := m.BalanceLoads()
	if loads["load-a"] != 10 || loads["load-b"] != 10 {
		t.Fatalf("non-atomic balance: %v", loads)
	}
	for _, release := range releases {
		release()
		release()
	}
	if len(m.BalanceLoads()) != 0 {
		t.Fatal("release not idempotent")
	}
}

func TestBalancedPartialResponsesKeepMeasuredWindows(t *testing.T) {
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	q := QuotaState{}
	q.ObserveResponseHeadersForProvider("claude", http.Header{"Anthropic-Ratelimit-Unified-5h-Utilization": []string{"0.4"}}, now)
	q.ObserveResponseHeadersForProvider("claude", http.Header{"Anthropic-Ratelimit-Unified-7d-Utilization": []string{"0.2"}}, now.Add(time.Minute))
	if len(q.BalanceWindows) != 2 || q.BalanceWindows[0].UsedPercent != 40 || q.BalanceWindows[1].UsedPercent != 20 {
		t.Fatalf("partial windows: %+v", q.BalanceWindows)
	}
}

func TestBalancedSameSessionConcurrentLoad(t *testing.T) {
	m := NewManager(nil, NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: &BalancedSelector{}}), nil)
	m.RegisterExecutor(&aliasRoutingExecutor{id: "gemini"})
	_, _ = m.Register(context.Background(), &Auth{ID: "single", Provider: "gemini"})
	opts := cliproxyexecutor.Options{Headers: http.Header{"Session-Id": []string{"same-session"}}, Metadata: map[string]any{cliproxyexecutor.CanonicalSessionIDMetadataKey: "same-session"}}
	_, _, _, first, err := m.pickExecutionAuth(context.Background(), []string{"gemini"}, "", opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, second, err := m.pickExecutionAuth(context.Background(), []string{"gemini"}, "", opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	if m.BalanceLoads()["single"] != 1 {
		t.Fatal("same session counted twice")
	}
	first()
	if m.BalanceLoads()["single"] != 1 {
		t.Fatal("session released while still busy")
	}
	second()
	if len(m.BalanceLoads()) != 0 {
		t.Fatal("same session load leaked")
	}
}

func TestClaudeProxyOnlySkipsBackgroundRefresh(t *testing.T) {
	m := NewManager(nil, nil, nil)
	m.SetConfig(&internalconfig.Config{Claude: internalconfig.ClaudeConfig{ProxyOnly: true}})
	a := &Auth{ID: "setup", Provider: "claude", Metadata: map[string]any{"refresh_token": "must-not-refresh", "expired": time.Now().Add(-time.Hour).Format(time.RFC3339)}}
	if m.shouldRefresh(a, time.Now()) {
		t.Fatal("Claude proxy-only scheduled a background refresh")
	}
}

func TestBalancedShortWindowDoesNotRepeatWeight(t *testing.T) {
	now := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	withFiveHour := func(a *Auth, used float64, reset time.Time) *Auth {
		a.Quota.Signals["Anthropic-Ratelimit-Unified-5h-Utilization"] = fmt.Sprint(used / 100)
		a.Quota.Signals["Anthropic-Ratelimit-Unified-5h-Reset"] = fmt.Sprint(reset.Unix())
		return a
	}
	selector := &BalancedSelector{Now: func() time.Time { return now }}
	// The 5x account has 1.5x the weight-adjusted 7d urgency and an idle 5h window.
	big := withFiveHour(balanceAuth("20x", 4, 50, now.Add(100*time.Hour), now), 0, now.Add(5*time.Hour))
	small := withFiveHour(balanceAuth("5x", 1, 10, now.Add(30*time.Hour), now), 0, now.Add(5*time.Hour))
	picked, err := selector.Pick(context.Background(), "claude", "", cliproxyexecutor.Options{}, []*Auth{big, small})
	if err != nil || picked.ID != "5x" {
		t.Fatalf("idle 5h window penalized the lower-weight account: %v %v", picked, err)
	}
	// A nearly exhausted 5h window still throttles the urgent account.
	small = withFiveHour(small, 90, now.Add(4*time.Hour))
	picked, err = selector.Pick(context.Background(), "claude", "", cliproxyexecutor.Options{}, []*Auth{big, small})
	if err != nil || picked.ID != "20x" {
		t.Fatalf("exhausted 5h window did not throttle: %v %v", picked, err)
	}
}

func TestQuotaObservationLogFields(t *testing.T) {
	reset := time.Date(2026, 10, 17, 2, 0, 0, 0, time.UTC)
	got := formatQuotaWindows([]BalanceWindow{{Name: "5h", UsedPercent: 24}, {Name: "7d", UsedPercent: 28.999999999999996, ResetsAt: reset}})
	if got != "5h=24%,7d=29%@2026-10-17T02:00:00Z" {
		t.Fatalf("windows: %q", got)
	}
	if got := formatQuotaWindows(nil); got != "-" {
		t.Fatalf("empty windows: %q", got)
	}
	canonical := cliproxyexecutor.Options{Metadata: map[string]any{cliproxyexecutor.CanonicalSessionIDMetadataKey: "claude:b0ef09ca-7f29-4716-90e0-c738737aeac5"}}
	if got := quotaObservationSessionID(canonical); got != "claude:b0ef09ca-7f29-4716-90e0-c738737aeac5" {
		t.Fatalf("canonical session: %q", got)
	}
	if got := quotaObservationSessionID(cliproxyexecutor.Options{}); got != "-" {
		t.Fatalf("missing session: %q", got)
	}
}

func TestTruncateSessionIDKeepsSessionsDistinct(t *testing.T) {
	a := truncateSessionID("codex:01a131cc-a72e-7143-b17f-9ba8d6d3d712")
	b := truncateSessionID("codex:01a131cc-d880-73c1-a42d-8012be917465")
	if a == b || a != "codex:01a131cc...d712" {
		t.Fatalf("truncated IDs: %q %q", a, b)
	}
	if got := truncateSessionID("short-session"); got != "short-session" {
		t.Fatalf("short ID: %q", got)
	}
}

func TestAccountBalanceWindowsRetainsResetOnPartialResponse(t *testing.T) {
	now := time.Date(2026, 10, 13, 14, 0, 0, 0, time.UTC)
	a := &Auth{Quota: QuotaState{ObservedAt: now, Signals: map[string]string{"Anthropic-Ratelimit-Unified-5h-Utilization": "0.97"}, BalanceWindows: []BalanceWindow{{Name: "5h", WindowSeconds: 18000, UsedPercent: 96, ObservedAt: now.Add(-time.Minute), ResetsAt: now.Add(time.Hour)}}}}
	got := AccountBalanceWindows(a)
	if len(got) != 1 || got[0].UsedPercent != 97 || !got[0].ResetsAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("partial window = %+v", got)
	}
	a.Quota.BalanceWindows[0].ResetsAt = now.Add(-time.Second)
	got = AccountBalanceWindows(a)
	if !got[0].ResetsAt.IsZero() {
		t.Fatal("expired reset carried into new window")
	}
}

// withShortWindow adds a 5h observation at now and history samples given as
// minutes-ago → used percent.
func withShortWindow(a *Auth, used float64, reset, now time.Time, history map[int]float64) *Auth {
	a.Quota.Signals["Anthropic-Ratelimit-Unified-5h-Utilization"] = fmt.Sprint(used / 100)
	a.Quota.Signals["Anthropic-Ratelimit-Unified-5h-Reset"] = fmt.Sprint(reset.Unix())
	a.Quota.ObservedAt = now
	a.Quota.BalanceHistory = nil
	for ago, u := range history {
		a.Quota.BalanceHistory = append(a.Quota.BalanceHistory, BalanceWindow{Name: "5h", UsedPercent: u, WindowSeconds: 18000, ResetsAt: reset, ObservedAt: now.Add(-time.Duration(ago) * time.Minute), Source: "response"})
	}
	return a
}

func pickID(t *testing.T, s *BalancedSelector, auths ...*Auth) string {
	t.Helper()
	picked, err := s.Pick(context.Background(), "claude", "", cliproxyexecutor.Options{}, auths)
	if err != nil {
		t.Fatal(err)
	}
	return picked.ID
}

func TestBalancedFiveHourSpillover(t *testing.T) {
	now := time.Date(2026, 10, 15, 17, 40, 0, 0, time.UTC)
	s := &BalancedSelector{Now: func() time.Time { return now }}
	// 2026-10-15: 20x had 63% weekly left resetting next morning, 5x was fresh.
	big := balanceAuth("20x", 4, 37, now.Add(14*time.Hour), now)
	small := balanceAuth("5x", 1, 2, now.Add(150*time.Hour), now)
	if got := pickID(t, s, big, small); got != "20x" {
		t.Fatalf("weekly ranking changed: %s", got)
	}
	// High burn: 23% → 60% in the last hour with 3h left projects far past 100%.
	withShortWindow(big, 60, now.Add(3*time.Hour), now, map[int]float64{55: 25, 30: 42})
	if got := pickID(t, s, big, small); got != "5x" {
		t.Fatalf("high burn did not spill: %s %+v", got, AccountBalance(big, now, 4, 0).ShortProjection)
	}
	if p := AccountBalance(big, now, 4, 0).ShortProjection; p.RateSource != "recent" || p.ProjectedPercent < 150 {
		t.Fatalf("projection: %+v", p)
	}
	if !BalancedSpilling(NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: s}))["20x"] {
		t.Fatal("spilling state not exposed through affinity")
	}
	// Low burn: 55% → 60% over the last hour fits before reset.
	s = &BalancedSelector{Now: func() time.Time { return now }}
	withShortWindow(big, 60, now.Add(3*time.Hour), now, map[int]float64{55: 55, 30: 58})
	if got := pickID(t, s, big, small); got != "20x" {
		t.Fatalf("low burn spilled: %s %+v", got, AccountBalance(big, now, 4, 0).ShortProjection)
	}
	// Both exhaust before reset: the account with more 5h headroom takes the session.
	withShortWindow(big, 60, now.Add(3*time.Hour), now, map[int]float64{55: 25, 30: 42})
	withShortWindow(small, 70, now.Add(3*time.Hour), now, map[int]float64{55: 60, 30: 65})
	if got := pickID(t, s, big, small); got != "5x" {
		t.Fatalf("most headroom not chosen: %s", got)
	}
}

func TestBalancedFiveHourSpilloverCascadesAcrossThree(t *testing.T) {
	now := time.Date(2026, 10, 15, 17, 40, 0, 0, time.UTC)
	s := &BalancedSelector{Now: func() time.Time { return now }}
	first := balanceAuth("a-20x", 4, 37, now.Add(14*time.Hour), now)
	second := balanceAuth("b-20x", 4, 50, now.Add(100*time.Hour), now)
	third := balanceAuth("c-5x", 1, 2, now.Add(150*time.Hour), now)
	if got := pickID(t, s, third, second, first); got != "a-20x" {
		t.Fatalf("ranking: %s", got)
	}
	hot := map[int]float64{55: 25, 30: 42}
	withShortWindow(first, 60, now.Add(3*time.Hour), now, hot)
	if got := pickID(t, s, third, second, first); got != "b-20x" {
		t.Fatalf("first spill: %s", got)
	}
	withShortWindow(second, 60, now.Add(3*time.Hour), now, hot)
	if got := pickID(t, s, third, second, first); got != "c-5x" {
		t.Fatalf("cascade: %s", got)
	}
	// The top account's burn cools down: new sessions return to it.
	withShortWindow(first, 60, now.Add(3*time.Hour), now, map[int]float64{55: 58, 30: 59})
	if got := pickID(t, s, third, second, first); got != "a-20x" {
		t.Fatalf("return to top: %s", got)
	}
	// Excluded accounts stay excluded even when they would fit.
	first.Disabled = true
	withShortWindow(second, 60, now.Add(3*time.Hour), now, hot)
	third.Attributes[AttributeWeight] = "0"
	if got := pickID(t, s, third, second, first); got != "b-20x" {
		t.Fatalf("excluded account picked: %s", got)
	}
}

func TestBalancedFiveHourYoungWindowAndEdgeCases(t *testing.T) {
	now := time.Date(2026, 10, 15, 17, 40, 0, 0, time.UTC)
	s := &BalancedSelector{Now: func() time.Time { return now }}
	big := balanceAuth("20x", 4, 37, now.Add(14*time.Hour), now)
	small := balanceAuth("5x", 1, 2, now.Add(150*time.Hour), now)
	// 12 minutes into the window: 2% → 8% in 10 minutes is too short a span for a
	// recent rate, and the elapsed average is floored at 30 minutes.
	reset := now.Add(5*time.Hour - 12*time.Minute)
	withShortWindow(big, 8, reset, now, map[int]float64{10: 2})
	p := AccountBalance(big, now, 4, 0).ShortProjection
	if p.RateSource != "average" || p.BurnRatePerHour != 16 || p.ProjectedPercent > 100 {
		t.Fatalf("young window: %+v", p)
	}
	if got := pickID(t, s, big, small); got != "20x" {
		t.Fatalf("young window spilled: %s", got)
	}
	// Samples from the previous window are ignored.
	withShortWindow(big, 8, reset, now, map[int]float64{10: 2, 40: 90})
	if p := AccountBalance(big, now, 4, 0).ShortProjection; p.RateSource != "average" {
		t.Fatalf("previous window used: %+v", p)
	}
	// No observation for over an hour: the account is idle and can win sessions back.
	withShortWindow(big, 90, now.Add(2*time.Hour), now.Add(-70*time.Minute), map[int]float64{100: 20})
	if p := AccountBalance(big, now, 4, 0).ShortProjection; p.RateSource != "idle" || p.ProjectedPercent != 90 {
		t.Fatalf("stale: %+v", p)
	}
	// An elapsed reset is a fresh, unknown window.
	withShortWindow(big, 99, now.Add(-time.Minute), now.Add(-2*time.Hour), nil)
	if p := AccountBalance(big, now, 4, 0).ShortProjection; p != nil {
		t.Fatalf("elapsed reset projected: %+v", p)
	}
	// No history at all: the elapsed average since window start (3h in, 90% used → 30%/h).
	withShortWindow(big, 90, now.Add(2*time.Hour), now, nil)
	if p := AccountBalance(big, now, 4, 0).ShortProjection; p.RateSource != "average" || p.ProjectedPercent != 150 {
		t.Fatalf("no history: %+v", p)
	}
	// Codex primary 5h windows project the same way.
	codex := &Auth{ID: "codex", Provider: "codex", Quota: QuotaState{ObservedAt: now, Signals: map[string]string{"X-Codex-Primary-Used-Percent": "50", "X-Codex-Primary-Window-Minutes": "300", "X-Codex-Primary-Reset-After-Seconds": "3600", "X-Codex-Secondary-Used-Percent": "10", "X-Codex-Secondary-Window-Minutes": "10080", "X-Codex-Secondary-Reset-After-Seconds": "86400"}}}
	if p := AccountBalance(codex, now, 1, 0).ShortProjection; p == nil || p.WindowSeconds != 18000 || p.ProjectedPercent != 62.5 {
		t.Fatalf("codex: %+v", p)
	}
}

func TestBalancedFiveHourHysteresis(t *testing.T) {
	now := time.Date(2026, 10, 15, 17, 40, 0, 0, time.UTC)
	s := &BalancedSelector{Now: func() time.Time { return now }}
	big := balanceAuth("20x", 4, 37, now.Add(14*time.Hour), now)
	small := balanceAuth("5x", 1, 2, now.Add(150*time.Hour), now)
	// used 50%, one hour left, ref 30 minutes ago: projected = 50 + 2*(50-ref).
	at := func(ref float64) *Auth {
		return withShortWindow(big, 50, now.Add(time.Hour), now, map[int]float64{30: ref})
	}
	for _, step := range []struct {
		ref  float64
		want string
	}{{26, "20x"}, {15, "5x"}, {26, "5x"}, {28, "20x"}, {26, "20x"}, {24, "5x"}} {
		at(step.ref)
		if got := pickID(t, s, big, small); got != step.want {
			t.Fatalf("ref %v (projected %.0f%%): got %s want %s", step.ref, AccountBalance(big, now, 4, 0).ShortProjection.ProjectedPercent, got, step.want)
		}
	}
}

func TestBalancedHistoryBoundedAndPersisted(t *testing.T) {
	now := time.Date(2026, 10, 15, 17, 0, 0, 0, time.UTC)
	q := QuotaState{}
	reset := fmt.Sprint(now.Add(4 * time.Hour).Unix())
	for i := 0; i < 600; i++ {
		used := fmt.Sprint(float64(i) / 1000)
		q.ObserveResponseHeadersForProvider("claude", http.Header{"Anthropic-Ratelimit-Unified-5h-Utilization": []string{used}, "Anthropic-Ratelimit-Unified-5h-Reset": []string{reset}, "Anthropic-Ratelimit-Unified-7d-Utilization": []string{"0.1"}}, now.Add(time.Duration(i)*15*time.Second))
	}
	h := q.BalanceHistory
	if len(h) < 40 || len(h) > balanceHistoryMax {
		t.Fatalf("history size %d", len(h))
	}
	for i := 1; i < len(h)-1; i++ {
		if h[i].ObservedAt.Sub(h[i-1].ObservedAt) < balanceHistorySpacing || h[i].WindowSeconds != 18000 {
			t.Fatalf("history spacing/window: %+v", h[i])
		}
	}
	if last := h[len(h)-1]; !last.ObservedAt.Equal(now.Add(599*15*time.Second)) || h[0].ObservedAt.Before(last.ObservedAt.Add(-balanceHistoryRetention)) {
		t.Fatalf("history bounds: %v..%v", h[0].ObservedAt, last.ObservedAt)
	}
	// A reset (utilization drop) starts a new history.
	q.ObserveResponseHeadersForProvider("claude", http.Header{"Anthropic-Ratelimit-Unified-5h-Utilization": []string{"0.01"}, "Anthropic-Ratelimit-Unified-5h-Reset": []string{fmt.Sprint(now.Add(9 * time.Hour).Unix())}}, now.Add(4*time.Hour+time.Minute))
	if len(q.BalanceHistory) != 1 {
		t.Fatalf("reset kept old samples: %d", len(q.BalanceHistory))
	}
	// Persisted metadata (after a JSON reload) is merged back with runtime samples.
	raw, _ := json.Marshal(h)
	var persisted any
	_ = json.Unmarshal(raw, &persisted)
	a := &Auth{Metadata: map[string]any{"window_history": persisted}, Quota: QuotaState{BalanceHistory: h[len(h)-1:]}}
	if got := AccountBalanceHistory(a); len(got) != len(h) {
		t.Fatalf("persisted history merge: %d != %d", len(got), len(h))
	}
}

func TestSessionAffinityOneHourSlidingTTL(t *testing.T) {
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	balanced := &BalancedSelector{Now: clock}
	affinity := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: balanced, TTL: time.Hour})
	defer affinity.Stop()
	affinity.cache.now = clock
	a := balanceAuth("a", 4, 10, now.Add(100*time.Hour), now)
	b := balanceAuth("b", 4, 50, now.Add(100*time.Hour), now)
	opts := func(session string) cliproxyexecutor.Options {
		return cliproxyexecutor.Options{Headers: http.Header{"X-Claude-Code-Session-Id": []string{session}}, Metadata: map[string]any{}}
	}
	pick := func(session string) string {
		t.Helper()
		picked, err := affinity.Pick(context.Background(), "claude", "model", opts(session), []*Auth{a, b})
		if err != nil {
			t.Fatal(err)
		}
		return picked.ID
	}
	if got := pick("root"); got != "a" {
		t.Fatalf("initial: %s", got)
	}
	// Account b becomes the better pick; the session stays while requests come under 1h apart.
	a.Quota.Signals["Anthropic-Ratelimit-Unified-7d-Utilization"] = "0.9"
	for i := 0; i < 4; i++ {
		now = now.Add(50 * time.Minute)
		if got := pick("root"); got != "a" {
			t.Fatalf("binding lost after %d refreshes: %s", i, got)
		}
	}
	// A successful result refreshes the binding too.
	now = now.Add(50 * time.Minute)
	affinity.OnResult(Result{AuthID: "a", Provider: "claude", Model: "model", Success: true, Options: opts("root")})
	now = now.Add(50 * time.Minute)
	if got := pick("root"); got != "a" {
		t.Fatalf("OnResult did not refresh: %s", got)
	}
	// One idle hour: the session gets a fresh pick.
	now = now.Add(time.Hour + time.Second)
	if got := pick("root"); got != "b" {
		t.Fatalf("idle binding not released: %s", got)
	}
}

// Spillover only routes sessions without a live binding: a bound session keeps
// its account (and prompt cache) while that account spills.
func TestBalancedSpilloverNeverMovesBoundSession(t *testing.T) {
	now := time.Date(2026, 10, 15, 17, 40, 0, 0, time.UTC)
	balanced := &BalancedSelector{Now: func() time.Time { return now }}
	affinity := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: balanced, TTL: time.Hour})
	defer affinity.Stop()
	big := balanceAuth("20x", 4, 37, now.Add(14*time.Hour), now)
	small := balanceAuth("5x", 1, 2, now.Add(150*time.Hour), now)
	auths := []*Auth{big, small}
	headers := func(kv ...string) cliproxyexecutor.Options {
		h := http.Header{}
		for i := 0; i < len(kv); i += 2 {
			h.Set(kv[i], kv[i+1])
		}
		return cliproxyexecutor.Options{Headers: h, Metadata: map[string]any{}}
	}
	pick := func(model string, opts cliproxyexecutor.Options) string {
		t.Helper()
		picked, err := affinity.Pick(context.Background(), "claude", model, opts, auths)
		if err != nil {
			t.Fatal(err)
		}
		return picked.ID
	}
	root := headers("X-Claude-Code-Session-Id", "root")
	if got := pick("claude-opus-4-8", root); got != "20x" {
		t.Fatalf("initial: %s", got)
	}
	// 20x starts burning its 5h window too fast and spills.
	withShortWindow(big, 60, now.Add(3*time.Hour), now, map[int]float64{55: 25, 30: 42})
	if got := pick("claude-opus-4-8", headers("X-Claude-Code-Session-Id", "new")); got != "5x" {
		t.Fatalf("new session did not spill: %s", got)
	}
	if !BalancedSpilling(affinity)["20x"] {
		t.Fatal("20x not spilling")
	}
	for i := 0; i < 3; i++ {
		if got := pick("claude-opus-4-8", root); got != "20x" {
			t.Fatalf("bound session moved by spillover: %s", got)
		}
	}
	// Only real unavailability (a 429 cooldown) moves the bound session.
	// Availability is checked against the wall clock.
	recover := time.Now().Add(time.Hour)
	big.Unavailable = true
	big.Status = StatusError
	big.NextRetryAfter = recover
	big.Quota = QuotaState{Exceeded: true, NextRecoverAt: recover}
	if got := pick("claude-opus-4-8", root); got != "5x" {
		t.Fatalf("unavailable account kept: %s", got)
	}
}

// With subagent affinity off (the Jetson setting), a plain subagent and a
// request on another model are routed freely and then keep their own binding;
// a Claude Code fork, which resends the parent's prefix, inherits the parent.
func TestBalancedFreeSubagentsAndInheritedForks(t *testing.T) {
	now := time.Date(2026, 10, 15, 17, 40, 0, 0, time.UTC)
	balanced := &BalancedSelector{Now: func() time.Time { return now }}
	off := false
	affinity := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{Fallback: balanced, TTL: time.Hour, SubagentAffinity: &off})
	defer affinity.Stop()
	big := balanceAuth("20x", 4, 37, now.Add(14*time.Hour), now)
	small := balanceAuth("5x", 1, 2, now.Add(150*time.Hour), now)
	auths := []*Auth{big, small}
	request := func(agent, body string) cliproxyexecutor.Options {
		h := http.Header{"X-Claude-Code-Session-Id": []string{"root"}}
		if agent != "" {
			h.Set("X-Claude-Code-Agent-Id", agent)
		}
		return cliproxyexecutor.Options{Headers: h, OriginalRequest: []byte(body), Metadata: map[string]any{}}
	}
	pick := func(model string, opts cliproxyexecutor.Options) string {
		t.Helper()
		picked, err := affinity.Pick(context.Background(), "claude", model, opts, auths)
		if err != nil {
			t.Fatal(err)
		}
		return picked.ID
	}
	plain := `{"messages":[{"role":"user","content":"Find the config loader"}]}`
	fork := `{"messages":[{"role":"user","content":"parent turn"},{"role":"assistant","content":[{"type":"tool_use","id":"t1","name":"Agent","input":{}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":[{"type":"text","text":"Fork started \u2014 processing in background"}]},{"type":"text","text":"<fork-boilerplate>\nYou are a worker fork. The transcript above is the parent's history.\n</fork-boilerplate>\nYour directive: check tests"}]}]}`
	if got := pick("claude-opus-5-5", request("", plain)); got != "20x" {
		t.Fatalf("parent: %s", got)
	}
	withShortWindow(big, 60, now.Add(3*time.Hour), now, map[int]float64{55: 25, 30: 42})
	if got := pick("claude-opus-5-5", request("explore-1", plain)); got != "5x" {
		t.Fatalf("plain subagent inherited the spilling parent account: %s", got)
	}
	if got := pick("claude-haiku-4-5", request("", plain)); got != "5x" {
		t.Fatalf("other-model request inherited the spilling session account: %s", got)
	}
	if got := pick("claude-opus-5-5", request("fork-1", fork)); got != "20x" {
		t.Fatalf("fork did not inherit the parent account: %s", got)
	}
	// Each keeps its own binding once the ranking flips back.
	withShortWindow(big, 60, now.Add(3*time.Hour), now, map[int]float64{55: 58, 30: 59})
	if got := pick("claude-opus-5-5", request("explore-1", plain)); got != "5x" {
		t.Fatalf("subagent binding lost: %s", got)
	}
	if got := pick("claude-haiku-4-5", request("", plain)); got != "5x" {
		t.Fatalf("other-model binding lost: %s", got)
	}
	if got := pick("claude-opus-5-5", request("", plain)); got != "20x" {
		t.Fatalf("parent moved: %s", got)
	}
}
