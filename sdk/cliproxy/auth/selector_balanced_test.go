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
