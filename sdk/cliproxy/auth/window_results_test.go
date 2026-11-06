package auth

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	internallogging "github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
)

func TestWindowResultLogRecordsResetWindowsOnce(t *testing.T) {
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "quota-resets.jsonl")
	results, err := OpenWindowResultLog(path)
	if err != nil {
		t.Fatal(err)
	}
	a := &Auth{ID: "claude-work-5x.json", Label: "work Team", Provider: "claude", Metadata: map[string]any{"windows": []BalanceWindow{
		{Name: "5h", UsedPercent: 92, WindowSeconds: 18000, ResetsAt: now.Add(-time.Hour), ObservedAt: now.Add(-90 * time.Minute)},
		{Name: "7d", UsedPercent: 40, WindowSeconds: 604800, ResetsAt: now.Add(-time.Hour), ObservedAt: now.Add(-90 * time.Minute)},
	}}}
	results.Record(a, now)
	results.Record(a, now.Add(time.Minute))
	got := results.Results(time.Time{})
	// Only weekly windows are kept; the reset 5h window is skipped.
	if len(got) != 1 || got[0].Name != "7d" || got[0].UsedPercent != 40 || got[0].Label != "work Team" || got[0].Weight != 1 {
		t.Fatalf("results: %+v", got)
	}
	reopened, err := OpenWindowResultLog(path)
	if err != nil {
		t.Fatal(err)
	}
	reopened.Record(a, now.Add(2*time.Minute))
	if got := reopened.Results(time.Time{}); len(got) != 1 || !got[0].ResetsAt.Equal(now.Add(-time.Hour)) {
		t.Fatalf("reopened: %+v", got)
	}
	if got := reopened.Results(now); len(got) != 0 {
		t.Fatalf("since filter: %+v", got)
	}
}

// A busy account's first response after a reset replaces the old window; its
// final usage must be recorded before that.
func TestMarkResultRecordsWindowBeforeObservationReplacesIt(t *testing.T) {
	now := time.Now()
	reset := now.Add(-time.Minute).Truncate(time.Second)
	m := NewManager(nil, nil, nil)
	results, err := OpenWindowResultLog(filepath.Join(t.TempDir(), "quota-resets.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	m.SetWindowResultLog(results)
	a := &Auth{ID: "claude-work-20x.json", Provider: "claude", Metadata: map[string]any{"weight": 2, "windows": []BalanceWindow{
		{Name: "7d", UsedPercent: 87, WindowSeconds: 604800, ResetsAt: reset, ObservedAt: now.Add(-10 * time.Minute)},
	}}}
	if _, errRegister := m.Register(context.Background(), a); errRegister != nil {
		t.Fatal(errRegister)
	}
	ctx := internallogging.WithResponseHeadersHolder(context.Background())
	internallogging.SetResponseHeaders(ctx, http.Header{
		"Anthropic-Ratelimit-Unified-7d-Utilization": []string{"0.01"},
		"Anthropic-Ratelimit-Unified-7d-Reset":       []string{fmt.Sprint(reset.Add(7 * 24 * time.Hour).Unix())},
	})
	m.MarkResult(ctx, Result{AuthID: a.ID, Provider: "claude", Model: "claude-opus-5-5", Success: true})
	got := results.Results(time.Time{})
	if len(got) != 1 || got[0].UsedPercent != 87 || !got[0].ResetsAt.Equal(reset) || got[0].Weight != 2 {
		t.Fatalf("results: %+v", got)
	}
	current, _ := m.GetByID(a.ID)
	if windows := AccountBalanceWindows(current); len(windows) != 1 || windows[0].UsedPercent != 1 {
		t.Fatalf("new window not observed: %+v", windows)
	}
}
