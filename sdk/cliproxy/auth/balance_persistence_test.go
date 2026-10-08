package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
)

func TestBalancedPassiveWindowsSurviveCredentialStoreReload(t *testing.T) {
	for _, provider := range []string{"claude", "codex"} {
		t.Run(provider, func(t *testing.T) {
			store := &reloadAuthStore{records: make(map[string]*Auth)}
			manager := NewManager(store, &BalancedSelector{}, nil)
			id := "persist-" + provider
			if _, err := manager.Register(context.Background(), &Auth{ID: id, Provider: provider, Metadata: map[string]any{"type": provider}}); err != nil {
				t.Fatal(err)
			}
			reset := fmt.Sprint(time.Now().Add(2 * time.Hour).Unix())
			for index, used := range []string{"20", "80"} {
				headers := make(http.Header)
				if provider == "claude" {
					name := []string{"5h", "7d"}[index]
					headers.Set("Anthropic-Ratelimit-Unified-"+name+"-Utilization", []string{"0.2", "0.8"}[index])
					headers.Set("Anthropic-Ratelimit-Unified-"+name+"-Reset", reset)
				} else {
					name := []string{"Secondary", "Primary"}[index]
					prefix := "X-Codex-" + name + "-"
					headers.Set(prefix+"Used-Percent", used)
					headers.Set(prefix+"Window-Minutes", []string{"300", "10080"}[index])
					headers.Set(prefix+"Reset-At", reset)
				}
				ctx := logging.WithResponseHeadersHolder(context.Background())
				logging.SetResponseHeaders(ctx, headers)
				manager.MarkResult(ctx, Result{AuthID: id, Provider: provider, Success: true})
			}
			// Disk stores and watchers reload JSON metadata, without runtime Quota.
			raw, err := json.Marshal(store.records[id].Metadata)
			if err != nil {
				t.Fatal(err)
			}
			var metadata map[string]any
			if err := json.Unmarshal(raw, &metadata); err != nil {
				t.Fatal(err)
			}
			windows := AccountBalanceWindows(&Auth{ID: id, Provider: provider, Metadata: metadata})
			if len(windows) != 2 || windows[0].UsedPercent != 20 || windows[1].UsedPercent != 80 || windows[0].ObservedAt.IsZero() || windows[1].Source != "response" {
				t.Fatalf("partial observations did not survive reload: %+v", windows)
			}
		})
	}
}
