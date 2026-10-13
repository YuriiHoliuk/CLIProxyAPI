package test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	executor "github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers/claude"
	auth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestClaudePoolRecoveryProtocol(t *testing.T) {
	for _, scenario := range []string{"known", "cached", "fallback"} {
		for _, mode := range []string{"execute", "stream", "count"} {
			t.Run(scenario+"/"+mode, func(t *testing.T) {
				now := time.Now()
				firstReset := now.Add(2 * time.Hour)
				if scenario == "fallback" {
					firstReset = now.Add(time.Hour)
				}
				var attempts atomic.Int32
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					attempts.Add(1)
					io.Copy(io.Discard, r.Body)
					// The earliest account responds first, so the final error has a later reset.
					delay := 4 * time.Hour
					if strings.HasSuffix(r.Header.Get("Authorization"), "a") {
						delay = 2 * time.Hour
					}
					if scenario == "known" {
						w.Header().Set("Anthropic-Ratelimit-Unified-5h-Status", "rejected")
						w.Header().Set("Anthropic-Ratelimit-Unified-5h-Reset", strconv.FormatInt(now.Add(delay).Unix(), 10))
					}
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(429)
					io.WriteString(w, `{"type":"error","error":{"type":"rate_limit_error","message":"quota exhausted"}}`)
				}))
				defer upstream.Close()
				cfg := &config.Config{Claude: config.ClaudeConfig{ProxyOnly: true}}
				m := auth.NewManager(nil, &auth.RoundRobinSelector{}, nil)
				m.SetConfig(cfg)
				m.SetRetryConfig(0, 0, 0)
				m.RegisterExecutor(executor.NewClaudeExecutor(cfg))
				model := "claude-opus-5-5"
				for _, id := range []string{"a", "b", "c"} {
					id = "recovery-" + scenario + "-" + mode + "-" + id
					account := &auth.Auth{ID: id, Provider: "claude", Status: auth.StatusActive, Attributes: map[string]string{"api_key": "sk-ant-oat01-" + id, "base_url": upstream.URL}, Metadata: map[string]any{"is_setup_token": true, "machine_device_id": strings.Repeat("d", 64)}}
					if scenario == "cached" {
						delay := 4 * time.Hour
						if strings.HasSuffix(id, "a") {
							delay = 2 * time.Hour
						}
						account.Quota.BalanceWindows = []auth.BalanceWindow{{Name: "5h", WindowSeconds: 18000, UsedPercent: 97, ResetsAt: now.Add(delay)}}
					}
					_, err := m.Register(context.Background(), account)
					if err != nil {
						t.Fatal(err)
					}
					registry.GetGlobalRegistry().RegisterClient(id, "claude", []*registry.ModelInfo{{ID: model}})
					t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
				}
				gin.SetMode(gin.TestMode)
				h := claude.NewClaudeCodeAPIHandler(handlers.NewBaseAPIHandlers(&cfg.SDKConfig, m))
				router := gin.New()
				router.POST("/v1/messages", h.ClaudeMessages)
				router.POST("/v1/messages/count_tokens", h.ClaudeCountTokens)
				proxy := httptest.NewServer(router)
				defer proxy.Close()
				for i := 0; i < 2; i++ {
					path := "/v1/messages"
					if mode == "count" {
						path += "/count_tokens"
					}
					payload := `{"model":"claude-opus-5-5","messages":[{"role":"user","content":"ok"}]}`
					if mode == "stream" {
						payload = `{"model":"claude-opus-5-5","stream":true,"messages":[{"role":"user","content":"ok"}]}`
					}
					r, _ := http.NewRequest("POST", proxy.URL+path, strings.NewReader(payload))
					r.Header.Set("User-Agent", "claude-cli/2.1.296 (external, cli)")
					response, err := http.DefaultClient.Do(r)
					if err != nil {
						t.Fatal(err)
					}
					body, _ := io.ReadAll(response.Body)
					response.Body.Close()
					reset, _ := strconv.ParseInt(response.Header.Get("Anthropic-Ratelimit-Unified-Reset"), 10, 64)
					if response.StatusCode != 429 || reset < firstReset.Unix() || reset > firstReset.Add(35*time.Second).Unix() {
						t.Fatalf("pool reset=%d status=%d body=%s", reset, response.StatusCode, body)
					}
					if response.Header.Get("Anthropic-Ratelimit-Unified-Representative-Claim") != "five_hour" {
						t.Fatal("Claude SDK usage-limit classification missing")
					}
				}
				if attempts.Load() != 3 {
					t.Fatalf("attempts=%d, want 3 then local cooldown", attempts.Load())
				}
				if binary := os.Getenv("CLIPROXY_CLAUDE_PROOF"); binary != "" && mode == "stream" {
					cmd := exec.Command(binary, "-p", "Reply OK", "--verbose", "--output-format", "stream-json", "--max-turns", "1", "--tools", "")
					cmd.Dir = t.TempDir()
					cmd.Env = append(os.Environ(), "ANTHROPIC_BASE_URL="+proxy.URL, "CLAUDE_CODE_OAUTH_TOKEN=sk-ant-oat01-proof-placeholder", "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC=1", "DISABLE_AUTOUPDATER=1")
					out, err := cmd.Output()
					if err == nil {
						t.Fatal("CLI should stop on quota")
					}
					found := false
					for _, line := range strings.Split(string(out), "\n") {
						var event struct {
							Type string `json:"type"`
							Info struct {
								Status string `json:"status"`
								Reset  int64  `json:"resetsAt"`
							} `json:"rate_limit_info"`
						}
						if json.Unmarshal([]byte(line), &event) == nil && event.Type == "rate_limit_event" && event.Info.Status == "rejected" && event.Info.Reset >= firstReset.Unix() {
							found = true
							t.Log(string(line))
						}
					}
					if !found {
						t.Fatalf("CLI did not emit reset event: %s", out)
					}
				}
			})
		}
	}
}
