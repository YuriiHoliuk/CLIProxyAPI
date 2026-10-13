package executor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
)

// Subscription credentials must never serve a client outside Claude Code.
func TestClaudeProxyOnlyRejectsNonClaudeCodeClients(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"type":"message","content":[],"usage":{"input_tokens":1,"output_tokens":1}}`))
	}))
	defer server.Close()
	e := NewClaudeExecutor(&config.Config{Claude: config.ClaudeConfig{ProxyOnly: true}})
	a := &cliproxyauth.Auth{ID: "setup", Provider: "claude", Attributes: map[string]string{"api_key": "sk-ant-oat01-selected", "base_url": server.URL}, Metadata: map[string]any{"is_setup_token": true, "machine_device_id": strings.Repeat("d", 64)}}
	request := cliproxyexecutor.Request{Model: "claude-opus-5-5", Payload: []byte(`{"model":"claude-opus-5-5","max_tokens":8,"messages":[{"role":"user","content":"x"}]}`)}
	cases := []struct {
		name   string
		format sdktranslator.Format
		agent  string
	}{
		{"codex responses", sdktranslator.FormatOpenAIResponse, "codex_cli_rs/0.150.0"},
		{"openai chat", sdktranslator.FormatOpenAI, "OpenAI/Python 2.0.0"},
		{"anthropic sdk", sdktranslator.FormatClaude, "Anthropic/Python 0.70.0"},
		{"curl", sdktranslator.FormatClaude, "curl/8.7.1"},
		{"no user agent", sdktranslator.FormatClaude, ""},
		{"spoofed prefix", sdktranslator.FormatClaude, "claude-cli/2.1.285 custom-gateway"},
	}
	for _, tc := range cases {
		headers := make(http.Header)
		if tc.agent != "" {
			headers.Set("User-Agent", tc.agent)
		}
		opts := cliproxyexecutor.Options{SourceFormat: tc.format, Headers: headers}
		if _, err := e.Execute(context.Background(), a, request, opts); err == nil {
			t.Errorf("%s: request accepted", tc.name)
		}
		if _, err := e.ExecuteStream(context.Background(), a, request, opts); err == nil {
			t.Errorf("%s: stream accepted", tc.name)
		}
		if _, err := e.CountTokens(context.Background(), a, request, opts); err == nil {
			t.Errorf("%s: count_tokens accepted", tc.name)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("rejected clients reached Anthropic %d times", calls.Load())
	}

	headers := make(http.Header)
	headers.Set("User-Agent", "claude-cli/2.1.285 (external, sdk-ts, agent-sdk/0.2.140)")
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatClaude, Headers: headers}
	if _, err := e.Execute(context.Background(), a, request, opts); err != nil {
		t.Fatalf("Agent SDK request rejected: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("Agent SDK request calls: %d", calls.Load())
	}
}
