package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	claudeauth "github.com/router-for-me/CLIProxyAPI/v8/internal/auth/claude"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestClaudeProxyOnlyNativeWireAndNoAuxiliaryTraffic(t *testing.T) {
	raw, err := os.ReadFile("testdata/native-jetson-setup-token.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Body    string
		Headers map[string]string
	}
	if err = json.Unmarshal(raw, &fixture); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	nativeReply := []byte(`{"type":"message","content":[{"type":"text","text":"ok"}],"usage":{"input_tokens":1,"output_tokens":1}}`)
	nativeSSE := []byte("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":1}}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, _ := io.ReadAll(r.Body)
		if r.Method != http.MethodPost || (r.URL.Path != "/v1/messages" && r.URL.Path != "/v1/messages/count_tokens") {
			t.Errorf("auxiliary request: %s %s", r.Method, r.URL)
		}
		if r.Header.Get("Authorization") != "Bearer sk-ant-oat01-selected" {
			t.Error("wrong upstream token")
		}
		if r.URL.Path == "/v1/messages" && !bytes.Equal(body, []byte(fixture.Body)) {
			t.Error("native request business payload changed")
		}
		for k, v := range fixture.Headers {
			if strings.EqualFold(k, "Connection") {
				continue
			}
			if r.Header.Get(k) != v {
				t.Errorf("native header %s changed: %q -> %q", k, v, r.Header.Get(k))
			}
		}
		if gjson.GetBytes(body, "stream").Bool() {
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write(nativeSSE)
		} else {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(nativeReply)
		}
	}))
	defer server.Close()
	cfg := &config.Config{Claude: config.ClaudeConfig{ProxyOnly: true}}
	e := NewClaudeExecutor(cfg)
	a := &cliproxyauth.Auth{ID: "setup", Provider: "claude", Attributes: map[string]string{"api_key": "sk-ant-oat01-selected", "base_url": server.URL}, Metadata: map[string]any{"is_setup_token": true, "machine_device_id": strings.Repeat("d", 64), "setup_account_uuid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", "refresh_token": "must-never-be-used"}}
	headers := make(http.Header)
	for k, v := range fixture.Headers {
		headers.Set(k, v)
	}
	headers.Set("Authorization", "Bearer client-placeholder")
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatClaude, Headers: headers}
	request := cliproxyexecutor.Request{Model: gjson.Get(fixture.Body, "model").String(), Payload: []byte(fixture.Body)}
	// Native SDK stream framing and bytes survive unchanged.
	stream, err := e.ExecuteStream(context.Background(), a, request, opts)
	if err != nil {
		t.Fatal(err)
	}
	var got bytes.Buffer
	for chunk := range stream.Chunks {
		if chunk.Err != nil {
			t.Fatal(chunk.Err)
		}
		got.Write(chunk.Payload)
	}
	if !bytes.Equal(got.Bytes(), nativeSSE) {
		t.Fatal("native SSE mutated")
	}
	// Nonstream and count only send the corresponding caller request.
	nonstream := request
	nonstream.Payload = bytes.Replace(request.Payload, []byte(`"stream":true`), []byte(`"stream":false`), 1)
	original := fixture.Body
	fixture.Body = string(nonstream.Payload)
	resp, err := e.Execute(context.Background(), a, nonstream, opts)
	fixture.Body = original
	if err != nil || !bytes.Equal(resp.Payload, nativeReply) {
		t.Fatalf("nonstream: %s %v", resp.Payload, err)
	}
	count := cliproxyexecutor.Request{Model: request.Model, Payload: []byte(`{"model":"` + request.Model + `","messages":[{"role":"user","content":"x"}]}`)}
	if _, err = e.CountTokens(context.Background(), a, count, opts); err != nil {
		t.Fatal(err)
	}
	if e.ShouldPrepareRequestAuth(a) {
		t.Fatal("proxy-only requested auth preparation")
	}
	prepared, err := e.PrepareRequestAuth(context.Background(), a)
	if err != nil || !reflect.DeepEqual(prepared, a) {
		t.Fatal("auth preparation changed setup-token")
	}
	if _, err = e.Refresh(context.Background(), a); err == nil {
		t.Fatal("refresh not blocked")
	}
	e.oauthProfileFetcher = func(context.Context, *cliproxyauth.Auth, string) (*claudeauth.OAuthProfile, error) {
		t.Fatal("profile fetch called")
		return nil, nil
	}
	if _, err = e.fetchClaudeOAuthProfile(context.Background(), a, "token"); err == nil {
		t.Fatal("profile not blocked")
	}
	if calls.Load() != 3 {
		t.Fatalf("unexpected outgoing calls: %d", calls.Load())
	}
}

func TestClaudeProxyOnlyPermittedChangesAndNoCacheNormalization(t *testing.T) {
	e := NewClaudeExecutor(&config.Config{Claude: config.ClaudeConfig{ProxyOnly: true}, ClaudeHeaderDefaults: config.ClaudeHeaderDefaults{OS: "Linux", Arch: "arm64"}})
	a := &cliproxyauth.Auth{Provider: "claude", Attributes: map[string]string{"api_key": "sk-ant-oat01-test"}, Metadata: map[string]any{"is_setup_token": true, "machine_device_id": strings.Repeat("d", 64)}}
	body := []byte(`{"model":"claude-opus-4-6","metadata":{"user_id":"{\"device_id\":\"client\",\"account_uuid\":\"other\",\"session_id\":\"session\",\"parent_session_id\":\"parent\"}"},"system":[{"type":"text","text":"x-anthropic-billing-header: cc_version=2.1.280; cc_entrypoint=sdk-cli; cch=00000;","cache_control":{"type":"ephemeral","ttl":"5m"}},{"type":"text","text":"unchanged","cache_control":{"type":"ephemeral","ttl":"1h"}}],"messages":[{"role":"assistant","content":[{"type":"thinking","thinking":"","signature":"opaque"}]}],"thinking":{"type":"enabled","budget_tokens":999},"tool_choice":{"type":"any"},"temperature":0.5,"stream":true}`)
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatClaude, Headers: http.Header{"Anthropic-Beta": []string{"native-beta"}, "X-Stainless-Os": []string{"MacOS"}, "X-Stainless-Arch": []string{"x64"}}}
	httpReq, changed, err := e.prepareProxyOnlyClaudeRequest(context.Background(), a, cliproxyexecutor.Request{Model: "claude-opus-4-6", Payload: body}, opts, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"messages", "thinking", "tool_choice", "temperature", "system.0.cache_control", "system.1", "stream"} {
		if gjson.GetBytes(body, path).Raw != gjson.GetBytes(changed, path).Raw {
			t.Errorf("native field mutated: %s", path)
		}
	}
	identity := gjson.Parse(gjson.GetBytes(changed, "metadata.user_id").String())
	if identity.Get("device_id").String() != strings.Repeat("d", 64) || identity.Get("account_uuid").String() != "" || identity.Get("session_id").String() != "session" || identity.Get("parent_session_id").String() != "parent" {
		t.Fatal("identity contract changed")
	}
	if strings.Contains(gjson.GetBytes(changed, "system.0.text").String(), "cch=00000") {
		t.Fatal("CCH not re-signed")
	}
	if httpReq.Header.Get("Anthropic-Beta") != "native-beta" || httpReq.Header.Get("X-Stainless-Os") != "Linux" {
		t.Fatal("header contract changed")
	}
	opts.SourceFormat = sdktranslator.FormatOpenAI
	if _, _, err = e.prepareProxyOnlyClaudeRequest(context.Background(), a, cliproxyexecutor.Request{Model: "claude-opus-4-6", Payload: body}, opts, false); err == nil {
		t.Fatal("translated entrypoint accepted")
	}
}
