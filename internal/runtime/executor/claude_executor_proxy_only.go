package executor

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/thinking"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// prepareProxyOnlyClaudeRequest preserves the native business payload and headers.
// Only credential/machine identity, resolved model, explicit payload rules and CCH change.
func (e *ClaudeExecutor) prepareProxyOnlyClaudeRequest(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, count bool) (*http.Request, []byte, error) {
	if opts.SourceFormat != sdktranslator.FormatClaude {
		return nil, nil, statusErr{code: 400, msg: "Claude proxy-only requires native Claude Code/Agent SDK requests"}
	}
	token, base := claudeCreds(auth)
	if base == "" {
		base = "https://api.anthropic.com"
	}
	if !isClaudeSetupToken(auth, token) {
		return nil, nil, statusErr{code: 400, msg: "Claude proxy-only requires a setup-token credential"}
	}
	body := append([]byte(nil), req.Payload...)
	if !gjson.ValidBytes(body) {
		return nil, nil, statusErr{code: 400, msg: "invalid native Claude JSON"}
	}
	body = helps.SetStringIfDifferent(body, "model", e.upstreamModel(thinking.ParseSuffix(req.Model).ModelName))
	if user := gjson.GetBytes(body, "metadata.user_id"); user.Exists() {
		if user.Type != gjson.String || !gjson.Valid(user.String()) {
			return nil, nil, statusErr{code: 400, msg: "invalid native Claude user_id"}
		}
		identity := []byte(user.String())
		device, _ := auth.Metadata["machine_device_id"].(string)
		if device == "" {
			return nil, nil, statusErr{code: 400, msg: "Claude proxy-only requires machine_device_id"}
		}
		var err error
		identity, err = sjson.SetBytes(identity, "device_id", device)
		if err != nil {
			return nil, nil, err
		}
		// Native setup-token auth has no OAuth account profile. Do not invent a UUID.
		account, _ := auth.Metadata["setup_account_uuid"].(string)
		identity, err = sjson.SetBytes(identity, "account_uuid", account)
		if err != nil {
			return nil, nil, err
		}
		body, err = sjson.SetBytes(body, "metadata.user_id", string(identity))
		if err != nil {
			return nil, nil, err
		}
	}
	original := body
	body = helps.ApplyPayloadConfigWithRequest(e.cfg, thinking.ParseSuffix(req.Model).ModelName, "claude", "claude", "", body, original, helps.PayloadRequestedModel(opts, req.Model), helps.PayloadRequestPath(opts), opts.Headers)
	if !count {
		// Re-sign an existing native billing block, without injecting a new block.
		var err error
		body, err = signAnthropicMessagesBody(body)
		if err != nil {
			return nil, nil, fmt.Errorf("sign native CCH: %w", err)
		}
	}
	path := "/v1/messages"
	if count {
		path += "/count_tokens"
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(base, "/")+path+"?beta=true", bytes.NewReader(body))
	if err != nil {
		return nil, nil, err
	}
	request.Header = opts.Headers.Clone()
	if request.Header == nil {
		request.Header = make(http.Header)
	}
	// Never send tailnet/client authentication, proxy headers or hop-by-hop framing upstream.
	for _, key := range strings.Split(request.Header.Get("Connection"), ",") {
		request.Header.Del(strings.TrimSpace(key))
	}
	for _, key := range []string{"Authorization", "X-Api-Key", "Proxy-Authorization", "Proxy-Connection", "Connection", "Keep-Alive", "Transfer-Encoding", "Te", "Trailer", "Upgrade", "Content-Length", "Host", "Forwarded", "X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto"} {
		request.Header.Del(key)
	}
	if strings.EqualFold(opts.Headers.Get("Connection"), "keep-alive") {
		request.Header.Set("Connection", "keep-alive")
	}
	request.Header.Set("Authorization", "Bearer "+token)
	if request.Header.Get("Content-Type") == "" {
		request.Header.Set("Content-Type", "application/json")
	}
	// Pin only machine-specific headers; native caller owns software/SDK versions and betas.
	if e.cfg.ClaudeHeaderDefaults.OS != "" {
		request.Header.Set("X-Stainless-Os", e.cfg.ClaudeHeaderDefaults.OS)
	}
	if e.cfg.ClaudeHeaderDefaults.Arch != "" {
		request.Header.Set("X-Stainless-Arch", e.cfg.ClaudeHeaderDefaults.Arch)
	}
	return request, body, nil
}

func (e *ClaudeExecutor) proxyOnlyClaudeHTTP(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, count bool) (*http.Response, io.ReadCloser, *helps.UsageReporter, error) {
	request, body, err := e.prepareProxyOnlyClaudeRequest(ctx, auth, req, opts, count)
	if err != nil {
		return nil, nil, nil, err
	}
	reporter := helps.NewExecutorUsageReporter(ctx, e, req.Model, auth)
	authID, authLabel, authType, authValue := claudeAuthLogIdentity(auth)
	helps.RecordAPIRequest(ctx, e.cfg, helps.UpstreamRequestLog{URL: request.URL.String(), Method: request.Method, Headers: request.Header.Clone(), Body: body, Provider: e.upstreamRequestLogProvider(), AuthID: authID, AuthLabel: authLabel, AuthType: authType, AuthValue: authValue})
	client := reporter.TrackHTTPClient(helps.NewUtlsHTTPClient(ctx, e.cfg, auth, 0))
	response, err := doClaudeUpstreamRequest(client, request)
	if err != nil {
		return nil, nil, reporter, err
	}
	helps.RecordAPIResponseMetadata(ctx, e.cfg, response.StatusCode, response.Header.Clone())
	decoded, err := decodeResponseBody(response.Body, claudeResponseContentEncoding(response.Header))
	if err != nil {
		_ = response.Body.Close()
		return nil, nil, reporter, err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		data, readErr := io.ReadAll(decoded)
		_ = decoded.Close()
		if readErr != nil {
			return nil, nil, reporter, readErr
		}
		return nil, nil, reporter, classifyClaudeUpstreamErrorWithCooling(response.StatusCode, response.Header, data, e.modelLevelCooling())
	}
	return response, decoded, reporter, nil
}

func (e *ClaudeExecutor) executeProxyOnlyClaude(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options, count bool) (resp cliproxyexecutor.Response, err error) {
	response, body, reporter, err := e.proxyOnlyClaudeHTTP(ctx, auth, req, opts, count)
	if reporter != nil {
		defer reporter.TrackFailure(ctx, &err)
	}
	if err != nil {
		return resp, err
	}
	defer body.Close()
	data, err := io.ReadAll(body)
	if err != nil {
		return resp, err
	}
	if !count {
		reporter.Publish(ctx, helps.ParseClaudeUsage(data))
	}
	return cliproxyexecutor.Response{Payload: data, Headers: response.Header.Clone()}, nil
}

func (e *ClaudeExecutor) streamProxyOnlyClaude(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (_ *cliproxyexecutor.StreamResult, err error) {
	response, body, reporter, err := e.proxyOnlyClaudeHTTP(ctx, auth, req, opts, false)
	if reporter != nil {
		defer reporter.TrackFailure(ctx, &err)
	}
	if err != nil {
		return nil, err
	}
	out := make(chan cliproxyexecutor.StreamChunk, 1)
	go func() {
		defer close(out)
		defer body.Close()
		reader := bufio.NewReader(body)
		var usage helps.StreamUsageBuffer
		for {
			line, readErr := reader.ReadBytes('\n')
			if len(line) > 0 {
				usage.ObserveClaudeStream(bytes.TrimSpace(line))
				select {
				case out <- cliproxyexecutor.StreamChunk{Payload: line}:
				case <-ctx.Done():
					return
				}
			}
			if readErr != nil {
				if readErr != io.EOF {
					select {
					case out <- cliproxyexecutor.StreamChunk{Err: readErr}:
					case <-ctx.Done():
					}
					usage.PublishFailure(ctx, reporter, readErr)
				} else {
					usage.Publish(ctx, reporter)
				}
				return
			}
		}
	}()
	return &cliproxyexecutor.StreamResult{Headers: response.Header.Clone(), Chunks: out}, nil
}
