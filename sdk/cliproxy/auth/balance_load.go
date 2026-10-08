package auth

import (
	"context"
	"strconv"
	"sync"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

type balanceLoadTracker struct {
	mu        sync.Mutex
	sessions  map[string]map[string]int
	anonymous uint64
}

func (m *Manager) BalanceLoads() map[string]int {
	m.balanceLoad.mu.Lock()
	defer m.balanceLoad.mu.Unlock()
	return m.balanceLoadsLocked()
}
func (m *Manager) balanceLoadsLocked() map[string]int {
	out := make(map[string]int, len(m.balanceLoad.sessions))
	for id, sessions := range m.balanceLoad.sessions {
		out[id] = len(sessions)
	}
	return out
}

// pickExecutionAuth reserves under the same lock as the load snapshot and pick.
// Read-only SelectAuth deliberately does not reserve an execution slot.
func (m *Manager) pickExecutionAuth(ctx context.Context, providers []string, model string, opts cliproxyexecutor.Options, tried map[string]struct{}) (*Auth, ProviderExecutor, string, func(), error) {
	m.balanceLoad.mu.Lock()
	defer m.balanceLoad.mu.Unlock()
	ctx = context.WithValue(ctx, balanceLoadsKey{}, m.balanceLoadsLocked())
	a, e, p, err := m.pickNextMixed(ctx, providers, model, opts, tried)
	if err != nil {
		return a, e, p, func() {}, err
	}
	if m.balanceLoad.sessions == nil {
		m.balanceLoad.sessions = make(map[string]map[string]int)
	}
	sessions := m.balanceLoad.sessions[a.ID]
	if sessions == nil {
		sessions = make(map[string]int)
		m.balanceLoad.sessions[a.ID] = sessions
	}
	session, _ := opts.Metadata[cliproxyexecutor.CanonicalSessionIDMetadataKey].(string)
	if session == "" {
		session, _ = extractExplicitSessionIDs(opts.Headers, opts.OriginalRequest, opts.Metadata)
	}
	if session == "" {
		m.balanceLoad.anonymous++
		session = "anonymous:" + strconv.FormatUint(m.balanceLoad.anonymous, 10)
	}
	sessions[session]++
	id := a.ID
	var once sync.Once
	release := func() {
		once.Do(func() {
			m.balanceLoad.mu.Lock()
			defer m.balanceLoad.mu.Unlock()
			sessions := m.balanceLoad.sessions[id]
			sessions[session]--
			if sessions[session] <= 0 {
				delete(sessions, session)
			}
			if len(sessions) == 0 {
				delete(m.balanceLoad.sessions, id)
			}
		})
	}
	return a, e, p, release, nil
}

// wrapBalancedStream keeps upstream completion/result observation ordered before
// downstream closure, including cancellation. Cancellation releases load promptly
// and drains the already-cancelled producer so its final result is still recorded.
func wrapBalancedStream(ctx context.Context, result *cliproxyexecutor.StreamResult, release func()) *cliproxyexecutor.StreamResult {
	if result == nil || result.Chunks == nil {
		release()
		return result
	}
	out := make(chan cliproxyexecutor.StreamChunk)
	go func() {
		defer close(out)
		defer release()
		for {
			select {
			case <-ctx.Done():
				release()
				for range result.Chunks {
				}
				return
			case chunk, ok := <-result.Chunks:
				if !ok {
					return
				}
				select {
				case out <- chunk:
				case <-ctx.Done():
					release()
					for range result.Chunks {
					}
					return
				}
			}
		}
	}()
	return &cliproxyexecutor.StreamResult{Headers: result.Headers, Chunks: out}
}
