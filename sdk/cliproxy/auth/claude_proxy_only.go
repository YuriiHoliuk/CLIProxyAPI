package auth

import internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"

// ClaudeProxyOnly reports whether Claude credentials serve only native Claude Code requests.
func (m *Manager) ClaudeProxyOnly() bool {
	if m == nil {
		return false
	}
	cfg, _ := m.runtimeConfig.Load().(*internalconfig.Config)
	return cfg != nil && cfg.Claude.ProxyOnly
}
