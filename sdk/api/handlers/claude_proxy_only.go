package handlers

import (
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
)

// WithoutProxyOnlyClaudeModels drops Claude-only models from non-Claude model lists.
// In Claude proxy-only mode those models serve native Claude Code requests only,
// so OpenAI-format clients such as Codex must not be offered them.
func (h *BaseAPIHandler) WithoutProxyOnlyClaudeModels(models []map[string]any) []map[string]any {
	if h == nil || !h.AuthManager.ClaudeProxyOnly() {
		return models
	}
	modelRegistry := registry.GetGlobalRegistry()
	kept := models[:0]
	for _, model := range models {
		id, _ := model["id"].(string)
		if isClaudeOnlyModel(modelRegistry.GetModelProviders(id)) {
			continue
		}
		kept = append(kept, model)
	}
	return kept
}

func isClaudeOnlyModel(providers []string) bool {
	if len(providers) == 0 {
		return false
	}
	for _, provider := range providers {
		if !strings.EqualFold(provider, "claude") {
			return false
		}
	}
	return true
}
