package handlers

import (
	"reflect"
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestWithoutProxyOnlyClaudeModelsHidesClaudeFromOpenAIClients(t *testing.T) {
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient("proxy-only-claude", "claude", []*registry.ModelInfo{{ID: "proxy-only-claude-model"}, {ID: "proxy-only-shared-model"}})
	reg.RegisterClient("proxy-only-codex", "codex", []*registry.ModelInfo{{ID: "proxy-only-codex-model"}, {ID: "proxy-only-shared-model"}})
	t.Cleanup(func() {
		reg.UnregisterClient("proxy-only-claude")
		reg.UnregisterClient("proxy-only-codex")
	})
	list := func() []map[string]any {
		return []map[string]any{{"id": "proxy-only-claude-model"}, {"id": "proxy-only-codex-model"}, {"id": "proxy-only-shared-model"}}
	}
	ids := func(models []map[string]any) []string {
		out := make([]string, 0, len(models))
		for _, model := range models {
			out = append(out, model["id"].(string))
		}
		return out
	}

	manager := coreauth.NewManager(nil, nil, nil)
	h := NewBaseAPIHandlers(nil, manager)
	if got := ids(h.WithoutProxyOnlyClaudeModels(list())); len(got) != 3 {
		t.Fatalf("without proxy-only: %v", got)
	}
	manager.SetConfig(&internalconfig.Config{Claude: internalconfig.ClaudeConfig{ProxyOnly: true}})
	got := ids(h.WithoutProxyOnlyClaudeModels(list()))
	if want := []string{"proxy-only-codex-model", "proxy-only-shared-model"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("proxy-only: got %v, want %v", got, want)
	}
}
