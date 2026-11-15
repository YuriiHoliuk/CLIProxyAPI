package handlers

import (
	"reflect"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

// With --local-model there is no catalog fetch to discover a newly released model.
// Register exactly the embedded Claude catalog, as setup-token accounts do.
func TestNewClaudeModelsRouteFromLocalClaudeCatalog(t *testing.T) {
	models := registry.GetClaudeModels()
	modelRegistry := registry.GetGlobalRegistry()
	const client = "claude-local-catalog"
	modelRegistry.RegisterClient(client, "claude", models)
	t.Cleanup(func() { modelRegistry.UnregisterClient(client) })
	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, coreauth.NewManager(nil, nil, nil))
	for _, id := range []string{"claude-haiku-5-5", "claude-fable-5-2"} {
		for _, model := range []string{id, id + "(medium)", id + "(xhigh)"} {
			providers, normalized, errMsg := handler.getRequestDetailsWithOptions(model, false, false)
			if errMsg != nil || !reflect.DeepEqual(providers, []string{"claude"}) || normalized != model {
				t.Fatalf("%s: providers=%v normalized=%s error=%v", model, providers, normalized, errMsg)
			}
		}
		found := false
		for _, info := range models {
			if info.ID != id {
				continue
			}
			found = true
			if info.Thinking == nil || !info.Thinking.DynamicAllowed || !reflect.DeepEqual(info.Thinking.Levels, []string{"low", "medium", "high", "xhigh", "max"}) {
				t.Fatalf("%s must support adaptive thinking and all five effort levels: %+v", id, info.Thinking)
			}
		}
		if !found {
			t.Fatalf("%s missing from embedded Claude catalog", id)
		}
	}
}
