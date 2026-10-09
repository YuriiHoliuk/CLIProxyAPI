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
func TestHaiku55RoutesFromLocalClaudeCatalog(t *testing.T) {
	models := registry.GetClaudeModels()
	modelRegistry := registry.GetGlobalRegistry()
	const client = "haiku-55-local-catalog"
	modelRegistry.RegisterClient(client, "claude", models)
	t.Cleanup(func() { modelRegistry.UnregisterClient(client) })
	handler := NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, coreauth.NewManager(nil, nil, nil))
	for _, model := range []string{"claude-haiku-5-5", "claude-haiku-5-5(medium)", "claude-haiku-5-5(xhigh)"} {
		providers, normalized, errMsg := handler.getRequestDetailsWithOptions(model, false, false)
		if errMsg != nil || !reflect.DeepEqual(providers, []string{"claude"}) || normalized != model {
			t.Fatalf("%s: providers=%v normalized=%s error=%v", model, providers, normalized, errMsg)
		}
	}
	for _, info := range models {
		if info.ID != "claude-haiku-5-5" {
			continue
		}
		if info.Thinking == nil || !info.Thinking.DynamicAllowed || !reflect.DeepEqual(info.Thinking.Levels, []string{"low", "medium", "high", "xhigh", "max"}) {
			t.Fatalf("Haiku 5.5 must support adaptive thinking and all five effort levels: %+v", info.Thinking)
		}
		return
	}
	t.Fatal("Haiku 5.5 missing from embedded Claude catalog")
}
