package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	proxyconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestPassiveUsageDashboardNoCredentialsAndNoAuthWhenUnconfigured(t *testing.T) {
	server := newTestServerWithConfig(t, &proxyconfig.Config{Routing: proxyconfig.RoutingConfig{Strategy: "balanced", SessionAffinity: true}})
	_, err := server.handlers.AuthManager.Register(context.Background(), &auth.Auth{ID: "passive-test", Provider: "claude", Label: "Claude Work", Metadata: map[string]any{"access_token": "secret-must-not-appear", "windows": []auth.BalanceWindow{{Name: "5h", UsedPercent: 25, WindowSeconds: 18000, Source: "metadata"}}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/usage", "/dashboard"} {
		response := httptest.NewRecorder()
		server.engine.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != 200 {
			t.Fatalf("%s status %d", path, response.Code)
		}
		if strings.Contains(response.Body.String(), "secret-must-not-appear") {
			t.Fatal("credential leaked")
		}
		if path == "/usage" && !strings.Contains(response.Body.String(), `"used_percent":25`) {
			t.Fatalf("normalized quota missing: %s", response.Body)
		}
	}
}
