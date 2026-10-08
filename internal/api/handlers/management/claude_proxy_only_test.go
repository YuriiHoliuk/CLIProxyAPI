package management

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestClaudeProxyOnlyManagementCannotProbeOrLogin(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.WriteHeader(200) }))
	defer upstream.Close()
	manager := coreauth.NewManager(nil, nil, nil)
	a, err := manager.Register(context.Background(), &coreauth.Auth{ID: "setup", Provider: "claude", Metadata: map[string]any{"access_token": "sk-ant-oat01-fixture"}})
	if err != nil {
		t.Fatal(err)
	}
	handler := &Handler{cfg: &config.Config{Claude: config.ClaudeConfig{ProxyOnly: true}}, authManager: manager}
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/call", handler.APICall)
	router.POST("/quota", handler.FetchCredentialQuota)
	router.POST("/login", handler.RequestAnthropicToken)
	for _, test := range []struct{ path, body string }{{"/call", `{"auth_index":"` + a.Index + `","method":"GET","url":"` + upstream.URL + `","header":{"Authorization":"Bearer $TOKEN$"}}`}, {"/quota", `{"auth_index":"` + a.Index + `"}`}, {"/login", `{}`}} {
		request := httptest.NewRequest(http.MethodPost, test.path, strings.NewReader(test.body))
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		router.ServeHTTP(response, request)
		if response.Code != http.StatusForbidden {
			t.Fatalf("%s got %d: %s", test.path, response.Code, response.Body)
		}
	}
	if calls.Load() != 0 {
		t.Fatal("Claude auxiliary traffic escaped")
	}
}
