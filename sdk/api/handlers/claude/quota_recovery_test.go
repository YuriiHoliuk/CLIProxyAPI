package claude

import (
	"errors"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	auth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

func TestClaudeQuotaRecoveryResponse(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  int
		err     error
		seconds int64
	}{
		{"unknown quota", 429, errors.New("quota exhausted"), 3600},
		{"selection cooldown", 429, auth.NewModelCooldownError("claude-opus-5-5", "claude", 2*time.Hour), 7200},
		{"fast entitlement", 429, auth.NewRequestScopedError("Usage credits are required for fast mode", 429), 0},
		{"unrelated server error", 500, errors.New("broken"), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gin.SetMode(gin.TestMode)
			w := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(w)
			h := NewClaudeCodeAPIHandler(&handlers.BaseAPIHandler{})
			start := time.Now().Unix()
			h.WriteErrorResponse(c, &interfaces.ErrorMessage{StatusCode: tc.status, Error: tc.err})
			raw := w.Header().Get("Anthropic-Ratelimit-Unified-Reset")
			if tc.seconds == 0 {
				if raw != "" || gjson.GetBytes(w.Body.Bytes(), "error.reset_at").Exists() {
					t.Fatal("unrelated error gained recovery")
				}
				return
			}
			reset, err := strconv.ParseInt(raw, 10, 64)
			if err != nil || reset < start+tc.seconds || reset > time.Now().Unix()+tc.seconds {
				t.Fatalf("reset=%q", raw)
			}
			at, err := time.Parse(time.RFC3339, gjson.GetBytes(w.Body.Bytes(), "error.reset_at").String())
			if err != nil || at.Unix() != reset {
				t.Fatalf("body and headers disagree: %s", w.Body.String())
			}
			if w.Header().Get("Retry-After") != strconv.FormatInt(tc.seconds, 10) || w.Header().Get("Anthropic-Ratelimit-Unified-Representative-Claim") == "" {
				t.Fatal("SDK recovery signals absent")
			}
		})
	}
}
