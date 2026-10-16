package cliproxy

import (
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

func TestSessionAffinityTTLNeverShorterThanPromptCache(t *testing.T) {
	for raw, want := range map[string]time.Duration{"": time.Hour, "30m": time.Hour, "1s": time.Hour, "1h": time.Hour, "2h": 2 * time.Hour, "bogus": time.Hour} {
		cfg := &config.Config{}
		cfg.Routing.SessionAffinity = true
		cfg.Routing.SessionAffinityTTL = raw
		if got := normalizedRoutingRuntimeState(cfg).sessionAffinityTTL; got != want {
			t.Fatalf("%q: got %v want %v", raw, got, want)
		}
	}
}
