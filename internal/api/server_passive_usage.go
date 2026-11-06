package api

import (
	_ "embed"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/util"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// passiveUsage exposes only local snapshots. It cannot call a provider or expose tokens.
func (s *Server) passiveUsage(c *gin.Context) {
	accounts := s.handlers.AuthManager.List()
	loads := s.handlers.AuthManager.BalanceLoads()
	weights := map[string]int64{}
	for _, a := range accounts {
		d := auth.AccountBalance(a, time.Now(), 1, 0)
		if d.Weight > weights[a.Provider] {
			weights[a.Provider] = d.Weight
		}
	}
	entries := make([]gin.H, 0, len(accounts))
	now := time.Now()
	spilling := auth.BalancedSpilling(s.handlers.AuthManager.Selector())
	results := s.handlers.AuthManager.WindowResultLog()
	for _, a := range accounts {
		// An idle account sends no observation that would record its reset.
		results.Record(a, now)
		balance := auth.AccountBalance(a, now, weights[a.Provider], loads[a.ID])
		if balance.ShortProjection != nil {
			balance.ShortProjection.Spilling = spilling[a.ID]
		}
		entries = append(entries, gin.H{"id": a.ID, "label": a.Label, "provider": a.Provider, "disabled": a.Disabled, "unavailable": a.Unavailable, "balance": balance})
	}
	c.JSON(http.StatusOK, gin.H{"observed_at": now, "strategy": s.cfg.Routing.Strategy, "session_affinity": s.cfg.Routing.SessionAffinity, "accounts": entries})
}

// passiveUsageResets returns each quota window's final usage at its reset,
// for the last ?days= days (default 90).
func (s *Server) passiveUsageResets(c *gin.Context) {
	days, errDays := strconv.Atoi(c.DefaultQuery("days", "90"))
	if errDays != nil || days < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "days must be a positive integer"})
		return
	}
	now := time.Now()
	results := s.handlers.AuthManager.WindowResultLog()
	for _, a := range s.handlers.AuthManager.List() {
		results.Record(a, now)
	}
	c.JSON(http.StatusOK, gin.H{"observed_at": now, "results": results.Results(now.AddDate(0, 0, -days))})
}

// windowResultLogPath keeps the log beside the config, or under WRITABLE_PATH.
func windowResultLogPath(configFilePath string) string {
	if base := util.WritablePath(); base != "" {
		return filepath.Join(base, "quota-resets.jsonl")
	}
	if configFilePath == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(configFilePath), "quota-resets.jsonl")
}

// passiveDashboardHTML draws every quota window as a bar on one shared time axis
// (a week tab and a 5-hour tab). It reads /usage only.
//
//go:embed passive_dashboard.html
var passiveDashboardHTML string

func (s *Server) passiveDashboard(c *gin.Context) {
	html := passiveDashboardHTML
	if len(s.cfg.APIKeys) == 0 {
		html = strings.Replace(html, "<details>", "<details hidden>", 1)
	}
	c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(html))
}
