package auth

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

// BalanceWindow is an observation from existing traffic or a local report, never a probe.
type BalanceWindow struct {
	Name          string    `json:"name"`
	UsedPercent   float64   `json:"used_percent"`
	WindowSeconds float64   `json:"window_seconds"`
	ResetsAt      time.Time `json:"resets_at,omitempty"`
	ObservedAt    time.Time `json:"observed_at,omitempty"`
	Source        string    `json:"source"`
}

type BalanceDetails struct {
	Weight     int64           `json:"weight"`
	Busy       int             `json:"busy"`
	Score      float64         `json:"score"`
	ShortScore float64         `json:"short_score"`
	LongScore  float64         `json:"long_score"`
	Windows    []BalanceWindow `json:"windows"`
}

// BalancedSelector matches Conveyor's remaining-capacity/reset-urgency score.
// Affinity wraps this selector and only calls it for cold bindings or failover.
type BalancedSelector struct{ Now func() time.Time }
type balanceLoadsKey struct{}

func (s *BalancedSelector) Pick(ctx context.Context, provider, model string, opts cliproxyexecutor.Options, auths []*Auth) (*Auth, error) {
	now := time.Now()
	if s.Now != nil {
		now = s.Now()
	}
	available, err := getSelectorAvailableAuths(ctx, positiveWeightAuths(auths), provider, model, now)
	if err != nil {
		return nil, err
	}
	loads, _ := ctx.Value(balanceLoadsKey{}).(map[string]int)
	maxWeight := int64(1)
	for _, a := range auths {
		if w := authWeight(a); w > maxWeight {
			maxWeight = w
		}
	}
	sort.SliceStable(available, func(i, j int) bool { return available[i].ID < available[j].ID })
	var best *Auth
	var bestScore BalanceDetails
	for _, a := range available {
		d := AccountBalance(a, now, maxWeight, loads[a.ID])
		if best == nil || d.Score > bestScore.Score || (d.Score == bestScore.Score && (d.LongScore > bestScore.LongScore || (d.LongScore == bestScore.LongScore && d.ShortScore > bestScore.ShortScore))) {
			best, bestScore = a, d
		}
	}
	if best == nil {
		return nil, &Error{Code: "auth_not_found", Message: "no positive-weight credential available"}
	}
	return best, nil
}

// AccountBalance is shared by routing and the passive dashboard.
func AccountBalance(a *Auth, now time.Time, maxWeight int64, busy int) BalanceDetails {
	d := BalanceDetails{Weight: authWeight(a), Busy: busy, Windows: AccountBalanceWindows(a)}
	if maxWeight < 1 {
		maxWeight = 1
	}
	if busy < 0 {
		busy = 0
	}
	short, long := (*BalanceWindow)(nil), (*BalanceWindow)(nil)
	for i := range d.Windows {
		w := &d.Windows[i]
		if w.WindowSeconds <= 86400 {
			if short == nil || w.WindowSeconds < short.WindowSeconds {
				short = w
			}
		} else if long == nil || w.WindowSeconds > long.WindowSeconds {
			long = w
		}
	}
	share := float64(d.Weight) / float64(busy+1)
	d.ShortScore = balanceWindowScore(short, now, 5, 1, 1, share, float64(maxWeight))
	d.LongScore = balanceWindowScore(long, now, 168, 5, 24, share, float64(maxWeight))
	d.Score = d.LongScore * math.Min(d.ShortScore/0.5, 1)
	return d
}

func balanceWindowScore(w *BalanceWindow, now time.Time, hours, resetFloor, elapsedFloor, share, maxWeight float64) float64 {
	pace := 1 / hours
	if w != nil {
		hours = w.WindowSeconds / 3600
		if hours != 5 && hours != 168 {
			resetFloor = math.Max(hours/168*5, math.Min(hours, 1))
			elapsedFloor = math.Max(hours/168*24, math.Min(hours, 1))
		}
		pace = 1 / hours
		if w.ResetsAt.IsZero() || w.ResetsAt.After(now) {
			used := w.UsedPercent / 100
			if !w.ObservedAt.IsZero() && now.Sub(w.ObservedAt) > time.Hour && !w.ResetsAt.IsZero() {
				elapsed := math.Max(w.ObservedAt.Sub(w.ResetsAt.Add(-time.Duration(w.WindowSeconds)*time.Second)).Hours(), elapsedFloor)
				used = math.Min(1, used+used/elapsed*now.Sub(w.ObservedAt).Hours())
			}
			remainingHours := hours
			if !w.ResetsAt.IsZero() {
				remainingHours = math.Max(w.ResetsAt.Sub(now).Hours(), resetFloor)
			}
			pace = math.Max(0, 1-used) / remainingHours
		}
	}
	return share * pace / (maxWeight / hours)
}

// AccountBalanceWindows combines valid observations by freshness per duration.
func AccountBalanceWindows(a *Auth) []BalanceWindow {
	if a == nil {
		return nil
	}
	windows := make(map[float64]BalanceWindow)
	add := func(w BalanceWindow) {
		if math.IsNaN(w.UsedPercent) || math.IsInf(w.UsedPercent, 0) || w.UsedPercent < 0 || w.UsedPercent > 100 || w.WindowSeconds <= 0 || math.IsNaN(w.WindowSeconds) || math.IsInf(w.WindowSeconds, 0) {
			return
		}
		old, ok := windows[w.WindowSeconds]
		if !ok || w.ObservedAt.After(old.ObservedAt) {
			windows[w.WindowSeconds] = w
		}
	}
	for _, w := range a.Quota.BalanceWindows {
		add(w)
	}
	signals := http.Header{}
	for k, v := range a.Quota.Signals {
		signals.Set(k, v)
	}
	number := func(k string) (float64, bool) {
		v, err := strconv.ParseFloat(signals.Get(k), 64)
		return v, err == nil && !math.IsNaN(v) && !math.IsInf(v, 0)
	}
	reset := func(k string) time.Time {
		v, ok := number(k)
		if ok && v > 0 {
			return time.Unix(int64(v), 0).UTC()
		}
		if t, err := time.Parse(time.RFC3339, signals.Get(k)); err == nil {
			return t
		}
		return time.Time{}
	}
	for _, p := range []struct {
		name string
		secs float64
	}{{"5h", 18000}, {"7d", 604800}} {
		prefix := "Anthropic-Ratelimit-Unified-" + p.name + "-"
		if used, ok := number(prefix + "Utilization"); ok {
			add(BalanceWindow{Name: p.name, UsedPercent: used * 100, WindowSeconds: p.secs, ResetsAt: reset(prefix + "Reset"), ObservedAt: a.Quota.ObservedAt, Source: "response"})
		}
	}
	for _, name := range []string{"Primary", "Secondary"} {
		prefix := "X-Codex-" + name + "-"
		used, ok := number(prefix + "Used-Percent")
		mins, okMinutes := number(prefix + "Window-Minutes")
		if !ok || !okMinutes {
			continue
		}
		at := reset(prefix + "Reset-At")
		if at.IsZero() && !a.Quota.ObservedAt.IsZero() {
			if secs, valid := number(prefix + "Reset-After-Seconds"); valid && secs >= 0 {
				at = a.Quota.ObservedAt.Add(time.Duration(secs) * time.Second)
			}
		}
		add(BalanceWindow{Name: strings.ToLower(name), UsedPercent: used, WindowSeconds: mins * 60, ResetsAt: at, ObservedAt: a.Quota.ObservedAt, Source: "response"})
	}
	var inline []BalanceWindow
	if raw, err := json.Marshal(a.Metadata["windows"]); err == nil {
		if json.Unmarshal(raw, &inline) == nil {
			for _, w := range inline {
				if w.Source == "" {
					w.Source = "metadata"
				}
				add(w)
			}
		}
	}
	if path, ok := a.Metadata["windows_file"].(string); ok && path != "" {
		if strings.HasPrefix(path, "~/") {
			if home, err := os.UserHomeDir(); err == nil {
				path = home + path[1:]
			}
		}
		if f, err := os.Open(path); err == nil {
			raw, readErr := io.ReadAll(io.LimitReader(f, 65537))
			_ = f.Close()
			var report struct {
				ReportedAt json.RawMessage `json:"reported_at"`
				UpdatedAt  json.RawMessage `json:"updated_at"`
				Five       *struct {
					Used  float64         `json:"used_percentage"`
					Reset json.RawMessage `json:"resets_at"`
				} `json:"five_hour"`
				Seven *struct {
					Used  float64         `json:"used_percentage"`
					Reset json.RawMessage `json:"resets_at"`
				} `json:"seven_day"`
			}
			if readErr == nil && len(raw) <= 65536 && json.Unmarshal(raw, &report) == nil {

				localReset := func(raw json.RawMessage) time.Time {
					var t time.Time
					if json.Unmarshal(raw, &t) == nil {
						return t
					}
					var n float64
					if json.Unmarshal(raw, &n) == nil && n > 0 {
						return time.Unix(int64(n), 0).UTC()
					}
					return time.Time{}
				}
				at := localReset(report.ReportedAt)
				if at.IsZero() {
					at = localReset(report.UpdatedAt)
				}
				if report.Five != nil {
					add(BalanceWindow{Name: "five_hour", UsedPercent: report.Five.Used, WindowSeconds: 18000, ResetsAt: localReset(report.Five.Reset), ObservedAt: at, Source: "local_file"})
				}
				if report.Seven != nil {
					add(BalanceWindow{Name: "seven_day", UsedPercent: report.Seven.Used, WindowSeconds: 604800, ResetsAt: localReset(report.Seven.Reset), ObservedAt: at, Source: "local_file"})
				}
			}
		}
	}
	out := make([]BalanceWindow, 0, len(windows))
	for _, w := range windows {
		out = append(out, w)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].WindowSeconds < out[j].WindowSeconds })
	return out
}
