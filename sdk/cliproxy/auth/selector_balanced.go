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
	"sync"
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
	// Model limits the window to one model family ("fable"). Empty applies to every model.
	Model string `json:"model,omitempty"`
}

type BalanceDetails struct {
	Weight     int64   `json:"weight"`
	Busy       int     `json:"busy"`
	Score      float64 `json:"score"`
	ShortScore float64 `json:"short_score"`
	LongScore  float64 `json:"long_score"`
	// PaceGap is how many percentage points the long window trails an even
	// burn (negative: ahead of pace). For a Fable request it is the lower of
	// the weekly and Fable gaps.
	PaceGap float64         `json:"pace_gap"`
	Windows []BalanceWindow `json:"windows"`
	// ShortProjection is nil when the short window or its reset is unknown.
	ShortProjection *BalanceProjection `json:"short_projection,omitempty"`
}

// BalanceProjection estimates short-window (5h) utilization at its reset.
type BalanceProjection struct {
	WindowSeconds    float64   `json:"window_seconds"`
	UsedPercent      float64   `json:"used_percent"`
	ProjectedPercent float64   `json:"projected_percent"`
	BurnRatePerHour  float64   `json:"burn_rate_percent_per_hour"`
	RateSource       string    `json:"rate_source"`
	ResetsAt         time.Time `json:"resets_at"`
	Spilling         bool      `json:"spilling"`
}

// MinSessionAffinityTTL is the floor for configured affinity TTLs: the prompt
// cache Claude Code writes lives 1h, and spillover must never move a session
// whose cache is still warm.
const MinSessionAffinityTTL = time.Hour

const (
	// A recent rate needs samples spread over at least balanceMinRateSpan within the lookback.
	balanceRateLookback = time.Hour
	balanceMinRateSpan  = 15 * time.Minute
	// The elapsed-average fallback treats a younger window as this old.
	balanceYoungWindowFloor = 30 * time.Minute
	// History is thinned to one sample per spacing and kept for the retention period.
	balanceHistorySpacing   = 2 * time.Minute
	balanceHistoryRetention = 90 * time.Minute
	balanceHistoryMax       = 64
	// An account starts spilling above balanceSpillEnter and stops at or below balanceSpillExit.
	balanceSpillEnter = 100.0
	balanceSpillExit  = 95.0
	// balanceSessionCost is the share of a weight-1 long window, in percentage
	// points, that each busy session is assumed to claim. It spreads parallel
	// new sessions only when accounts are close to each other's pace.
	balanceSessionCost = 2.0
	// FableWindowModel marks the Fable-only weekly window (Claude 7d_oi).
	FableWindowModel = "fable"
)

// BalancedSelector ranks accounts by pace: the share of the long (weekly)
// window that has elapsed minus the share used. The account furthest behind
// its pace takes a new session, so every account tracks its own reset instead
// of one draining before the next. A Fable request ranks on the tighter of the
// weekly and Fable windows. A new session goes to the top account unless its
// projected 5h use at reset exceeds 100%; then it spills down the ranking to
// the first account whose projection fits. Affinity wraps this selector and
// only calls it for cold bindings or failover.
type BalancedSelector struct {
	Now func() time.Time

	mu       sync.Mutex
	spilling map[string]bool
}
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
	if len(available) == 0 {
		return nil, &Error{Code: "auth_not_found", Message: "no positive-weight credential available"}
	}
	sort.SliceStable(available, func(i, j int) bool { return available[i].ID < available[j].ID })
	ranked := make([]BalanceDetails, len(available))
	for i, a := range available {
		ranked[i] = AccountBalance(a, now, model, loads[a.ID])
	}
	order := make([]int, len(available))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(i, j int) bool {
		a, b := ranked[order[i]], ranked[order[j]]
		if a.Score != b.Score {
			return a.Score > b.Score
		}
		if a.LongScore != b.LongScore {
			return a.LongScore > b.LongScore
		}
		return a.ShortScore > b.ShortScore
	})

	s.mu.Lock()
	if s.spilling == nil {
		s.spilling = make(map[string]bool)
	}
	fits := make([]bool, len(available))
	for i, a := range available {
		p := ranked[i].ShortProjection
		switch {
		case p == nil:
			delete(s.spilling, a.ID)
		case s.spilling[a.ID]:
			s.spilling[a.ID] = p.ProjectedPercent > balanceSpillExit
		default:
			s.spilling[a.ID] = p.ProjectedPercent > balanceSpillEnter
		}
		fits[i] = !s.spilling[a.ID]
	}
	s.mu.Unlock()

	pick := -1
	for _, i := range order {
		if fits[i] {
			pick = i
			break
		}
	}
	if pick < 0 {
		// Nothing fits: the most 5h headroom delays the first exhaustion longest.
		pick = order[0]
		for _, i := range order[1:] {
			if ranked[i].ShortProjection.ProjectedPercent < ranked[pick].ShortProjection.ProjectedPercent {
				pick = i
			}
		}
	}
	if top := order[0]; pick != top {
		p := ranked[top].ShortProjection
		selectorLogEntry(ctx).Debugf("balanced: 5h spillover | top=%s projected=%.0f%% rate=%.1f%%/h picked=%s", available[top].ID, p.ProjectedPercent, p.BurnRatePerHour, available[pick].ID)
	}
	return available[pick], nil
}

// BalancedSpilling returns the accounts whose 5h projection currently sends new
// sessions to lower-ranked accounts. It unwraps session affinity.
func BalancedSpilling(sel Selector) map[string]bool {
	if affinity, ok := sel.(*SessionAffinitySelector); ok && affinity != nil {
		sel = affinity.fallback
	}
	out := map[string]bool{}
	s, ok := sel.(*BalancedSelector)
	if !ok || s == nil {
		return out
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, spilling := range s.spilling {
		if spilling {
			out[id] = true
		}
	}
	return out
}

// AccountBalance is shared by routing and the passive dashboard. model is the
// routed model; a Fable model also ranks on the Fable window.
func AccountBalance(a *Auth, now time.Time, model string, busy int) BalanceDetails {
	d := BalanceDetails{Weight: authWeight(a), Busy: busy, Windows: AccountBalanceWindows(a)}
	if busy < 0 {
		busy = 0
	}
	short, long, scoped := (*BalanceWindow)(nil), (*BalanceWindow)(nil), (*BalanceWindow)(nil)
	family := modelWindowFamily(model)
	for i := range d.Windows {
		w := &d.Windows[i]
		if w.Model != "" {
			if w.Model == family && (scoped == nil || w.WindowSeconds > scoped.WindowSeconds) {
				scoped = w
			}
			continue
		}
		if w.WindowSeconds <= 86400 {
			if short == nil || w.WindowSeconds < short.WindowSeconds {
				short = w
			}
		} else if long == nil || w.WindowSeconds > long.WindowSeconds {
			long = w
		}
	}
	// The short score is per-weight health (1 = nominal 5h pace for one session).
	// Weight already scales the long score; scaling here too halved every
	// weight-1 account's score even with an idle 5h window. Without a 5h limit
	// it stays neutral: busy sessions already divide the long score.
	d.ShortScore = 1
	if hasShortLimit(a) {
		d.ShortScore = shortWindowScore(short, now, 1/float64(busy+1))
	}
	d.PaceGap = paceGap(long, now)
	if scoped != nil {
		d.PaceGap = math.Min(d.PaceGap, paceGap(scoped, now))
	}
	// 1 is exactly on pace. Busy sessions claim a little of the account; a
	// heavier plan feels each one less. An exhausted window serves nothing
	// until its reset, however close that reset is.
	d.LongScore = math.Max(0, 1+d.PaceGap/100-float64(busy)*balanceSessionCost/100/float64(d.Weight))
	if exhausted(long, now) || exhausted(scoped, now) {
		d.LongScore = 0
	}
	d.Score = d.LongScore * math.Min(d.ShortScore/0.5, 1)
	if short != nil {
		d.ShortProjection = projectShortWindow(*short, AccountBalanceHistory(a), now)
	}
	return d
}

// modelWindowFamily names the model-scoped windows that also limit model.
func modelWindowFamily(model string) string {
	if strings.Contains(strings.ToLower(model), FableWindowModel) {
		return FableWindowModel
	}
	return ""
}

func exhausted(w *BalanceWindow, now time.Time) bool {
	return w != nil && w.UsedPercent >= 100 && (w.ResetsAt.IsZero() || w.ResetsAt.After(now))
}

// paceGap returns how many percentage points w's usage trails an even burn
// across the window: the elapsed share minus the used share. Old usage is not
// projected forward: this proxy's traffic is what moves the window, so an
// account it stopped picking stays where it was observed and falls behind pace
// until it wins new sessions back. A window whose reset passed is a fresh
// window that opened at that reset. A window without a reset counts as just
// opened. An unknown window is on pace.
func paceGap(w *BalanceWindow, now time.Time) float64 {
	if w == nil || w.WindowSeconds <= 0 {
		return 0
	}
	if w.ResetsAt.IsZero() {
		return -w.UsedPercent
	}
	length := time.Duration(w.WindowSeconds * float64(time.Second))
	if !w.ResetsAt.After(now) {
		return 100 * float64(now.Sub(w.ResetsAt)%length) / float64(length)
	}
	start := w.ResetsAt.Add(-length)
	elapsed := math.Min(1, math.Max(0, float64(now.Sub(start))/float64(length)))
	return 100*elapsed - w.UsedPercent
}

// projectShortWindow extrapolates the current burn rate to the window's reset.
// The rate is the change over the last hour of samples. Without a sample span
// of balanceMinRateSpan (young window, restart, sparse traffic) it falls back to
// the elapsed average since window start. An account without observations for
// the whole lookback burns nothing: this proxy's own traffic is what moves it,
// and an idle account must be able to win new sessions back.
func projectShortWindow(w BalanceWindow, history []BalanceWindow, now time.Time) *BalanceProjection {
	if w.ResetsAt.IsZero() || !w.ResetsAt.After(now) {
		return nil
	}
	start := w.ResetsAt.Add(-time.Duration(w.WindowSeconds * float64(time.Second)))
	p := &BalanceProjection{WindowSeconds: w.WindowSeconds, UsedPercent: w.UsedPercent, ResetsAt: w.ResetsAt}
	var ref *BalanceWindow
	for i := range history {
		h := &history[i]
		if h.WindowSeconds != w.WindowSeconds || h.ObservedAt.Before(start) || h.ObservedAt.Before(now.Add(-balanceRateLookback)) || h.UsedPercent > w.UsedPercent {
			continue
		}
		if ref == nil || h.ObservedAt.Before(ref.ObservedAt) {
			ref = h
		}
	}
	switch {
	case !w.ObservedAt.IsZero() && now.Sub(w.ObservedAt) > balanceRateLookback:
		p.RateSource = "idle"
	case ref != nil && w.ObservedAt.Sub(ref.ObservedAt) >= balanceMinRateSpan:
		p.RateSource = "recent"
		p.BurnRatePerHour = (w.UsedPercent - ref.UsedPercent) / now.Sub(ref.ObservedAt).Hours()
	default:
		p.RateSource = "average"
		p.BurnRatePerHour = w.UsedPercent / math.Max(now.Sub(start).Hours(), balanceYoungWindowFloor.Hours())
	}
	p.BurnRatePerHour = math.Max(0, p.BurnRatePerHour)
	p.ProjectedPercent = w.UsedPercent + p.BurnRatePerHour*w.ResetsAt.Sub(now).Hours()
	return p
}

// hasShortLimit is false when the auth sets "five_hour_limit": false. Such a
// plan (Claude Max) has only a weekly limit, so short windows (5h, at most a
// day) are ignored even when an old observation or local report still has one.
func hasShortLimit(a *Auth) bool {
	if a == nil {
		return true
	}
	limited, ok := a.Metadata["five_hour_limit"].(bool)
	return !ok || limited
}

// AccountBalanceHistory merges runtime and persisted short-window samples.
func AccountBalanceHistory(a *Auth) []BalanceWindow {
	if a == nil || !hasShortLimit(a) {
		return nil
	}
	samples := append([]BalanceWindow(nil), a.Quota.BalanceHistory...)
	var persisted []BalanceWindow
	if raw, err := json.Marshal(a.Metadata["window_history"]); err == nil && json.Unmarshal(raw, &persisted) == nil {
		samples = append(samples, persisted...)
	}
	return pruneBalanceHistory(samples)
}

// recordBalanceHistory appends the short windows observed at observedAt.
func recordBalanceHistory(history, windows []BalanceWindow, observedAt time.Time) []BalanceWindow {
	for _, w := range windows {
		if w.WindowSeconds <= 86400 && w.ObservedAt.Equal(observedAt) {
			history = append(history, w)
		}
	}
	return pruneBalanceHistory(history)
}

// pruneBalanceHistory keeps, per duration, the samples of the newest window:
// utilization only grows inside a window, so a drop marks the previous one.
func pruneBalanceHistory(samples []BalanceWindow) []BalanceWindow {
	byWindow := map[float64][]BalanceWindow{}
	for _, w := range samples {
		if w.ObservedAt.IsZero() || w.WindowSeconds <= 0 || w.WindowSeconds > 86400 || math.IsNaN(w.UsedPercent) || math.IsInf(w.UsedPercent, 0) {
			continue
		}
		byWindow[w.WindowSeconds] = append(byWindow[w.WindowSeconds], w)
	}
	var out []BalanceWindow
	for _, list := range byWindow {
		sort.SliceStable(list, func(i, j int) bool { return list[i].ObservedAt.Before(list[j].ObservedAt) })
		latest := list[len(list)-1]
		first := len(list) - 1
		for first > 0 {
			w := list[first-1]
			if w.UsedPercent > list[first].UsedPercent || latest.ObservedAt.Sub(w.ObservedAt) > balanceHistoryRetention || (!latest.ResetsAt.IsZero() && w.ObservedAt.Before(latest.ResetsAt.Add(-time.Duration(latest.WindowSeconds*float64(time.Second))))) {
				break
			}
			first--
		}
		// Thin forward so stored samples stay spaced while the latest is always kept.
		var keep []BalanceWindow
		for _, w := range list[first : len(list)-1] {
			if w.ObservedAt.Equal(latest.ObservedAt) {
				continue
			}
			if len(keep) == 0 || w.ObservedAt.Sub(keep[len(keep)-1].ObservedAt) >= balanceHistorySpacing {
				keep = append(keep, w)
			}
		}
		keep = append(keep, latest)
		if len(keep) > balanceHistoryMax {
			keep = keep[len(keep)-balanceHistoryMax:]
		}
		out = append(out, keep...)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].WindowSeconds != out[j].WindowSeconds {
			return out[i].WindowSeconds < out[j].WindowSeconds
		}
		return out[i].ObservedAt.Before(out[j].ObservedAt)
	})
	return out
}

// shortWindowScore is the 5h health: remaining capacity per hour until reset
// relative to the window's nominal refill (1 = nominal pace for one session).
// Like paceGap, it takes old usage as observed rather than projecting it.
func shortWindowScore(w *BalanceWindow, now time.Time, share float64) float64 {
	hours, resetFloor := 5.0, 1.0
	pace := 1 / hours
	if w != nil {
		hours = w.WindowSeconds / 3600
		if hours != 5 {
			resetFloor = math.Max(hours/168*5, math.Min(hours, 1))
		}
		pace = 1 / hours
		if w.ResetsAt.IsZero() || w.ResetsAt.After(now) {
			used := w.UsedPercent / 100
			remainingHours := hours
			if !w.ResetsAt.IsZero() {
				remainingHours = math.Max(w.ResetsAt.Sub(now).Hours(), resetFloor)
			}
			pace = math.Max(0, 1-used) / remainingHours
		}
	}
	return share * pace * hours
}

// AccountBalanceWindows combines valid observations by freshness per duration
// and model scope.
func AccountBalanceWindows(a *Auth) []BalanceWindow {
	if a == nil {
		return nil
	}
	type slot struct {
		model string
		secs  float64
	}
	windows := make(map[slot]BalanceWindow)
	add := func(w BalanceWindow) {
		if math.IsNaN(w.UsedPercent) || math.IsInf(w.UsedPercent, 0) || w.UsedPercent < 0 || w.UsedPercent > 100 || w.WindowSeconds <= 0 || math.IsNaN(w.WindowSeconds) || math.IsInf(w.WindowSeconds, 0) {
			return
		}
		k := slot{w.Model, w.WindowSeconds}
		old, ok := windows[k]
		if !ok || w.ObservedAt.After(old.ObservedAt) {
			// A partial response can refresh utilization without repeating reset.
			// Retain the deadline only within the same still-open window.
			if ok && w.ResetsAt.IsZero() && old.ResetsAt.After(w.ObservedAt) {
				w.ResetsAt = old.ResetsAt
			}
			windows[k] = w
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
	// 7d_oi is the Fable-only weekly limit. Only Fable responses carry it, so
	// the stored window survives the other models' responses.
	// A rejected window is used up whatever its utilization says: usage made
	// outside this proxy reaches it only through the rejection, and a 429 may
	// omit the utilization header altogether.
	claim := strings.ToLower(strings.TrimSpace(signals.Get("Anthropic-Ratelimit-Unified-Representative-Claim")))
	unifiedRejected := strings.EqualFold(strings.TrimSpace(signals.Get("Anthropic-Ratelimit-Unified-Status")), "rejected")
	for _, p := range []struct {
		name, claim, model string
		secs               float64
	}{{"5h", "five_hour", "", 18000}, {"7d", "seven_day", "", 604800}, {"7d_oi", "", FableWindowModel, 604800}} {
		prefix := "Anthropic-Ratelimit-Unified-" + p.name + "-"
		used, ok := number(prefix + "Utilization")
		used *= 100
		if strings.EqualFold(strings.TrimSpace(signals.Get(prefix+"Status")), "rejected") || (unifiedRejected && p.claim != "" && claim == p.claim) {
			used, ok = 100, true
		}
		if ok {
			add(BalanceWindow{Name: p.name, UsedPercent: math.Min(used, 100), WindowSeconds: p.secs, ResetsAt: reset(prefix + "Reset"), ObservedAt: a.Quota.ObservedAt, Source: "response", Model: p.model})
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
	// A window name is one slot per source: when a newer observation reports
	// that slot with another duration (a Codex plan switching its primary
	// window from 7 days to 30), the older duration no longer exists.
	newest := make(map[string]time.Time, len(windows))
	for _, w := range windows {
		if k := w.Source + "/" + w.Name; w.ObservedAt.After(newest[k]) {
			newest[k] = w.ObservedAt
		}
	}
	out := make([]BalanceWindow, 0, len(windows))
	for _, w := range windows {
		if w.WindowSeconds <= 86400 && !hasShortLimit(a) {
			continue
		}
		if w.Name != "" && newest[w.Source+"/"+w.Name].After(w.ObservedAt) {
			continue
		}
		out = append(out, w)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].WindowSeconds != out[j].WindowSeconds {
			return out[i].WindowSeconds < out[j].WindowSeconds
		}
		return out[i].Model < out[j].Model
	})
	return out
}
