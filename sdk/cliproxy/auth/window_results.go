package auth

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

// WindowResult is how much of a quota window an account had used when the
// window reset: its last observation before the reset. Quota left at the reset
// expired unused.
type WindowResult struct {
	AuthID        string    `json:"auth_id"`
	Label         string    `json:"label,omitempty"`
	Provider      string    `json:"provider"`
	Weight        int64     `json:"weight"`
	Name          string    `json:"name"`
	WindowSeconds float64   `json:"window_seconds"`
	ResetsAt      time.Time `json:"resets_at"`
	UsedPercent   float64   `json:"used_percent"`
	ObservedAt    time.Time `json:"observed_at,omitempty"`
	Source        string    `json:"source,omitempty"`
}

func (r WindowResult) key() string {
	return fmt.Sprintf("%s|%g|%d", r.AuthID, r.WindowSeconds, r.ResetsAt.Unix())
}

// WindowResultLog appends one JSON line per reset window to a local file.
type WindowResultLog struct {
	path    string
	mu      sync.Mutex
	seen    map[string]bool
	results []WindowResult
}

// OpenWindowResultLog loads the results already in path. A missing file is an
// empty log; unreadable lines are skipped.
func OpenWindowResultLog(path string) (*WindowResultLog, error) {
	l := &WindowResultLog{path: path, seen: map[string]bool{}}
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return l, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open window result log: %w", err)
	}
	defer func() {
		if errClose := f.Close(); errClose != nil {
			log.Errorf("close window result log: %v", errClose)
		}
	}()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var r WindowResult
		if json.Unmarshal(scanner.Bytes(), &r) != nil || r.AuthID == "" || r.ResetsAt.IsZero() {
			continue
		}
		if !l.seen[r.key()] {
			l.seen[r.key()] = true
			l.results = append(l.results, r)
		}
	}
	if errScan := scanner.Err(); errScan != nil {
		return nil, fmt.Errorf("read window result log: %w", errScan)
	}
	return l, nil
}

// Record stores every weekly (longer than a day) window of a whose reset has
// passed and that is not yet in the log. Call it before a new observation
// replaces those windows.
func (l *WindowResultLog) Record(a *Auth, now time.Time) {
	if l == nil || a == nil {
		return
	}
	var fresh []WindowResult
	for _, w := range AccountBalanceWindows(a) {
		if w.WindowSeconds <= 86400 || w.ResetsAt.IsZero() || w.ResetsAt.After(now) {
			continue
		}
		fresh = append(fresh, WindowResult{AuthID: a.ID, Label: a.Label, Provider: a.Provider, Weight: authWeight(a), Name: w.Name, WindowSeconds: w.WindowSeconds, ResetsAt: w.ResetsAt.UTC(), UsedPercent: w.UsedPercent, ObservedAt: w.ObservedAt, Source: w.Source})
	}
	if len(fresh) == 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, r := range fresh {
		if l.seen[r.key()] {
			continue
		}
		if err := l.appendLocked(r); err != nil {
			log.Errorf("window result log: %v", err)
			return
		}
		l.seen[r.key()] = true
		l.results = append(l.results, r)
		log.Infof("quota window reset | auth=%s window=%s resets_at=%s used=%.0f%% observed_at=%s", r.AuthID, r.Name, r.ResetsAt.Format(time.RFC3339), r.UsedPercent, r.ObservedAt.Format(time.RFC3339))
	}
}

func (l *WindowResultLog) appendLocked(r WindowResult) error {
	if l.path == "" {
		return nil
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return fmt.Errorf("encode: %w", err)
	}
	if errMkdir := os.MkdirAll(filepath.Dir(l.path), 0o700); errMkdir != nil {
		return fmt.Errorf("create directory: %w", errMkdir)
	}
	f, err := os.OpenFile(l.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	if _, errWrite := f.Write(append(raw, '\n')); errWrite != nil {
		_ = f.Close()
		return fmt.Errorf("write: %w", errWrite)
	}
	if errClose := f.Close(); errClose != nil {
		return fmt.Errorf("close: %w", errClose)
	}
	return nil
}

// Results returns the recorded windows that reset at or after since, oldest first.
func (l *WindowResultLog) Results(since time.Time) []WindowResult {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]WindowResult, 0, len(l.results))
	for _, r := range l.results {
		if !r.ResetsAt.Before(since) {
			out = append(out, r)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].ResetsAt.Before(out[j].ResetsAt) })
	return out
}

// SetWindowResultLog makes the manager record each quota window that resets.
func (m *Manager) SetWindowResultLog(l *WindowResultLog) {
	if m != nil {
		m.windowResults.Store(l)
	}
}

// WindowResultLog returns the log set by SetWindowResultLog, or nil.
func (m *Manager) WindowResultLog() *WindowResultLog {
	if m == nil {
		return nil
	}
	return m.windowResults.Load()
}
