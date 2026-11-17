// Package metrics records latency distributions and counters.
// Kept dependency-free on purpose; swap in Prometheus later if needed.
package metrics

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Histogram keeps the most recent N samples in a ring buffer and computes
// percentiles on demand. Exact over the window, O(n log n) per read.
type Histogram struct {
	mu      sync.Mutex
	samples []time.Duration
	next    int
	full    bool
	count   int64
}

// NewHistogram creates a histogram that keeps up to window samples.
func NewHistogram(window int) *Histogram {
	if window <= 0 {
		window = 1
	}
	return &Histogram{samples: make([]time.Duration, window)}
}

// Observe records one sample.
func (h *Histogram) Observe(d time.Duration) {
	h.mu.Lock()
	h.samples[h.next] = d
	h.next++
	if h.next == len(h.samples) {
		h.next = 0
		h.full = true
	}
	h.count++
	h.mu.Unlock()
}

// Snapshot is a summary of a histogram at one point in time.
type Snapshot struct {
	Count              int64
	P50, P90, P99, Max time.Duration
	Mean               time.Duration
}

// Snapshot computes percentiles over the retained window.
// Uses nearest-rank: P(q) = sorted[ceil(q*n)-1].
func (h *Histogram) Snapshot() Snapshot {
	h.mu.Lock()
	n := h.next
	if h.full {
		n = len(h.samples)
	}
	cp := make([]time.Duration, n)
	copy(cp, h.samples[:n])
	count := h.count
	h.mu.Unlock()

	if n == 0 {
		return Snapshot{Count: count}
	}
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	var sum time.Duration
	for _, d := range cp {
		sum += d
	}
	rank := func(q float64) time.Duration {
		i := int(math.Ceil(q*float64(n))) - 1
		if i < 0 {
			i = 0
		}
		return cp[i]
	}
	return Snapshot{
		Count: count,
		P50:   rank(0.50),
		P90:   rank(0.90),
		P99:   rank(0.99),
		Max:   cp[n-1],
		Mean:  sum / time.Duration(n),
	}
}

// Registry groups named histograms and counters.
type Registry struct {
	mu       sync.Mutex
	hists    map[string]*Histogram
	counters map[string]*atomic.Int64
	window   int
}

// NewRegistry creates a registry whose histograms keep window samples.
func NewRegistry(window int) *Registry {
	return &Registry{hists: map[string]*Histogram{}, counters: map[string]*atomic.Int64{}, window: window}
}

// Hist returns (creating if needed) the named histogram.
func (r *Registry) Hist(name string) *Histogram {
	r.mu.Lock()
	defer r.mu.Unlock()
	h, ok := r.hists[name]
	if !ok {
		h = NewHistogram(r.window)
		r.hists[name] = h
	}
	return h
}

// Inc adds one to the named counter.
func (r *Registry) Inc(name string) { r.Add(name, 1) }

// Add adds delta to the named counter.
func (r *Registry) Add(name string, delta int64) {
	r.mu.Lock()
	c, ok := r.counters[name]
	if !ok {
		c = &atomic.Int64{}
		r.counters[name] = c
	}
	r.mu.Unlock()
	c.Add(delta)
}

// Counter reads the named counter (0 if never touched).
func (r *Registry) Counter(name string) int64 {
	r.mu.Lock()
	c, ok := r.counters[name]
	r.mu.Unlock()
	if !ok {
		return 0
	}
	return c.Load()
}

// Report renders all metrics as sorted plain text.
func (r *Registry) Report() string {
	r.mu.Lock()
	hn := make([]string, 0, len(r.hists))
	for k := range r.hists {
		hn = append(hn, k)
	}
	cn := make([]string, 0, len(r.counters))
	for k := range r.counters {
		cn = append(cn, k)
	}
	r.mu.Unlock()
	sort.Strings(hn)
	sort.Strings(cn)

	var b strings.Builder
	for _, k := range cn {
		fmt.Fprintf(&b, "%-32s %d\n", k, r.Counter(k))
	}
	for _, k := range hn {
		s := r.Hist(k).Snapshot()
		fmt.Fprintf(&b, "%-32s n=%d p50=%v p90=%v p99=%v max=%v\n", k, s.Count, s.P50, s.P90, s.P99, s.Max)
	}
	return b.String()
}
