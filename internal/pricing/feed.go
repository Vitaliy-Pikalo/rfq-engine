// Package pricing provides a reference mid-price feed that dealers quote
// around and check against during last look.
package pricing

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
)

// Feed holds a mid price per symbol. Mids move by a bounded random walk
// when Tick is called. Safe for concurrent use.
type Feed struct {
	mu   sync.RWMutex
	mids map[string]float64
	rng  *rand.Rand
	// volBps is the standard deviation of one tick, in basis points.
	volBps float64
}

// NewFeed creates a feed seeded with the given mids.
func NewFeed(mids map[string]float64, volBps float64, seed int64) *Feed {
	m := make(map[string]float64, len(mids))
	for k, v := range mids {
		m[k] = v
	}
	return &Feed{mids: m, rng: rand.New(rand.NewSource(seed)), volBps: volBps}
}

// Mid returns the current mid for symbol.
func (f *Feed) Mid(symbol string) (float64, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	px, ok := f.mids[symbol]
	if !ok {
		return 0, fmt.Errorf("unknown symbol %q", symbol)
	}
	return px, nil
}

// Set overrides a mid. Used by tests and scenario scripts.
func (f *Feed) Set(symbol string, px float64) {
	f.mu.Lock()
	f.mids[symbol] = px
	f.mu.Unlock()
}

// Tick moves every mid by one random-walk step.
func (f *Feed) Tick() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for s, px := range f.mids {
		step := f.rng.NormFloat64() * f.volBps / 1e4
		f.mids[s] = math.Max(px*(1+step), 1e-9)
	}
}

// Symbols lists known symbols.
func (f *Feed) Symbols() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	out := make([]string, 0, len(f.mids))
	for s := range f.mids {
		out = append(out, s)
	}
	return out
}
