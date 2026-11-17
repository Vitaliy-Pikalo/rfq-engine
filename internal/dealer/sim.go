// Package dealer provides simulated liquidity providers with configurable
// spread, response latency, decline rate, and a price-tolerance last look.
package dealer

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/Vitaliy-Pikalo/rfq-engine/internal/pricing"
	"github.com/Vitaliy-Pikalo/rfq-engine/internal/rfq"
)

// Config describes one simulated dealer's behavior.
type Config struct {
	Name string
	// SpreadBps is the full bid/offer spread at small size, in basis points.
	SpreadBps float64
	// SizeScale widens the spread linearly with size:
	// spread = SpreadBps * (1 + qty/SizeScale). 0 disables size widening.
	SizeScale float64
	// LatencyMean and LatencyJitter set response time: mean ± uniform jitter.
	LatencyMean, LatencyJitter time.Duration
	// DeclineProb is the chance of not quoting at all, in [0,1].
	DeclineProb float64
	// LastLookTolBps: reject an accept if the mid has moved more than this
	// against the dealer since the quote was made. <0 disables last look.
	LastLookTolBps float64
	// LastLookHold is how long the dealer holds the client before answering.
	LastLookHold time.Duration
	Seed         int64
}

// Sim is a simulated dealer. Safe for concurrent use.
type Sim struct {
	cfg  Config
	feed *pricing.Feed

	mu  sync.Mutex
	rng *rand.Rand
}

var _ rfq.Dealer = (*Sim)(nil)

// New builds a simulated dealer that prices off feed.
func New(cfg Config, feed *pricing.Feed) (*Sim, error) {
	switch {
	case cfg.Name == "":
		return nil, fmt.Errorf("dealer: empty name")
	case cfg.SpreadBps < 0:
		return nil, fmt.Errorf("dealer %s: negative spread", cfg.Name)
	case cfg.DeclineProb < 0 || cfg.DeclineProb > 1:
		return nil, fmt.Errorf("dealer %s: decline prob %v outside [0,1]", cfg.Name, cfg.DeclineProb)
	case feed == nil:
		return nil, fmt.Errorf("dealer %s: nil price feed", cfg.Name)
	}
	return &Sim{cfg: cfg, feed: feed, rng: rand.New(rand.NewSource(cfg.Seed))}, nil
}

// Name implements rfq.Dealer.
func (s *Sim) Name() string { return s.cfg.Name }

// Quote implements rfq.Dealer.
func (s *Sim) Quote(ctx context.Context, req rfq.Request) (float64, float64, error) {
	s.mu.Lock()
	decline := s.rng.Float64() < s.cfg.DeclineProb
	delay := s.cfg.LatencyMean
	if s.cfg.LatencyJitter > 0 {
		delay += time.Duration((s.rng.Float64()*2 - 1) * float64(s.cfg.LatencyJitter))
	}
	s.mu.Unlock()

	if err := sleep(ctx, delay); err != nil {
		return 0, 0, err
	}
	if decline {
		return 0, 0, rfq.ErrNoQuote
	}
	mid, err := s.feed.Mid(req.Symbol)
	if err != nil {
		return 0, 0, rfq.ErrNoQuote // dealers don't quote what they don't trade
	}
	half := s.SpreadBpsFor(req.Qty) / 2 / 1e4
	return mid * (1 - half), mid * (1 + half), nil
}

// SpreadBpsFor returns the full spread this dealer shows at qty.
func (s *Sim) SpreadBpsFor(qty float64) float64 {
	if s.cfg.SizeScale <= 0 {
		return s.cfg.SpreadBps
	}
	return s.cfg.SpreadBps * (1 + qty/s.cfg.SizeScale)
}

// LastLook implements rfq.Dealer. It rejects when the market has moved
// against the dealer by more than LastLookTolBps since the quote.
func (s *Sim) LastLook(ctx context.Context, q rfq.Quote, side rfq.Side) (bool, string) {
	if err := sleep(ctx, s.cfg.LastLookHold); err != nil {
		return false, "last look timeout"
	}
	if s.cfg.LastLookTolBps < 0 {
		return true, ""
	}
	symbol, err := s.symbolOf(q)
	if err != nil {
		return false, err.Error()
	}
	mid, err := s.feed.Mid(symbol)
	if err != nil {
		return false, "no reference price"
	}
	moved := MoveAgainstDealerBps((q.Bid+q.Offer)/2, mid, side)
	if moved > s.cfg.LastLookTolBps {
		return false, fmt.Sprintf("price moved %.2fbps against dealer (tol %.2f)", moved, s.cfg.LastLookTolBps)
	}
	return true, ""
}

func (s *Sim) symbolOf(q rfq.Quote) (string, error) {
	if q.Symbol != "" {
		return q.Symbol, nil
	}
	return "", fmt.Errorf("quote %s missing symbol", q.ID)
}

// MoveAgainstDealerBps returns how far the mid moved against a dealer who
// quoted at quoteMid, in bps. Positive means the dealer would lose.
// Client buys (dealer sells): a rising mid hurts the dealer.
// Client sells (dealer buys): a falling mid hurts the dealer.
func MoveAgainstDealerBps(quoteMid, nowMid float64, side rfq.Side) float64 {
	if quoteMid <= 0 || math.IsNaN(nowMid) {
		return math.Inf(1)
	}
	d := (nowMid - quoteMid) / quoteMid * 1e4
	if side == rfq.Sell {
		d = -d
	}
	return d
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// DefaultRoster is a mixed panel of dealers used by the demo server and the
// load test: one tight-but-picky market maker, one wide-but-reliable bank,
// a fast HFT-style LP, a slow one that sometimes misses the window, and a
// flaky one that often declines.
func DefaultRoster(seed int64) []Config {
	return []Config{
		{Name: "tightmm", SpreadBps: 3, SizeScale: 50, LatencyMean: 3 * time.Millisecond, LatencyJitter: 2 * time.Millisecond, LastLookTolBps: 0.5, LastLookHold: 2 * time.Millisecond, Seed: seed + 1},
		{Name: "bank", SpreadBps: 10, SizeScale: 200, LatencyMean: 15 * time.Millisecond, LatencyJitter: 5 * time.Millisecond, LastLookTolBps: -1, Seed: seed + 2},
		{Name: "hft", SpreadBps: 5, SizeScale: 20, LatencyMean: 500 * time.Microsecond, LatencyJitter: 400 * time.Microsecond, LastLookTolBps: 2, LastLookHold: 500 * time.Microsecond, Seed: seed + 3},
		{Name: "slow", SpreadBps: 6, SizeScale: 100, LatencyMean: 45 * time.Millisecond, LatencyJitter: 20 * time.Millisecond, LastLookTolBps: 3, Seed: seed + 4},
		{Name: "flaky", SpreadBps: 4, SizeScale: 30, LatencyMean: 8 * time.Millisecond, LatencyJitter: 6 * time.Millisecond, DeclineProb: 0.4, LastLookTolBps: 1, Seed: seed + 5},
	}
}
