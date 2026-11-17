package dealer

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/Vitaliy-Pikalo/rfq-engine/internal/pricing"
	"github.com/Vitaliy-Pikalo/rfq-engine/internal/rfq"
)

func feed() *pricing.Feed {
	return pricing.NewFeed(map[string]float64{"BTC-USD": 100_000}, 1, 1)
}

func mustSim(t *testing.T, cfg Config, f *pricing.Feed) *Sim {
	t.Helper()
	s, err := New(cfg, f)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestQuoteIsSymmetricAroundMid(t *testing.T) {
	s := mustSim(t, Config{Name: "A", SpreadBps: 10, LastLookTolBps: -1}, feed())
	bid, offer, err := s.Quote(context.Background(), rfq.Request{ID: "r", Symbol: "BTC-USD", Side: rfq.Buy, Qty: 1})
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(bid-99_950) > 1e-6 || math.Abs(offer-100_050) > 1e-6 {
		t.Fatalf("bid=%v offer=%v, want 99950/100050 for 10bps around 100000", bid, offer)
	}
}

func TestSpreadWidensWithSize(t *testing.T) {
	s := mustSim(t, Config{Name: "A", SpreadBps: 10, SizeScale: 5}, feed())
	if got := s.SpreadBpsFor(0); got != 10 {
		t.Fatalf("spread@0 = %v, want 10", got)
	}
	if got := s.SpreadBpsFor(5); got != 20 {
		t.Fatalf("spread@5 = %v, want 20", got)
	}
}

func TestUnknownSymbolDeclines(t *testing.T) {
	s := mustSim(t, Config{Name: "A", SpreadBps: 10}, feed())
	_, _, err := s.Quote(context.Background(), rfq.Request{ID: "r", Symbol: "DOGE-USD", Side: rfq.Buy, Qty: 1})
	if !errors.Is(err, rfq.ErrNoQuote) {
		t.Fatalf("err=%v, want ErrNoQuote", err)
	}
}

func TestDeclineProbOneAlwaysDeclines(t *testing.T) {
	s := mustSim(t, Config{Name: "A", SpreadBps: 10, DeclineProb: 1}, feed())
	for i := 0; i < 20; i++ {
		if _, _, err := s.Quote(context.Background(), rfq.Request{ID: "r", Symbol: "BTC-USD", Side: rfq.Buy, Qty: 1}); !errors.Is(err, rfq.ErrNoQuote) {
			t.Fatalf("err=%v, want ErrNoQuote", err)
		}
	}
}

func TestQuoteRespectsContext(t *testing.T) {
	s := mustSim(t, Config{Name: "slow", SpreadBps: 10, LatencyMean: time.Second}, feed())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, _, err := s.Quote(ctx, rfq.Request{ID: "r", Symbol: "BTC-USD", Side: rfq.Buy, Qty: 1})
	if err == nil || time.Since(start) > 200*time.Millisecond {
		t.Fatalf("err=%v after %v, want fast context error", err, time.Since(start))
	}
}

func TestMoveAgainstDealer(t *testing.T) {
	cases := []struct {
		name         string
		qmid, nowMid float64
		side         rfq.Side
		want         float64
	}{
		{"client buys, mid up 5bps hurts dealer", 100, 100.05, rfq.Buy, 5},
		{"client buys, mid down helps dealer", 100, 99.95, rfq.Buy, -5},
		{"client sells, mid down 5bps hurts dealer", 100, 99.95, rfq.Sell, 5},
		{"client sells, mid up helps dealer", 100, 100.05, rfq.Sell, -5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := MoveAgainstDealerBps(tc.qmid, tc.nowMid, tc.side); math.Abs(got-tc.want) > 1e-9 {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestLastLookTolerance(t *testing.T) {
	f := feed()
	s := mustSim(t, Config{Name: "A", SpreadBps: 10, LastLookTolBps: 2}, f)
	q := rfq.Quote{ID: "q", Symbol: "BTC-USD", Bid: 99_950, Offer: 100_050} // mid 100000

	f.Set("BTC-USD", 100_010) // +1bp against dealer on a client buy: within tol
	if ok, why := s.LastLook(context.Background(), q, rfq.Buy); !ok {
		t.Fatalf("1bp move rejected: %s", why)
	}
	f.Set("BTC-USD", 100_050) // +5bp: over tol
	if ok, _ := s.LastLook(context.Background(), q, rfq.Buy); ok {
		t.Fatal("5bp adverse move should be rejected")
	}
	// Same +5bp move is favorable to the dealer when the client sells.
	if ok, why := s.LastLook(context.Background(), q, rfq.Sell); !ok {
		t.Fatalf("favorable move rejected: %s", why)
	}
}

func TestNewValidates(t *testing.T) {
	f := feed()
	bad := []Config{
		{Name: "", SpreadBps: 1},
		{Name: "A", SpreadBps: -1},
		{Name: "A", SpreadBps: 1, DeclineProb: 1.5},
	}
	for _, c := range bad {
		if _, err := New(c, f); err == nil {
			t.Errorf("New(%+v) should fail", c)
		}
	}
	if _, err := New(Config{Name: "A"}, nil); err == nil {
		t.Error("nil feed should fail")
	}
}

// End to end: engine + simulated dealers, with the market moving between
// quote and accept so last look actually triggers.
func TestEngineWithSimDealers(t *testing.T) {
	f := feed()
	tight := mustSim(t, Config{Name: "tight", SpreadBps: 4, LastLookTolBps: 1}, f)
	wide := mustSim(t, Config{Name: "wide", SpreadBps: 12, LastLookTolBps: -1}, f)
	e, err := rfq.NewEngine(rfq.Config{CollectWindow: 50 * time.Millisecond, QuoteTTL: time.Second}, nil, tight, wide)
	if err != nil {
		t.Fatal(err)
	}
	s, err := e.Submit(context.Background(), rfq.Request{ID: "r1", Symbol: "BTC-USD", Side: rfq.Buy, Qty: 1})
	if err != nil {
		t.Fatal(err)
	}
	if s.Best.Dealer != "tight" {
		t.Fatalf("best=%s, want tight", s.Best.Dealer)
	}
	f.Set("BTC-USD", 100_030) // +3bp: tight dealer's 1bp tolerance trips
	fill, err := e.Accept(context.Background(), "r1", s.Best.ID)
	if err != nil {
		t.Fatal(err)
	}
	if fill.Filled {
		t.Fatalf("expected last-look reject, got %+v", fill)
	}
}
