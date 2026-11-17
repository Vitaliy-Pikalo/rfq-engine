package rfq

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock is a manually advanced clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *fakeClock { return &fakeClock{t: time.Date(2027, 6, 1, 14, 0, 0, 0, time.UTC)} }
func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// stubDealer returns fixed prices, optionally after a delay.
type stubDealer struct {
	name       string
	bid, offer float64
	delay      time.Duration
	decline    bool
	rejectLL   bool
	llDelay    time.Duration
	llCalls    atomic.Int32
}

func (d *stubDealer) Name() string { return d.name }
func (d *stubDealer) Quote(ctx context.Context, _ Request) (float64, float64, error) {
	if d.delay > 0 {
		select {
		case <-time.After(d.delay):
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		}
	}
	if d.decline {
		return 0, 0, ErrNoQuote
	}
	return d.bid, d.offer, nil
}
func (d *stubDealer) LastLook(ctx context.Context, _ Quote, _ Side) (bool, string) {
	d.llCalls.Add(1)
	if d.llDelay > 0 {
		select {
		case <-time.After(d.llDelay):
		case <-ctx.Done():
		}
	}
	if d.rejectLL {
		return false, "price moved"
	}
	return true, ""
}

func newTestEngine(t *testing.T, clk *fakeClock, ds ...Dealer) *Engine {
	t.Helper()
	cfg := Config{CollectWindow: 50 * time.Millisecond, QuoteTTL: time.Second, LastLookTimeout: 30 * time.Millisecond}
	if clk != nil {
		cfg.Now = clk.Now
	}
	e, err := NewEngine(cfg, nil, ds...)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestRequestValidate(t *testing.T) {
	cases := []struct {
		name string
		req  Request
		ok   bool
	}{
		{"valid", Request{ID: "r1", Symbol: "BTC-USD", Side: Buy, Qty: 1}, true},
		{"empty id", Request{Symbol: "BTC-USD", Side: Buy, Qty: 1}, false},
		{"empty symbol", Request{ID: "r1", Side: Buy, Qty: 1}, false},
		{"bad side", Request{ID: "r1", Symbol: "BTC-USD", Side: 7, Qty: 1}, false},
		{"zero qty", Request{ID: "r1", Symbol: "BTC-USD", Side: Sell, Qty: 0}, false},
		{"negative qty", Request{ID: "r1", Symbol: "BTC-USD", Side: Sell, Qty: -3}, false},
		{"NaN qty", Request{ID: "r1", Symbol: "BTC-USD", Side: Sell, Qty: math.NaN()}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.req.Validate(); (err == nil) != tc.ok {
				t.Fatalf("Validate() err=%v, want ok=%v", err, tc.ok)
			}
		})
	}
}

func TestTransitions(t *testing.T) {
	legal := [][2]State{{Pending, Quoted}, {Pending, NoQuotes}, {Quoted, Accepted}, {Quoted, Expired}, {Accepted, Filled}, {Accepted, Rejected}}
	for _, p := range legal {
		if !CanTransition(p[0], p[1]) {
			t.Errorf("%s -> %s should be legal", p[0], p[1])
		}
	}
	illegal := [][2]State{{Pending, Filled}, {Quoted, Filled}, {Filled, Accepted}, {Rejected, Filled}, {Expired, Accepted}, {NoQuotes, Quoted}}
	for _, p := range illegal {
		if CanTransition(p[0], p[1]) {
			t.Errorf("%s -> %s should be illegal", p[0], p[1])
		}
	}
}

func TestBestQuoteBySide(t *testing.T) {
	ds := []Dealer{
		&stubDealer{name: "A", bid: 99.0, offer: 101.0},
		&stubDealer{name: "B", bid: 99.5, offer: 100.8},
		&stubDealer{name: "C", bid: 99.7, offer: 101.2},
	}
	e := newTestEngine(t, nil, ds...)

	buy, err := e.Submit(context.Background(), Request{ID: "b1", Symbol: "X", Side: Buy, Qty: 1})
	if err != nil {
		t.Fatal(err)
	}
	if buy.State != Quoted || buy.Best.Dealer != "B" || buy.Best.Offer != 100.8 {
		t.Fatalf("buy best = %+v state=%s, want dealer B offer 100.8", buy.Best, buy.State)
	}
	sell, err := e.Submit(context.Background(), Request{ID: "s1", Symbol: "X", Side: Sell, Qty: 1})
	if err != nil {
		t.Fatal(err)
	}
	if sell.Best.Dealer != "C" || sell.Best.Bid != 99.7 {
		t.Fatalf("sell best = %+v, want dealer C bid 99.7", sell.Best)
	}
	if got := len(sell.Quotes); got != 3 {
		t.Fatalf("quotes = %d, want 3", got)
	}
}

func TestTieBreakIsDeterministic(t *testing.T) {
	clk := newClock() // frozen clock: equal arrival times, so dealer name decides
	e := newTestEngine(t, clk,
		&stubDealer{name: "zeta", bid: 99, offer: 100},
		&stubDealer{name: "alpha", bid: 99, offer: 100},
	)
	s, err := e.Submit(context.Background(), Request{ID: "t", Symbol: "X", Side: Buy, Qty: 1})
	if err != nil {
		t.Fatal(err)
	}
	if s.Best.Dealer != "alpha" {
		t.Fatalf("tie-break picked %s, want alpha", s.Best.Dealer)
	}
}

func TestBadAndMissingQuotesAreDropped(t *testing.T) {
	e := newTestEngine(t, nil,
		&stubDealer{name: "crossed", bid: 101, offer: 100}, // offer < bid
		&stubDealer{name: "zero", bid: 0, offer: 100},
		&stubDealer{name: "inf", bid: 99, offer: math.Inf(1)},
		&stubDealer{name: "nan", bid: math.NaN(), offer: 100},
		&stubDealer{name: "declines", decline: true},
		&stubDealer{name: "slow", bid: 1, offer: 2, delay: time.Second}, // misses the window
		&stubDealer{name: "good", bid: 99, offer: 100},
	)
	s, err := e.Submit(context.Background(), Request{ID: "r", Symbol: "X", Side: Buy, Qty: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(s.Quotes) != 1 || s.Best.Dealer != "good" {
		t.Fatalf("quotes=%+v, want only dealer good", s.Quotes)
	}
	if got := e.Stats().Counter("dealer.timed_out"); got != 1 {
		t.Fatalf("timed_out = %d, want 1", got)
	}
}

func TestNoQuotesIsTerminal(t *testing.T) {
	e := newTestEngine(t, nil, &stubDealer{name: "A", decline: true})
	s, err := e.Submit(context.Background(), Request{ID: "r", Symbol: "X", Side: Buy, Qty: 1})
	if err != nil {
		t.Fatal(err)
	}
	if s.State != NoQuotes || s.Best != nil {
		t.Fatalf("state=%s best=%v, want NO_QUOTES and nil", s.State, s.Best)
	}
	if _, err := e.Accept(context.Background(), "r", "r.A"); !errors.Is(err, ErrBadTransition) {
		t.Fatalf("accept on NO_QUOTES err=%v, want ErrBadTransition", err)
	}
}

func TestDuplicateRFQ(t *testing.T) {
	e := newTestEngine(t, nil, &stubDealer{name: "A", bid: 1, offer: 2})
	req := Request{ID: "dup", Symbol: "X", Side: Buy, Qty: 1}
	if _, err := e.Submit(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Submit(context.Background(), req); !errors.Is(err, ErrDuplicateRFQ) {
		t.Fatalf("err=%v, want ErrDuplicateRFQ", err)
	}
}

func TestAcceptFill(t *testing.T) {
	e := newTestEngine(t, nil, &stubDealer{name: "A", bid: 99, offer: 100})
	s, _ := e.Submit(context.Background(), Request{ID: "r", Symbol: "X", Side: Sell, Qty: 2})
	f, err := e.Accept(context.Background(), "r", s.Best.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !f.Filled || f.Price != 99 || f.Qty != 2 || f.Side != Sell {
		t.Fatalf("fill=%+v, want filled sell 2 @ 99", f)
	}
	got, _ := e.Get("r")
	if got.State != Filled || got.Fill == nil {
		t.Fatalf("state=%s, want FILLED with fill", got.State)
	}
}

func TestLastLookReject(t *testing.T) {
	e := newTestEngine(t, nil, &stubDealer{name: "A", bid: 99, offer: 100, rejectLL: true})
	s, _ := e.Submit(context.Background(), Request{ID: "r", Symbol: "X", Side: Buy, Qty: 1})
	f, err := e.Accept(context.Background(), "r", s.Best.ID)
	if err != nil {
		t.Fatal(err)
	}
	if f.Filled || f.Reason != "price moved" {
		t.Fatalf("fill=%+v, want rejected with reason", f)
	}
	got, _ := e.Get("r")
	if got.State != Rejected {
		t.Fatalf("state=%s, want REJECTED", got.State)
	}
}

func TestLastLookTimeoutRejects(t *testing.T) {
	e := newTestEngine(t, nil, &stubDealer{name: "A", bid: 99, offer: 100, llDelay: time.Second})
	s, _ := e.Submit(context.Background(), Request{ID: "r", Symbol: "X", Side: Buy, Qty: 1})
	start := time.Now()
	f, err := e.Accept(context.Background(), "r", s.Best.ID)
	if err != nil {
		t.Fatal(err)
	}
	if f.Filled || f.Reason != "last look timeout" {
		t.Fatalf("fill=%+v, want timeout reject", f)
	}
	if el := time.Since(start); el > 500*time.Millisecond {
		t.Fatalf("accept took %v, timeout not enforced", el)
	}
}

func TestQuoteExpiry(t *testing.T) {
	clk := newClock()
	e := newTestEngine(t, clk, &stubDealer{name: "A", bid: 99, offer: 100})
	s, _ := e.Submit(context.Background(), Request{ID: "r", Symbol: "X", Side: Buy, Qty: 1})

	clk.Advance(time.Second) // exactly at ExpiresAt counts as expired
	if _, err := e.Accept(context.Background(), "r", s.Best.ID); !errors.Is(err, ErrQuoteExpired) {
		t.Fatalf("err=%v, want ErrQuoteExpired", err)
	}
	got, _ := e.Get("r")
	if got.State != Expired {
		t.Fatalf("state=%s, want EXPIRED", got.State)
	}
}

func TestExpiredQuoteDoesNotKillOtherQuotes(t *testing.T) {
	clk := newClock()
	e := newTestEngine(t, clk, &stubDealer{name: "A", bid: 99, offer: 100})
	s, _ := e.Submit(context.Background(), Request{ID: "r", Symbol: "X", Side: Buy, Qty: 1})
	// Make the RFQ hold one stale and one fresh quote.
	e.mu.Lock()
	fresh := s.Quotes[0]
	fresh.ID, fresh.Dealer, fresh.ExpiresAt = "r.fresh", "A", clk.Now().Add(10*time.Second)
	e.rfqs["r"].quotes = append(e.rfqs["r"].quotes, fresh)
	e.mu.Unlock()

	clk.Advance(2 * time.Second)
	if _, err := e.Accept(context.Background(), "r", s.Best.ID); !errors.Is(err, ErrQuoteExpired) {
		t.Fatalf("err=%v, want ErrQuoteExpired", err)
	}
	if got, _ := e.Get("r"); got.State != Quoted {
		t.Fatalf("state=%s, want still QUOTED", got.State)
	}
	if f, err := e.Accept(context.Background(), "r", "r.fresh"); err != nil || !f.Filled {
		t.Fatalf("fresh accept: fill=%+v err=%v", f, err)
	}
}

func TestAcceptErrors(t *testing.T) {
	e := newTestEngine(t, nil, &stubDealer{name: "A", bid: 99, offer: 100})
	if _, err := e.Accept(context.Background(), "nope", "q"); !errors.Is(err, ErrUnknownRFQ) {
		t.Fatalf("err=%v, want ErrUnknownRFQ", err)
	}
	e.Submit(context.Background(), Request{ID: "r", Symbol: "X", Side: Buy, Qty: 1})
	if _, err := e.Accept(context.Background(), "r", "r.ghost"); !errors.Is(err, ErrUnknownQuote) {
		t.Fatalf("err=%v, want ErrUnknownQuote", err)
	}
}

// Many clients racing to accept the same RFQ: exactly one wins,
// and the dealer sees exactly one last look.
func TestConcurrentAcceptOnlyOneWins(t *testing.T) {
	d := &stubDealer{name: "A", bid: 99, offer: 100, llDelay: 5 * time.Millisecond}
	e := newTestEngine(t, nil, d)
	s, _ := e.Submit(context.Background(), Request{ID: "r", Symbol: "X", Side: Buy, Qty: 1})

	var wins, losses atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := e.Accept(context.Background(), "r", s.Best.ID); err == nil {
				wins.Add(1)
			} else if errors.Is(err, ErrBadTransition) {
				losses.Add(1)
			} else {
				t.Errorf("unexpected err: %v", err)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 || losses.Load() != 49 {
		t.Fatalf("wins=%d losses=%d, want 1/49", wins.Load(), losses.Load())
	}
	if d.llCalls.Load() != 1 {
		t.Fatalf("last look called %d times, want 1", d.llCalls.Load())
	}
}

func TestConcurrentSubmits(t *testing.T) {
	e := newTestEngine(t, nil,
		&stubDealer{name: "A", bid: 99, offer: 100},
		&stubDealer{name: "B", bid: 98, offer: 101, delay: time.Millisecond},
	)
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("r%d", i)
			s, err := e.Submit(context.Background(), Request{ID: id, Symbol: "X", Side: Buy, Qty: 1})
			if err != nil || s.State != Quoted {
				t.Errorf("%s: state=%s err=%v", id, s.State, err)
				return
			}
			if _, err := e.Accept(context.Background(), id, s.Best.ID); err != nil {
				t.Errorf("%s accept: %v", id, err)
			}
		}(i)
	}
	wg.Wait()
	if got := e.Stats().Counter("rfq.FILLED"); got != 200 {
		t.Fatalf("filled=%d, want 200", got)
	}
}

func TestEvict(t *testing.T) {
	clk := newClock()
	e := newTestEngine(t, clk, &stubDealer{name: "A", bid: 99, offer: 100})
	s, _ := e.Submit(context.Background(), Request{ID: "done", Symbol: "X", Side: Buy, Qty: 1})
	e.Accept(context.Background(), "done", s.Best.ID)
	e.Submit(context.Background(), Request{ID: "live", Symbol: "X", Side: Buy, Qty: 1})

	clk.Advance(500 * time.Millisecond) // "live" quote still firm
	if n := e.Evict(clk.Now()); n != 1 {
		t.Fatalf("evicted %d, want 1", n)
	}
	if _, err := e.Get("live"); err != nil {
		t.Fatalf("live rfq was evicted: %v", err)
	}
	clk.Advance(time.Second) // now "live" has expired too
	if n := e.Evict(clk.Now()); n != 1 {
		t.Fatalf("second evict removed %d, want 1", n)
	}
}

func TestNewEngineRejectsBadDealerSets(t *testing.T) {
	if _, err := NewEngine(Config{}, nil); err == nil {
		t.Fatal("want error with zero dealers")
	}
	a := &stubDealer{name: "A"}
	if _, err := NewEngine(Config{}, nil, a, a); err == nil {
		t.Fatal("want error on duplicate dealer names")
	}
}

// A dealer with the best raw price but a bad last-look record should lose
// the top spot once the penalty is on, and keep it when the penalty is off.
func TestRejectAdjustedRanking(t *testing.T) {
	run := func(penalty float64) string {
		picky := &stubDealer{name: "picky", bid: 99.9, offer: 100.00, rejectLL: true}
		solid := &stubDealer{name: "solid", bid: 99.8, offer: 100.02}
		e, err := NewEngine(Config{CollectWindow: 50 * time.Millisecond, QuoteTTL: time.Second,
			LastLookPenaltyBps: penalty, MinLastLookSamples: 5}, nil, picky, solid)
		if err != nil {
			t.Fatal(err)
		}
		// Build history: accept picky 5 times, all rejected.
		for i := 0; i < 5; i++ {
			id := fmt.Sprintf("warm%d", i)
			e.Submit(context.Background(), Request{ID: id, Symbol: "X", Side: Buy, Qty: 1})
			if _, err := e.Accept(context.Background(), id, id+".picky"); err != nil {
				t.Fatal(err)
			}
		}
		if rate, n := e.RejectRate("picky"); rate != 1 || n != 5 {
			t.Fatalf("picky reject rate=%v n=%d, want 1/5", rate, n)
		}
		s, _ := e.Submit(context.Background(), Request{ID: "final", Symbol: "X", Side: Buy, Qty: 1})
		return s.Best.Dealer
	}
	if got := run(0); got != "picky" {
		t.Fatalf("penalty off: best=%s, want picky (raw best price)", got)
	}
	// 100.00 * (1 + 1.0*5bps) = 100.05 > 100.02, so solid wins.
	if got := run(5); got != "solid" {
		t.Fatalf("penalty 5bps: best=%s, want solid", got)
	}
}

func TestPenaltyIgnoredBelowMinSamples(t *testing.T) {
	picky := &stubDealer{name: "picky", bid: 99.9, offer: 100.00, rejectLL: true}
	solid := &stubDealer{name: "solid", bid: 99.8, offer: 100.02}
	e, _ := NewEngine(Config{CollectWindow: 50 * time.Millisecond, LastLookPenaltyBps: 50, MinLastLookSamples: 10}, nil, picky, solid)
	s, _ := e.Submit(context.Background(), Request{ID: "a", Symbol: "X", Side: Buy, Qty: 1})
	e.Accept(context.Background(), "a", s.Best.ID) // 1 sample: not enough
	s, _ = e.Submit(context.Background(), Request{ID: "b", Symbol: "X", Side: Buy, Qty: 1})
	if s.Best.Dealer != "picky" {
		t.Fatalf("best=%s, want picky until enough samples", s.Best.Dealer)
	}
}
