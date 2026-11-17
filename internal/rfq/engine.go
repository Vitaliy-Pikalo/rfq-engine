package rfq

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/Vitaliy-Pikalo/rfq-engine/internal/metrics"
)

// ErrNoQuote is returned by a Dealer that declines to quote.
var ErrNoQuote = errors.New("dealer declined to quote")

// Dealer is a liquidity provider the engine can ask for prices.
type Dealer interface {
	Name() string
	// Quote returns a firm two-way price for req, or ErrNoQuote.
	// It must respect ctx cancellation.
	Quote(ctx context.Context, req Request) (bid, offer float64, err error)
	// LastLook gives the dealer a final chance to reject a client accept.
	// ok=false means reject, with reason.
	LastLook(ctx context.Context, q Quote, side Side) (ok bool, reason string)
}

// Config tunes the engine.
type Config struct {
	// CollectWindow is how long to wait for dealer quotes after fan-out.
	CollectWindow time.Duration
	// QuoteTTL is how long a quote stays firm after it arrives.
	QuoteTTL time.Duration
	// LastLookTimeout bounds how long a dealer may hold a client accept.
	LastLookTimeout time.Duration
	// Now is the clock. Defaults to time.Now; tests inject a fake.
	Now func() time.Time
	// LastLookPenaltyBps, when > 0, ranks quotes by a reject-adjusted price
	// instead of the raw price: each dealer's quote is worsened by
	// (its observed last-look reject rate) x LastLookPenaltyBps. A dealer
	// that shows the best price but rejects often stops winning every RFQ.
	LastLookPenaltyBps float64
	// MinLastLookSamples is how many accepts a dealer needs before its
	// reject rate is used. Default 20.
	MinLastLookSamples int
}

// DefaultConfig returns settings in the range of real FX/crypto RFQ venues.
func DefaultConfig() Config {
	return Config{
		CollectWindow:   250 * time.Millisecond,
		QuoteTTL:        2 * time.Second,
		LastLookTimeout: 100 * time.Millisecond,
		Now:             time.Now,
	}
}

// Snapshot is a read-only copy of one RFQ's state.
type Snapshot struct {
	Request   Request
	State     State
	Quotes    []Quote // sorted best-first for the request's side
	Best      *Quote
	Fill      *Fill
	CreatedAt time.Time
}

type record struct {
	req       Request
	state     State
	quotes    []Quote
	fill      *Fill
	createdAt time.Time
}

// Engine runs RFQs. Safe for concurrent use by many clients.
type Engine struct {
	cfg     Config
	dealers map[string]Dealer
	order   []string // dealer names in registration order
	stats   *metrics.Registry

	mu   sync.Mutex
	rfqs map[string]*record
	ll   map[string]*llStats // per-dealer last-look history, guarded by mu
}

type llStats struct{ accepts, rejects int }

// NewEngine builds an engine. stats may be nil.
func NewEngine(cfg Config, stats *metrics.Registry, dealers ...Dealer) (*Engine, error) {
	def := DefaultConfig()
	if cfg.CollectWindow <= 0 {
		cfg.CollectWindow = def.CollectWindow
	}
	if cfg.QuoteTTL <= 0 {
		cfg.QuoteTTL = def.QuoteTTL
	}
	if cfg.LastLookTimeout <= 0 {
		cfg.LastLookTimeout = def.LastLookTimeout
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.MinLastLookSamples <= 0 {
		cfg.MinLastLookSamples = 20
	}
	if stats == nil {
		stats = metrics.NewRegistry(1 << 16)
	}
	e := &Engine{cfg: cfg, dealers: map[string]Dealer{}, stats: stats, rfqs: map[string]*record{}, ll: map[string]*llStats{}}
	for _, d := range dealers {
		if _, dup := e.dealers[d.Name()]; dup {
			return nil, fmt.Errorf("rfq: duplicate dealer name %q", d.Name())
		}
		e.dealers[d.Name()] = d
		e.order = append(e.order, d.Name())
		e.ll[d.Name()] = &llStats{}
	}
	if len(e.dealers) == 0 {
		return nil, errors.New("rfq: need at least one dealer")
	}
	return e, nil
}

// Stats exposes the engine's metrics registry.
func (e *Engine) Stats() *metrics.Registry { return e.stats }

type dealerReply struct {
	dealer     string
	bid, offer float64
	err        error
	latency    time.Duration
}

// Submit registers req, fans it out to every dealer, waits up to
// CollectWindow (or until all dealers answer), and returns the result.
// The RFQ ends in Quoted or NoQuotes.
func (e *Engine) Submit(ctx context.Context, req Request) (Snapshot, error) {
	if err := req.Validate(); err != nil {
		e.stats.Inc("rfq.invalid")
		return Snapshot{}, err
	}
	start := e.cfg.Now()

	e.mu.Lock()
	if _, exists := e.rfqs[req.ID]; exists {
		e.mu.Unlock()
		return Snapshot{}, fmt.Errorf("%w: %s", ErrDuplicateRFQ, req.ID)
	}
	e.rfqs[req.ID] = &record{req: req, state: Pending, createdAt: start}
	e.mu.Unlock()
	e.stats.Inc("rfq.submitted")

	cctx, cancel := context.WithTimeout(ctx, e.cfg.CollectWindow)
	defer cancel()

	replies := make(chan dealerReply, len(e.order)) // buffered: late dealers never block
	for _, name := range e.order {
		d := e.dealers[name]
		go func() {
			t0 := time.Now()
			bid, offer, err := d.Quote(cctx, req)
			replies <- dealerReply{dealer: d.Name(), bid: bid, offer: offer, err: err, latency: time.Since(t0)}
		}()
	}

	var quotes []Quote
	pending := len(e.order)
collect:
	for pending > 0 {
		select {
		case r := <-replies:
			pending--
			e.stats.Hist("dealer.quote_latency." + r.dealer).Observe(r.latency)
			if r.err != nil {
				e.stats.Inc("dealer.no_quote." + r.dealer)
				continue
			}
			if !validPrices(r.bid, r.offer) {
				e.stats.Inc("dealer.bad_quote." + r.dealer)
				continue
			}
			now := e.cfg.Now()
			quotes = append(quotes, Quote{
				ID:        fmt.Sprintf("%s.%s", req.ID, r.dealer),
				RFQID:     req.ID,
				Dealer:    r.dealer,
				Symbol:    req.Symbol,
				Bid:       r.bid,
				Offer:     r.offer,
				Qty:       req.Qty,
				ArrivedAt: now,
				ExpiresAt: now.Add(e.cfg.QuoteTTL),
			})
		case <-cctx.Done():
			e.stats.Add("dealer.timed_out", int64(pending))
			break collect
		}
	}
	e.mu.Lock()
	rankQuotes(quotes, req.Side, e.penaltiesLocked())
	rec := e.rfqs[req.ID]
	rec.quotes = quotes
	next := Quoted
	if len(quotes) == 0 {
		next = NoQuotes
	}
	if err := e.transitionLocked(rec, next); err != nil {
		e.mu.Unlock()
		return Snapshot{}, err
	}
	snap := snapshotOf(rec)
	e.mu.Unlock()

	e.stats.Hist("rfq.time_to_quote").Observe(e.cfg.Now().Sub(start))
	e.stats.Inc("rfq." + next.String())
	return snap, nil
}

// Accept lifts or hits quoteID on rfqID. It checks the TTL, then runs the
// dealer's last look outside the engine lock, then records the outcome.
// A dealer rejection is not an error: it comes back as Fill{Filled:false}.
func (e *Engine) Accept(ctx context.Context, rfqID, quoteID string) (Fill, error) {
	start := e.cfg.Now()

	e.mu.Lock()
	rec, ok := e.rfqs[rfqID]
	if !ok {
		e.mu.Unlock()
		return Fill{}, fmt.Errorf("%w: %s", ErrUnknownRFQ, rfqID)
	}
	if rec.state != Quoted {
		st := rec.state
		e.mu.Unlock()
		return Fill{}, fmt.Errorf("%w: rfq %s is %s, cannot accept", ErrBadTransition, rfqID, st)
	}
	var q *Quote
	for i := range rec.quotes {
		if rec.quotes[i].ID == quoteID {
			q = &rec.quotes[i]
			break
		}
	}
	if q == nil {
		e.mu.Unlock()
		return Fill{}, fmt.Errorf("%w: %s", ErrUnknownQuote, quoteID)
	}
	if !start.Before(q.ExpiresAt) {
		// Only this quote is stale; the RFQ expires once every quote has.
		if allExpired(rec.quotes, start) {
			_ = e.transitionLocked(rec, Expired)
			e.stats.Inc("rfq.EXPIRED")
		}
		e.mu.Unlock()
		return Fill{}, fmt.Errorf("%w: %s expired at %s", ErrQuoteExpired, quoteID, q.ExpiresAt.Format(time.RFC3339Nano))
	}
	if err := e.transitionLocked(rec, Accepted); err != nil {
		e.mu.Unlock()
		return Fill{}, err
	}
	quote, side := *q, rec.req.Side
	e.mu.Unlock()

	// Last look runs without the lock so one slow dealer can't stall every RFQ.
	lctx, cancel := context.WithTimeout(ctx, e.cfg.LastLookTimeout)
	okLL, reason := e.dealers[quote.Dealer].LastLook(lctx, quote, side)
	if lctx.Err() != nil && okLL {
		// A dealer that answers after the timeout doesn't get the trade.
		okLL, reason = false, "last look timeout"
	}
	cancel()

	fill := Fill{
		RFQID: rfqID, QuoteID: quote.ID, Dealer: quote.Dealer, Side: side,
		Price: quote.PriceFor(side), Qty: quote.Qty, Filled: okLL, Reason: reason, At: e.cfg.Now(),
	}
	next := Filled
	if !okLL {
		next = Rejected
	}

	e.mu.Lock()
	rec.fill = &fill
	st := e.ll[quote.Dealer]
	st.accepts++
	if !okLL {
		st.rejects++
	}
	err := e.transitionLocked(rec, next)
	e.mu.Unlock()
	if err != nil {
		return Fill{}, err
	}
	e.stats.Hist("rfq.accept_to_fill").Observe(e.cfg.Now().Sub(start))
	e.stats.Inc("rfq." + next.String())
	if !okLL {
		e.stats.Inc("dealer.last_look_reject." + quote.Dealer)
	}
	return fill, nil
}

// Get returns a snapshot of rfqID.
func (e *Engine) Get(rfqID string) (Snapshot, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	rec, ok := e.rfqs[rfqID]
	if !ok {
		return Snapshot{}, fmt.Errorf("%w: %s", ErrUnknownRFQ, rfqID)
	}
	return snapshotOf(rec), nil
}

// Evict drops RFQs in a terminal state created before cutoff and returns how
// many it removed. Call it periodically so memory stays bounded.
// Note: a Quoted RFQ whose TTL has passed is moved to Expired here too.
func (e *Engine) Evict(cutoff time.Time) int {
	now := e.cfg.Now()
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	for id, rec := range e.rfqs {
		if rec.state == Quoted && allExpired(rec.quotes, now) {
			_ = e.transitionLocked(rec, Expired)
		}
		if rec.state.Terminal() && rec.createdAt.Before(cutoff) {
			delete(e.rfqs, id)
			n++
		}
	}
	return n
}

func (e *Engine) transitionLocked(rec *record, to State) error {
	if !CanTransition(rec.state, to) {
		return fmt.Errorf("%w: %s -> %s (rfq %s)", ErrBadTransition, rec.state, to, rec.req.ID)
	}
	rec.state = to
	return nil
}

func allExpired(qs []Quote, now time.Time) bool {
	for _, q := range qs {
		if now.Before(q.ExpiresAt) {
			return false
		}
	}
	return true
}

func validPrices(bid, offer float64) bool {
	return bid > 0 && offer > 0 && !math.IsInf(bid, 0) && !math.IsInf(offer, 0) && offer >= bid
}

// penaltiesLocked returns each dealer's ranking penalty in bps:
// observed reject rate x LastLookPenaltyBps. Empty when the feature is off.
func (e *Engine) penaltiesLocked() map[string]float64 {
	if e.cfg.LastLookPenaltyBps <= 0 {
		return nil
	}
	out := make(map[string]float64, len(e.ll))
	for name, st := range e.ll {
		if st.accepts >= e.cfg.MinLastLookSamples {
			out[name] = float64(st.rejects) / float64(st.accepts) * e.cfg.LastLookPenaltyBps
		}
	}
	return out
}

// RejectRate reports a dealer's observed last-look reject rate and sample size.
func (e *Engine) RejectRate(dealer string) (rate float64, accepts int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	st, ok := e.ll[dealer]
	if !ok || st.accepts == 0 {
		return 0, 0
	}
	return float64(st.rejects) / float64(st.accepts), st.accepts
}

// effectivePrice worsens a quote's price by penaltyBps for ranking only.
// The client still trades at the quoted price.
func effectivePrice(q Quote, side Side, penaltyBps float64) float64 {
	p := q.PriceFor(side)
	if side == Buy {
		return p * (1 + penaltyBps/1e4)
	}
	return p * (1 - penaltyBps/1e4)
}

// rankQuotes sorts best-first for the client's side, using reject-adjusted
// prices when penalties is non-nil. Ties go to the quote that arrived
// first, then dealer name, so ranking is deterministic.
func rankQuotes(qs []Quote, side Side, penalties map[string]float64) {
	sort.SliceStable(qs, func(i, j int) bool {
		pi := effectivePrice(qs[i], side, penalties[qs[i].Dealer])
		pj := effectivePrice(qs[j], side, penalties[qs[j].Dealer])
		if pi != pj {
			if side == Buy {
				return pi < pj // client buys: lower offer is better
			}
			return pi > pj // client sells: higher bid is better
		}
		if !qs[i].ArrivedAt.Equal(qs[j].ArrivedAt) {
			return qs[i].ArrivedAt.Before(qs[j].ArrivedAt)
		}
		return qs[i].Dealer < qs[j].Dealer
	})
}

func snapshotOf(rec *record) Snapshot {
	s := Snapshot{Request: rec.req, State: rec.state, CreatedAt: rec.createdAt}
	s.Quotes = append([]Quote(nil), rec.quotes...)
	if len(s.Quotes) > 0 {
		b := s.Quotes[0]
		s.Best = &b
	}
	if rec.fill != nil {
		f := *rec.fill
		s.Fill = &f
	}
	return s
}
