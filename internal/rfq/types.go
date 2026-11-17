// Package rfq implements the request-for-quote lifecycle: fan a client
// request out to dealers, collect firm quotes with a time-to-live, pick the
// best one, and run the accept / last-look / fill flow.
package rfq

import (
	"errors"
	"fmt"
	"time"
)

// Side is the client's side of the trade.
type Side int

const (
	Buy  Side = 1 // client buys, so it lifts the dealer's offer
	Sell Side = 2 // client sells, so it hits the dealer's bid
)

func (s Side) String() string {
	switch s {
	case Buy:
		return "BUY"
	case Sell:
		return "SELL"
	}
	return fmt.Sprintf("Side(%d)", int(s))
}

// State is a point in the RFQ lifecycle.
//
//	Pending ──quotes──▶ Quoted ──accept──▶ Accepted ──last look ok──▶ Filled
//	   │                  │                    └──last look reject──▶ Rejected
//	   └──no quotes──▶ NoQuotes   └──TTL passes──▶ Expired
type State int

const (
	Pending State = iota
	Quoted
	NoQuotes
	Accepted
	Filled
	Rejected
	Expired
)

var stateNames = [...]string{"PENDING", "QUOTED", "NO_QUOTES", "ACCEPTED", "FILLED", "REJECTED", "EXPIRED"}

func (s State) String() string {
	if int(s) < len(stateNames) {
		return stateNames[s]
	}
	return fmt.Sprintf("State(%d)", int(s))
}

// Terminal reports whether no further transitions are possible.
func (s State) Terminal() bool {
	return s == NoQuotes || s == Filled || s == Rejected || s == Expired
}

// allowed lists legal transitions. Anything else is a bug or a stale client.
var allowed = map[State][]State{
	Pending:  {Quoted, NoQuotes},
	Quoted:   {Accepted, Expired},
	Accepted: {Filled, Rejected},
}

// CanTransition reports whether from -> to is legal.
func CanTransition(from, to State) bool {
	for _, s := range allowed[from] {
		if s == to {
			return true
		}
	}
	return false
}

// Request is what a client asks for.
type Request struct {
	ID     string
	Symbol string
	Side   Side
	Qty    float64
}

// Validate checks client input before any dealer sees it.
func (r Request) Validate() error {
	switch {
	case r.ID == "":
		return errors.New("rfq: empty request id")
	case r.Symbol == "":
		return errors.New("rfq: empty symbol")
	case r.Side != Buy && r.Side != Sell:
		return fmt.Errorf("rfq: invalid side %d", int(r.Side))
	case !(r.Qty > 0): // also rejects NaN
		return fmt.Errorf("rfq: qty must be positive, got %v", r.Qty)
	}
	return nil
}

// Quote is a firm two-way price from one dealer, valid until ExpiresAt.
type Quote struct {
	ID        string
	RFQID     string
	Dealer    string
	Symbol    string
	Bid       float64
	Offer     float64
	Qty       float64
	ArrivedAt time.Time
	ExpiresAt time.Time
}

// PriceFor returns the price the client trades at for the given side.
func (q Quote) PriceFor(side Side) float64 {
	if side == Buy {
		return q.Offer
	}
	return q.Bid
}

// Fill is the outcome of an accepted quote.
type Fill struct {
	RFQID   string
	QuoteID string
	Dealer  string
	Side    Side
	Price   float64
	Qty     float64
	Filled  bool   // false means the dealer rejected on last look
	Reason  string // set when Filled is false
	At      time.Time
}

// Errors returned to callers. Wrapped with context via fmt.Errorf("%w").
var (
	ErrUnknownRFQ    = errors.New("unknown rfq")
	ErrDuplicateRFQ  = errors.New("duplicate rfq id")
	ErrUnknownQuote  = errors.New("unknown quote")
	ErrQuoteExpired  = errors.New("quote expired")
	ErrBadTransition = errors.New("illegal state transition")
)
