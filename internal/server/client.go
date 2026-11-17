package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/Vitaliy-Pikalo/rfq-engine/internal/fixmsg"
	"github.com/Vitaliy-Pikalo/rfq-engine/internal/rfq"
)

// ErrRejected wraps server-side rejections (QuoteRequestReject, Reject).
var ErrRejected = errors.New("rejected by server")

// Client talks to a Server. Safe for concurrent use: replies are routed to
// callers by QuoteReqID, so many RFQs can be in flight on one connection.
type Client struct {
	conn net.Conn
	wmu  sync.Mutex

	mu      sync.Mutex
	waiters map[string]chan *fixmsg.Message
	err     error // set once the read loop dies
	done    chan struct{}
}

// Dial connects to addr.
func Dial(ctx context.Context, addr string) (*Client, error) {
	var d net.Dialer
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	c := &Client{conn: nc, waiters: map[string]chan *fixmsg.Message{}, done: make(chan struct{})}
	go c.readLoop()
	return c, nil
}

// Close closes the connection.
func (c *Client) Close() error { return c.conn.Close() }

func (c *Client) readLoop() {
	r := fixmsg.NewReader(c.conn)
	var err error
	for {
		var m *fixmsg.Message
		m, err = r.Read()
		if err != nil {
			break
		}
		id, _ := m.Get(fixmsg.TagQuoteReqID)
		c.mu.Lock()
		ch := c.waiters[id]
		c.mu.Unlock()
		if ch != nil {
			select {
			case ch <- m:
			default: // caller gave up; drop
			}
		}
	}
	c.mu.Lock()
	c.err = fmt.Errorf("connection closed: %w", err)
	c.mu.Unlock()
	close(c.done)
}

func (c *Client) register(id string, buf int) (chan *fixmsg.Message, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return nil, c.err
	}
	if _, busy := c.waiters[id]; busy {
		return nil, fmt.Errorf("request %s already in flight on this client", id)
	}
	ch := make(chan *fixmsg.Message, buf)
	c.waiters[id] = ch
	return ch, nil
}

func (c *Client) unregister(id string) {
	c.mu.Lock()
	delete(c.waiters, id)
	c.mu.Unlock()
}

func (c *Client) send(m *fixmsg.Message) error {
	b, err := m.Encode()
	if err != nil {
		return err
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, err = c.conn.Write(b)
	return err
}

func (c *Client) recv(ctx context.Context, ch chan *fixmsg.Message) (*fixmsg.Message, error) {
	select {
	case m := <-ch:
		return m, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		c.mu.Lock()
		defer c.mu.Unlock()
		return nil, c.err
	}
}

// RequestQuotes sends an RFQ and returns all quotes, best first.
func (c *Client) RequestQuotes(ctx context.Context, req rfq.Request) ([]rfq.Quote, error) {
	ch, err := c.register(req.ID, 64)
	if err != nil {
		return nil, err
	}
	defer c.unregister(req.ID)

	err = c.send(fixmsg.New(fixmsg.MsgQuoteRequest).
		Set(fixmsg.TagQuoteReqID, req.ID).
		Set(fixmsg.TagSymbol, req.Symbol).
		Set(fixmsg.TagSide, strconv.Itoa(int(req.Side))).
		SetFloat(fixmsg.TagOrderQty, req.Qty))
	if err != nil {
		return nil, err
	}

	var quotes []rfq.Quote
	want := -1
	for want < 0 || len(quotes) < want {
		m, err := c.recv(ctx, ch)
		if err != nil {
			return quotes, err
		}
		switch m.Type {
		case fixmsg.MsgQuoteRequestReject, fixmsg.MsgReject:
			txt, _ := m.Get(fixmsg.TagText)
			return nil, fmt.Errorf("%w: %s", ErrRejected, txt)
		case fixmsg.MsgQuote:
			q, n, err := decodeQuote(m)
			if err != nil {
				return nil, err
			}
			want = n
			quotes = append(quotes, q)
		}
	}
	return quotes, nil
}

// Accept lifts/hits a quote and waits for the execution report.
func (c *Client) Accept(ctx context.Context, rfqID, quoteID string) (rfq.Fill, error) {
	ch, err := c.register(rfqID, 4)
	if err != nil {
		return rfq.Fill{}, err
	}
	defer c.unregister(rfqID)

	err = c.send(fixmsg.New(fixmsg.MsgNewOrderSingle).
		Set(fixmsg.TagQuoteReqID, rfqID).
		Set(fixmsg.TagQuoteID, quoteID).
		Set(fixmsg.TagClOrdID, rfqID+".acc"))
	if err != nil {
		return rfq.Fill{}, err
	}
	for {
		m, err := c.recv(ctx, ch)
		if err != nil {
			return rfq.Fill{}, err
		}
		if m.Type == fixmsg.MsgReject {
			txt, _ := m.Get(fixmsg.TagText)
			return rfq.Fill{}, fmt.Errorf("%w: %s", ErrRejected, txt)
		}
		if m.Type != fixmsg.MsgExecutionReport {
			continue
		}
		status, _ := m.Get(fixmsg.TagOrdStatus)
		dealer, _ := m.Get(fixmsg.TagPartyID)
		txt, _ := m.Get(fixmsg.TagText)
		f := rfq.Fill{RFQID: rfqID, QuoteID: quoteID, Dealer: dealer, At: time.Now()}
		if side, err := m.Int(fixmsg.TagSide); err == nil {
			f.Side = rfq.Side(side)
		}
		if status != fixmsg.OrdStatusFilled {
			f.Reason = txt
			if dealer == "" { // engine-level rejection, not a dealer last look
				return f, fmt.Errorf("%w: %s", ErrRejected, txt)
			}
			return f, nil
		}
		f.Filled = true
		f.Price, _ = m.Float(fixmsg.TagLastPx)
		f.Qty, _ = m.Float(fixmsg.TagLastQty)
		return f, nil
	}
}

func decodeQuote(m *fixmsg.Message) (rfq.Quote, int, error) {
	var q rfq.Quote
	var err error
	if q.RFQID, err = m.Must(fixmsg.TagQuoteReqID); err != nil {
		return q, 0, err
	}
	if q.ID, err = m.Must(fixmsg.TagQuoteID); err != nil {
		return q, 0, err
	}
	q.Dealer, _ = m.Get(fixmsg.TagPartyID)
	q.Symbol, _ = m.Get(fixmsg.TagSymbol)
	if q.Bid, err = m.Float(fixmsg.TagBidPx); err != nil {
		return q, 0, err
	}
	if q.Offer, err = m.Float(fixmsg.TagOfferPx); err != nil {
		return q, 0, err
	}
	q.Qty, _ = m.Float(fixmsg.TagOrderQty)
	if v, ok := m.Get(fixmsg.TagValidUntil); ok {
		q.ExpiresAt, _ = time.Parse("20060102-15:04:05.000", v)
	}
	n, err := m.Int(fixmsg.TagQuoteCount)
	if err != nil {
		return q, 0, err
	}
	return q, n, nil
}
