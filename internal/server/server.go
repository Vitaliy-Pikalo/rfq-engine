// Package server exposes the RFQ engine over TCP using the fixmsg codec,
// and provides a matching client.
package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/Vitaliy-Pikalo/rfq-engine/internal/fixmsg"
	"github.com/Vitaliy-Pikalo/rfq-engine/internal/rfq"
)

// Server accepts FIX-style connections and routes messages to an Engine.
//
// Per connection: one reader goroutine; each request is handled in its own
// goroutine so a slow RFQ doesn't block others on the same connection;
// writes are serialized by a per-connection mutex.
type Server struct {
	Engine *rfq.Engine
	Log    *slog.Logger
	// IdleTimeout closes connections that send nothing for this long.
	IdleTimeout time.Duration

	mu     sync.Mutex
	ln     net.Listener
	conns  map[net.Conn]struct{}
	wg     sync.WaitGroup
	closed bool
}

// Serve accepts connections on ln until Close is called.
func (s *Server) Serve(ln net.Listener) error {
	if s.Log == nil {
		s.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if s.IdleTimeout <= 0 {
		s.IdleTimeout = 5 * time.Minute
	}
	s.mu.Lock()
	s.ln = ln
	s.conns = map[net.Conn]struct{}{}
	s.mu.Unlock()

	for {
		c, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return nil
			}
			return err
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			c.Close()
			return nil
		}
		s.conns[c] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go s.handle(c)
	}
}

// Close stops accepting, closes live connections, and waits for handlers.
func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	var err error
	if s.ln != nil {
		err = s.ln.Close()
	}
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return err
}

type conn struct {
	net.Conn
	wmu sync.Mutex
}

func (c *conn) send(m *fixmsg.Message) error {
	b, err := m.Encode()
	if err != nil {
		return err
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err = c.Write(b)
	return err
}

func (s *Server) handle(nc net.Conn) {
	ctx, cancel := context.WithCancel(context.Background())
	var reqs sync.WaitGroup
	defer func() {
		cancel()
		reqs.Wait()
		nc.Close()
		s.mu.Lock()
		delete(s.conns, nc)
		s.mu.Unlock()
		s.wg.Done()
	}()
	c := &conn{Conn: nc}
	r := fixmsg.NewReader(nc)
	log := s.Log.With("remote", nc.RemoteAddr().String())

	for {
		nc.SetReadDeadline(time.Now().Add(s.IdleTimeout))
		m, err := r.Read()
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				log.Warn("dropping connection", "err", err)
				// Best effort: tell the peer why before hanging up.
				c.send(fixmsg.New(fixmsg.MsgReject).Set(fixmsg.TagText, err.Error()))
			}
			return
		}
		reqs.Add(1)
		go func() {
			defer reqs.Done()
			if err := s.dispatch(ctx, c, m); err != nil {
				log.Debug("send failed", "err", err)
			}
		}()
	}
}

func (s *Server) dispatch(ctx context.Context, c *conn, m *fixmsg.Message) error {
	switch m.Type {
	case fixmsg.MsgQuoteRequest:
		return s.onQuoteRequest(ctx, c, m)
	case fixmsg.MsgNewOrderSingle:
		return s.onAccept(ctx, c, m)
	default:
		return c.send(fixmsg.New(fixmsg.MsgReject).Set(fixmsg.TagText, "unsupported MsgType "+m.Type))
	}
}

func (s *Server) onQuoteRequest(ctx context.Context, c *conn, m *fixmsg.Message) error {
	id, _ := m.Get(fixmsg.TagQuoteReqID)
	reject := func(err error) error {
		return c.send(fixmsg.New(fixmsg.MsgQuoteRequestReject).
			Set(fixmsg.TagQuoteReqID, id).Set(fixmsg.TagText, err.Error()))
	}
	req, err := DecodeRequest(m)
	if err != nil {
		return reject(err)
	}
	snap, err := s.Engine.Submit(ctx, req)
	if err != nil {
		return reject(err)
	}
	if snap.State == rfq.NoQuotes {
		return reject(errors.New("no dealer quoted"))
	}
	n := strconv.Itoa(len(snap.Quotes))
	for i, q := range snap.Quotes { // best first
		out := fixmsg.New(fixmsg.MsgQuote).
			Set(fixmsg.TagQuoteReqID, q.RFQID).
			Set(fixmsg.TagQuoteID, q.ID).
			Set(fixmsg.TagSymbol, q.Symbol).
			Set(fixmsg.TagPartyID, q.Dealer).
			SetFloat(fixmsg.TagBidPx, q.Bid).
			SetFloat(fixmsg.TagOfferPx, q.Offer).
			SetFloat(fixmsg.TagOrderQty, q.Qty).
			Set(fixmsg.TagValidUntil, q.ExpiresAt.UTC().Format("20060102-15:04:05.000")).
			Set(fixmsg.TagQuoteCount, n).
			Set(fixmsg.TagQuoteRank, strconv.Itoa(i+1))
		if err := c.send(out); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) onAccept(ctx context.Context, c *conn, m *fixmsg.Message) error {
	rfqID, _ := m.Get(fixmsg.TagQuoteReqID)
	quoteID, _ := m.Get(fixmsg.TagQuoteID)
	clOrd, _ := m.Get(fixmsg.TagClOrdID)
	er := fixmsg.New(fixmsg.MsgExecutionReport).
		Set(fixmsg.TagQuoteReqID, rfqID).
		Set(fixmsg.TagQuoteID, quoteID).
		Set(fixmsg.TagClOrdID, clOrd)

	if rfqID == "" || quoteID == "" {
		return c.send(er.Set(fixmsg.TagOrdStatus, fixmsg.OrdStatusRejected).
			Set(fixmsg.TagText, "accept needs tags 131 and 117"))
	}
	fill, err := s.Engine.Accept(ctx, rfqID, quoteID)
	if err != nil {
		return c.send(er.Set(fixmsg.TagOrdStatus, fixmsg.OrdStatusRejected).Set(fixmsg.TagText, err.Error()))
	}
	er.Set(fixmsg.TagPartyID, fill.Dealer).Set(fixmsg.TagSide, strconv.Itoa(int(fill.Side)))
	if !fill.Filled {
		return c.send(er.Set(fixmsg.TagOrdStatus, fixmsg.OrdStatusRejected).Set(fixmsg.TagText, fill.Reason))
	}
	return c.send(er.Set(fixmsg.TagOrdStatus, fixmsg.OrdStatusFilled).
		SetFloat(fixmsg.TagLastPx, fill.Price).
		SetFloat(fixmsg.TagLastQty, fill.Qty))
}

// DecodeRequest maps a QuoteRequest message to an rfq.Request.
func DecodeRequest(m *fixmsg.Message) (rfq.Request, error) {
	id, err := m.Must(fixmsg.TagQuoteReqID)
	if err != nil {
		return rfq.Request{}, err
	}
	sym, err := m.Must(fixmsg.TagSymbol)
	if err != nil {
		return rfq.Request{}, err
	}
	side, err := m.Int(fixmsg.TagSide)
	if err != nil {
		return rfq.Request{}, err
	}
	qty, err := m.Float(fixmsg.TagOrderQty)
	if err != nil {
		return rfq.Request{}, err
	}
	req := rfq.Request{ID: id, Symbol: sym, Side: rfq.Side(side), Qty: qty}
	if err := req.Validate(); err != nil {
		return rfq.Request{}, fmt.Errorf("invalid request: %w", err)
	}
	return req, nil
}
