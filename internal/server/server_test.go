package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Vitaliy-Pikalo/rfq-engine/internal/dealer"
	"github.com/Vitaliy-Pikalo/rfq-engine/internal/fixmsg"
	"github.com/Vitaliy-Pikalo/rfq-engine/internal/pricing"
	"github.com/Vitaliy-Pikalo/rfq-engine/internal/rfq"
)

type harness struct {
	addr string
	feed *pricing.Feed
	srv  *Server
	eng  *rfq.Engine
}

func start(t *testing.T, cfgs ...dealer.Config) *harness {
	t.Helper()
	f := pricing.NewFeed(map[string]float64{"BTC-USD": 100_000, "ETH-USD": 4_000}, 1, 7)
	var ds []rfq.Dealer
	for _, c := range cfgs {
		d, err := dealer.New(c, f)
		if err != nil {
			t.Fatal(err)
		}
		ds = append(ds, d)
	}
	eng, err := rfq.NewEngine(rfq.Config{CollectWindow: 100 * time.Millisecond, QuoteTTL: time.Second}, nil, ds...)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &Server{Engine: eng}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	return &harness{addr: ln.Addr().String(), feed: f, srv: srv, eng: eng}
}

func dial(t *testing.T, addr string) *Client {
	t.Helper()
	c, err := Dial(context.Background(), addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

var twoDealers = []dealer.Config{
	{Name: "alpha", SpreadBps: 4, LastLookTolBps: 2},
	{Name: "beta", SpreadBps: 8, LastLookTolBps: -1},
}

func TestQuoteAndFillOverTCP(t *testing.T) {
	h := start(t, twoDealers...)
	c := dial(t, h.addr)
	ctx := context.Background()

	qs, err := c.RequestQuotes(ctx, rfq.Request{ID: "r1", Symbol: "BTC-USD", Side: rfq.Buy, Qty: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(qs) != 2 || qs[0].Dealer != "alpha" || qs[0].Offer >= qs[1].Offer {
		t.Fatalf("quotes=%+v, want alpha best with lower offer", qs)
	}
	f, err := c.Accept(ctx, "r1", qs[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if !f.Filled || f.Price != qs[0].Offer || f.Dealer != "alpha" {
		t.Fatalf("fill=%+v", f)
	}
}

func TestLastLookRejectOverTCP(t *testing.T) {
	h := start(t, twoDealers...)
	c := dial(t, h.addr)
	ctx := context.Background()
	qs, err := c.RequestQuotes(ctx, rfq.Request{ID: "r1", Symbol: "BTC-USD", Side: rfq.Buy, Qty: 1})
	if err != nil {
		t.Fatal(err)
	}
	h.feed.Set("BTC-USD", 100_100) // +10bps against alpha
	f, err := c.Accept(ctx, "r1", qs[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if f.Filled || f.Reason == "" {
		t.Fatalf("fill=%+v, want last-look reject with reason", f)
	}
}

func TestRejectsOverTCP(t *testing.T) {
	h := start(t, twoDealers...)
	c := dial(t, h.addr)
	ctx := context.Background()

	_, err := c.RequestQuotes(ctx, rfq.Request{ID: "bad", Symbol: "BTC-USD", Side: rfq.Buy, Qty: -1})
	if !errors.Is(err, ErrRejected) {
		t.Fatalf("negative qty err=%v, want ErrRejected", err)
	}
	_, err = c.RequestQuotes(ctx, rfq.Request{ID: "nosym", Symbol: "DOGE-USD", Side: rfq.Buy, Qty: 1})
	if !errors.Is(err, ErrRejected) {
		t.Fatalf("unknown symbol err=%v, want ErrRejected (no dealer quoted)", err)
	}
	if _, err := c.Accept(ctx, "ghost", "ghost.alpha"); !errors.Is(err, ErrRejected) {
		t.Fatalf("accept unknown rfq err=%v, want ErrRejected", err)
	}
}

func TestManyRFQsOnOneConnection(t *testing.T) {
	h := start(t, append(twoDealers, dealer.Config{Name: "gamma", SpreadBps: 6, LatencyMean: 2 * time.Millisecond, LatencyJitter: time.Millisecond, LastLookTolBps: -1})...)
	c := dial(t, h.addr)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("r%d", i)
			side := rfq.Buy
			if i%2 == 1 {
				side = rfq.Sell
			}
			qs, err := c.RequestQuotes(context.Background(), rfq.Request{ID: id, Symbol: "ETH-USD", Side: side, Qty: 1})
			if err != nil {
				t.Errorf("%s: %v", id, err)
				return
			}
			if len(qs) != 3 {
				t.Errorf("%s: %d quotes", id, len(qs))
				return
			}
			if _, err := c.Accept(context.Background(), id, qs[0].ID); err != nil {
				t.Errorf("%s accept: %v", id, err)
			}
		}(i)
	}
	wg.Wait()
	if got := h.eng.Stats().Counter("rfq.FILLED"); got != 100 {
		t.Fatalf("filled=%d, want 100", got)
	}
}

func TestGarbageInputDropsConnectionWithReject(t *testing.T) {
	h := start(t, twoDealers...)
	nc, err := net.Dial("tcp", h.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	nc.Write([]byte("GET / HTTP/1.1\r\n\r\n\x01"))
	nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	m, err := fixmsg.NewReader(nc).Read()
	if err != nil {
		t.Fatalf("expected a Reject before close, got err=%v", err)
	}
	if m.Type != fixmsg.MsgReject {
		t.Fatalf("got %s, want Reject", m.Type)
	}
	// Server must still accept fresh connections afterward.
	c := dial(t, h.addr)
	if _, err := c.RequestQuotes(context.Background(), rfq.Request{ID: "ok", Symbol: "BTC-USD", Side: rfq.Sell, Qty: 1}); err != nil {
		t.Fatal(err)
	}
}

func TestUnsupportedMsgType(t *testing.T) {
	h := start(t, twoDealers...)
	nc, err := net.Dial("tcp", h.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	b, _ := fixmsg.New("0").Encode() // heartbeat: not handled
	nc.Write(b)
	nc.SetReadDeadline(time.Now().Add(2 * time.Second))
	m, err := fixmsg.NewReader(nc).Read()
	if err != nil || m.Type != fixmsg.MsgReject {
		t.Fatalf("m=%v err=%v, want Reject", m, err)
	}
	if txt, _ := m.Get(fixmsg.TagText); !bytes.Contains([]byte(txt), []byte("unsupported")) {
		t.Fatalf("text=%q", txt)
	}
}

func TestCloseWithLiveConnections(t *testing.T) {
	h := start(t, twoDealers...)
	c := dial(t, h.addr)
	done := make(chan error, 1)
	go func() { done <- h.srv.Close() }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Close hung with a live client connection")
	}
	if _, err := c.RequestQuotes(context.Background(), rfq.Request{ID: "x", Symbol: "BTC-USD", Side: rfq.Buy, Qty: 1}); err == nil {
		t.Fatal("request after server close should fail")
	}
}
