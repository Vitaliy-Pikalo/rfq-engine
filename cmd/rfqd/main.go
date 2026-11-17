// Command rfqd runs the RFQ server with a panel of simulated dealers.
//
//	go run ./cmd/rfqd -addr :9878
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Vitaliy-Pikalo/rfq-engine/internal/dealer"
	"github.com/Vitaliy-Pikalo/rfq-engine/internal/pricing"
	"github.com/Vitaliy-Pikalo/rfq-engine/internal/rfq"
	"github.com/Vitaliy-Pikalo/rfq-engine/internal/server"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9878", "listen address")
	collect := flag.Duration("collect", 50*time.Millisecond, "how long to wait for dealer quotes")
	ttl := flag.Duration("ttl", 2*time.Second, "quote time-to-live")
	tick := flag.Duration("tick", 10*time.Millisecond, "reference price update interval")
	vol := flag.Float64("vol", 0.5, "price move per tick, std dev in bps")
	report := flag.Duration("report", 10*time.Second, "metrics report interval (0 = off)")
	seed := flag.Int64("seed", 42, "random seed for dealers and prices")
	penalty := flag.Float64("penalty", 0, "last-look penalty bps for reject-adjusted ranking (0 = raw price)")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(log, *addr, *collect, *ttl, *tick, *vol, *report, *seed, *penalty); err != nil {
		log.Error("rfqd exited", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger, addr string, collect, ttl, tick time.Duration, vol float64, report time.Duration, seed int64, penalty float64) error {
	feed := pricing.NewFeed(map[string]float64{"BTC-USD": 100_000, "ETH-USD": 4_000, "SOL-USD": 200}, vol, seed)
	var ds []rfq.Dealer
	for _, c := range dealer.DefaultRoster(seed) {
		d, err := dealer.New(c, feed)
		if err != nil {
			return err
		}
		ds = append(ds, d)
	}
	eng, err := rfq.NewEngine(rfq.Config{CollectWindow: collect, QuoteTTL: ttl, LastLookPenaltyBps: penalty}, nil, ds...)
	if err != nil {
		return err
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := &server.Server{Engine: eng, Log: log}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go every(ctx, tick, feed.Tick)
	go every(ctx, time.Minute, func() { eng.Evict(time.Now().Add(-time.Minute)) })
	if report > 0 {
		go every(ctx, report, func() { fmt.Fprint(os.Stderr, "--- metrics ---\n"+eng.Stats().Report()) })
	}
	go func() { <-ctx.Done(); srv.Close() }()

	log.Info("rfqd listening", "addr", ln.Addr().String(), "dealers", len(ds), "collect", collect, "ttl", ttl)
	return srv.Serve(ln)
}

func every(ctx context.Context, d time.Duration, f func()) {
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			f()
		}
	}
}
